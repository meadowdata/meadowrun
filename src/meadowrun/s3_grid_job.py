"""
There's no such thing as an "S3 grid job". The code in this module just helps us
implement grid jobs that use an S3-compatible object store to transfer data, e.g. for
EC2 and Kubernetes
"""


from __future__ import annotations

import asyncio
import io
import pickle
import time
import typing

from typing import (
    Any,
    AsyncIterable,
    Iterable,
    List,
    Optional,
    Set,
    TYPE_CHECKING,
    Tuple,
)

import aiobotocore.session
import botocore.exceptions
from meadowrun.aws_integration import s3

from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import (
    JobCompletion,
    MeadowrunException,
    TaskProcessState,
    WorkerProcessState,
)
from meadowrun.storage_keys import (
    STORAGE_KEY_PROCESS_STATE_SUFFIX,
    STORAGE_KEY_TASK_RESULT_SUFFIX,
    parse_storage_key_process_state,
    parse_storage_key_task_result,
    storage_key_process_state,
    storage_key_task_args,
    storage_key_task_result,
    storage_prefix_outputs,
)

if TYPE_CHECKING:
    import types_aiobotocore_s3
    from typing_extensions import Literal


def get_storage_client_from_args(
    storage_endpoint_url: Optional[str],
    storage_access_key_id: Optional[str],
    storage_secret_access_key: Optional[str],
) -> typing.AsyncContextManager[types_aiobotocore_s3.S3Client]:
    kwargs = {}
    if storage_access_key_id is not None:
        kwargs["aws_access_key_id"] = storage_access_key_id
    if storage_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = storage_secret_access_key
    if storage_endpoint_url is not None:
        kwargs["endpoint_url"] = storage_endpoint_url

    # TODO if all the parameters are None then we're implicitly falling back on AWS
    # S3, which we should make explicit
    session = aiobotocore.session.get_session()
    return session.create_client("s3", **kwargs)  # type: ignore


async def read_storage(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
) -> Any:
    response = await storage_client.get_object(
        Bucket=storage_bucket, Key=storage_filename
    )
    async with response["Body"] as stream:
        return await stream.read()


async def write_storage_pickle(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
    obj: Any,
    pickle_protocol: Optional[int],
) -> None:
    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer, protocol=pickle_protocol)
        buffer.seek(0)
        await storage_client.put_object(
            Bucket=storage_bucket,
            Key=storage_filename,
            Body=buffer,
        )


async def write_storage_file(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    local_filename: str,
    storage_filename: str,
) -> None:
    with open(local_filename, "rb") as f:
        await storage_client.put_object(
            Bucket=storage_bucket, Key=storage_filename, Body=f
        )


async def try_get_storage_file(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
    local_filename: str,
) -> bool:
    try:
        response = await storage_client.get_object(
            Bucket=storage_bucket, Key=storage_filename
        )
        async with response["Body"] as stream:
            with open(local_filename, "wb") as f:
                f.write(await stream.read())
        return True
    except botocore.exceptions.ClientError as error:
        # don't raise an error saying the file doesn't exist, we'll just upload it
        # in that case by falling through to the next bit of code
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise error

        return False


async def upload_task_args(
    s3_client: types_aiobotocore_s3.S3Client,
    bucket_name: str,
    job_id: str,
    args: Iterable[Any],
) -> List[Tuple[int, int]]:
    range_from = 0
    ranges = []
    with io.BytesIO() as buffer:
        for i, arg in enumerate(args):
            pickle.dump(((arg,), {}), buffer)
            range_to = buffer.tell() - 1
            ranges.append((range_from, range_to))
            range_from = range_to + 1

        await s3_client.put_object(
            Bucket=bucket_name,
            Key=storage_key_task_args(job_id),
            Body=buffer.getvalue(),
        )

    return ranges


async def download_task_arg(
    s3_client: types_aiobotocore_s3.S3Client,
    bucket_name: str,
    job_id: str,
    byte_range: Tuple[int, int],
) -> Any:
    return (
        await s3.download_async(
            s3_client, bucket_name, storage_key_task_args(job_id), byte_range
        )
    )[1]


async def complete_task(
    s3_client: types_aiobotocore_s3.S3Client,
    bucket_name: str,
    job_id: str,
    task_id: int,
    attempt: int,
    process_state: ProcessState,
) -> None:
    """Uploads the result of the task to S3."""
    await s3_client.put_object(
        Bucket=bucket_name,
        Key=storage_key_task_result(job_id, task_id, attempt),
        Body=process_state.SerializeToString(),
    )


async def receive_results(
    s3_client: types_aiobotocore_s3.S3Client,
    bucket_name: str,
    job_id: str,
    stop_receiving: asyncio.Event,
    all_workers_exited: asyncio.Event,
    initial_wait_seconds: int = 1,
    receive_message_wait_seconds: int = 20,
    read_worker_process_states: bool = True,
) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
    """
    Listens to a result queue until we have results for num_tasks.

    As results become available, yields (task results, worker results). Task results
    will be a list of TaskProcessState. Worker results will be a list of
    WorkerProcessState
    """

    # Behavior is that if stop_receiving is set, we want to return immediately. If
    # all_workers_exited is set, then keep trying for about 3 seconds (just in case some
    # results are still coming in), and then return

    delete_tasks = []

    try:
        results_prefix = storage_prefix_outputs(job_id)
        download_keys_received: Set[str] = set()
        wait = initial_wait_seconds
        workers_exited_wait_count = 0
        while not stop_receiving.is_set() and (
            workers_exited_wait_count < 3 or wait == 0
        ):
            if all_workers_exited.is_set():
                workers_exited_wait_count += 1

            if wait:
                events_to_wait_for = [asyncio.create_task(stop_receiving.wait())]
                if workers_exited_wait_count == 0:
                    events_to_wait_for.append(
                        asyncio.create_task(all_workers_exited.wait())
                    )
                else:
                    # poll more frequently if workers are done, but still wait 1 second
                    # (unless stop_receiving is set)
                    wait = 1

                done, pending = await asyncio.wait(
                    events_to_wait_for,
                    timeout=wait,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for p in pending:
                    p.cancel()
                if stop_receiving.is_set():
                    break

            keys = await s3.list_objects_async(
                s3_client, bucket_name, results_prefix, ""
            )

            download_tasks = []
            for key in keys:
                if key not in download_keys_received:
                    download_tasks.append(
                        asyncio.create_task(
                            s3.download_async(s3_client, bucket_name, key)
                        )
                    )

            download_keys_received.update(keys)

            if len(download_tasks) == 0:
                if wait == 0:
                    wait = 1
                else:
                    wait = min(wait * 2, receive_message_wait_seconds)
            else:
                wait = 0
                task_results = []
                worker_results = []
                for task_result_future in asyncio.as_completed(download_tasks):
                    key, process_state_bytes = await task_result_future
                    process_state = ProcessState()
                    process_state.ParseFromString(process_state_bytes)
                    if key.endswith(STORAGE_KEY_TASK_RESULT_SUFFIX):
                        task_id, attempt = parse_storage_key_task_result(
                            key, results_prefix
                        )
                        task_results.append(
                            TaskProcessState(task_id, attempt, process_state)
                        )
                    elif key.endswith(STORAGE_KEY_PROCESS_STATE_SUFFIX):
                        if not read_worker_process_states:
                            continue

                        worker_index = parse_storage_key_process_state(
                            key, results_prefix
                        )
                        worker_results.append(
                            WorkerProcessState(worker_index, process_state)
                        )
                    else:
                        print(f"Warning, unrecognized key {key}, will ignore")
                        continue  # don't delete if we can't parse the key

                    delete_tasks.append(
                        asyncio.create_task(
                            s3_client.delete_object(Bucket=bucket_name, Key=key)
                        )
                    )
                yield task_results, worker_results
    finally:
        await asyncio.gather(*delete_tasks, return_exceptions=True)


async def get_job_completion_from_process_state(
    storage_client: Any,
    storage_bucket: str,
    job_id: str,
    worker_index: str,
    job_spec_type: Literal["py_command", "py_function", "py_agent"],
    timeout_seconds: int,
    public_address: str,
) -> JobCompletion[Any]:
    """
    Polls for the specified .process_state file, and then creates a JobCompletion object
    based on the process_state

    file_suffix is used by indexed completion jobs to distinguish between the
    completions of different workers, this should be set to the job completion index.
    """

    t0 = time.time()
    process_state_bytes = None
    process_state_key = storage_key_process_state(job_id, worker_index)
    wait = 1
    while time.time() - t0 < timeout_seconds:
        try:
            process_state_bytes = await read_storage(
                storage_client,
                storage_bucket,
                process_state_key,
            )
            break
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise

        await asyncio.sleep(wait)
        wait = max(wait + 1, 20)

    if process_state_bytes is None:
        raise TimeoutError(
            f"Waited {timeout_seconds} seconds but {process_state_key} does not exist"
        )

    process_state = ProcessState.FromString(process_state_bytes)
    if process_state.state == ProcessState.ProcessStateEnum.SUCCEEDED:
        # we must have a result from functions, in other cases we can optionally have a
        # result
        if job_spec_type == "py_function" or process_state.pickled_result:
            result = pickle.loads(process_state.pickled_result)
        else:
            result = None
        return JobCompletion(
            result,
            process_state.state,
            process_state.log_file_name,
            process_state.return_code,
            public_address,
        )
    else:
        raise MeadowrunException(process_state)
