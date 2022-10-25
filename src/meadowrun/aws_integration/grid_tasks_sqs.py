from __future__ import annotations

import itertools
import json
import os
import time
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import aiobotocore.session

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _MEADOWRUN_TAG,
    _MEADOWRUN_TAG_VALUE,
)
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.storage_grid_job import (
    S3Bucket,
    complete_task,
    download_task_arg,
    get_aws_s3_bucket,
    upload_task_args,
)
from meadowrun.run_job_local import restart_worker

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient
    from meadowrun.run_job_local import TaskWorkerServer, WorkerMonitor

_REQUEST_QUEUE_NAME_PREFIX = "meadowrun-task-"

_T = TypeVar("_T")


async def create_request_queue(job_id: str, sqs: SQSClient) -> str:
    """
    See create_queues_and_add_tasks

    Mote that queue names can't contain anything other than letters, numbers, and
    hyphens
    """

    # interestingly, this call will not fail if the exact same queue already exists,
    # but if the queue exists but has different attributes/tags, it will throw an
    # exception
    # TODO try to detect job id collisions?
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.create_queue
    request_queue_response = await sqs.create_queue(
        QueueName=f"{_REQUEST_QUEUE_NAME_PREFIX}{job_id}",
        tags={_MEADOWRUN_TAG: _MEADOWRUN_TAG_VALUE},
    )

    # FIFO SQS queues would be nice because they give an "at most once" message delivery
    # guarantee, but we don't need the ordering guarantee, and the performance penalty
    # introduced by the ordering guarantee is unacceptable. It's possible we'll end up
    # executing a task more than once, but according to the docs this should be very
    # rare. If it does happen, then we'll overwrite the result, which is fine.
    # TODO We should put in a check to notify the user that a task ran more than once

    request_queue_url = request_queue_response["QueueUrl"]

    return request_queue_url


def _chunker(it: Iterable[_T], size: int) -> Iterable[Tuple[_T, ...]]:
    """E.g. _chunker([1, 2, 3, 4, 5], 2) -> [1, 2], [3, 4], [5]"""
    iterator = iter(it)
    while True:
        chunk = tuple(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk


MESSAGE_PREFIX_TASK = "task"
MESSAGE_PREFIX_WORKER_SHUTDOWN = "worker-shutdown"


async def add_tasks(
    job_id: str,
    request_queue_url: str,
    s3_bucket: S3Bucket,
    sqs: SQSClient,
    run_map_args: Iterable[Any],
) -> List[Tuple[int, int]]:
    """
    This can only be called once per request_queue_url. If we wanted to support calling
    add_tasks more than once for the same job, we would need the caller to manage the
    task_ids
    """

    byte_ranges = await upload_task_args(s3_bucket, job_id, run_map_args)

    for byte_ranges_chunk in _chunker(enumerate(byte_ranges), 10):
        # this function can only take 10 messages at a time, so we chunk into
        # batches of 10
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message_batch

        entries = [
            {
                "Id": str(i),
                "MessageBody": json.dumps(
                    {
                        "task_id": i,
                        "attempt": 1,
                        "range_from": range_from,
                        "range_end": range_to,
                    }
                ),
            }
            for i, (range_from, range_to) in byte_ranges_chunk
        ]

        result = await sqs.send_message_batch(
            QueueUrl=request_queue_url, Entries=entries  # type: ignore
        )
        if "Failed" in result:
            raise ValueError(f"Some grid tasks could not be queued: {result['Failed']}")

    return byte_ranges


async def retry_task(
    request_queue_url: str,
    task_id: int,
    attempt: int,
    byte_range: Tuple[int, int],
    sqs_client: SQSClient,
) -> None:
    task = json.dumps(
        dict(
            task_id=task_id,
            attempt=attempt,
            range_from=byte_range[0],
            range_end=byte_range[1],
        )
    )

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
    await sqs_client.send_message(QueueUrl=request_queue_url, MessageBody=task)


async def add_worker_shutdown_messages(
    request_queue_url: str,
    num_messages_to_send: int,
    sqs_client: SQSClient,
) -> None:
    messages_sent = 0
    while messages_sent < num_messages_to_send:
        num_messages_to_send_batch = min(10, num_messages_to_send - messages_sent)
        # the id only needs to be unique within a single request
        entries = [
            {"Id": str(i), "MessageBody": "{}"}
            for i in range(num_messages_to_send_batch)
        ]

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
        result = await sqs_client.send_message_batch(
            QueueUrl=request_queue_url, Entries=entries  # type: ignore
        )
        if "Failed" in result:
            raise ValueError(
                "Some worker shutdown messages could not be queued: "
                f"{result['Failed']}"
            )

        messages_sent += num_messages_to_send_batch


_GET_TASK_TIMEOUT_SECONDS = 60 * 2  # 2 minutes


async def _get_task(
    sqs: SQSClient,
    s3_bucket: S3Bucket,
    request_queue_url: str,
    job_id: str,
    receive_message_wait_seconds: int,
) -> Optional[Tuple[int, int, bytes]]:
    """
    Gets the next task from the specified request_queue, downloads the task argument
    from S3, and then deletes the message from the request_queue.

    Returns a tuple of task_id and pickled argument if there was a task, otherwise
    returns None. Waits receive_message_wait_seconds for a task.
    """
    # get the task message
    t0 = time.time()
    while True:
        if time.time() - t0 > _GET_TASK_TIMEOUT_SECONDS:
            raise TimeoutError(
                f"Waited more than {_GET_TASK_TIMEOUT_SECONDS} for the next task but no"
                " task was available. This is unexpected--the GridJobDriver should have"
                " sent a shutdown message or a SIGINT explicitly"
            )

        result = await sqs.receive_message(
            QueueUrl=request_queue_url, WaitTimeSeconds=receive_message_wait_seconds
        )

        if "Messages" in result:
            break

    messages = result["Messages"]
    if len(messages) != 1:
        raise ValueError(f"Requested one message but got {len(messages)}")

    # parse the task request
    task = json.loads(messages[0]["Body"])
    if "task_id" in task:
        task_id, attempt, range_from, range_end = (
            task["task_id"],
            task["attempt"],
            task["range_from"],
            task["range_end"],
        )
        arg = await download_task_arg(s3_bucket, job_id, (range_from, range_end))

        # TODO store somewhere (DynamoDB?) that this worker has picked up the task.

        # acknowledge receipt/delete the task request message so we don't have duplicate
        # tasks running
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        await sqs.delete_message(
            QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
        )

        return task_id, attempt, arg

    # it's a worker exit message, so return None to exit
    await sqs.delete_message(
        QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
    )
    return None


async def _worker_iteration(
    sqs: SQSClient,
    s3_bucket: S3Bucket,
    request_queue_url: str,
    job_id: str,
    log_file_name: str,
    pid: int,
    worker_server: TaskWorkerServer,
    worker_monitor: WorkerMonitor,
) -> bool:
    task = await _get_task(
        sqs,
        s3_bucket,
        request_queue_url,
        job_id,
        3,
    )
    if not task:
        print("Meadowrun agent: Received shutdown message. Exiting.")
        return False

    task_id, attempt, arg = task
    print(f"Meadowrun agent: About to execute task #{task_id}, attempt #{attempt}")
    try:
        worker_monitor.start_stats()
        await worker_server.send_message(arg)

        state, result = await worker_server.receive_message()
        stats = await worker_monitor.stop_stats()

        process_state = ProcessState(
            state=ProcessState.ProcessStateEnum.SUCCEEDED
            if state == "SUCCEEDED"
            else ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
            pid=pid,
            pickled_result=result,
            return_code=0,
            log_file_name=log_file_name,
            max_memory_used_gb=stats.max_memory_used_gb,
        )

    except Exception:
        stats = await worker_monitor.stop_stats()

        process_state = ProcessState(
            state=ProcessState.ProcessStateEnum.UNEXPECTED_WORKER_EXIT,
            pid=pid,
            return_code=(await worker_monitor.try_get_return_code()) or 0,
            log_file_name=log_file_name,
            max_memory_used_gb=stats.max_memory_used_gb,
        )

        print(
            f"Meadowrun agent: Unexpected worker exit, restarting worker. "
            f"In task #{task_id}, attempt #{attempt}, {traceback.format_exc()}"
        )

        await restart_worker(worker_server, worker_monitor)

    print(
        f"Meadowrun agent: Completed task #{task_id}, attempt #{attempt}, "
        f"state {ProcessState.ProcessStateEnum.Name(process_state.state)}, max "
        f"memory {process_state.max_memory_used_gb}GB "
    )

    await complete_task(s3_bucket, job_id, task_id, attempt, process_state)

    return True


async def worker_function(
    request_queue_url: str,
    job_id: str,
    region_name: str,
    public_address: str,
    log_file_name: str,
    worker_server: TaskWorkerServer,
    worker_monitor: WorkerMonitor,
) -> None:
    """
    Runs a loop that gets tasks off of the request_queue, communicates that via reader
    and writer to the task worker, and uploads the results to S3.
    """
    pid = os.getpid()
    log_file_name = f"{public_address}:{log_file_name}"
    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs", region_name=region_name
    ) as sqs, get_aws_s3_bucket(region_name) as s3_bucket:
        await worker_server.wait_for_task_worker_connection()
        while await _worker_iteration(
            sqs,
            s3_bucket,
            request_queue_url,
            job_id,
            log_file_name,
            pid,
            worker_server,
            worker_monitor,
        ):
            pass
