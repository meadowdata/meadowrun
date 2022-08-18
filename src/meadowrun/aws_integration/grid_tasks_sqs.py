from __future__ import annotations

import asyncio
import functools
import itertools
import json
import os
import pickle
import traceback
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

import aiobotocore.session
import boto3
from meadowrun.aws_integration import s3
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import (
    SSH_USER,
    EC2InstanceRegistrar,
)
from meadowrun.aws_integration.ec2_ssh_keys import get_meadowrun_ssh_key
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
)
from meadowrun.instance_allocation import allocate_jobs_to_instances

if TYPE_CHECKING:
    from meadowrun.instance_selection import ResourcesInternal
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_sqs.client import SQSClient

from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import RunMapHelper
from meadowrun.shared import pickle_exception

_REQUEST_QUEUE_NAME_PREFIX = "meadowrun-task-request-"
_QUEUE_NAME_SUFFIX = ".fifo"

_T = TypeVar("_T")
_U = TypeVar("_U")


async def create_queues_and_add_tasks(
    region_name: str, run_map_args: Iterable[Any], num_workers: int
) -> Tuple[str, str]:
    """
    Creates the queues necessary to run a grid job. Returns (request_queue_url,
    run_map_id). The request queue contains messages that represent tasks that need to
    run.

    Then adds tasks to the specified request queue - one task is created for each
    argument in the given run_map_args.
    """

    # this id is just used for creating the job's queues. It has no relationship to any
    # Job.job_ids
    job_id = str(uuid.uuid4())
    print(f"The current run_map's id is {job_id}")
    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs", region_name=region_name
    ) as sqs, session.create_client("s3", region_name=region_name) as s3c:
        request_queue_url = await _create_request_queue(job_id, sqs)
        await _add_tasks(job_id, request_queue_url, s3c, sqs, run_map_args)
        await _add_end_of_job_messages(job_id, request_queue_url, sqs, num_workers)
    return request_queue_url, job_id


async def _create_request_queue(job_id: str, sqs: SQSClient) -> str:
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
        QueueName=f"{_REQUEST_QUEUE_NAME_PREFIX}{job_id}{_QUEUE_NAME_SUFFIX}",
        Attributes={"FifoQueue": "true"},
        tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
    )

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


def _s3_args_key(job_id: str) -> str:
    return f"task-args/{job_id}"


async def _add_tasks(
    job_id: str,
    request_queue_url: str,
    s3c: S3Client,
    sqs: SQSClient,
    run_map_args: Iterable[Any],
) -> None:
    """
    See create_queues_and_add_tasks

    This can only be called once per request_queue_url. If we wanted to support calling
    add_tasks more than once for the same job, we would need the caller to manage the
    task_ids
    """

    pickles = bytearray()
    grid_tasks: List[str] = []
    range_from = 0
    for i, arg in enumerate(run_map_args):
        arg_pkl = pickle.dumps(arg)
        pickles.extend(arg_pkl)
        range_to = len(pickles) - 1
        grid_tasks.append(
            json.dumps(dict(task_id=i, range_from=range_from, range_end=range_to))
        )
        range_from = range_to + 1

    await s3.upload_async(
        _s3_args_key(job_id), bytes(pickles), s3c.meta.region_name, s3c
    )

    for tasks_chunk in _chunker(enumerate(grid_tasks), 10):
        # this function can only take 10 messages at a time, so we chunk into
        # batches of 10
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message_batch
        result = await _send_or_upload_message_batch(
            sqs, request_queue_url, "task", tasks_chunk
        )
        if "Failed" in result:
            raise ValueError(f"Some grid tasks could not be queued: {result['Failed']}")


async def _add_end_of_job_messages(
    job_id: str,
    request_queue_url: str,
    sqs: SQSClient,
    num_workers: int,
) -> None:
    # done_message: Dict[str, Any] = dict(worker_done=True)
    batch: List[str] = [
        json.dumps(dict(worker_id=i, worker_done=True)) for i in range(num_workers)
    ]
    for msg_chunk in _chunker(enumerate(batch), 10):
        result = await _send_or_upload_message_batch(
            sqs, request_queue_url, "worker-done", msg_chunk
        )
        if "Failed" in result:
            raise ValueError(
                f"Some worker done messages could not be queued: {result['Failed']}"
            )


async def _send_or_upload_message_batch(
    client: Any,
    queue: str,
    message_id_prefix: str,
    tasks_chunk: Iterable[Tuple[int, str]],
) -> Any:
    entries = []
    for i, task in tasks_chunk:
        entries.append(
            {
                "Id": str(i),
                "MessageBody": task,
                # TODO replace 0 with a retry count
                "MessageDeduplicationId": f"{message_id_prefix}-{i}:0",
                "MessageGroupId": "_",
            }
        )

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
    return await client.send_message_batch(QueueUrl=queue, Entries=entries)


def _get_task(
    request_queue_url: str,
    job_id: str,
    region_name: str,
    receive_message_wait_seconds: int,
) -> Optional[Tuple[int, bytes]]:
    """
    Gets the next task from the specified request_queue, downloads the task argument
    from S3, and then deletes the message from the request_queue.

    Returns a tuple of task_id and pickled argument if there was a task, otherwise
    returns None. Waits receive_message_wait_seconds for a task.
    """
    sqs = boto3.client("sqs", region_name=region_name)

    # get the task message
    while True:
        result = sqs.receive_message(
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
        task_id, range_from, range_end = (
            task["task_id"],
            task["range_from"],
            task["range_end"],
        )
        arg = s3.download(_s3_args_key(job_id), region_name, (range_from, range_end))

        # TODO store somewhere (DynamoDB?) that this worker has picked up the task.

        # acknowledge receipt/delete the task request message so we don't have duplicate
        # tasks running
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        sqs.delete_message(
            QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
        )

        return task_id, arg

    # it's a worker exit message, so return None to exit
    sqs.delete_message(
        QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
    )
    return None


def _s3_results_prefix(job_id: str) -> str:
    return f"task-results/{job_id}/"


def _s3_results_key(job_id: str, task_id: int) -> str:
    # A million tasks should be enough for everybody. Formatting the task is important
    # because when we task download results from S3, we use the StartFrom argument to
    # S3's ListObjects to exclude most tasks we've already downloaded.
    return f"{_s3_results_prefix(job_id)}{task_id:06d}"


def _s3_result_key_to_task_id(key: str, results_prefix: str) -> int:
    return int(key.replace(results_prefix, ""))


def _complete_task(
    job_id: str, region_name: str, task_id: int, process_state: ProcessState
) -> None:
    """
    Uploads the result of the task to S3.
    """
    process_state_bytes = process_state.SerializeToString()
    s3.upload(_s3_results_key(job_id, task_id), process_state_bytes, region_name)


def worker_loop(
    function: Callable[[Any], Any],
    request_queue_url: str,
    job_id: str,
    region_name: str,
    public_address: str,
    worker_id: int,
) -> None:
    """
    Runs a loop that gets tasks off of the request_queue, calls function on the
    arguments of the task, and uploads the results to S3.

    public_address is the public address of the current (worker) machine and worker_id
    is a unique identifier for this worker within this grid job.

    TODO right now we have no way of knowing whether the grid job is done (i.e. there
    are no more tasks to run) or there's just nothing left in the task queue. So we are
    careful to make sure to launch workers after we populate the task queue, but there
    are other potentially problematic cases. E.g. there is one task left and two
    workers. Worker 1 receives the last task, Worker 2 sees that there are no tasks on
    the queue and exits. Worker 1 crashes before deleting the message on the request
    queue, so after the VisibilityTimeout it goes back on the queue but now there are no
    more workers. We should have the client send a special message on the "request
    queue" to tell workers they are done.
    """
    pid = os.getpid()

    while True:
        task = _get_task(
            request_queue_url,
            job_id,
            region_name,
            3,
        )
        if not task:
            break

        task_id, arg = task
        try:
            result = function(pickle.loads(arg))
        except Exception as e:
            traceback.print_exc()

            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pid=pid,
                pickled_result=pickle_exception(e, pickle.HIGHEST_PROTOCOL),
                return_code=0,
            )
        else:
            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pid=pid,
                pickled_result=pickle.dumps(result, protocol=pickle.HIGHEST_PROTOCOL),
                return_code=0,
            )

        _complete_task(
            job_id,
            region_name,
            task_id,
            process_state,
        )


async def get_results_unordered(
    job_id: str,
    region_name: str,
    num_tasks: int,
    receive_message_wait_seconds: int = 20,
    workers_done: Optional[asyncio.Event] = None,
) -> AsyncIterable[Tuple[int, ProcessState]]:
    """
    Listens to a result queue until we have results for num_tasks. Returns the unpickled
    results of those tasks.
    """

    # Note: download here is all via S3. It's important that downloads are fast enough -
    # in some cases (many workers, small-ish tasks) the rate of downloading the results
    # to the client can be a limiting factor. In the original implementation results
    # were put on an SQS queue, which only allows 10 messages to be downloaded at a
    # time, some of which may be too big so need an additional S3 download. The current
    # implementation only relies on S3 instead, which allows faster and more concurrent
    # downloads.

    # TODO monitor workers and react appropriately if a worker crashes.
    if workers_done is None:
        workers_done = asyncio.Event()

    num_tasks_completed = 0
    results_prefix = _s3_results_prefix(job_id)
    download_keys_received: Set[str] = set()
    wait = True
    workers_done_wait_count = 0
    session = aiobotocore.session.get_session()
    async with session.create_client("s3", region_name=region_name) as s3c:
        while num_tasks_completed < num_tasks and workers_done_wait_count < 3:

            if workers_done.is_set():
                print(
                    "All workers exited, waiting for task results. "
                    f"Requested: {num_tasks}, received: {num_tasks_completed}"
                )
                workers_done_wait_count += 1
            else:
                print(
                    f"Waiting for task results. Requested: {num_tasks}, "
                    f"received: {num_tasks_completed}"
                )

            if wait:
                try:
                    await asyncio.wait_for(
                        workers_done.wait(),
                        timeout=1.0  # poll more frequently if workers are done.
                        if workers_done.is_set()
                        else receive_message_wait_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

            keys = await s3.list_objects_async(results_prefix, "", region_name, s3c)

            download_tasks = []
            for key in keys:
                if key not in download_keys_received:
                    download_tasks.append(
                        asyncio.create_task(s3.download_async(key, region_name, s3c))
                    )

            download_keys_received.update(keys)

            if len(download_tasks) == 0:
                wait = True
            else:
                wait = False
                for task_result_future in asyncio.as_completed(download_tasks):
                    key, process_state_bytes = await task_result_future
                    process_state = ProcessState()
                    process_state.ParseFromString(process_state_bytes)
                    yield _s3_result_key_to_task_id(key, results_prefix), process_state
                    num_tasks_completed += 1

        if num_tasks_completed < num_tasks:
            print(
                "Gave up retrieving task results. "
                f"Received {num_tasks_completed}/{num_tasks} task results."
            )
        else:
            print(f"Received all {num_tasks} task results.")


async def prepare_ec2_run_map(
    function: Callable[[_T], _U],
    tasks: Sequence[_T],
    region_name: Optional[str],
    resources_required_per_task: ResourcesInternal,
    num_concurrent_tasks: int,
    ports: Optional[Sequence[str]],
) -> RunMapHelper:
    """This code is tightly coupled with run_map"""

    if not region_name:
        region_name = await _get_default_region_name()

    pkey = get_meadowrun_ssh_key(region_name)

    # create SQS queues and add tasks to the request queue
    queues_future = asyncio.create_task(
        create_queues_and_add_tasks(region_name, tasks, num_concurrent_tasks)
    )

    # get hosts
    async with EC2InstanceRegistrar(region_name, "create") as instance_registrar:
        allocated_hosts = await allocate_jobs_to_instances(
            instance_registrar,
            resources_required_per_task,
            num_concurrent_tasks,
            region_name,
            ports,
        )

    request_queue, job_id = await queues_future

    return RunMapHelper(
        region_name,
        allocated_hosts,
        worker_function=functools.partial(
            worker_loop, function, request_queue, job_id, region_name
        ),
        ssh_username=SSH_USER,
        ssh_private_key=pkey,
        num_tasks=len(tasks),
        process_state_futures=functools.partial(
            get_results_unordered, job_id, region_name, len(tasks)
        ),
    )
