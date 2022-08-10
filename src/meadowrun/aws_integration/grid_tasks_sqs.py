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
    AsyncGenerator,
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

from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import RunMapHelper
from meadowrun.shared import pickle_exception

_REQUEST_QUEUE_NAME_PREFIX = "meadowrun-task-request-"
_RESULT_QUEUE_NAME_PREFIX = "meadowrun-task-result-"
_QUEUE_NAME_SUFFIX = ".fifo"

_T = TypeVar("_T")
_U = TypeVar("_U")


async def create_queues_and_add_tasks(
    region_name: str, tasks: Iterable[Any]
) -> Tuple[str, str]:
    """
    Creates the queues necessary to run a grid job. Returns (request_queue_url,
    result_queue_url). The request queue contains GridTasks that represent tasks that
    need to run. The result queue contains GridTaskStateResponses that represent updates
    to the state of a task from a grid worker.

    Then adds tasks to the specified request queue. tasks should contain arguments that
    will be passed to the function corresponding to this request queue.
    """

    # this id is just used for creating the job's queues. It has no relationship to any
    # Job.job_ids
    job_id = str(uuid.uuid4())
    print(f"The current run_map's id is {job_id}")
    request_queue_url, result_queue_url = await _create_queues_for_job(
        job_id, region_name
    )
    await _add_tasks(job_id, request_queue_url, region_name, tasks)
    return request_queue_url, job_id


async def _create_queues_for_job(job_id: str, region_name: str) -> Tuple[str, str]:
    """
    See create_queues_and_add_tasks

    Mote that queue names can't contain anything other than letters, numbers, and
    hyphens
    """

    async with aiobotocore.session.get_session().create_client(
        "sqs", region_name=region_name
    ) as client:
        # interestingly, this call will not fail if the exact same queue already exists,
        # but if the queue exists but has different attributes/tags, it will throw an
        # exception
        # TODO try to detect job id collisions?
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.create_queue
        request_queue_future = asyncio.create_task(
            client.create_queue(
                QueueName=f"{_REQUEST_QUEUE_NAME_PREFIX}{job_id}{_QUEUE_NAME_SUFFIX}",
                Attributes={"FifoQueue": "true"},
                tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
            )
        )
        result_queue_future = asyncio.create_task(
            client.create_queue(
                QueueName=f"{_RESULT_QUEUE_NAME_PREFIX}{job_id}{_QUEUE_NAME_SUFFIX}",
                Attributes={"FifoQueue": "true"},
                tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
            )
        )

        request_queue_url = (await request_queue_future)["QueueUrl"]
        result_queue_url = (await result_queue_future)["QueueUrl"]

    return request_queue_url, result_queue_url


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
    job_id: str, request_queue_url: str, region_name: str, run_map_args: Iterable[Any]
) -> None:
    """
    See create_queues_and_add_tasks

    This can only be called once per request_queue_url. If we wanted to support calling
    add_tasks more than once for the same job, we would need the caller to manage the
    task_ids
    """
    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs", region_name=region_name
    ) as sqs, session.create_client("s3", region_name=region_name) as s3c:

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

        await s3.upload_async(_s3_args_key(job_id), bytes(pickles), region_name, s3c)

        for tasks_chunk in _chunker(enumerate(grid_tasks), 10):
            # this function can only take 10 messages at a time, so we chunk into
            # batches of 10
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message_batch
            result = await _send_or_upload_message_batch(
                sqs, request_queue_url, tasks_chunk
            )
            if "Failed" in result:
                raise ValueError(
                    f"Some grid tasks could not be queued: {result['Failed']}"
                )


async def _send_or_upload_message_batch(
    client: Any,
    queue: str,
    tasks_chunk: Iterable[Tuple[int, str]],
) -> Any:
    entries = []
    for i, task in tasks_chunk:
        entries.append(
            {
                "Id": str(i),
                "MessageBody": task,
                # TODO replace 0 with a retry count
                "MessageDeduplicationId": f"{i}:0",
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
    Gets the next task from the specified request_queue, sends a RUN_REQUESTED
    GridTaskStateResponse on the result_queue, and then deletes the message from the
    request_queue.

    Returns the GridTask from the queue if there was a task, otherwise returns None.
    Waits receive_message_wait_seconds for a task.
    """
    sqs = boto3.client("sqs", region_name=region_name)

    # get the GridTask message
    result = sqs.receive_message(
        QueueUrl=request_queue_url, WaitTimeSeconds=receive_message_wait_seconds
    )

    if "Messages" not in result:
        return None  # there was nothing in the queue

    messages = result["Messages"]
    if len(messages) != 1:
        raise ValueError(f"Requested one message but got {len(messages)}")

    # parse the task request
    task = json.loads(messages[0]["Body"])
    task_id, range_from, range_end = (
        task["task_id"],
        task["range_from"],
        task["range_end"],
    )
    arg = s3.download(_s3_args_key(job_id), region_name, (range_from, range_end))
    # task = _get_result_from_body(messages[0]["Body"], region_name, GridTask)

    # TODO store somewhere (DynamoDB?) that this worker has picked up the task.

    # acknowledge receipt/delete the task request message so we don't have duplicate
    # tasks running
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
    sqs.delete_message(
        QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
    )

    return task_id, arg


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
    Runs a loop that gets GridTasks off of the request_queue, calls function on the
    arguments in the GridTask, and sends the results back on the result_queue.

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
            1,
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
) -> AsyncGenerator[Tuple[int, ProcessState], None]:
    """
    Listens to a result queue until we have results for num_tasks. Returns the unpickled
    results of those tasks.
    """

    # TODO monitor workers and react appropriately if a worker crashes.
    if workers_done is None:
        workers_done = asyncio.Event()

    num_tasks_completed = 0
    results_prefix = _s3_results_prefix(job_id)
    download_keys_received: Set[str] = set()
    wait = True

    session = aiobotocore.session.get_session()
    async with session.create_client("s3", region_name=region_name) as s3c:
        while num_tasks_completed < num_tasks:
            print(
                f"Waiting for grid tasks. Requested: {num_tasks}, "
                f"completed: {num_tasks_completed}"
            )
            if wait and not workers_done.is_set():
                try:
                    await asyncio.wait_for(
                        workers_done.wait(), timeout=receive_message_wait_seconds
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
    queues_future = asyncio.create_task(create_queues_and_add_tasks(region_name, tasks))

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
        result_futures=functools.partial(
            get_results_unordered, job_id, region_name, len(tasks)
        ),
    )
