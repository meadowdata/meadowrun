from __future__ import annotations

import asyncio
import base64
import functools
import os
import pickle
import traceback
import uuid
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)
import itertools

import aiobotocore.session
import boto3

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import EC2InstanceRegistrar
from meadowrun.aws_integration.ec2_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
)
from meadowrun.instance_allocation import allocate_jobs_to_instances
from meadowrun.meadowrun_pb2 import GridTask, ProcessState, GridTaskStateResponse
from meadowrun.run_job_core import RunMapHelper, AllocCloudInstancesInternal
from meadowrun.shared import pickle_exception


_REQUEST_QUEUE_NAME_PREFIX = "meadowrunTaskRequestQueue-"
_RESULT_QUEUE_NAME_PREFIX = "meadowrunTaskResultQueue-"
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
    await _add_tasks(request_queue_url, region_name, tasks)
    return request_queue_url, result_queue_url


async def _create_queues_for_job(job_id: str, region_name: str) -> Tuple[str, str]:
    """
    See create_queues_and_add_tasks

    Mote that queue names can't contain anything other than letters, numbers, and
    hyphens
    """

    # TODO we should automatically delete these queues also. I think the best way is to
    # have get_results periodically tag the queue with a timestamp and have a lambda
    # periodically clean them up

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


def _chunker(it: Iterable[_T], size: int) -> Iterable[List[_T]]:
    """E.g. _chunker([1, 2, 3, 4, 5], 2) -> [1, 2], [3, 4], [5]"""
    iterator = iter(it)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk


async def _add_tasks(
    request_queue_url: str, region_name: str, tasks: Iterable[Any]
) -> None:
    """
    See create_queues_and_add_tasks

    This can only be called once per request_queue_url. If we wanted to support calling
    add_tasks more than once for the same job, we would need the caller to manage the
    task_ids
    """
    async with aiobotocore.session.get_session().create_client(
        "sqs", region_name=region_name
    ) as client:
        for tasks_chunk in _chunker(enumerate(tasks), 10):
            # this function can only take 10 messages at a time, so we chunk into
            # batches of 10
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message_batch
            result = await client.send_message_batch(
                QueueUrl=request_queue_url,
                Entries=[
                    {
                        "Id": str(i),
                        "MessageBody": base64.b64encode(
                            GridTask(
                                task_id=i, pickled_function_arguments=pickle.dumps(task)
                            ).SerializeToString()
                        ).decode("utf-8"),
                        # TODO replace 0 with a retry count
                        "MessageDeduplicationId": f"{i}:0",
                        "MessageGroupId": "_",
                    }
                    for i, task in tasks_chunk
                ],
            )
            if "Failed" in result:
                raise ValueError(
                    f"Some grid tasks could not be queued: {result['Failed']}"
                )


def _get_task(
    request_queue_url: str,
    result_queue_url: str,
    region_name: str,
    receive_message_wait_seconds: int,
    public_address: str,
    worker_id: int,
) -> Optional[GridTask]:
    """
    Gets the next task from the specified request_queue, sends a RUN_REQUESTED
    GridTaskStateResponse on the result_queue, and then deletes the message from the
    request_queue.

    Returns the GridTask from the queue if there was a task, otherwise returns None.
    Waits receive_message_wait_seconds for a task.
    """
    client = boto3.client("sqs", region_name=region_name)

    # get the GridTask message
    result = client.receive_message(
        QueueUrl=request_queue_url, WaitTimeSeconds=receive_message_wait_seconds
    )

    if "Messages" not in result:
        return None  # there was nothing in the queue

    messages = result["Messages"]
    if len(messages) != 1:
        raise ValueError(f"Requested one message but got {len(messages)}")
    else:
        # parse the task request
        task = GridTask()
        task.ParseFromString(base64.b64decode(messages[0]["Body"].encode("utf-8")))

        # send the RUN_REQUESTED message on the result queue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.send_message
        client.send_message(
            QueueUrl=result_queue_url,
            MessageBody=base64.b64encode(
                GridTaskStateResponse(
                    task_id=task.task_id,
                    process_state=ProcessState(
                        state=ProcessState.ProcessStateEnum.RUN_REQUESTED,
                        # TODO needs to include public address and worker id
                    ),
                ).SerializeToString()
            ).decode("utf-8"),
            # TODO replace 0 here with a retry count
            MessageDeduplicationId=(
                f"{task.task_id}:0:{public_address}:{worker_id}:requested"
            ),
            MessageGroupId="_",
        )

        # acknowledge receipt/delete the task request message so we don't have duplicate
        # tasks running
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_message
        client.delete_message(
            QueueUrl=request_queue_url, ReceiptHandle=messages[0]["ReceiptHandle"]
        )

        return task


def _complete_task(
    result_queue_url: str,
    region_name: str,
    task: GridTask,
    process_state: ProcessState,
    public_address: str,
    worker_id: int,
) -> None:
    """
    Sends a message to the result queue that the specified task has completed with the
    specified process_state
    """
    boto3.client("sqs", region_name=region_name).send_message(
        QueueUrl=result_queue_url,
        MessageBody=base64.b64encode(
            GridTaskStateResponse(
                task_id=task.task_id, process_state=process_state
            ).SerializeToString()
        ).decode("utf-8"),
        # TODO replace 0 here with a retry count
        MessageDeduplicationId=(
            f"{task.task_id}:0:{public_address}:{worker_id}:completed"
        ),
        MessageGroupId="_",
    )


def worker_loop(
    function: Callable[[Any], Any],
    request_queue_url: str,
    result_queue_url: str,
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
            result_queue_url,
            region_name,
            1,
            public_address,
            worker_id,
        )
        if not task:
            break

        try:
            result = function(pickle.loads(task.pickled_function_arguments))
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
            result_queue_url,
            region_name,
            task,
            process_state,
            public_address,
            worker_id,
        )


async def get_results(
    result_queue_url: str,
    region_name: str,
    num_tasks: int,
    receive_message_wait_seconds: int = 20,
) -> List[Any]:
    """
    Listens to a result queue until we have results for num_tasks. Returns the unpickled
    results of those tasks.
    """

    task_results_received = 0
    # TODO currently, we get back messages saying that a task is running on a particular
    # worker. We don't really do anything with these messages, but eventually we should
    # use them to react appropriately if a worker crashes unexpectedly.
    running_tasks: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]
    task_results: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]

    async with aiobotocore.session.get_session().create_client(
        "sqs", region_name=region_name
    ) as client:
        while task_results_received < num_tasks:
            print(
                f"Waiting for grid tasks. Requested: {num_tasks}, "
                f"running: {sum(1 for task in running_tasks if task is not None)}, "
                f"completed: {sum(1 for task in task_results if task is not None)}"
            )

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
            receive_result = await client.receive_message(
                QueueUrl=result_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=receive_message_wait_seconds,
            )

            if "Messages" in receive_result:
                receipt_handles = []
                for message in receive_result["Messages"]:
                    receipt_handles.append(message["ReceiptHandle"])
                    task_result = GridTaskStateResponse()
                    task_result.ParseFromString(
                        base64.b64decode(message["Body"].encode("utf-8"))
                    )

                    if (
                        task_result.process_state.state
                        == ProcessState.ProcessStateEnum.RUN_REQUESTED
                    ):
                        running_tasks[task_result.task_id] = task_result.process_state
                    elif task_results[task_result.task_id] is None:
                        task_results[task_result.task_id] = task_result.process_state
                        task_results_received += 1

                delete_result = await client.delete_message_batch(
                    QueueUrl=result_queue_url,
                    Entries=[
                        {
                            "Id": str(i),
                            "ReceiptHandle": receipt_handle,
                        }
                        for i, receipt_handle in enumerate(receipt_handles)
                    ],
                )
                if "Failed" in delete_result:
                    raise ValueError(
                        f"Failed to delete messages: {delete_result['Failed']}"
                    )

    # we're guaranteed by the logic in the while loop that we don't have any Nones left
    # in task_results
    task_results = cast(List[ProcessState], task_results)

    failed_tasks = [
        result
        for result in task_results
        if result.state != ProcessState.ProcessStateEnum.SUCCEEDED
    ]
    if failed_tasks:
        # TODO better error message
        raise ValueError(f"Some tasks failed: {failed_tasks}")

    # TODO try/catch on pickle.loads?
    return [pickle.loads(result.pickled_result) for result in task_results]


async def prepare_ec2_run_map(
    function: Callable[[_T], _U],
    tasks: Sequence[_T],
    region_name: Optional[str],
    logical_cpu_required_per_task: int,
    memory_gb_required_per_task: float,
    interruption_probability_threshold: float,
    num_concurrent_tasks: int,
) -> RunMapHelper:
    """This code is tightly coupled with run_map"""

    if not region_name:
        region_name = await _get_default_region_name()

    pkey = ensure_meadowrun_key_pair(region_name)

    # create SQS queues and add tasks to the request queue
    queues_future = asyncio.create_task(create_queues_and_add_tasks(region_name, tasks))

    # get hosts
    async with EC2InstanceRegistrar(region_name, "create") as instance_registrar:
        allocated_hosts = await allocate_jobs_to_instances(
            instance_registrar,
            AllocCloudInstancesInternal(
                logical_cpu_required_per_task,
                memory_gb_required_per_task,
                interruption_probability_threshold,
                num_concurrent_tasks,
                region_name,
            ),
        )

    request_queue, result_queue = await queues_future

    return RunMapHelper(
        region_name,
        allocated_hosts,
        functools.partial(
            worker_loop, function, request_queue, result_queue, region_name
        ),
        {"user": "ubuntu", "connect_kwargs": {"pkey": pkey}},
        get_results(result_queue, region_name, len(tasks)),
    )
