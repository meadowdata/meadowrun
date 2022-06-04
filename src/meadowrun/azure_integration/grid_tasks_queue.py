"""See grid_tasks_sqs.py"""

import asyncio
import dataclasses
import datetime
import functools
import os
import pickle
import time
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

from meadowrun.azure_integration.azure_instance_allocation import AzureInstanceRegistrar
from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_storage_account,
    get_default_location,
    record_last_used,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.azure.azure_rest_api import (
    azure_rest_api,
)
from meadowrun.azure_integration.mgmt_functions.azure.azure_storage_api import (
    StorageAccount,
    queue_delete_message,
    queue_receive_messages,
    queue_send_message,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    GRID_TASK_QUEUE,
    QUEUE_NAME_TIMESTAMP_FORMAT,
    _REQUEST_QUEUE_NAME_PREFIX,
    _RESULT_QUEUE_NAME_PREFIX,
)
from meadowrun.instance_allocation import allocate_jobs_to_instances
from meadowrun.meadowrun_pb2 import GridTask, GridTaskStateResponse, ProcessState
from meadowrun.run_job_core import RunMapHelper, AllocCloudInstancesInternal
from meadowrun.shared import pickle_exception

_T = TypeVar("_T")
_U = TypeVar("_U")


@dataclasses.dataclass(frozen=True)
class Queue:
    """Everything we need to be able to access a queue"""

    queue_job_id: str
    queue_name: str
    storage_account: StorageAccount


async def create_queues_and_add_tasks(
    location: str, tasks: Iterable[Any]
) -> Tuple[Queue, Queue]:
    job_id = str(uuid.uuid4())
    print(f"The current run_map's id is {job_id}")
    request_queue, result_queue = await _create_queues_for_job(job_id, location)
    await _add_tasks(request_queue, tasks)
    return request_queue, result_queue


async def _create_queues_for_job(job_id: str, location: str) -> Tuple[Queue, Queue]:
    storage_account = await ensure_meadowrun_storage_account(location, "create")
    now = datetime.datetime.utcnow().strftime(QUEUE_NAME_TIMESTAMP_FORMAT)
    request_queue_task = azure_rest_api(
        "PUT",
        f"{storage_account.get_path()}/queueServices/default/queues/"
        f"{_REQUEST_QUEUE_NAME_PREFIX}-{job_id}-{now}",
        "2021-09-01",
        json_content={},
    )
    result_queue_task = azure_rest_api(
        "PUT",
        f"{storage_account.get_path()}/queueServices/default/queues/"
        f"{_RESULT_QUEUE_NAME_PREFIX}-{job_id}-{now}",
        "2021-09-01",
        json_content={},
    )

    return (
        Queue(job_id, (await request_queue_task)["name"], storage_account),
        Queue(job_id, (await result_queue_task)["name"], storage_account),
    )


async def _add_tasks(request_queue: Queue, tasks: Iterable[Any]) -> None:
    await asyncio.wait(
        [
            queue_send_message(
                request_queue.storage_account,
                request_queue.queue_name,
                GridTask(
                    task_id=i, pickled_function_arguments=pickle.dumps(task)
                ).SerializeToString(),
            )
            for i, task in enumerate(tasks)
        ]
    )


async def _get_task(request_queue: Queue, result_queue: Queue) -> Optional[GridTask]:
    messages = await queue_receive_messages(
        request_queue.storage_account,
        request_queue.queue_name,
        visibility_timeout_secs=5,
    )

    # there was nothing in the queue
    if len(messages) == 0:
        return None

    if len(messages) > 1:
        raise ValueError(
            "queue_receive_messages returned more than 1 message unexpectedly"
        )

    task = GridTask()
    task.ParseFromString(messages[0].message_content)

    await queue_send_message(
        result_queue.storage_account,
        result_queue.queue_name,
        GridTaskStateResponse(
            task_id=task.task_id,
            process_state=ProcessState(
                state=ProcessState.ProcessStateEnum.RUN_REQUESTED,
                # TODO needs to include public address and worker id
            ),
        ).SerializeToString(),
    )

    await queue_delete_message(
        request_queue.storage_account,
        request_queue.queue_name,
        messages[0].message_id,
        messages[0].pop_receipt,
    )

    return task


async def _complete_task(
    result_queue: Queue,
    task: GridTask,
    process_state: ProcessState,
    public_address: str,
    worker_id: int,
) -> None:
    await queue_send_message(
        result_queue.storage_account,
        result_queue.queue_name,
        GridTaskStateResponse(
            task_id=task.task_id, process_state=process_state
        ).SerializeToString(),
    )


async def worker_loop_async(
    function: Callable[[Any], Any],
    request_queue: Queue,
    result_queue: Queue,
    public_address: str,
    worker_id: int,
) -> None:
    pid = os.getpid()

    while True:
        task = await _get_task(request_queue, result_queue)
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

        await _complete_task(
            result_queue, task, process_state, public_address, worker_id
        )


def worker_loop(
    function: Callable[[Any], Any],
    request_queue: Queue,
    result_queue: Queue,
    public_address: str,
    worker_id: int,
) -> None:
    asyncio.run(
        worker_loop_async(
            function, request_queue, result_queue, public_address, worker_id
        )
    )


async def get_results(result_queue: Queue, num_tasks: int, location: str) -> List[Any]:
    task_results_received = 0
    # TODO currently, we get back messages saying that a task is running on a particular
    # worker. We don't really do anything with these messages, but eventually we should
    # use them to react appropriately if a worker crashes unexpectedly.
    running_tasks: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]
    task_results: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]

    t0 = None
    updated = True
    while task_results_received < num_tasks:
        if updated or t0 is None or time.time() - t0 > 20:
            # log this message every 20 seconds, or whenever there's an update
            t0 = time.time()
            updated = False
            print(
                f"Waiting for grid tasks. Requested: {num_tasks}, "
                f"running: {sum(1 for task in running_tasks if task is not None)}, "
                f"completed: {sum(1 for task in task_results if task is not None)}"
            )
            await record_last_used(GRID_TASK_QUEUE, result_queue.queue_job_id, location)

        for message in await queue_receive_messages(
            result_queue.storage_account,
            result_queue.queue_name,
            visibility_timeout_secs=10,
            num_messages=32,  # the max number of messages to get at once
        ):
            updated = True

            task_result = GridTaskStateResponse()
            task_result.ParseFromString(message.message_content)

            if (
                task_result.process_state.state
                == ProcessState.ProcessStateEnum.RUN_REQUESTED
            ):
                running_tasks[task_result.task_id] = task_result.process_state
            elif task_results[task_result.task_id] is None:
                task_results[task_result.task_id] = task_result.process_state
                task_results_received += 1

            await queue_delete_message(
                result_queue.storage_account,
                result_queue.queue_name,
                message.message_id,
                message.pop_receipt,
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


async def prepare_azure_vm_run_map(
    function: Callable[[_T], _U],
    tasks: Sequence[_T],
    location: Optional[str],
    logical_cpu_required_per_task: int,
    memory_gb_required_per_task: float,
    interruption_probability_threshold: float,
    num_concurrent_tasks: int,
) -> RunMapHelper:
    """This code is tightly coupled with run_map"""
    if not location:
        location = get_default_location()

    key_pair_future = asyncio.create_task(ensure_meadowrun_key_pair(location))
    queues_future = asyncio.create_task(create_queues_and_add_tasks(location, tasks))

    async with AzureInstanceRegistrar(location, "create") as instance_registrar:
        allocated_hosts = await allocate_jobs_to_instances(
            instance_registrar,
            AllocCloudInstancesInternal(
                logical_cpu_required_per_task,
                memory_gb_required_per_task,
                interruption_probability_threshold,
                num_concurrent_tasks,
                location,
            ),
        )

    private_key, public_key = await key_pair_future
    request_queue, result_queue = await queues_future

    return RunMapHelper(
        location,
        allocated_hosts,
        functools.partial(worker_loop, function, request_queue, result_queue),
        {
            "user": "meadowrunuser",
            "connect_kwargs": {"pkey": private_key},
        },
        get_results(result_queue, len(tasks), location),
    )
