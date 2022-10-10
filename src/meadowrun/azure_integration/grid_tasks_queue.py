"""See grid_tasks_sqs.py"""
from __future__ import annotations

import asyncio
import dataclasses
import datetime
import os
import pickle
import time
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_storage_account,
    record_last_used,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    _REQUEST_QUEUE_NAME_PREFIX,
    _RESULT_QUEUE_NAME_PREFIX,
    GRID_TASK_QUEUE,
    QUEUE_NAME_TIMESTAMP_FORMAT,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_storage_api import (
    StorageAccount,
    queue_delete_message,
    queue_receive_messages,
    queue_send_message,
)
from meadowrun.meadowrun_pb2 import GridTask, GridTaskStateResponse, ProcessState
from meadowrun.run_job_core import TaskProcessState
from meadowrun.run_job_local import WorkerMonitor, restart_worker

if TYPE_CHECKING:
    from meadowrun.run_job_core import TaskWorkerServer, WorkerProcessState

_T = TypeVar("_T")
_U = TypeVar("_U")


@dataclasses.dataclass(frozen=True)
class Queue:
    """Everything we need to be able to access a queue"""

    queue_job_id: str
    queue_name: str
    storage_account: StorageAccount


async def create_queues_for_job(job_id: str, location: str) -> Tuple[Queue, Queue]:
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


_WORKER_SHUTDOWN_MESSAGE = b"worker-shutdown"


async def add_tasks(request_queue: Queue, tasks: Iterable[Any]) -> None:
    await asyncio.wait(
        [
            asyncio.create_task(
                queue_send_message(
                    request_queue.storage_account,
                    request_queue.queue_name,
                    GridTask(
                        task_id=i,
                        attempt=1,
                        pickled_function_arguments=pickle.dumps(((task,), {})),
                    ).SerializeToString(),
                )
            )
            for i, task in enumerate(tasks)
        ]
    )


async def retry_task(
    request_queue: Queue, task_id: int, attempt: int, task: Any
) -> None:
    await queue_send_message(
        request_queue.storage_account,
        request_queue.queue_name,
        GridTask(
            task_id=task_id,
            attempt=attempt,
            pickled_function_arguments=pickle.dumps(((task,), {})),
        ).SerializeToString(),
    )


async def add_worker_shutdown_message(request_queue: Queue, num_workers: int) -> None:
    # TODO I think these could be sent in a single API call
    await asyncio.wait(
        [
            asyncio.create_task(
                queue_send_message(
                    request_queue.storage_account,
                    request_queue.queue_name,
                    _WORKER_SHUTDOWN_MESSAGE,
                )
            )
            for _ in range(num_workers)
        ]
    )


async def _get_task(request_queue: Queue, result_queue: Queue) -> Optional[GridTask]:
    while True:
        messages = await queue_receive_messages(
            request_queue.storage_account,
            request_queue.queue_name,
            visibility_timeout_secs=5,
        )

        if len(messages) > 0:
            break

    if len(messages) > 1:
        raise ValueError(
            "queue_receive_messages returned more than 1 message unexpectedly"
        )

    # if we got a worker done message, return None
    if messages[0].message_content == _WORKER_SHUTDOWN_MESSAGE:
        return None

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
) -> None:
    await queue_send_message(
        result_queue.storage_account,
        result_queue.queue_name,
        GridTaskStateResponse(
            task_id=task.task_id, attempt=task.attempt, process_state=process_state
        ).SerializeToString(),
    )


async def _worker_iteration(
    request_queue: Queue,
    result_queue: Queue,
    log_file_name: str,
    pid: int,
    worker_server: TaskWorkerServer,
    worker_monitor: WorkerMonitor,
) -> bool:
    task = await _get_task(request_queue, result_queue)
    if not task:
        print("Meadowrun agent: Received shutdown message. Exiting.")
        return False

    print(
        f"Meadowrun agent: About to execute task #{task.task_id}, attempt "
        f"#{task.attempt}"
    )

    try:
        worker_monitor.start_stats()
        await worker_server.send_message(task.pickled_function_arguments)
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
            f"Meadowrun agent: Unexpected worker exit, restarting worker. In "
            f"task #{task.task_id}, attempt #{task.attempt}, {traceback.format_exc()}"
        )

        await restart_worker(worker_server, worker_monitor)

    print(
        f"Meadowrun agent: Completed task #{task.task_id}, attempt #{task.attempt}, "
        f"state {ProcessState.ProcessStateEnum.Name(process_state.state)}, max "
        f"memory {process_state.max_memory_used_gb}GB "
    )

    await _complete_task(result_queue, task, process_state)
    return True


async def worker_function(
    request_queue: Queue,
    result_queue: Queue,
    public_address: str,
    log_file_name: str,
    worker_server: TaskWorkerServer,
    worker_monitor: WorkerMonitor,
) -> None:
    pid = os.getpid()
    log_file_name = f"{public_address}:{log_file_name}"

    while await _worker_iteration(
        request_queue, result_queue, log_file_name, pid, worker_server, worker_monitor
    ):
        pass


async def get_results_unordered(
    result_queue: Queue,
    location: str,
    stop_receiving: asyncio.Event,
    all_workers_exited: asyncio.Event,
) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:

    # TODO currently, we get back messages saying that a task is running on a particular
    # worker. We don't really do anything with these messages, but eventually we should
    # use them to react appropriately if a worker crashes unexpectedly.

    num_tasks_running, num_tasks_completed = 0, 0
    t0 = None
    updated = True
    stop_receiving_wait_task = asyncio.create_task(stop_receiving.wait())
    all_workers_exited_task = asyncio.create_task(all_workers_exited.wait())
    while not stop_receiving.is_set():
        if updated or t0 is None or time.time() - t0 > 20:
            t0 = time.time()
            updated = False
            await record_last_used(GRID_TASK_QUEUE, result_queue.queue_job_id, location)

        receive_messages_task = asyncio.create_task(
            queue_receive_messages(
                result_queue.storage_account,
                result_queue.queue_name,
                visibility_timeout_secs=10,
                num_messages=32,  # the max number of messages to get at once
            )
        )
        await asyncio.wait(
            cast(
                List[asyncio.Task],
                [
                    receive_messages_task,
                    stop_receiving_wait_task,
                    all_workers_exited_task,
                ],
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_receiving_wait_task.done() or all_workers_exited_task.done():
            return
        else:
            results = []

            for message in receive_messages_task.result():
                updated = True

                task_result = GridTaskStateResponse()
                task_result.ParseFromString(message.message_content)

                if (
                    task_result.process_state.state
                    == ProcessState.ProcessStateEnum.RUN_REQUESTED
                ):
                    num_tasks_running += 1
                else:
                    num_tasks_running -= 1
                    num_tasks_completed += 1
                    results.append(
                        TaskProcessState(
                            task_result.task_id,
                            task_result.attempt,
                            task_result.process_state,
                        )
                    )
                await queue_delete_message(
                    result_queue.storage_account,
                    result_queue.queue_name,
                    message.message_id,
                    message.pop_receipt,
                )

            yield results, []
