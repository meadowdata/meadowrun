"""See grid_tasks_sqs.py"""

import asyncio
import dataclasses
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

from azure.mgmt.storage.aio import StorageManagementClient
from azure.storage.queue import BinaryBase64EncodePolicy, BinaryBase64DecodePolicy
from azure.storage.queue.aio import QueueClient

from meadowrun.azure_integration.azure_core import (
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.azure_instance_allocation import AzureInstanceRegistrar
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.azure_storage import ensure_meadowrun_storage_account
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
    get_credential_aio,
)
from meadowrun.instance_allocation import allocate_jobs_to_instances
from meadowrun.meadowrun_pb2 import GridTask, GridTaskStateResponse, ProcessState
from meadowrun.run_job_core import RunMapHelper, AllocCloudInstancesInternal
from meadowrun.shared import pickle_exception

_REQUEST_QUEUE_NAME_PREFIX = "meadowruntaskrequestqueue-"
_RESULT_QUEUE_NAME_PREFIX = "meadowruntaskresultqueue-"

_T = TypeVar("_T")
_U = TypeVar("_U")


@dataclasses.dataclass(frozen=True)
class QueueClientParameters:
    """Basically a QueueClient constructor lambda"""
    _queue_name: str
    _storage_connection_string: str

    def get_queue_client(self) -> QueueClient:
        # cast is necessary because type hint for from_connection_string is wrong
        return cast(
            QueueClient,
            QueueClient.from_connection_string(
                self._storage_connection_string,
                self._queue_name,
                message_encode_policy=BinaryBase64EncodePolicy(),
                message_decode_policy=BinaryBase64DecodePolicy(),
            ),
        )


async def create_queues_and_add_tasks(
    location: str,
    tasks: Iterable[Any],
) -> Tuple[QueueClientParameters, QueueClientParameters]:
    job_id = str(uuid.uuid4())
    request_queue, result_queue = await _create_queues_for_job(job_id, location)
    await _add_tasks(request_queue, tasks)
    return request_queue, result_queue


async def _create_queues_for_job(
    job_id: str, location: str
) -> Tuple[QueueClientParameters, QueueClientParameters]:
    storage_account_name, storage_account_key = await ensure_meadowrun_storage_account(
        location, "create"
    )
    storage_connection_string = (
        f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};"
        f"AccountKey={storage_account_key}"
    )

    async with StorageManagementClient(
        get_credential_aio(), await get_subscription_id()
    ) as mgmt_client:
        request_queue_task = asyncio.create_task(
            mgmt_client.queue.create(
                MEADOWRUN_RESOURCE_GROUP_NAME,
                storage_account_name,
                f"{_REQUEST_QUEUE_NAME_PREFIX}{job_id}",
                {},
            )
        )
        result_queue_task = asyncio.create_task(
            mgmt_client.queue.create(
                MEADOWRUN_RESOURCE_GROUP_NAME,
                storage_account_name,
                f"{_RESULT_QUEUE_NAME_PREFIX}{job_id}",
                {},
            )
        )
        return (
            QueueClientParameters(
                (await request_queue_task).name, storage_connection_string
            ),
            QueueClientParameters(
                (await result_queue_task).name, storage_connection_string
            ),
        )


async def _add_tasks(
    request_queue: QueueClientParameters, tasks: Iterable[Any]
) -> None:
    async with request_queue.get_queue_client() as client:
        for i, task in enumerate(tasks):
            await client.send_message(
                GridTask(
                    task_id=i, pickled_function_arguments=pickle.dumps(task)
                ).SerializeToString()
            )


async def _get_task(
    request_queue: QueueClientParameters, result_queue: QueueClientParameters
) -> Optional[GridTask]:
    async with request_queue.get_queue_client() as request_queue_client:
        message = await request_queue_client.receive_message(visibility_timeout=5)

        # there was nothing in the queue
        if message is None:
            return None

        task = GridTask()
        task.ParseFromString(message["content"])

        async with result_queue.get_queue_client() as result_queue_client:
            await result_queue_client.send_message(
                GridTaskStateResponse(
                    task_id=task.task_id,
                    process_state=ProcessState(
                        state=ProcessState.ProcessStateEnum.RUN_REQUESTED,
                        # TODO needs to include public address and worker id
                    ),
                ).SerializeToString()
            )

        await request_queue_client.delete_message(message["id"], message["pop_receipt"])

    return task


async def _complete_task(
    result_queue: QueueClientParameters,
    task: GridTask,
    process_state: ProcessState,
    public_address: str,
    worker_id: int,
) -> None:
    async with result_queue.get_queue_client() as client:
        await client.send_message(
            GridTaskStateResponse(
                task_id=task.task_id, process_state=process_state
            ).SerializeToString()
        )


async def worker_loop_async(
    function: Callable[[Any], Any],
    request_queue: QueueClientParameters,
    result_queue: QueueClientParameters,
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
    request_queue: QueueClientParameters,
    result_queue: QueueClientParameters,
    public_address: str,
    worker_id: int,
) -> None:
    asyncio.run(
        worker_loop_async(
            function, request_queue, result_queue, public_address, worker_id
        )
    )


async def get_results(result_queue: QueueClientParameters, num_tasks: int) -> List[Any]:
    task_results_received = 0
    # TODO currently, we get back messages saying that a task is running on a particular
    # worker. We don't really do anything with these messages, but eventually we should
    # use them to react appropriately if a worker crashes unexpectedly.
    running_tasks: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]
    task_results: List[Optional[ProcessState]] = [None for _ in range(num_tasks)]

    t0 = None
    updated = True
    async with result_queue.get_queue_client() as client:
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

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
            async for message in client.receive_messages(visibility_timeout=10):
                updated = True

                task_result = GridTaskStateResponse()
                task_result.ParseFromString(message["content"])

                if (
                    task_result.process_state.state
                    == ProcessState.ProcessStateEnum.RUN_REQUESTED
                ):
                    running_tasks[task_result.task_id] = task_result.process_state
                elif task_results[task_result.task_id] is None:
                    task_results[task_result.task_id] = task_result.process_state
                    task_results_received += 1

                await client.delete_message(message["id"], message["pop_receipt"])

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
        get_results(result_queue, len(tasks)),
    )
