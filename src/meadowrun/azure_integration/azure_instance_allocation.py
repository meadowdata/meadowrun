from __future__ import annotations

import asyncio
import dataclasses
import datetime
import functools
import json
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

import meadowrun.azure_integration.azure_vms
from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_table,
    get_default_location,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.blob_storage import get_azure_blob_container
from meadowrun.azure_integration.grid_tasks_queue import (
    Queue,
    add_tasks,
    add_worker_shutdown_message,
    create_queues_for_job,
    get_results_unordered,
    retry_task,
    worker_function,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    ALLOCATED_TIME,
    LAST_UPDATE_TIME,
    NON_CONSUMABLE_RESOURCES,
    PREVENT_FURTHER_ALLOCATION,
    PUBLIC_ADDRESS,
    RESOURCES_ALLOCATED,
    RESOURCES_AVAILABLE,
    RUNNING_JOBS,
    SINGLE_PARTITION_KEY,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_storage_api import (
    StorageAccount,
    azure_table_api,
    azure_table_api_paged,
    table_key_url,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import VM_ALLOC_TABLE_NAME
from meadowrun.instance_allocation import (
    InstanceRegistrar,
    _InstanceState,
    _TInstanceState,
    allocate_single_job_to_instance,
)
from meadowrun.instance_selection import CloudInstance, ResourcesInternal
from meadowrun.run_job_core import (
    AllocVM,
    CloudProviderType,
    GridJobCloudInterface,
    JobCompletion,
    SshHost,
    WaitOption,
)

if TYPE_CHECKING:
    from types import TracebackType

    import asyncssh
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket
    from meadowrun.meadowrun_pb2 import Job
    from meadowrun.run_job_core import TaskProcessState, WorkerProcessState
    from meadowrun.run_job_local import TaskWorkerServer, WorkerMonitor
    from typing_extensions import Literal


_T = TypeVar("_T")
_U = TypeVar("_U")


@dataclasses.dataclass
class AzureVMInstanceState(_InstanceState):
    # See https://microsoft.github.io/AzureTipsAndTricks/blog/tip88.html
    etag: str


class AzureInstanceRegistrar(InstanceRegistrar[AzureVMInstanceState]):
    def __init__(
        self, table_location: str, on_table_missing: Literal["create", "raise"]
    ):
        # this is just the location where the table backing this InstanceRegistrar
        # lives. We can register VMs from any location
        self._table_location = table_location
        self._on_table_missing = on_table_missing

        self._storage_account: Optional[StorageAccount] = None

    async def __aenter__(self) -> AzureInstanceRegistrar:
        self._storage_account = await ensure_table(
            VM_ALLOC_TABLE_NAME, self._table_location, self._on_table_missing
        )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    def get_region_name(self) -> str:
        return self._table_location

    async def register_instance(
        self,
        public_address: str,
        name: str,
        resources_available: ResourcesInternal,
        running_jobs: List[Tuple[str, ResourcesInternal]],
    ) -> None:
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        now = datetime.datetime.utcnow().isoformat()
        try:
            # See EC2InstanceRegistrar.register_instance for more details on the schema
            # https://docs.microsoft.com/en-us/rest/api/storageservices/insert-entity
            await azure_table_api(
                "POST",  # inserts rather than replace/update
                self._storage_account,
                VM_ALLOC_TABLE_NAME,
                json_content={
                    "PartitionKey": SINGLE_PARTITION_KEY,
                    "RowKey": name,
                    PUBLIC_ADDRESS: public_address,
                    RESOURCES_AVAILABLE: json.dumps(resources_available.consumable),
                    NON_CONSUMABLE_RESOURCES: json.dumps(
                        resources_available.non_consumable
                    ),
                    RUNNING_JOBS: json.dumps(
                        {
                            job_id: {
                                RESOURCES_ALLOCATED: allocated_resources.consumable,
                                ALLOCATED_TIME: now,
                            }
                            for job_id, allocated_resources in running_jobs
                        }
                    ),
                    PREVENT_FURTHER_ALLOCATION: False,
                    LAST_UPDATE_TIME: now,
                },
            )
        except ResourceExistsError:
            # It's possible that an existing EC2 instance crashed unexpectedly, the
            # coordinator record hasn't been deleted yet, and a new instance was created
            # that has the same address
            raise ValueError(
                f"Tried to register a VM {public_address} but it already exists,"
                " this should never happen!"
            )

    async def get_registered_instances(self) -> List[AzureVMInstanceState]:
        """
        For the AzureInstanceRegistrar, we always populate all the fields on
        AzureVMInstanceState because we will need them in allocate_jobs_to_instance and
        deallocate_job_from_instance
        """
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        # https://docs.microsoft.com/en-us/rest/api/storageservices/query-entities
        return [
            AzureVMInstanceState(
                item[PUBLIC_ADDRESS],
                item["RowKey"],
                ResourcesInternal(
                    json.loads(item[RESOURCES_AVAILABLE]),
                    json.loads(item[NON_CONSUMABLE_RESOURCES]),
                ),
                json.loads(item[RUNNING_JOBS]),
                item[PREVENT_FURTHER_ALLOCATION],
                item["odata.etag"],
            )
            async for page in azure_table_api_paged(
                "GET",
                self._storage_account,
                VM_ALLOC_TABLE_NAME,
                query_parameters={
                    "$select": ",".join(
                        [
                            "RowKey",
                            RESOURCES_AVAILABLE,
                            NON_CONSUMABLE_RESOURCES,
                            RUNNING_JOBS,
                            PUBLIC_ADDRESS,
                            PREVENT_FURTHER_ALLOCATION,
                        ]
                    ),
                },
            )
            for item in page["value"]
        ]

    async def get_registered_instance(self, name: str) -> AzureVMInstanceState:
        """
        For the AzureInstanceRegistrar, we always populate all the fields on
        AzureVMInstanceState because we will need them in allocate_jobs_to_instance and
        deallocate_job_from_instance
        """
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        try:
            item = await azure_table_api(
                "GET",
                self._storage_account,
                table_key_url(VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, name),
                query_parameters={
                    "$select": ",".join(
                        [
                            "RowKey",
                            RESOURCES_AVAILABLE,
                            NON_CONSUMABLE_RESOURCES,
                            RUNNING_JOBS,
                            PUBLIC_ADDRESS,
                            PREVENT_FURTHER_ALLOCATION,
                        ]
                    )
                },
            )
        except ResourceNotFoundError:
            raise ValueError(f"VM {name} was not found")

        return AzureVMInstanceState(
            item[PUBLIC_ADDRESS],
            item["RowKey"],
            ResourcesInternal(
                json.loads(item[RESOURCES_AVAILABLE]),
                json.loads(item[NON_CONSUMABLE_RESOURCES]),
            ),
            json.loads(item[RUNNING_JOBS]),
            item[PREVENT_FURTHER_ALLOCATION],
            item["odata.etag"],
        )

    async def allocate_jobs_to_instance(
        self,
        instance: AzureVMInstanceState,
        resources_allocated_per_job: ResourcesInternal,
        new_job_ids: List[str],
    ) -> bool:
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        now = datetime.datetime.utcnow().isoformat()

        if len(new_job_ids) == 0:
            raise ValueError("Must provide at least one new_job_ids")

        new_running_jobs = instance.get_running_jobs().copy()
        for job_id in new_job_ids:
            if job_id in instance.get_running_jobs():
                # TODO this should probably be an exception?
                return False

            new_running_jobs[job_id] = {
                RESOURCES_ALLOCATED: resources_allocated_per_job.consumable,
                ALLOCATED_TIME: now,
            }

        new_resources_available = instance.get_available_resources().subtract(
            resources_allocated_per_job.multiply(len(new_job_ids))
        )
        if new_resources_available is None:
            return False  # this shouldn't really happen

        try:
            # https://docs.microsoft.com/en-us/rest/api/storageservices/update-entity2
            await azure_table_api(
                "PUT",  # replace rather than merge
                self._storage_account,
                table_key_url(VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.name),
                json_content={
                    PUBLIC_ADDRESS: instance.public_address,
                    RESOURCES_AVAILABLE: json.dumps(new_resources_available.consumable),
                    NON_CONSUMABLE_RESOURCES: json.dumps(
                        new_resources_available.non_consumable
                    ),
                    RUNNING_JOBS: json.dumps(new_running_jobs),
                    LAST_UPDATE_TIME: now,
                },
                additional_headers={"If-Match": instance.etag},
            )
            return True
        except ResourceModifiedError:
            # this is how the API indicates that the etag does not match, i.e. the
            # optimistic concurrency check failed
            return False

    async def deallocate_job_from_instance(
        self, instance: AzureVMInstanceState, job_id: str
    ) -> bool:
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        if job_id not in instance.get_running_jobs():
            return False

        job = instance.get_running_jobs()[job_id]
        new_resources_available = instance.get_available_resources().add(
            ResourcesInternal(job[RESOURCES_ALLOCATED], {})
        )
        new_running_jobs = instance.get_running_jobs().copy()
        del new_running_jobs[job_id]
        now = datetime.datetime.utcnow().isoformat()

        try:
            # https://docs.microsoft.com/en-us/rest/api/storageservices/update-entity2
            await azure_table_api(
                "PUT",
                self._storage_account,
                table_key_url(VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.name),
                json_content={
                    PUBLIC_ADDRESS: instance.public_address,
                    RESOURCES_AVAILABLE: json.dumps(new_resources_available.consumable),
                    NON_CONSUMABLE_RESOURCES: json.dumps(
                        new_resources_available.non_consumable
                    ),
                    RUNNING_JOBS: json.dumps(new_running_jobs),
                    LAST_UPDATE_TIME: now,
                },
                additional_headers={"If-Match": instance.etag},
            )
            return True
        except ResourceModifiedError:
            # this is how the API indicates that the etag does not match, i.e. the
            # optimistic concurrency check failed
            return False

    async def set_prevent_further_allocation(self, name: str, value: bool) -> bool:
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        instance = await self.get_registered_instance(name)

        now = datetime.datetime.utcnow().isoformat()

        try:
            # https://docs.microsoft.com/en-us/rest/api/storageservices/update-entity2
            await azure_table_api(
                "PUT",  # replace rather than merge
                self._storage_account,
                table_key_url(VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.name),
                json_content={
                    PUBLIC_ADDRESS: instance.public_address,
                    RESOURCES_AVAILABLE: json.dumps(
                        instance.get_available_resources().consumable
                    ),
                    NON_CONSUMABLE_RESOURCES: json.dumps(
                        instance.get_available_resources().non_consumable
                    ),
                    RUNNING_JOBS: json.dumps(instance.running_jobs),
                    PREVENT_FURTHER_ALLOCATION: value,
                    LAST_UPDATE_TIME: now,
                },
                additional_headers={"If-Match": instance.etag},
            )
            return True
        except ResourceModifiedError:
            # this is how the API indicates that the etag does not match, i.e. the
            # optimistic concurrency check failed
            return False

    async def launch_instances(
        self,
        resources_required_per_task: ResourcesInternal,
        num_concurrent_tasks: int,
        alloc_cloud_instances: AllocVM,
        abort: Optional[asyncio.Event],
    ) -> Sequence[CloudInstance]:
        if not isinstance(alloc_cloud_instances, AllocAzureVM):
            # TODO do this in the type checker somehow
            raise ValueError(
                "Programming error: AzureInstanceRegistrar can only be used with "
                "AllocAzureVM"
            )

        region_name = alloc_cloud_instances._get_location()
        return await meadowrun.azure_integration.azure_vms.launch_vms(
            resources_required_per_task,
            num_concurrent_tasks,
            (await ensure_meadowrun_key_pair(region_name))[1],
            region_name,
        )

    async def authorize_current_ip(self, alloc_cloud_instances: AllocVM) -> None:
        # TODO currently Azure instances are open to everyone
        pass

    async def open_ports(
        self,
        ports: Optional[Sequence[str]],
        allocated_existing_instances: Iterable[_TInstanceState],
        allocated_new_instances: Iterable[CloudInstance],
    ) -> None:
        if ports:
            raise NotImplementedError(
                "Opening ports on Azure is not implemented, please comment on "
                "https://github.com/meadowdata/meadowrun/issues/126"
            )


@dataclasses.dataclass
class AllocAzureVM(AllocVM):
    """
    Specifies that the job should be run on a dynamically allocated Azure VM. Any
    existing Meadowrun-managed VMs will be reused if available. If none are available,
    Meadowrun will launch the cheapest VM type that meets the resource requirements for
    a job.

    `resources_required` must be provided with the AllocAzureVM Host.

    Attributes:
        location: Specifies the location for the Azure VM, e.g. "eastus". None will use
            the default location.
    """

    location: Optional[str] = None

    def get_cloud_provider(self) -> CloudProviderType:
        return "AzureVM"

    async def set_defaults(self) -> None:
        self.location = get_default_location()

    def _get_location(self) -> str:
        # Small wrapper around region_name that throws if it is None. Should only be
        # used internally.
        if self.location is None:
            raise ValueError(
                "Programming error: location is None but it should have been set by "
                "set_defaults earlier"
            )
        return self.location

    def get_runtime_resources(self) -> ResourcesInternal:
        return ResourcesInternal({}, {})

    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        if resources_required is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocAzureVM"
            )

        return await run_job_azure_vm_instance_registrar(
            job, resources_required, self, wait_for_result
        )

    def _create_grid_job_cloud_interface(self) -> GridJobCloudInterface:
        return AzureVMGridJobInterface(self)

    async def get_storage_bucket(self) -> AbstractStorageBucket:
        return await get_azure_blob_container(self._get_location())


async def run_job_azure_vm_instance_registrar(
    job: Job,
    resources_required: ResourcesInternal,
    alloc_azure_vm: AllocAzureVM,
    wait_for_result: WaitOption,
) -> JobCompletion[Any]:
    location = alloc_azure_vm._get_location()

    pkey, public_key = await ensure_meadowrun_key_pair(location)

    async with AzureInstanceRegistrar(location, "create") as instance_registrar:
        if job.ports:
            raise NotImplementedError(
                "Opening ports on Azure is not implemented, please comment on "
                "https://github.com/meadowdata/meadowrun/issues/126"
            )
        host, job_id = await allocate_single_job_to_instance(
            instance_registrar,
            resources_required,
            alloc_azure_vm,
            job.ports,
        )

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_id

    return await SshHost(host, "meadowrunuser", pkey, ("AzureVM", location)).run_job(
        resources_required, job, wait_for_result
    )


class AzureVMGridJobInterface(GridJobCloudInterface, Generic[_T, _U]):
    """
    This is a relatively thin wrapper around some functionality in grid_tasks_queue.
    This class should be in grid_tasks_queue, but it's here because of circular import
    issues.
    """

    def create_queue(self) -> int:
        raise NotImplementedError(
            "Retrying with more resources is not supported on Azure yet"
        )

    def __init__(self, alloc_cloud_instance: AllocAzureVM):
        self._cloud_provider = alloc_cloud_instance.get_cloud_provider()
        self._location = alloc_cloud_instance._get_location()

        self._ssh_private_key: Optional[
            asyncio.Task[Tuple[asyncssh.SSHKey, str]]
        ] = None

        self._request_result_queues: Optional[asyncio.Task[Tuple[Queue, Queue]]] = None

        self._tasks: Optional[Sequence[_T]] = None

        # this id is just used for creating the job's queues. It has no relationship to
        # any Job.job_ids
        self._job_id = str(uuid.uuid4())

    def create_instance_registrar(self) -> InstanceRegistrar:
        return AzureInstanceRegistrar(self._location, "create")

    async def setup_and_add_tasks(self, tasks: Sequence[_T]) -> None:
        print(f"The current run_map's id is {self._job_id}")
        self._ssh_private_key = asyncio.create_task(
            ensure_meadowrun_key_pair(self._location)
        )
        self._request_result_queues = asyncio.create_task(
            create_queues_for_job(self._job_id, self._location)
        )
        self._tasks = tasks
        await add_tasks((await self._request_result_queues)[0], tasks)

    async def ssh_host_from_address(self, address: str, instance_name: str) -> SshHost:
        if self._ssh_private_key is None:
            raise ValueError(
                "Must call setup_and_add_tasks before calling ssh_host_from_address"
            )

        return SshHost(
            address,
            "meadowrunuser",
            (await self._ssh_private_key)[0],
            (self._cloud_provider, self._location),
            instance_name,
        )

    async def shutdown_workers(self, num_workers: int, queue_index: int) -> None:
        if queue_index != 0:
            raise NotImplementedError(
                "Retrying with more resources is not implemented for Azure yet"
            )

        if self._request_result_queues is None:
            raise ValueError(
                "Must call setup_and_add_tasks before calling shutdown_workers"
            )

        await add_worker_shutdown_message(
            (await self._request_result_queues)[0], num_workers
        )

    async def get_worker_function(
        self, queue_index: int
    ) -> Callable[
        [str, str, TaskWorkerServer, WorkerMonitor],
        Coroutine[Any, Any, None],
    ]:
        if self._request_result_queues is None:
            raise ValueError(
                "Must call setup_and_add_tasks before calling get_worker_function"
            )
        if queue_index != 0:
            raise NotImplementedError(
                "Retrying with more resources is not implemented for Azure yet"
            )

        request_queue, result_queue = await self._request_result_queues

        return functools.partial(
            worker_function,
            request_queue,
            result_queue,
        )

    async def receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
        if self._request_result_queues is None:
            raise ValueError(
                "Must call setup_and_add_tasks before calling receive_task_results"
            )

        return get_results_unordered(
            (await self._request_result_queues)[1],
            self._location,
            stop_receiving,
            workers_done,
        )

    async def retry_task(
        self, task_id: int, attempts_so_far: int, queue_index: int
    ) -> None:
        if self._tasks is None or self._request_result_queues is None:
            raise ValueError("Must call setup_and_add_tasks before calling retry_task")

        await retry_task(
            (await self._request_result_queues)[0],
            task_id,
            attempts_so_far + 1,
            self._tasks[task_id],
        )
