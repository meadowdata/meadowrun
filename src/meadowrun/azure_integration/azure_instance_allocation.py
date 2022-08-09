from __future__ import annotations

import dataclasses
import datetime
import json
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Sequence, Tuple, Type

if TYPE_CHECKING:
    from typing_extensions import Literal
    from types import TracebackType

import meadowrun.azure_integration.azure_vms
from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_table,
    get_default_location,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    ALLOCATED_TIME,
    LAST_UPDATE_TIME,
    NON_CONSUMABLE_RESOURCES,
    PREVENT_FURTHER_ALLOCATION,
    RESOURCES_ALLOCATED,
    RESOURCES_AVAILABLE,
    RUNNING_JOBS,
    SINGLE_PARTITION_KEY,
    VM_NAME,
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
    allocate_jobs_to_instances,
)
from meadowrun.instance_selection import CloudInstance, ResourcesInternal

if TYPE_CHECKING:
    from meadowrun.meadowrun_pb2 import Job

from meadowrun.run_job_core import JobCompletion, SshHost


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
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
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
                    "RowKey": public_address,
                    VM_NAME: name,
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
                item["RowKey"],
                item[VM_NAME],
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
                            VM_NAME,
                            PREVENT_FURTHER_ALLOCATION,
                        ]
                    ),
                },
            )
            for item in page["value"]
        ]

    async def get_registered_instance(
        self, public_address: str
    ) -> AzureVMInstanceState:
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
                table_key_url(
                    VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, public_address
                ),
                query_parameters={
                    "$select": ",".join(
                        [
                            "RowKey",
                            RESOURCES_AVAILABLE,
                            NON_CONSUMABLE_RESOURCES,
                            RUNNING_JOBS,
                            VM_NAME,
                            PREVENT_FURTHER_ALLOCATION,
                        ]
                    )
                },
            )
        except ResourceNotFoundError:
            raise ValueError(f"VM {public_address} was not found")

        return AzureVMInstanceState(
            item["RowKey"],
            item[VM_NAME],
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
                table_key_url(
                    VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.public_address
                ),
                json_content={
                    VM_NAME: instance.name,
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
                table_key_url(
                    VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.public_address
                ),
                json_content={
                    VM_NAME: instance.name,
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

    async def set_prevent_further_allocation(
        self, public_address: str, value: bool
    ) -> bool:
        if self._storage_account is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        instance = await self.get_registered_instance(public_address)

        now = datetime.datetime.utcnow().isoformat()

        try:
            # https://docs.microsoft.com/en-us/rest/api/storageservices/update-entity2
            await azure_table_api(
                "PUT",  # replace rather than merge
                self._storage_account,
                table_key_url(
                    VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, instance.public_address
                ),
                json_content={
                    VM_NAME: instance.name,
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
        region_name: str,
    ) -> Sequence[CloudInstance]:
        return await meadowrun.azure_integration.azure_vms.launch_vms(
            resources_required_per_task,
            num_concurrent_tasks,
            (await ensure_meadowrun_key_pair(region_name))[1],
            region_name,
        )

    async def authorize_current_ip(self) -> None:
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
                "Opening ports on Azure is not implemented, please create an issue at "
                "https://github.com/meadowdata/meadowrun/issues"
            )


async def run_job_azure_vm_instance_registrar(
    job: Job,
    resources_required: ResourcesInternal,
    location: Optional[str],
    wait_for_result: bool,
) -> JobCompletion[Any]:
    if not location:
        location = get_default_location()
    pkey, public_key = await ensure_meadowrun_key_pair(location)

    async with AzureInstanceRegistrar(location, "create") as instance_registrar:
        if job.ports:
            raise NotImplementedError(
                "Support for opening ports on Azure is not yet implemented, please "
                "create an issue at https://github.com/meadowdata/meadowrun/issues"
            )
        hosts = await allocate_jobs_to_instances(
            instance_registrar,
            resources_required,
            1,
            location,
            job.ports,
        )

    if len(hosts) != 1:
        raise ValueError(f"Asked for one host, but got back {len(hosts)}")
    host, job_ids = list(hosts.items())[0]
    if len(job_ids) != 1:
        raise ValueError(f"Asked for one job allocation but got {len(job_ids)}")

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_ids[0]

    return await SshHost(host, "meadowrunuser", pkey, ("AzureVM", location)).run_job(
        resources_required, job, wait_for_result
    )
