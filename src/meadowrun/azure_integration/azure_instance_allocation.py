from __future__ import annotations

import dataclasses
import datetime
import json
from types import TracebackType
from typing import Tuple, List, Optional, Sequence, Literal, Type, Any, Dict

import azure.core.exceptions
from azure.core import MatchConditions
from azure.data.tables import UpdateMode
from azure.data.tables.aio import TableClient

import meadowrun.azure_integration.azure_vms
from meadowrun.azure_integration.azure_core import (
    get_default_location,
    ensure_table,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    ALLOCATED_TIME,
    LAST_UPDATE_TIME,
    LOGICAL_CPU_ALLOCATED,
    LOGICAL_CPU_AVAILABLE,
    MEMORY_GB_ALLOCATED,
    MEMORY_GB_AVAILABLE,
    RUNNING_JOBS,
    SINGLE_PARTITION_KEY,
    VM_NAME,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import VM_ALLOC_TABLE_NAME
from meadowrun.instance_allocation import (
    _InstanceState,
    InstanceRegistrar,
    allocate_jobs_to_instances,
)
from meadowrun.instance_selection import Resources, CloudInstance
from meadowrun.meadowrun_pb2 import Job
from meadowrun.run_job_core import AllocCloudInstancesInternal, JobCompletion, SshHost


@dataclasses.dataclass
class AzureVMInstanceState(_InstanceState):
    name: str
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

        self._table_client: Optional[TableClient] = None

    async def __aenter__(self) -> AzureInstanceRegistrar:
        self._table_client = await ensure_table(
            VM_ALLOC_TABLE_NAME, self._table_location, self._on_table_missing
        )
        return self

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self._table_client is not None:
            await self._table_client.__aexit__(exc_typ, exc_val, exc_tb)

    def get_region_name(self) -> str:
        return self._table_location

    async def register_instance(
        self,
        public_address: str,
        name: str,
        resources_available: Resources,
        running_jobs: List[Tuple[str, Resources]],
    ) -> None:
        if self._table_client is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        now = datetime.datetime.utcnow().isoformat()
        try:
            # See EC2InstanceRegistrar.register_instance for more details on the schema
            await self._table_client.create_entity(
                {
                    "PartitionKey": SINGLE_PARTITION_KEY,
                    "RowKey": public_address,
                    VM_NAME: name,
                    LOGICAL_CPU_AVAILABLE: resources_available.logical_cpu,
                    MEMORY_GB_AVAILABLE: resources_available.memory_gb,
                    RUNNING_JOBS: json.dumps(
                        {
                            job_id: {
                                LOGICAL_CPU_ALLOCATED: allocated_resources.logical_cpu,
                                MEMORY_GB_ALLOCATED: allocated_resources.memory_gb,
                                ALLOCATED_TIME: now,
                            }
                            for job_id, allocated_resources in running_jobs
                        }
                    ),
                    LAST_UPDATE_TIME: now,
                }
            )
        except azure.core.exceptions.ResourceExistsError:
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
        if self._table_client is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        return [
            AzureVMInstanceState(
                item["RowKey"],
                Resources(
                    float(item[MEMORY_GB_AVAILABLE]),
                    int(item[LOGICAL_CPU_AVAILABLE]),
                    {},
                ),
                json.loads(item[RUNNING_JOBS]),
                item[VM_NAME],
                item.metadata["etag"],
            )
            async for item in self._table_client.list_entities(
                select=[
                    "RowKey",
                    LOGICAL_CPU_AVAILABLE,
                    MEMORY_GB_AVAILABLE,
                    RUNNING_JOBS,
                    VM_NAME,
                ]
            )
        ]

    async def get_registered_instance(
        self, public_address: str
    ) -> AzureVMInstanceState:
        """
        For the AzureInstanceRegistrar, we always populate all the fields on
        AzureVMInstanceState because we will need them in allocate_jobs_to_instance and
        deallocate_job_from_instance
        """
        if self._table_client is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        try:
            item = await self._table_client.get_entity(
                SINGLE_PARTITION_KEY,
                public_address,
                select=[
                    "RowKey",
                    LOGICAL_CPU_AVAILABLE,
                    MEMORY_GB_AVAILABLE,
                    RUNNING_JOBS,
                    VM_NAME,
                ],
            )

        except azure.core.exceptions.ResourceNotFoundError:
            raise ValueError(f"VM {public_address} was not found")

        return AzureVMInstanceState(
            item["RowKey"],
            Resources(
                float(item[MEMORY_GB_AVAILABLE]), int(item[LOGICAL_CPU_AVAILABLE]), {}
            ),
            json.loads(item[RUNNING_JOBS]),
            item[VM_NAME],
            item.metadata["etag"],
        )

    async def allocate_jobs_to_instance(
        self,
        instance: AzureVMInstanceState,
        resources_allocated_per_job: Resources,
        new_job_ids: List[str],
    ) -> bool:
        if self._table_client is None:
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
                LOGICAL_CPU_ALLOCATED: resources_allocated_per_job.logical_cpu,
                MEMORY_GB_ALLOCATED: resources_allocated_per_job.memory_gb,
                ALLOCATED_TIME: now,
            }

        new_logical_cpu_available = (
            instance.get_available_resources().logical_cpu
            - resources_allocated_per_job.logical_cpu * len(new_job_ids)
        )
        new_memory_gb_available = (
            instance.get_available_resources().memory_gb
            - resources_allocated_per_job.memory_gb * len(new_job_ids)
        )

        try:
            await self._table_client.update_entity(
                {
                    "PartitionKey": SINGLE_PARTITION_KEY,
                    "RowKey": instance.public_address,
                    VM_NAME: instance.name,
                    LOGICAL_CPU_AVAILABLE: new_logical_cpu_available,
                    MEMORY_GB_AVAILABLE: new_memory_gb_available,
                    RUNNING_JOBS: json.dumps(new_running_jobs),
                    LAST_UPDATE_TIME: now,
                },
                UpdateMode.REPLACE,
                etag=instance.etag,
                match_condition=MatchConditions.IfNotModified,
            )
            return True
        except azure.core.exceptions.ResourceModifiedError:
            # this is how the API indicates that the etag does not match, i.e. the
            # optimistic concurrency check failed
            return False

    async def deallocate_job_from_instance(
        self, instance: AzureVMInstanceState, job_id: str
    ) -> bool:
        if self._table_client is None:
            raise ValueError(
                "Tried to use AzureInstanceRegistrar without calling __aenter__"
            )

        if job_id not in instance.get_running_jobs():
            return False

        job = instance.get_running_jobs()[job_id]
        new_logical_cpu_available = (
            instance.get_available_resources().logical_cpu + job[LOGICAL_CPU_ALLOCATED]
        )
        new_memory_gb_available = (
            instance.get_available_resources().memory_gb + job[MEMORY_GB_ALLOCATED]
        )
        new_running_jobs = instance.get_running_jobs().copy()
        del new_running_jobs[job_id]
        now = datetime.datetime.utcnow().isoformat()

        try:
            await self._table_client.update_entity(
                {
                    "PartitionKey": SINGLE_PARTITION_KEY,
                    "RowKey": instance.public_address,
                    VM_NAME: instance.name,
                    LOGICAL_CPU_AVAILABLE: new_logical_cpu_available,
                    MEMORY_GB_AVAILABLE: new_memory_gb_available,
                    RUNNING_JOBS: json.dumps(new_running_jobs),
                    LAST_UPDATE_TIME: now,
                },
                UpdateMode.REPLACE,
                etag=instance.etag,
                match_condition=MatchConditions.IfNotModified,
            )
            return True
        except azure.core.exceptions.ResourceModifiedError:
            # this is how the API indicates that the etag does not match, i.e. the
            # optimistic concurrency check failed
            return False

    async def launch_instances(
        self, instances_spec: AllocCloudInstancesInternal
    ) -> Sequence[CloudInstance]:
        return await meadowrun.azure_integration.azure_vms.launch_vms(
            instances_spec.logical_cpu_required_per_task,
            instances_spec.memory_gb_required_per_task,
            instances_spec.num_concurrent_tasks,
            instances_spec.interruption_probability_threshold,
            (await ensure_meadowrun_key_pair(instances_spec.region_name))[1],
            instances_spec.region_name,
        )


async def run_job_azure_vm_instance_registrar(
    job: Job,
    logical_cpu_required: int,
    memory_gb_required: float,
    eviction_rate: float,
    location: Optional[str],
) -> JobCompletion[Any]:
    if not location:
        location = get_default_location()
    pkey, public_key = await ensure_meadowrun_key_pair(location)

    async with AzureInstanceRegistrar(location, "create") as instance_registrar:
        hosts = await allocate_jobs_to_instances(
            instance_registrar,
            AllocCloudInstancesInternal(
                logical_cpu_required, memory_gb_required, eviction_rate, 1, location
            ),
        )

    fabric_kwargs: Dict[str, Any] = {
        "user": "meadowrunuser",
        "connect_kwargs": {"pkey": pkey},
    }

    if len(hosts) != 1:
        raise ValueError(f"Asked for one host, but got back {len(hosts)}")
    host, job_ids = list(hosts.items())[0]
    if len(job_ids) != 1:
        raise ValueError(f"Asked for one job allocation but got {len(job_ids)}")

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_ids[0]

    return await SshHost(host, fabric_kwargs, ("AzureVM", location)).run_job(job)
