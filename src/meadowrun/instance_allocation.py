from __future__ import annotations

import abc
import asyncio
import dataclasses
import itertools
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from meadowrun.instance_selection import (
    CloudInstance,
    ResourcesInternal,
    remaining_resources_sort_key,
)
from meadowrun.shared import assert_is_not_none

if TYPE_CHECKING:
    from types import TracebackType
    from meadowrun.run_job_core import AllocVM


@dataclasses.dataclass
class _InstanceState:
    """Represents an existing instance"""

    public_address: str

    name: str

    available_resources: Optional[ResourcesInternal]

    running_jobs: Optional[Dict[str, Dict[str, Any]]]

    prevent_further_allocation: bool

    def get_available_resources(self) -> ResourcesInternal:
        """
        The current available resources on the instance. This will change as jobs get
        allocated/deallocated to the instance.

        We use this function instead of directly accessing the property as a bit of a
        hack to get some duck-typing so that we only need to populate
        available_resources when it will actually be used. Pulling these properties when
        they're not needed is potentially expensive, as that data comes from DynamoDB or
        an Azure Table. Functions that return _InstanceState should document whether
        this property will be available.
        """
        if self.available_resources is None:
            raise ValueError(
                "Programming error--_InstanceState.available_resources was requested "
                "but is None"
            )
        return self.available_resources

    def get_running_jobs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns job_id -> metadata, where metadata has keys _LOGICAL_CPU_ALLOCATED,
        _MEMORY_GB_ALLOCATED, _ALLOCATED_TIME

        Also see comment on get_available_resources
        """
        if self.running_jobs is None:
            raise ValueError(
                "Programming error--_InstanceState.get_running_jobs was requested but "
                "is None"
            )
        return self.running_jobs


_TInstanceState = TypeVar("_TInstanceState", bound=_InstanceState)


class InstanceRegistrar(abc.ABC, Generic[_TInstanceState]):
    """
    An implementation of InstanceRegistrar provides a way to register "instances" (e.g.
    EC2 instances or Azure VMs) as they're created, and then allocate/deallocate jobs to
    these instances. Another way of thinking about it is a serverless cluster manager.
    """

    @abc.abstractmethod
    async def __aenter__(self) -> InstanceRegistrar:
        """Initializes the InstanceRegistrar"""
        pass

    @abc.abstractmethod
    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    @abc.abstractmethod
    def get_region_name(self) -> str:
        pass

    @abc.abstractmethod
    async def register_instance(
        self,
        public_address: str,
        name: str,
        resources_available: ResourcesInternal,
        running_jobs: List[Tuple[str, ResourcesInternal]],
    ) -> None:
        """Registers a (presumably newly created) instance"""
        pass

    @abc.abstractmethod
    async def get_registered_instances(self) -> List[_TInstanceState]:
        """
        Gets all registered instances. Must have available_resources populated.
        running_jobs is optional depending on whether the corresponding implementation
        of allocate_jobs_to_instance will need it.
        """
        pass

    @abc.abstractmethod
    async def get_registered_instance(self, public_address: str) -> _TInstanceState:
        """
        Gets the InstanceRegistrar's representation of the specified instance.

        Must populate running_jobs, available_resources is optional depending on whether
        the corresponding implementation of deallocate_job_from_instance will need it
        """
        pass

    @abc.abstractmethod
    async def allocate_jobs_to_instance(
        self,
        instance: _TInstanceState,
        resources_allocated_per_job: ResourcesInternal,
        new_job_ids: List[str],
    ) -> bool:
        """
        Adds the specified job_ids to the specified instance's running_jobs. This will
        allocate resources_allocated_per_job for each job from the instance's
        available_resources.

        This function should return True if successful, False if we fail an optimistic
        concurrency check OR if the job_id is already in use by the specified instance,
        and raise an exception for any other issues.

        TODO consider more robust handling of job_id collisions--probably indicates a
        consistency issue
        """
        pass

    def allocate_jobs_to_instance_max_chunk(self) -> int:
        """
        allocate_jobs_to_instance in some cases has relatively low limits for how many
        jobs can be allocated to an instance a time. This tells the caller that
        new_job_ids in allocate_jobs_to_instance shouldn't be larger than the limit
        returned by this function.
        """
        return -1

    @abc.abstractmethod
    async def deallocate_job_from_instance(
        self, instance: _TInstanceState, job_id: str
    ) -> bool:
        """
        Removes the specified job from the specified instance and restores the resources
        that were allocated to that job.

        Returns True if the job was removed, returns False if the job does not exist
        (e.g. it was already removed or never existed in the first place).
        """
        pass

    @abc.abstractmethod
    async def launch_instances(
        self,
        resources_required_per_task: ResourcesInternal,
        num_concurrent_tasks: int,
        alloc_cloud_instances: AllocVM,
        abort: Optional[asyncio.Event],
    ) -> Sequence[CloudInstance]:
        """
        This isn't per se part of the "instance registration" process, but it's helpful
        to have on the same object.
        """
        pass

    @abc.abstractmethod
    async def set_prevent_further_allocation(
        self, public_address: str, value: bool
    ) -> bool:
        pass

    @abc.abstractmethod
    async def authorize_current_ip(self) -> None:
        pass

    @abc.abstractmethod
    async def open_ports(
        self,
        ports: Optional[Sequence[str]],
        allocated_existing_instances: Iterable[_TInstanceState],
        allocated_new_instances: Iterable[CloudInstance],
    ) -> None:
        pass


@dataclasses.dataclass
class _InstanceWithProposedJobs(Generic[_TInstanceState]):
    """Just used in _choose_existing_instances"""

    orig_instance: _TInstanceState
    proposed_jobs: List[str]
    proposed_available_resources: ResourcesInternal


async def _choose_existing_instances(
    instance_registrar: InstanceRegistrar[_TInstanceState],
    resources_required_per_job: ResourcesInternal,
    num_jobs: int,
) -> Tuple[Dict[str, List[str]], Dict[str, _TInstanceState]]:
    """
    Chooses existing registered instances to run the specified job(s). The general
    strategy is to pack instances as tightly as possible to allow larger jobs to come
    along later.

    Returns two dictionaries: {public_address: [job_ids]}, {public_address:
    instance_state}. The keys of the two dictionaries will be the same set of public
    addresses.
    """

    # these represent jobs that have been allocated in the InstanceRegistrar
    num_jobs_allocated = 0
    # {public_address: [job_ids]}
    allocated_jobs: Dict[str, List[str]] = {}
    # {public_address: instance_state}
    allocated_instances: Dict[str, _TInstanceState] = {}

    # try to allocate a maximum of 3 times. We will retry if there's an optimistic
    # concurrency issue (i.e. someone else allocates to an instance at the same time
    # as us). Failing just means that we're unable to allocate the jobs.
    i = 0
    all_success = False
    while i < 3 and not all_success:
        instances = [
            _InstanceWithProposedJobs(instance, [], instance.get_available_resources())
            for instance in await instance_registrar.get_registered_instances()
            if not instance.prevent_further_allocation
        ]

        sort_keys = [
            remaining_resources_sort_key(
                instance.proposed_available_resources, resources_required_per_job
            )
            for instance in instances
        ]

        # these represent proposed allocations--they are not actually allocated
        # until we update InstanceRegistrar
        num_jobs_proposed = 0

        if sort_keys:
            while num_jobs_allocated + num_jobs_proposed < num_jobs:
                # choose an instance
                chosen_index = min(range(len(sort_keys)), key=lambda i: sort_keys[i])
                # if the indicator is 1, that means none of the instances can run our
                # job
                if sort_keys[chosen_index][0] == 1:
                    break

                # we successfully chose an instance!
                chosen_instance = instances[chosen_index]
                chosen_instance.proposed_jobs.append(str(uuid.uuid4()))
                num_jobs_proposed += 1

                # decrease the agent's available_resources
                chosen_instance.proposed_available_resources = assert_is_not_none(
                    (
                        chosen_instance.proposed_available_resources.subtract(
                            resources_required_per_job
                        )
                    )
                )
                # decrease the sort key for the chosen agent
                sort_keys[chosen_index] = remaining_resources_sort_key(
                    chosen_instance.proposed_available_resources,
                    resources_required_per_job,
                )

        # now that we've chosen which instance(s) will run our job(s), try to actually
        # get the allocation in the InstanceRegistrar. This could fail if another
        # process is trying to do an allocation at the same time as us so the instances
        # we've chosen actually don't have enough resources (even though they did at the
        # top of this function).

        allocate_jobs_max_chunk = (
            instance_registrar.allocate_jobs_to_instance_max_chunk()
        )
        all_success = True
        for top_instance in instances:
            if top_instance.proposed_jobs:
                if allocate_jobs_max_chunk != -1:
                    # split proposed_jobs into chunks of size allocate_jobs_max_chunk
                    it = iter(top_instance.proposed_jobs)
                    chunked_instances: Iterable[_InstanceWithProposedJobs] = (
                        dataclasses.replace(
                            top_instance, proposed_jobs=proposed_jobs_chunk
                        )
                        for proposed_jobs_chunk in iter(
                            lambda: list(itertools.islice(it, allocate_jobs_max_chunk)),
                            [],
                        )
                    )
                else:
                    chunked_instances = (top_instance,)

                for instance in chunked_instances:
                    success = await instance_registrar.allocate_jobs_to_instance(
                        # it's very important to pass the orig_instance
                        # here--allocate_jobs_to_instance can rely on
                        # available_resources being correct and we want the original
                        # one, not the proposed_available_resources that we've modified.
                        instance.orig_instance,
                        resources_required_per_job,
                        instance.proposed_jobs,
                    )
                    if success:
                        allocated_jobs.setdefault(
                            instance.orig_instance.public_address, []
                        ).extend(instance.proposed_jobs)
                        allocated_instances[
                            instance.orig_instance.public_address
                        ] = instance.orig_instance
                        num_jobs_allocated += len(instance.proposed_jobs)
                    else:
                        all_success = False

        i += 1

    if num_jobs == 1:
        if num_jobs_allocated == 0:
            print(
                "Job was not allocated to any existing instances, will launch a new"
                " instance"
            )
        else:
            print(
                "Job was allocated to an existing instances: "
                + " ".join(allocated_jobs.keys())
            )
    else:
        print(
            f"{num_jobs_allocated}/{num_jobs} workers allocated to existing instances: "
            + " ".join(allocated_jobs.keys())
        )

    return allocated_jobs, allocated_instances


async def _launch_new_instances(
    instance_registrar: InstanceRegistrar,
    instance_type_resources_required_per_task: ResourcesInternal,
    num_concurrent_tasks: int,
    alloc_cloud_instance: AllocVM,
    original_num_concurrent_tasks: int,
    abort: Optional[asyncio.Event],
) -> Tuple[Dict[str, List[str]], Dict[str, CloudInstance]]:
    """
    Chooses the cheapest instances to launch that can run the specified jobs, launches
    them, adds them to the InstanceRegistrar, and allocates the specified jobs to them.

    Returns two dictionaries, {public_address: [job_ids]}, {public_address:
    cloud_instance}. The keys of the two dictionaries will be the same.

    The CloudInstance objects returned will have a resources field that includes both
    the instance type resources (like CPU and memory) that were used to select an
    instance type as well as the "runtime resources" provided by alloc_cloud_instance
    (like the AMI id and subnet ID) which describe how the instance was actaully
    launched.

    original_num_jobs is only needed to produce more coherent logging.
    """

    instances = await instance_registrar.launch_instances(
        instance_type_resources_required_per_task,
        num_concurrent_tasks,
        alloc_cloud_instance,
        abort,
    )

    description_strings = []
    total_num_allocated_jobs = 0
    total_cost_per_hour: float = 0
    allocated_jobs = {}
    allocated_instances = {}

    for instance in instances:
        # just to make the code more readable
        instance_info = instance.instance_type.instance_type
        all_resources = instance_info.resources.combine(
            alloc_cloud_instance.get_runtime_resources()
        )

        # the number of jobs to allocate to this instance
        num_allocated_jobs = min(
            num_concurrent_tasks - total_num_allocated_jobs,
            instance.instance_type.workers_per_instance_full,
        )
        total_num_allocated_jobs += num_allocated_jobs
        job_ids = [str(uuid.uuid4()) for _ in range(num_allocated_jobs)]

        await instance_registrar.register_instance(
            instance.public_dns_name,
            instance.name,
            assert_is_not_none(
                all_resources.subtract(
                    instance_type_resources_required_per_task.multiply(
                        num_allocated_jobs
                    )
                )
            ),
            [(job_id, instance_type_resources_required_per_task) for job_id in job_ids],
        )

        allocated_jobs[instance.public_dns_name] = job_ids
        allocated_instances[instance.public_dns_name] = instance
        description_strings.append(
            f"{instance.public_dns_name}: {instance_info.name} "
            f"({instance_info.resources.format_cpu_memory_gpu()}), "
            f"{instance_info.on_demand_or_spot} (${instance_info.price}/hr, "
            f"{instance_info.resources.format_eviction_rate()}), will run "
            f"{num_allocated_jobs} workers"
        )
        total_cost_per_hour += instance_info.price

    if original_num_concurrent_tasks == 1:
        # there should only ever be one description_strings
        print(f"Launched a new instance for the job: {' '.join(description_strings)}")
    else:
        print(
            f"Launched {len(description_strings)} new instance(s) (total "
            f"${total_cost_per_hour}/hr) for the remaining "
            f"{num_concurrent_tasks} workers:\n"
            + "\n".join(["\t" + s for s in description_strings])
        )

    return allocated_jobs, allocated_instances


async def allocate_jobs_to_instances(
    instance_registrar: InstanceRegistrar,
    resources_required_per_task: ResourcesInternal,
    num_concurrent_tasks: int,
    alloc_cloud_instance: AllocVM,
    ports: Optional[Sequence[str]],
    abort: Optional[asyncio.Event],
) -> AsyncIterable[Dict[str, List[str]]]:
    """
    This function first tries to re-use existing instances, and if necessary launches
    the cheapest possible new instances that have the requested resources.

    Returns {public_address: [job_ids]}
    """

    authorize_current_ip_task = asyncio.create_task(
        instance_registrar.authorize_current_ip()
    )

    (
        existing_instances_allocated_jobs,
        allocated_existing_instances,
    ) = await _choose_existing_instances(
        instance_registrar,
        resources_required_per_task.combine(
            alloc_cloud_instance.get_runtime_resources()
        ),
        num_concurrent_tasks,
    )

    if len(existing_instances_allocated_jobs) > 0:
        await instance_registrar.open_ports(
            ports, allocated_existing_instances.values(), ()
        )
        await authorize_current_ip_task  # this should almost always be complete by now
        yield existing_instances_allocated_jobs

    num_concurrent_tasks_remaining = num_concurrent_tasks - sum(
        len(jobs) for jobs in existing_instances_allocated_jobs.values()
    )

    if num_concurrent_tasks_remaining > 0:
        (
            new_instances_allocated_jobs,
            allocated_new_instances,
        ) = await _launch_new_instances(
            instance_registrar,
            resources_required_per_task,
            num_concurrent_tasks_remaining,
            alloc_cloud_instance,
            num_concurrent_tasks,
            abort,
        )
        # TODO if this fails and we have existing instances, we should just carry on
        # with those

        await instance_registrar.open_ports(ports, (), allocated_new_instances.values())
        if len(existing_instances_allocated_jobs) == 0:
            # we need to await this if and only if we didn't await it before
            await authorize_current_ip_task

        yield new_instances_allocated_jobs


async def allocate_single_job_to_instance(
    instance_registrar: InstanceRegistrar,
    resources_required: ResourcesInternal,
    alloc_cloud_instance: AllocVM,
    ports: Optional[Sequence[str]],
) -> Tuple[str, str]:
    """
    A thin wrapper around allocate_jobs_to_instances with a simpler return type for when
    you only need one job.
    """

    result = None

    async for allocated_hosts in allocate_jobs_to_instances(
        instance_registrar, resources_required, 1, alloc_cloud_instance, ports, None
    ):
        if len(allocated_hosts) == 0:
            pass
        elif len(allocated_hosts) > 1:
            raise ValueError(f"Requested 1 host, but got back {len(allocated_hosts)}")
        else:
            if result is None:
                host, job_ids = list(allocated_hosts.items())[0]
                if len(job_ids) != 1:
                    raise ValueError(
                        f"Requested 1 job allocation but got back {len(job_ids)}"
                    )
                result = host, job_ids[0]
            else:
                raise ValueError("Requested 1 host, but got back more than one")

    if result is None:
        raise ValueError("Requested 1 host, but did not get any back")

    return result
