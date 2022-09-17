"""Tests allocate_jobs_to_instances using fake data"""
from __future__ import annotations

import asyncio
import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import pytest

from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance

from meadowrun.instance_allocation import (
    InstanceRegistrar,
    _InstanceState,
    _TInstanceState,
    allocate_jobs_to_instances,
)
from meadowrun.instance_selection import (
    CloudInstance,
    CloudInstanceType,
    ResourcesInternal,
    choose_instance_types_for_job,
)

if TYPE_CHECKING:
    from types import TracebackType
    from meadowrun.run_job_core import AllocVM


class MockInstanceRegistrar(InstanceRegistrar[_InstanceState]):
    """
    A fake instance registrar for testing instance_allocation.py. See test_gpu which has
    some extra comments on it to see how to use this class.
    """

    # core implementation

    def __init__(self, instance_types: List[CloudInstanceType]):
        self._instance_types: List[CloudInstanceType] = instance_types
        self._registered_instances: Dict[str, _InstanceState] = {}
        # public_address -> instance type
        self._launched_instance_types: Dict[str, str] = {}
        self.i = 0

    async def __aenter__(self) -> InstanceRegistrar:
        pass

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def get_region_name(self) -> str:
        return "test_region_name"

    async def register_instance(
        self,
        public_address: str,
        name: str,
        resources_available: ResourcesInternal,
        running_jobs: List[Tuple[str, ResourcesInternal]],
    ) -> None:
        now = datetime.datetime.utcnow().isoformat()
        self._registered_instances[name] = _InstanceState(
            public_address,
            name,
            resources_available,
            {
                job_id: {
                    "RESOURCES_ALLOCATED": resources.consumable,
                    "ALLOCATED_TIME": now,
                }
                for job_id, resources in running_jobs
            },
            False,
        )

    async def get_registered_instances(self) -> List[_InstanceState]:
        return list(self._registered_instances.values())

    async def get_registered_instance(self, name: str) -> _InstanceState:
        return self._registered_instances[name]

    async def allocate_jobs_to_instance(
        self,
        instance: _InstanceState,
        resources_allocated_per_job: ResourcesInternal,
        new_job_ids: List[str],
    ) -> bool:
        now = datetime.datetime.utcnow().isoformat()
        new_available_resources = instance.get_available_resources().subtract(
            resources_allocated_per_job.multiply(len(new_job_ids))
        )
        if new_available_resources is None:
            return False
        instance.available_resources = new_available_resources
        for job_id in new_job_ids:
            if job_id in instance.get_running_jobs():
                return False
            instance.get_running_jobs()[job_id] = {
                "RESOURCES_ALLOCATED": resources_allocated_per_job.consumable,
                "ALLOCATED_TIME": now,
            }
        return True

    async def deallocate_job_from_instance(
        self, instance: _InstanceState, job_id: str
    ) -> bool:
        if job_id not in instance.get_running_jobs():
            return False

        job = instance.get_running_jobs().pop(job_id)
        instance.available_resources = instance.get_available_resources().add(
            ResourcesInternal(job["RESOURCES_ALLOCATED"], {})
        )
        return True

    async def set_prevent_further_allocation(self, name: str, value: bool) -> bool:
        return True

    async def launch_instances(
        self,
        resources_required_per_task: ResourcesInternal,
        num_concurrent_tasks: int,
        alloc_cloud_instances: AllocVM,
        abort: Optional[asyncio.Event],
    ) -> Sequence[CloudInstance]:
        result = []
        for chosen_instance_type in choose_instance_types_for_job(
            resources_required_per_task,
            num_concurrent_tasks,
            self._instance_types,
        ):
            for _ in range(chosen_instance_type.num_instances):
                name = f"i{self.i}"
                result.append(CloudInstance(name, name, chosen_instance_type))
                self._launched_instance_types[
                    name
                ] = chosen_instance_type.instance_type.name
                self.i += 1

        return result

    async def authorize_current_ip(self, alloc_cloud_instances: AllocVM) -> None:
        pass

    async def open_ports(
        self,
        ports: Optional[Sequence[str]],
        allocated_existing_instances: Iterable[_TInstanceState],
        allocated_new_instances: Iterable[CloudInstance],
    ) -> None:
        pass

    # helpers for tests

    @classmethod
    def from_tuples(
        cls,
        specs: List[
            Tuple[float, Tuple, Optional[Tuple[Dict[str, float], Dict[str, float]]]]
        ],
    ) -> MockInstanceRegistrar:
        instance_types = []
        for i, (price, positional_args, keyword_args_tuple) in enumerate(specs):
            if keyword_args_tuple is not None:
                kwargs: Dict[str, Any] = {
                    "other_consumables": keyword_args_tuple[0],
                    "other_non_consumables": keyword_args_tuple[1],
                }
            else:
                kwargs = {}

            instance_types.append(
                CloudInstanceType(
                    f"t{i}",
                    "spot",
                    price,
                    ResourcesInternal.from_cpu_and_memory(*positional_args, **kwargs),
                )
            )

        return cls(instance_types)

    def where_is_job(self, job_id: str) -> Optional[Tuple[str, str]]:
        """
        Returns the instance number and instance type of the instance that the specified
        job is running on. Returns None if the job_id isn't currently running
        """
        for instance in self._registered_instances.values():
            if job_id in instance.get_running_jobs():
                return (
                    instance.name,
                    self._launched_instance_types[instance.name],
                )

        return None

    async def allocate_jobs(
        self, resources: ResourcesInternal, num_concurrent_jobs: int
    ) -> Tuple[List[Tuple[str, str]], List[str]]:
        instances_job_ids = {}
        async for allocated_jobs in allocate_jobs_to_instances(
            self,
            resources,
            num_concurrent_jobs,
            AllocEC2Instance("test_region"),
            None,
            None,
        ):
            instances_job_ids.update(allocated_jobs)

        all_job_ids = []
        instances = []
        for (public_address, instance_name), instance_job_ids in sorted(
            instances_job_ids.items()
        ):
            instance_type = self._launched_instance_types[public_address]
            for job_id in instance_job_ids:
                all_job_ids.append(job_id)
                instances.append((public_address, instance_type))
        return instances, all_job_ids

    async def deallocate_job(self, job_id: str) -> None:
        instance = self.where_is_job(job_id)
        if instance is not None:
            await self.deallocate_job_from_instance(
                await self.get_registered_instance(instance[0]), job_id
            )

    async def deallocate_all_jobs(self) -> None:
        all_job_ids = [
            job_id
            for instance in self._registered_instances.values()
            for job_id in instance.get_running_jobs().keys()
        ]
        for job_id in all_job_ids:
            await self.deallocate_job(job_id)


@pytest.mark.asyncio
async def test_gpu() -> None:
    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            # this is instance type t0. It costs $0.5/hr, has 2 CPU, 4GB of memory, and
            # 2 gpus
            (0.5, (2, 4, 20, 2), None),
            # this is instance type t1. It costs $0.25/hr, has 2 CPU, 4GB of memory, and
            # no gpus
            (0.25, (2, 4, 40, None), None),
        ]
    )

    # allocate_jobs will create instances that are named sequentially. The first
    # instance created is i0, then i1, etc. Here we're asserting that allocate_jobs will
    # create an instance of type t1:

    # choose the cheaper instance type
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 80), 1
        )
    )[0] == [("i0", "t1")]
    await instance_registrar.deallocate_all_jobs()

    # Here we're asserting that even though we deallocated i0 so it's empty, we'll
    # create a new instance i1, which will be of type t0 because we requested that we
    # need a GPU

    # if we request a GPU, we need a GPU instance
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 80, 1), 1
        )
    )[0] == [("i1", "t0")]
    await instance_registrar.deallocate_all_jobs()

    # a new job will always choose the instance without the GPU first if it doesn't need
    # it
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(1, 1, 80), 1
        )
    )[0] == [("i0", "t1")]

    # but a new job that does require a GPU will go to the right instance
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(1, 1, 80, 1), 1
        )
    )[0] == [("i1", "t0")]


@pytest.mark.asyncio
async def test_impossible_resources() -> None:
    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            (0.25, (2, 4, 40, None), None),
            (0.5, (2, 4, 20, None), None),
        ]
    )
    # requesting impossible resources fails
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(3, 4, 80), 1
        )
    )[0] == []
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 5, 80), 1
        )
    )[0] == []
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 10), 1
        )
    )[0] == []


@pytest.mark.asyncio
async def test_packing() -> None:
    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            (0.25, (2, 4, 40, None), None),
        ]
    )
    # cpu-bound
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(1, 1, 80), 3
        )
    )[0] == [("i0", "t0"), ("i0", "t0"), ("i1", "t0")]

    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            (0.25, (2, 4, 40, None), None),
        ]
    )
    # memory-bound
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(0.5, 2, 80), 3
        )
    )[0] == [("i0", "t0"), ("i0", "t0"), ("i1", "t0")]


@pytest.mark.asyncio
async def test_max_eviction_rate() -> None:
    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            (0.25, (2, 4, 40, None), None),
            (0.5, (2, 4, 20, None), None),
        ]
    )
    # choose the cheaper instance type, even if it has a higher eviction rate
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 80), 1
        )
    )[0] == [("i0", "t0")]
    # deallocate and reallocate with the same specs puts us back on the same instance
    await instance_registrar.deallocate_all_jobs()
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 80), 1
        )
    )[0] == [("i0", "t0")]
    # but if we specify a lower max eviction rate we have to make a new instance
    await instance_registrar.deallocate_all_jobs()
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 30), 1
        )
    )[0] == [("i1", "t1")]

    instance_registrar = MockInstanceRegistrar.from_tuples(
        [
            (0.5, (2, 4, 40, None), None),
            (0.5, (2, 4, 20, None), None),
        ]
    )
    # if the instance types have the same price, then choose the instance type with the
    # lowest eviction rate
    assert (
        await instance_registrar.allocate_jobs(
            ResourcesInternal.from_cpu_and_memory(2, 4, 80), 3
        )
    )[0] == [("i0", "t1"), ("i1", "t1"), ("i2", "t1")]
