import abc
import asyncio
import os
import platform
import time
from typing import Tuple, TypeVar, Generic
import datetime

import pytest

from meadowrun import run_function, AllocEC2Instance
from meadowrun.config import LOGICAL_CPU, MEMORY_GB
from meadowrun.instance_allocation import (
    InstanceRegistrar,
    _InstanceState,
    _choose_existing_instances,
    allocate_single_job_to_instance,
)
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.run_job_core import Resources, Host

_TInstanceRegistrar = TypeVar("_TInstanceRegistrar", bound=InstanceRegistrar)


class InstanceRegistrarProvider(abc.ABC, Generic[_TInstanceRegistrar]):
    """
    Similar to HostProvider but for lower level tests that use the InstanceRegistrar
    directly. In a way this is basically a broader version of the InstanceRegistrar.
    """

    @abc.abstractmethod
    async def get_instance_registrar(self) -> _TInstanceRegistrar:
        pass

    async def clear_instance_registrar(
        self, instance_registrar: _TInstanceRegistrar
    ) -> None:
        """
        This function could live on InstanceRegistrar directly, but there's no use for
        it outside of tests yet.
        """
        registered_instances = await instance_registrar.get_registered_instances()
        if registered_instances:
            await asyncio.wait(
                [
                    asyncio.create_task(
                        self.deregister_instance(
                            instance_registrar, instance.public_address, False
                        )
                    )
                    for instance in registered_instances
                ]
            )

    @abc.abstractmethod
    async def deregister_instance(
        self,
        instance_registrar: _TInstanceRegistrar,
        public_address: str,
        require_no_running_jobs: bool,
    ) -> bool:
        """
        This function could also live on InstanceRegistrar directly, but currently it
        has a separate implementation so that it can be used by the management lambdas
        without dragging in the rest of the InstanceRegistrar code.
        """
        pass

    @abc.abstractmethod
    async def num_currently_running_instances(
        self, instance_registrar: _TInstanceRegistrar
    ) -> int:
        """
        Returns how many instances are currently running (regardless of what's been
        registered)
        """
        pass

    @abc.abstractmethod
    async def run_adjust(self, instance_registrar: _TInstanceRegistrar) -> None:
        pass

    @abc.abstractmethod
    async def terminate_all_instances(
        self, instance_registrar: _TInstanceRegistrar
    ) -> None:
        pass

    @abc.abstractmethod
    def get_host(self) -> Host:
        """This is used to call run_function/run_command for this instance registrar"""
        pass


TERMINATE_INSTANCES_IF_IDLE_FOR_TEST = datetime.timedelta(seconds=10)


class InstanceRegistrarSuite(InstanceRegistrarProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_allocate_deallocate_mechanics(self) -> None:
        """Tests allocating and deallocating, does not involve real machines"""
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            await instance_registrar.register_instance(
                "testhost-1",
                "testhost-1-name",
                ResourcesInternal.from_cpu_and_memory(8, 64),
                [],
            )
            await instance_registrar.register_instance(
                "testhost-2",
                "testhost-2-name",
                ResourcesInternal.from_cpu_and_memory(4, 32),
                [("worker-1", ResourcesInternal.from_cpu_and_memory(2, 1))],
            )

            # Can't register the same instance twice
            with pytest.raises(ValueError):
                await instance_registrar.register_instance(
                    "testhost-2",
                    "testhost-2-name",
                    ResourcesInternal.from_cpu_and_memory(4, 32),
                    [],
                )

            host2 = await instance_registrar.get_registered_instance("testhost-2")
            assert await instance_registrar.deallocate_job_from_instance(
                host2, "worker-1"
            )
            # Can't deallocate the same worker twice
            assert not await instance_registrar.deallocate_job_from_instance(
                host2, "worker-1"
            )

            async def get_instances() -> Tuple[_InstanceState, _InstanceState]:
                instances = await instance_registrar.get_registered_instances()
                return [i for i in instances if i.public_address == "testhost-1"][0], [
                    i for i in instances if i.public_address == "testhost-2"
                ][0]

            testhost1, testhost2 = await get_instances()
            assert testhost2.get_available_resources().consumable[LOGICAL_CPU] == 6
            assert testhost2.get_available_resources().consumable[MEMORY_GB] == 33

            assert await instance_registrar.allocate_jobs_to_instance(
                testhost1,
                ResourcesInternal.from_cpu_and_memory(2, 4),
                ["worker-2", "worker-3"],
            )
            testhost1, _ = await get_instances()
            assert await instance_registrar.allocate_jobs_to_instance(
                testhost1, ResourcesInternal.from_cpu_and_memory(1, 3), ["worker-4"]
            )
            # cannot allocate if the worker id already is in use
            testhost1, _ = await get_instances()
            assert not await instance_registrar.allocate_jobs_to_instance(
                testhost1, ResourcesInternal.from_cpu_and_memory(2, 4), ["worker-2"]
            )

            # make sure our resources available is correct
            testhost1, _ = await get_instances()
            assert testhost1.get_available_resources().consumable[LOGICAL_CPU] == 3
            assert testhost1.get_available_resources().consumable[MEMORY_GB] == 53

            # we've kind of already tested deallocation, but just for good measure
            testhost1 = await instance_registrar.get_registered_instance("testhost-1")
            assert await instance_registrar.deallocate_job_from_instance(
                testhost1, "worker-4"
            )
            testhost1, _ = await get_instances()
            assert testhost1.get_available_resources().consumable[LOGICAL_CPU] == 4
            assert testhost1.get_available_resources().consumable[MEMORY_GB] == 56

    @pytest.mark.asyncio
    async def test_allocate_existing_instances(self) -> None:
        """
        Tests logic for allocating existing EC2 instances, does not involve actual
        instances
        """
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            await instance_registrar.register_instance(
                "testhost-3",
                "testhost-3-name",
                ResourcesInternal.from_cpu_and_memory(2, 16),
                [],
            )
            await instance_registrar.register_instance(
                "testhost-4",
                "testhost-4-name",
                ResourcesInternal.from_cpu_and_memory(4, 32),
                [],
            )

            resources_required = ResourcesInternal.from_cpu_and_memory(1, 2)
            results, _ = await _choose_existing_instances(
                instance_registrar, resources_required, 3
            )

            # we should put 2 tasks on testhost-3 because that's more "compact"
            assert len(results["testhost-3"]) == 2
            assert len(results["testhost-4"]) == 1

    @pytest.mark.asyncio
    async def test_prevent_further_allocation(self) -> None:
        """
        Tests logic for allocating existing EC2 instances, does not involve actual
        instances
        """
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            await instance_registrar.register_instance(
                "testhost-5",
                "testhost-5-name",
                ResourcesInternal.from_cpu_and_memory(2, 16),
                [],
            )
            await instance_registrar.register_instance(
                "testhost-6",
                "testhost-6-name",
                ResourcesInternal.from_cpu_and_memory(4, 32),
                [],
            )

            await instance_registrar.set_prevent_further_allocation("testhost-5", True)

            resources_required = ResourcesInternal.from_cpu_and_memory(1, 2)
            results, _ = await _choose_existing_instances(
                instance_registrar, resources_required, 3
            )

            # we should want to 2 tasks on testhost-5 because that's more "compact", but
            # we can't because we set prevent_future allocation
            assert "testhost-5" not in results
            assert len(results["testhost-6"]) == 3

            # now we should only be able to allocate one worker on testhost-6
            results, _ = await _choose_existing_instances(
                instance_registrar, resources_required, 3
            )
            assert "testhost-5" not in results
            assert len(results["testhost-6"]) == 1

            await instance_registrar.set_prevent_further_allocation("testhost-5", False)

            # now we can use testhost-5 again
            results, _ = await _choose_existing_instances(
                instance_registrar, resources_required, 3
            )
            assert len(results["testhost-5"]) == 2
            assert "testhost-6" not in results

    @pytest.mark.asyncio
    async def test_allocate_many_workers(self) -> None:
        """Tests allocating and deallocating, does not involve real machines"""
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            await instance_registrar.register_instance(
                "testhost-1",
                "testhost-1-name",
                ResourcesInternal.from_cpu_and_memory(2 * 200, 4 * 200),
                [],
            )

            await _choose_existing_instances(
                instance_registrar, ResourcesInternal.from_cpu_and_memory(2, 4), 200
            )

            instance = await instance_registrar.get_registered_instance("testhost-1")
            assert len(instance.get_running_jobs()) == 200

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_launch_one_instance(self) -> None:
        """Launches instances that must be cleaned up manually"""
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            def remote_function() -> Tuple[int, str]:
                return os.getpid(), platform.node()

            pid1, host1 = await run_function(
                remote_function, self.get_host(), Resources(1, 0.5, 15)
            )
            time.sleep(1)
            pid2, host2 = await run_function(
                remote_function, self.get_host(), Resources(1, 0.5, 15)
            )
            time.sleep(1)
            pid3, host3 = await run_function(
                remote_function, self.get_host(), Resources(1, 0.5, 15)
            )

            # these should have all run on the same host, but in different processes
            assert pid1 != pid2 and pid2 != pid3
            assert host1 == host2 and host2 == host3

            instances = await instance_registrar.get_registered_instances()
            assert len(instances) == 1
            assert instances[0].get_available_resources().consumable[LOGICAL_CPU] >= 1
            assert instances[0].get_available_resources().consumable[MEMORY_GB] >= 0.5

            # remember to kill the instance when you're done!

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_launch_multiple_instances(self) -> None:
        """Launches instances that must be cleaned up manually"""
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            def remote_function() -> Tuple[int, str]:
                return os.getpid(), platform.node()

            task1 = asyncio.create_task(
                run_function(remote_function, self.get_host(), Resources(1, 0.5, 15))
            )
            task2 = asyncio.create_task(
                run_function(remote_function, self.get_host(), Resources(1, 0.5, 15))
            )
            task3 = asyncio.create_task(
                run_function(remote_function, self.get_host(), Resources(1, 0.5, 15))
            )

            results = await asyncio.gather(task1, task2, task3)
            ((pid1, host1), (pid2, host2), (pid3, host3)) = results

            # These should all have ended up on different hosts
            assert host1 != host2 and host2 != host3
            instances = await instance_registrar.get_registered_instances()
            assert len(instances) == 3
            assert all(
                instance.get_available_resources().consumable[LOGICAL_CPU] >= 1
                and instance.get_available_resources().consumable[MEMORY_GB] >= 0.5
                for instance in instances
            )

    @pytest.mark.asyncio
    async def test_deregister(self) -> None:
        """Tests registering and deregistering, does not involve real machines"""
        async with await self.get_instance_registrar() as instance_registrar:
            await self.clear_instance_registrar(instance_registrar)

            await instance_registrar.register_instance(
                "testhost-1",
                "testhost-1-name",
                ResourcesInternal.from_cpu_and_memory(8, 64),
                [],
            )
            await instance_registrar.register_instance(
                "testhost-2",
                "testhost-2-name",
                ResourcesInternal.from_cpu_and_memory(4, 32),
                [("worker-1", ResourcesInternal.from_cpu_and_memory(2, 1))],
            )

            assert await self.deregister_instance(
                instance_registrar, "testhost-1", True
            )
            # with require_no_running_jobs=True, testhost-2 should fail to deregister
            assert not await self.deregister_instance(
                instance_registrar, "testhost-2", True
            )
            assert await self.deregister_instance(
                instance_registrar, "testhost-2", False
            )

    @pytest.mark.asyncio
    async def test_adjust_instances(self) -> None:
        """
        Tests the adjust function, involves running real machines, but they should all
        get terminated automatically by the test.

        If use_lambda is false, the test will just call the adjust function directly.
        For a slightly more realistic test, you can run with use_lambda set to true. In
        that case, the test will invoke the ec2 alloc lambda, assuming it has already
        been created.
        """
        async with await self.get_instance_registrar() as instance_registrar:
            await self.terminate_all_instances(instance_registrar)
            assert await self.num_currently_running_instances(instance_registrar) == 0

            await self.clear_instance_registrar(instance_registrar)

            # first, register an instance that's not actually running
            await instance_registrar.register_instance(
                "testhost-1",
                "testhost-1-name",
                ResourcesInternal.from_cpu_and_memory(8, 64),
                [("worker-1", ResourcesInternal.from_cpu_and_memory(2, 1))],
            )
            assert len(await instance_registrar.get_registered_instances()) == 1

            # adjust should deregister the instance (even if there are "jobs" supposedly
            # running on it) because there's no actual instance running
            await self.run_adjust(instance_registrar)
            assert len(await instance_registrar.get_registered_instances()) == 0

            # now, launch two instances (which should get registered automatically)
            public_address1, job1 = await allocate_single_job_to_instance(
                instance_registrar,
                ResourcesInternal.from_cpu_and_memory(1, 0.5, 15),
                # won't actually create an EC2 instance given instance_registrar
                AllocEC2Instance(instance_registrar.get_region_name()),
                None,
            )

            public_address2, job2 = await allocate_single_job_to_instance(
                instance_registrar,
                ResourcesInternal.from_cpu_and_memory(1, 0.5, 15),
                # won't actually create an EC2 instance given instance_registrar
                AllocEC2Instance(instance_registrar.get_region_name()),
                None,
            )

            assert len(await instance_registrar.get_registered_instances()) == 2

            # deregister one without turning off the instance then adjust should
            # terminate it automatically
            await self.deregister_instance(instance_registrar, public_address1, False)
            await self.run_adjust(instance_registrar)
            assert len(await instance_registrar.get_registered_instances()) == 1
            assert await self.num_currently_running_instances(instance_registrar) == 1
            print(
                f"Optionally, manually check that {public_address1} is being terminated"
                f" but {public_address2} is still running"
            )

            # now deallocate the job from the second instance
            await instance_registrar.deallocate_job_from_instance(
                await instance_registrar.get_registered_instance(public_address2), job2
            )

            # adjust should NOT deregister/terminate the instance because the timeout
            # has not happened yet.
            await self.run_adjust(instance_registrar)
            assert len(await instance_registrar.get_registered_instances()) == 1

            # after 11 seconds, run adjust again, now that instance should get
            # deregistered/terminated
            time.sleep(11)
            await self.run_adjust(instance_registrar)
            assert len(await instance_registrar.get_registered_instances()) == 0
            assert await self.num_currently_running_instances(instance_registrar) == 0
