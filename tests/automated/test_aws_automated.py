"""
These tests require an AWS account to be set up, but don't require any manual
intervention beyond some initial setup. Also, these tests create instances (which cost
money!). Either `meadowrun-manage install` needs to be set up, or `meadowrun-manage
clean` needs to be run periodically
"""

import asyncio
import io
import os
import platform
import pprint
import threading
import time
import uuid
from typing import Tuple

import fabric
import pytest

from basics import BasicsSuite, HostProvider, ErrorsSuite
from meadowrun.aws_integration.aws_core import (
    _get_default_region_name,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.ec2_alloc import (
    _EC2InstanceState,
    _allocate_job_to_ec2_instance,
    _choose_existing_ec2_instances,
    _ensure_ec2_alloc_table,
    _get_ec2_instances,
    _register_ec2_instance,
    deallocate_job_from_ec2_instance,
    get_jobs_on_ec2_instance,
)
from meadowrun.aws_integration.grid_tasks_sqs import (
    _add_tasks,
    _complete_task,
    _create_queues_for_job,
    _get_task,
    get_results,
    worker_loop,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _deregister_ec2_instance,
)
from meadowrun.aws_integration.ec2_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.instance_selection import choose_instance_types_for_job, Resources
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job import (
    Host,
    EC2AllocHost,
    JobCompletion,
    run_map,
    EC2AllocHosts,
    run_function,
)


class AwsHostProvider(HostProvider):
    # TODO don't always run tests in us-east-2

    def get_host(self) -> Host:
        return EC2AllocHost(1, 2, 80, "us-east-2")

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    def get_log_file_text(self, job_completion: JobCompletion) -> str:
        with fabric.Connection(
            job_completion.public_address,
            user="ubuntu",
            connect_kwargs={"pkey": ensure_meadowrun_key_pair("us-east-2")},
        ) as conn:
            with io.BytesIO() as local_copy:
                conn.get(job_completion.log_file_name, local_copy)
                return local_copy.getvalue().decode("utf-8")


class TestBasicsAws(AwsHostProvider, BasicsSuite):
    pass

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_run_map(self):
        """Runs a "real" run_map"""
        results = await run_map(
            lambda x: x**x, [1, 2, 3, 4], EC2AllocHosts(1, 0.5, 15)
        )

        assert results == [1, 4, 27, 256]


class TestErrorsAws(AwsHostProvider, ErrorsSuite):
    pass


@pytest.mark.asyncio
async def test_get_ec2_instance_types():
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types("us-east-2")
    # the actual number of instance types will fluctuate based on AWS' whims.
    assert len(instance_types) > 600

    chosen_instance_types = choose_instance_types_for_job(
        Resources(5, 3, {}), 52, 10, instance_types
    )
    total_cpu = sum(
        instance_type.ec2_instance_type.logical_cpu * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_cpu >= 3 * 52
    total_memory_gb = sum(
        instance_type.ec2_instance_type.memory_gb * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_memory_gb >= 5 * 52
    assert all(
        instance_type.ec2_instance_type.interruption_probability <= 10
        for instance_type in chosen_instance_types
    )
    pprint.pprint(chosen_instance_types)

    chosen_instance_types = choose_instance_types_for_job(
        Resources(24000, 1000, {}), 1, 10, instance_types
    )
    assert len(chosen_instance_types) == 0


class TestGridTaskQueue:
    def test_grid_task_queue(self):
        """
        Tests the grid_task_queue functions without actually running any tasks. Uses SQS
        resources.
        """
        region_name = asyncio.run(_get_default_region_name())
        task_arguments = ["hello", ("hey", "there"), {"a": 1}]

        # dummy variables
        job_id = str(uuid.uuid4())
        public_address = "foo"
        worker_id = 1

        request_queue_url, result_queue_url = asyncio.run(
            _create_queues_for_job(job_id, region_name)
        )

        # get results in a different thread as we're adding/completing tasks
        results = None

        def get_results_thread():
            nonlocal results
            results = asyncio.run(
                get_results(result_queue_url, region_name, len(task_arguments), 1)
            )

        results_thread = threading.Thread(target=get_results_thread)
        results_thread.start()

        # add some tasks
        asyncio.run(_add_tasks(request_queue_url, region_name, task_arguments))

        # get some tasks and complete them
        task1 = _get_task(
            request_queue_url,
            result_queue_url,
            region_name,
            0,
            public_address,
            worker_id,
        )
        assert task1 is not None
        task2 = _get_task(
            request_queue_url,
            result_queue_url,
            region_name,
            0,
            public_address,
            worker_id,
        )
        assert task2 is not None
        _complete_task(
            result_queue_url,
            region_name,
            task1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=task1.pickled_function_arguments,
            ),
            public_address,
            worker_id,
        )
        task3 = _get_task(
            request_queue_url,
            result_queue_url,
            region_name,
            0,
            public_address,
            worker_id,
        )
        assert task3 is not None
        # there should be no more tasks to get
        assert (
            _get_task(
                request_queue_url,
                result_queue_url,
                region_name,
                0,
                public_address,
                worker_id,
            )
            is None
        )
        _complete_task(
            result_queue_url,
            region_name,
            task2,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=task2.pickled_function_arguments,
            ),
            public_address,
            worker_id,
        )
        _complete_task(
            result_queue_url,
            region_name,
            task3,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=task3.pickled_function_arguments,
            ),
            public_address,
            worker_id,
        )

        results_thread.join()
        assert results == task_arguments

    def test_worker_loop(self):
        region_name = asyncio.run(_get_default_region_name())
        task_arguments = [1, 2, 3, 4]

        # dummy variables
        job_id = str(uuid.uuid4())
        public_address = "foo"
        worker_id = 1

        request_queue_url, result_queue_url = asyncio.run(
            _create_queues_for_job(job_id, region_name)
        )

        # get results on another thread
        results = None

        def get_results_thread():
            nonlocal results
            results = asyncio.run(
                get_results(result_queue_url, region_name, len(task_arguments), 1)
            )

        results_thread = threading.Thread(target=get_results_thread)
        results_thread.start()

        # add tasks
        asyncio.run(_add_tasks(request_queue_url, region_name, task_arguments))

        # start a worker_loop which will get tasks and complete them
        worker_thread = threading.Thread(
            target=lambda: worker_loop(
                lambda x: x**x,
                request_queue_url,
                result_queue_url,
                region_name,
                public_address,
                worker_id,
            )
        )
        worker_thread.start()

        results_thread.join()
        worker_thread.join()
        assert results == [1, 4, 27, 256]


class TestEC2Alloc:
    @pytest.mark.asyncio
    async def test_allocate_deallocate_mechanics(self):
        """Tests allocating and deallocating, does not involve real machines"""
        await _clear_ec2_instances_table()

        await _register_ec2_instance("testhost-1", 8, 64, [])
        await _register_ec2_instance(
            "testhost-2", 4, 32, [("worker-1", Resources(1, 2, {}))]
        )

        # Can't create the same table twice
        with pytest.raises(ValueError):
            await _register_ec2_instance("testhost-2", 4, 32, [])

        worker1_job = (await get_jobs_on_ec2_instance("testhost-2"))["worker-1"]
        assert await deallocate_job_from_ec2_instance(
            "testhost-2", "worker-1", worker1_job
        )
        # Can't deallocate the same worker twice
        assert not await deallocate_job_from_ec2_instance(
            "testhost-2", "worker-1", worker1_job
        )

        table = await _ensure_ec2_alloc_table()

        def get_instances() -> Tuple[_EC2InstanceState, _EC2InstanceState]:
            instances = _get_ec2_instances(table)
            return [i for i in instances if i.public_address == "testhost-1"][0], [
                i for i in instances if i.public_address == "testhost-2"
            ][0]

        testhost1, testhost2 = get_instances()
        assert testhost2.available_resources.logical_cpu == 6
        assert testhost2.available_resources.memory_gb == 33

        assert _allocate_job_to_ec2_instance(
            table,
            testhost1.public_address,
            Resources(4, 2, {}),
            ["worker-2", "worker-3"],
        )
        assert _allocate_job_to_ec2_instance(
            table, testhost1.public_address, Resources(3, 1, {}), ["worker-4"]
        )
        # cannot allocate if the worker id already is in use
        assert not _allocate_job_to_ec2_instance(
            table, testhost1.public_address, Resources(4, 2, {}), ["worker-2"]
        )

        # make sure our resources available is correct
        testhost1, _ = get_instances()
        assert testhost1.available_resources.logical_cpu == 3
        assert testhost1.available_resources.memory_gb == 53

        # we've kind of already tested deallocation, but just for good measure
        worker4_job = (await get_jobs_on_ec2_instance("testhost-1"))["worker-4"]
        assert await deallocate_job_from_ec2_instance(
            "testhost-1", "worker-4", worker4_job
        )
        testhost1, _ = get_instances()
        assert testhost1.available_resources.logical_cpu == 4
        assert testhost1.available_resources.memory_gb == 56

    @pytest.mark.asyncio
    async def test_allocate_existing_instances(self):
        """
        Tests logic for allocating existing EC2 instances, does not involve actual
        instances
        """
        await _clear_ec2_instances_table()

        await _register_ec2_instance("testhost-3", 2, 16, [])
        await _register_ec2_instance("testhost-4", 4, 32, [])

        resources_required = Resources(2, 1, {})
        results = await _choose_existing_ec2_instances(resources_required, 3)

        # we should put 2 tasks on testhost-3 because that's more "compact"
        assert len(results["testhost-3"]) == 2
        assert len(results["testhost-4"]) == 1

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_launch_one_instance(self):
        """Launches instances that must be cleaned up manually"""
        await _clear_ec2_instances_table()

        def remote_function():
            return os.getpid(), platform.node()

        pid1, host1 = await run_function(remote_function, EC2AllocHost(1, 0.5, 15))
        time.sleep(1)
        pid2, host2 = await run_function(remote_function, EC2AllocHost(1, 0.5, 15))
        time.sleep(1)
        pid3, host3 = await run_function(remote_function, EC2AllocHost(1, 0.5, 15))

        # these should have all run on the same host, but in different processes
        assert pid1 != pid2 and pid2 != pid3
        assert host1 == host2 and host2 == host3

        instances = _get_ec2_instances(await _ensure_ec2_alloc_table())
        assert len(instances) == 1
        assert instances[0].available_resources.logical_cpu >= 1
        assert instances[0].available_resources.memory_gb >= 0.5

        # remember to kill the instance when you're done!

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_launch_multiple_instances(self):
        """Launches instances that must be cleaned up manually"""
        await _clear_ec2_instances_table()

        def remote_function():
            return os.getpid(), platform.node()

        task1 = asyncio.create_task(
            run_function(remote_function, EC2AllocHost(1, 0.5, 15))
        )
        task2 = asyncio.create_task(
            run_function(remote_function, EC2AllocHost(1, 0.5, 15))
        )
        task3 = asyncio.create_task(
            run_function(remote_function, EC2AllocHost(1, 0.5, 15))
        )

        results = await asyncio.gather(task1, task2, task3)
        ((pid1, host1), (pid2, host2), (pid3, host3)) = results

        # These should all have ended up on different hosts
        assert host1 != host2 and host2 != host3
        instances = _get_ec2_instances(await _ensure_ec2_alloc_table())
        assert len(instances) == 3
        assert all(
            instance.available_resources.logical_cpu >= 1
            and instance.available_resources.memory_gb >= 0.5
            for instance in instances
        )

    @pytest.mark.asyncio
    async def test_deregister(self):
        """Tests registering and deregistering, does not involve real machines"""
        await _clear_ec2_instances_table()

        await _register_ec2_instance("testhost-1", 8, 64, [])
        await _register_ec2_instance(
            "testhost-2", 4, 32, [("worker-1", Resources(1, 2, {}))]
        )

        region_name = await _get_default_region_name()

        assert _deregister_ec2_instance("testhost-1", True, region_name)
        # with require_no_running_jobs=True, testhost-2 should fail to deregister
        assert not _deregister_ec2_instance("testhost-2", True, region_name)
        assert _deregister_ec2_instance("testhost-2", False, region_name)


async def _clear_ec2_instances_table() -> None:
    # pretend that we're running in a lambda for _deregister_ec2_instance

    table = await _ensure_ec2_alloc_table()
    instances = _get_ec2_instances(table)
    for instance in instances:
        _deregister_ec2_instance(
            instance.public_address, False, await _get_default_region_name()
        )
