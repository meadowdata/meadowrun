"""
These tests require an AWS account to be set up, but don't require any manual
intervention beyond some initial setup. Also, these tests create instances (which cost
money!). Either `meadowrun-manage install` needs to be set up, or `meadowrun-manage
clean` needs to be run periodically
"""

import asyncio
import datetime
import io
import pprint
import threading
import uuid

import boto3
import fabric
import pytest

import meadowrun.aws_integration.aws_install_uninstall
from meadowrun.aws_integration.config import SSH_USER
import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances as adjust_ec2_instances  # noqa: E501
from basics import BasicsSuite, HostProvider, ErrorsSuite, MapSuite
from instance_registrar_suite import (
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
)
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import EC2InstanceRegistrar
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.ec2_ssh_keys import get_meadowrun_ssh_key
from meadowrun.aws_integration.grid_tasks_sqs import (
    _add_tasks,
    _complete_task,
    _create_queues_for_job,
    _get_task,
    get_results,
    worker_loop,
)
from meadowrun.instance_allocation import InstanceRegistrar
from meadowrun.instance_selection import choose_instance_types_for_job, Resources
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job import AllocCloudInstance
from meadowrun.run_job_core import Host, JobCompletion, CloudProviderType

# TODO don't always run tests in us-east-2
REGION = "us-east-2"


class AwsHostProvider(HostProvider):
    def get_host(self) -> Host:
        return AllocCloudInstance(1, 2, 80, "EC2", REGION)

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        with fabric.Connection(
            job_completion.public_address,
            user=SSH_USER,
            connect_kwargs={"pkey": get_meadowrun_ssh_key(REGION)},
        ) as conn:
            with io.BytesIO() as local_copy:
                conn.get(job_completion.log_file_name, local_copy)
                return local_copy.getvalue().decode("utf-8")


class TestBasicsAws(AwsHostProvider, BasicsSuite):
    pass


class TestErrorsAws(AwsHostProvider, ErrorsSuite):
    pass


class TestMapAws(MapSuite):
    def cloud_provider(self) -> CloudProviderType:
        return "EC2"


class EC2InstanceRegistrarProvider(InstanceRegistrarProvider[InstanceRegistrar]):
    async def get_instance_registrar(self) -> InstanceRegistrar:
        return EC2InstanceRegistrar(await _get_default_region_name(), "create")

    async def deregister_instance(
        self,
        instance_registrar: InstanceRegistrar,
        public_address: str,
        require_no_running_jobs: bool,
    ) -> bool:
        return adjust_ec2_instances._deregister_ec2_instance(
            public_address,
            require_no_running_jobs,
            instance_registrar.get_region_name(),
        )

    async def num_currently_running_instances(
        self, instance_registrar: InstanceRegistrar
    ) -> int:
        ec2 = boto3.resource("ec2", region_name=instance_registrar.get_region_name())
        return sum(1 for _ in adjust_ec2_instances._get_running_instances(ec2))

    async def run_adjust(self, instance_registrar: InstanceRegistrar) -> None:
        adjust_ec2_instances._deregister_and_terminate_instances(
            instance_registrar.get_region_name(),
            TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
            datetime.timedelta.min,
        )

    async def terminate_all_instances(
        self, instance_registrar: InstanceRegistrar
    ) -> None:
        meadowrun.aws_integration.aws_install_uninstall.terminate_all_instances(
            instance_registrar.get_region_name(), False
        )

    def cloud_provider(self) -> CloudProviderType:
        return "EC2"


class TestEC2InstanceRegistrar(EC2InstanceRegistrarProvider, InstanceRegistrarSuite):
    pass


@pytest.mark.asyncio
async def test_get_ec2_instance_types():
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types(REGION)
    # the actual number of instance types will fluctuate based on AWS' whims.
    assert len(instance_types) > 600

    chosen_instance_types = choose_instance_types_for_job(
        Resources(5, 3, {}), 52, 10, instance_types
    )
    total_cpu = sum(
        instance_type.instance_type.logical_cpu * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_cpu >= 3 * 52
    total_memory_gb = sum(
        instance_type.instance_type.memory_gb * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_memory_gb >= 5 * 52
    assert all(
        instance_type.instance_type.interruption_probability <= 10
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
