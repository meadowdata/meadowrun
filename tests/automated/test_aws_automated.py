from __future__ import annotations
import asyncio
from concurrent.futures import ThreadPoolExecutor

"""
These tests require an AWS account to be set up, but don't require any manual
intervention beyond some initial setup. Also, these tests create instances (which cost
money!). Either `meadowrun-manage install` needs to be set up, or `meadowrun-manage
clean` needs to be run periodically
"""
import datetime
import pickle
import pprint
import threading
from typing import TYPE_CHECKING, List, Tuple

import asyncssh
import boto3
import meadowrun.aws_integration.aws_install_uninstall
import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances as adjust_ec2_instances  # noqa: E501
import pytest
from basics import BasicsSuite, ErrorsSuite, HostProvider, MapSuite
from instance_registrar_suite import (
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
)
from meadowrun import (
    Deployment,
    PipRequirementsFile,
    Resources,
    run_command,
    run_function,
    ssh,
)
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import (
    AllocEC2Instance,
    EC2InstanceRegistrar,
    EC2GridJobDriver,
    SSH_USER,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.ec2_ssh_keys import get_meadowrun_ssh_key
from meadowrun.aws_integration.grid_tasks_sqs import (
    _complete_task,
    _get_task,
    create_queues_and_add_tasks,
    receive_results,
)
from meadowrun.config import EVICTION_RATE_INVERSE, LOGICAL_CPU, MEMORY_GB
from meadowrun.instance_allocation import InstanceRegistrar
from meadowrun.instance_selection import (
    ResourcesInternal,
    choose_instance_types_for_job,
)
from meadowrun.meadowrun_pb2 import ProcessState

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host, JobCompletion

# TODO don't always run tests in us-east-2
REGION = "us-east-2"


class AwsHostProvider(HostProvider):
    def get_resources_required(self) -> Resources:
        return Resources(1, 4, 80)

    def get_host(self) -> Host:
        return AllocEC2Instance(REGION)

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        async with ssh.connect(
            job_completion.public_address,
            username=SSH_USER,
            private_key=get_meadowrun_ssh_key(REGION),
        ) as conn:
            return await ssh.read_text_from_file(conn, job_completion.log_file_name)


class TestBasicsAws(AwsHostProvider, BasicsSuite):
    pass


class TestErrorsAws(AwsHostProvider, ErrorsSuite):
    pass


class TestMapAws(AwsHostProvider, MapSuite):
    pass


class TestAWSOnlyFeatures(AwsHostProvider):
    """
    These tests should get moved into BasicsSuite as we add support for them on all
    platforms
    """

    @pytest.mark.asyncio
    async def test_nvidia_gpu(self) -> None:
        deployment = Deployment.git_repo(
            "https://github.com/meadowdata/test_repo",
            interpreter=PipRequirementsFile("requirements_with_torch.txt", "3.9"),
        )

        for resources in (
            Resources(1, 1, gpus=1, flags="nvidia"),
            Resources(1, 1, gpu_memory=1, flags="nvidia"),
        ):
            results: Tuple[bool, Tuple[int, int]] = await run_function(
                "gpu_tests.check_for_cuda", self.get_host(), resources, deployment
            )
            assert results[0]
            if resources.gpu_memory is not None:
                assert results[1][1] / (1000**3) > resources.gpu_memory

            job_completion = await run_command(
                "nvidia-smi", self.get_host(), resources, deployment
            )
            assert job_completion.return_code == 0

    @pytest.mark.asyncio
    async def manual_test_ports(self) -> None:
        """
        This test requires manually checking that <host>:80 returns something.

        TODO In order to make this an automated test, we'd need to allow getting the
        name of the host we're running on while we're running, and then have a
        programmatic way of killing the job (or we could just build a test container
        that exits on its own after some number of seconds)
        """
        await run_command(
            "python app.py",
            self.get_host(),
            self.get_resources_required(),
            Deployment.container_image("okteto/sample-app"),
            ports=80,
        )


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

    def get_host(self) -> Host:
        return AllocEC2Instance()


class TestEC2InstanceRegistrar(EC2InstanceRegistrarProvider, InstanceRegistrarSuite):
    pass


@pytest.mark.asyncio
async def test_get_ec2_instance_types() -> None:
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types(REGION)
    # the actual number of instance types will fluctuate based on AWS' whims.
    assert len(instance_types) > 600

    chosen_instance_types = list(
        choose_instance_types_for_job(
            ResourcesInternal.from_cpu_and_memory(3, 5, 10), 52, instance_types
        )
    )
    total_cpu = sum(
        instance_type.instance_type.resources.consumable[LOGICAL_CPU]
        * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_cpu >= 3 * 52
    total_memory_gb = sum(
        instance_type.instance_type.resources.consumable[MEMORY_GB]
        * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_memory_gb >= 5 * 52
    assert all(
        (
            100
            - instance_type.instance_type.resources.non_consumable[
                EVICTION_RATE_INVERSE
            ]
        )
        <= 10
        for instance_type in chosen_instance_types
    )
    pprint.pprint(chosen_instance_types)

    chosen_instance_types = list(
        choose_instance_types_for_job(
            ResourcesInternal.from_cpu_and_memory(1000, 24000, 10), 1, instance_types
        )
    )
    assert len(chosen_instance_types) == 0


class TestGridTaskQueue:
    @pytest.mark.asyncio
    async def test_grid_task_queue(self) -> None:
        """
        Tests the grid_task_queue functions without actually running any tasks. Uses SQS
        and S3 resources.
        """
        region_name = await _get_default_region_name()
        task_arguments = ["hello", ("hey", "there"), {"a": 1}, ["abcdefg"] * 100_000]

        request_queue_url, job_id, ranges = await create_queues_and_add_tasks(
            region_name, task_arguments
        )

        def complete_tasks() -> None:
            tasks: List = []

            def assert_task(index: int) -> None:
                assert tasks[index] is not None
                assert tasks[index][1] == 1  # attempt
                assert pickle.loads(tasks[index][2]) == task_arguments[index]

            def complete_task(index: int) -> None:
                _complete_task(
                    job_id,
                    region_name,
                    tasks[index][0],
                    tasks[index][1],
                    ProcessState(
                        state=ProcessState.ProcessStateEnum.SUCCEEDED,
                        pickled_result=tasks[index][2],
                    ),
                )

            # get some tasks and complete them
            tasks.append(_get_task(request_queue_url, job_id, region_name, 0))
            assert_task(0)

            tasks.append(_get_task(request_queue_url, job_id, region_name, 0))
            assert_task(1)

            complete_task(0)

            tasks.append(_get_task(request_queue_url, job_id, region_name, 0))
            assert_task(2)

            tasks.append(_get_task(request_queue_url, job_id, region_name, 0))
            assert_task(3)

            # there should be no more tasks to get
            # worker now checks forever - put in timeout after enabling worker restarts
            # assert _get_task(request_queue_url, job_id, region_name, 0) is None

            complete_task(1)
            complete_task(2)
            complete_task(3)

        results_thread = threading.Thread(target=complete_tasks)
        results_thread.start()

        stop_receiving = asyncio.Event()
        results: List = [None] * len(task_arguments)
        async for task_id, attempt, process_state in receive_results(
            job_id,
            region_name,
            1,
            request_queue_url,
            stop_receiving=stop_receiving,
            all_workers_exited=asyncio.Event(),
        ):
            assert attempt == 1
            results[task_id] = pickle.loads(process_state.pickled_result)
            if len(results) == 4:
                stop_receiving.set()

        results_thread.join()
        assert results == task_arguments

    async def _create_driver(self) -> Tuple[EC2GridJobDriver, str, str, str]:
        region_name = await _get_default_region_name()
        task_arguments = [1, 2, 3, 4]

        request_queue_url, job_id, ranges = await create_queues_and_add_tasks(
            region_name, task_arguments
        )

        driver = EC2GridJobDriver[int, int](
            region_name,
            {},
            "",
            asyncssh.SSHKey(),
            len(task_arguments),
            1,
            lambda x: x**x,
            request_queue_url,
            job_id,
            ranges,
        )
        return driver, request_queue_url, job_id, region_name

    @pytest.mark.asyncio
    async def test_worker_loop(self) -> None:

        driver, _, _, _ = await self._create_driver()

        # start a worker_loop which will get tasks and complete them
        public_address = "foo"
        worker_id = 1
        worker_thread = threading.Thread(
            target=lambda: driver.worker_function()(
                public_address,
                worker_id,
            )
        )
        worker_thread.start()
        results = []
        async for result in driver.get_results_as_completed(asyncio.Event(), 1):
            results.append(result.result_or_raise())
        worker_thread.join()
        assert set(results) == {1, 4, 27, 256}

    @pytest.mark.asyncio
    async def test_worker_loop_with_retries(self) -> None:
        driver, request_queue_url, job_id, region_name = await self._create_driver()

        # only complete one task - get_results should still exit.
        def complete_tasks() -> None:
            def complete_task(task: Tuple[int, int, bytes], fail: bool = False) -> None:
                _complete_task(
                    job_id,
                    region_name,
                    task[0],
                    task[1],
                    ProcessState(
                        state=ProcessState.ProcessStateEnum.SUCCEEDED
                        if not fail
                        else ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                        pickled_result=task[2],
                    ),
                )

            for i in range(8):
                task = _get_task(request_queue_url, job_id, region_name, 0)
                assert task is not None
                complete_task(task, fail=True)
            for i in range(4):
                task = _get_task(request_queue_url, job_id, region_name, 0)
                assert task is not None
                complete_task(task, fail=False)

        with ThreadPoolExecutor() as e:
            complete_future = asyncio.get_running_loop().run_in_executor(
                e, complete_tasks
            )

            results = []
            async for result in driver.get_results_as_completed(asyncio.Event(), 3):
                results.append(result.result_or_raise())
            await complete_future
        assert set(results) == {1, 2, 3, 4}

    @pytest.mark.asyncio
    async def test_worker_loop_unfinished(self) -> None:

        driver, request_queue_url, job_id, region_name = await self._create_driver()

        def complete_tasks() -> None:
            task = _get_task(request_queue_url, job_id, region_name, 0)
            assert task is not None
            _complete_task(
                job_id,
                region_name,
                task[0],
                task[1],
                ProcessState(
                    state=ProcessState.ProcessStateEnum.SUCCEEDED,
                    pickled_result=task[2],
                ),
            )

        e = ThreadPoolExecutor()
        await asyncio.get_running_loop().run_in_executor(e, complete_tasks)

        event = asyncio.Event()
        event.set()
        num_results = 0
        async for _ in driver.get_results_as_completed(event, 3):
            num_results += 1
        assert num_results == 1
