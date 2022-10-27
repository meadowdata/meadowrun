from __future__ import annotations

import asyncio
import importlib
import pickle
import uuid
from typing import TYPE_CHECKING, AsyncContextManager, Callable, List, Optional, Tuple

import aiobotocore
import aiobotocore.session
import pytest
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import (
    AllocEC2Instance,
    EC2GridJobInterface,
)
from meadowrun.aws_integration.grid_tasks_sqs import (
    _get_task,
    add_tasks,
    add_worker_shutdown_messages,
    create_request_queue,
)
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import TaskResult
from meadowrun.storage_grid_job import (
    complete_task,
    get_aws_s3_bucket,
    receive_results,
)

if TYPE_CHECKING:
    from pathlib import Path

    from meadowrun.run_job_local import TaskWorkerServer, WorkerProcessMonitor


class TestGridWorker:
    # These tests are a mess because they re-implement aspects of the grid job driver.

    @pytest.mark.asyncio
    async def test_receive_results_happy_path(self) -> None:
        """
        Tests the grid_task_queue functions without actually running any tasks. Uses SQS
        and S3 resources.
        """
        region_name = await _get_default_region_name()
        task_arguments = ["hello", ("hey", "there"), {"a": 1}, ["abcdefg"] * 100_000]

        job_id = str(uuid.uuid4())
        session = aiobotocore.session.get_session()
        async with session.create_client(
            "sqs", region_name=region_name
        ) as sqs, get_aws_s3_bucket(region_name) as s3_bucket:

            request_queue_url = await create_request_queue(job_id, sqs)
            _ = await add_tasks(
                job_id, request_queue_url, s3_bucket, sqs, task_arguments
            )
            print("added tasks ")

            async def complete_tasks() -> None:
                tasks: List = []

                def assert_task(index: int) -> None:
                    assert tasks[index] is not None
                    assert tasks[index][1] == 1  # attempt
                    # can't do this - no longer using FIFO queue
                    # args, kwargs = pickle.loads(tasks[index][2])
                    # assert args[0] == task_arguments[index]
                    # assert len(kwargs) == 0

                async def complete_task_wrapper(index: int) -> None:
                    await complete_task(
                        s3_bucket,
                        job_id,
                        tasks[index][0],
                        tasks[index][1],
                        ProcessState(
                            state=ProcessState.ProcessStateEnum.SUCCEEDED,
                            pickled_result=tasks[index][2],
                        ),
                    )

                async def get_next_task() -> Optional[Tuple[int, int, bytes]]:
                    return await _get_task(sqs, s3_bucket, request_queue_url, job_id, 0)

                print("completing tasks")
                # get some tasks and complete them
                tasks.append(await get_next_task())
                assert_task(0)
                print("task 0")

                tasks.append(await get_next_task())
                assert_task(1)
                print("task 1")

                await complete_task_wrapper(0)

                tasks.append(await get_next_task())
                assert_task(2)

                tasks.append(await get_next_task())
                assert_task(3)

                await complete_task_wrapper(1)
                await complete_task_wrapper(2)
                await complete_task_wrapper(3)

                assert await get_next_task() is None
                print("Mock worker completed tasks")

            complete_tasks_future = asyncio.create_task(complete_tasks())

            stop_receiving = asyncio.Event()
            results: List = [None] * len(task_arguments)
            received = 0
            async for batch, _ in receive_results(
                s3_bucket,
                job_id,
                stop_receiving=stop_receiving,
                all_workers_exited=asyncio.Event(),
                # receive_message_wait_seconds=2,
            ):
                for task_process_state in batch:
                    assert task_process_state.attempt == 1
                    (args,), kwargs = pickle.loads(
                        task_process_state.result.pickled_result
                    )
                    results[task_process_state.task_id] = args
                    received += 1
                    if received >= 4:
                        stop_receiving.set()
                        await add_worker_shutdown_messages(request_queue_url, 1, sqs)

            await complete_tasks_future
            assert results == task_arguments

    async def _create_driver_interface(self) -> EC2GridJobInterface:
        region_name = await _get_default_region_name()
        interface = EC2GridJobInterface(AllocEC2Instance(region_name))
        return interface

    @pytest.mark.asyncio
    async def test_worker_loop_happy_path(
        self,
        agent_server: TaskWorkerServer,
        task_worker_process_monitor: Callable[
            [str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]
        ],
    ) -> None:

        async with await self._create_driver_interface() as interface:
            await interface.setup_and_add_tasks([1, 2, 3, 4])
            (
                agent_function_name,
                agent_function_arguments,
            ) = await interface.get_agent_function(0, pickle.HIGHEST_PROTOCOL)
            agent_function = getattr(
                importlib.import_module(agent_function_name.module_name),
                agent_function_name.function_name,
            )
            public_address = "foo"
            log_file_name = "worker_1.log"

            async with task_worker_process_monitor(
                "example_package.example", "tetration"
            ) as (monitor, _):
                agent_task = asyncio.create_task(
                    agent_function(
                        *agent_function_arguments,
                        public_address,
                        log_file_name,
                        agent_server,
                        monitor,
                    )
                )

                results = []
                stop_receiving, workers_done = asyncio.Event(), asyncio.Event()
                async for batch, _ in await interface.receive_task_results(
                    stop_receiving=stop_receiving, workers_done=workers_done
                ):
                    for task_process_state in batch:
                        task_result = TaskResult.from_process_state(task_process_state)
                        assert task_result.is_success
                        results.append(task_result.result)
                        if len(results) == 4:
                            stop_receiving.set()
                            workers_done.set()
                            await interface.shutdown_workers(1, 0)
                await agent_task
                assert set(results) == {1, 4, 27, 256}
                await agent_server.close()

    @pytest.mark.asyncio
    async def test_worker_loop_failures(
        self,
        agent_server: TaskWorkerServer,
        task_worker_process_monitor: Callable[
            [str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]
        ],
    ) -> None:

        async with await self._create_driver_interface() as interface:
            await interface.setup_and_add_tasks([1, 2, 3, 4])
            (
                agent_function_name,
                agent_function_arguments,
            ) = await interface.get_agent_function(0, pickle.HIGHEST_PROTOCOL)
            agent_function = getattr(
                importlib.import_module(agent_function_name.module_name),
                agent_function_name.function_name,
            )
            public_address = "foo"
            log_file_name = "worker_1.log"

            async with task_worker_process_monitor(
                "example_package.example", "example_function_raises"
            ) as (monitor, _):
                worker_task = asyncio.create_task(
                    agent_function(
                        *agent_function_arguments,
                        public_address,
                        log_file_name,
                        agent_server,
                        monitor,
                    )
                )

                results = []
                stop_receiving, workers_done = asyncio.Event(), asyncio.Event()
                async for batch, _ in await interface.receive_task_results(
                    stop_receiving=stop_receiving, workers_done=workers_done
                ):
                    for task_process_state in batch:
                        task_result = TaskResult.from_process_state(task_process_state)
                        assert not task_result.is_success
                        results.append(task_result.result)
                        if len(results) == 8:
                            stop_receiving.set()
                            workers_done.set()
                            await interface.shutdown_workers(1, 0)
                        else:
                            await interface.retry_task(
                                task_process_state.task_id,
                                task_process_state.attempt,
                                0,
                            )
                await worker_task
                await agent_server.close()

    @pytest.mark.asyncio
    async def test_worker_loop_crash(
        self,
        agent_server: TaskWorkerServer,
        task_worker_process_monitor: Callable[
            [str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]
        ],
    ) -> None:

        async with await self._create_driver_interface() as interface:
            await interface.setup_and_add_tasks([1, 2, 3, 4])
            (
                agent_function_name,
                agent_function_arguments,
            ) = await interface.get_agent_function(0, pickle.HIGHEST_PROTOCOL)
            agent_function = getattr(
                importlib.import_module(agent_function_name.module_name),
                agent_function_name.function_name,
            )
            public_address = "foo"
            log_file_name = "worker_1.log"

            async with task_worker_process_monitor(
                "example_package.example", "crash"
            ) as (monitor, _):
                worker_task = asyncio.create_task(
                    agent_function(
                        *agent_function_arguments,
                        public_address,
                        log_file_name,
                        agent_server,
                        monitor,
                    )
                )

                results = []
                stop_receiving, workers_done = asyncio.Event(), asyncio.Event()
                async for batch, _ in await interface.receive_task_results(
                    stop_receiving=stop_receiving, workers_done=workers_done
                ):
                    for task_process_state in batch:
                        task_result = TaskResult.from_process_state(task_process_state)
                        assert not task_result.is_success
                        results.append(task_result.result)
                        if len(results) == 4:
                            stop_receiving.set()
                            workers_done.set()
                            await interface.shutdown_workers(1, 0)
                await worker_task
                await agent_server.close()
