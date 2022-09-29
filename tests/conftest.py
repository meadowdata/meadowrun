from __future__ import annotations

import asyncio

import contextlib
from pathlib import Path
import pathlib
import pickle
import sys
from typing import (
    TYPE_CHECKING,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Iterable,
    Tuple,
)

import pytest

from meadowrun.run_job_local import WorkerContainerMonitor, WorkerProcessMonitor

if TYPE_CHECKING:
    from meadowrun.run_job_local import TaskWorkerServer

# See comment in meadowrun/__init__.py
if sys.platform == "win32" and sys.version_info < (3, 8):

    @pytest.fixture
    def event_loop() -> Iterable[asyncio.AbstractEventLoop]:
        loop = asyncio.ProactorEventLoop()
        yield loop
        loop.close()


@pytest.fixture
@pytest.mark.asyncio
async def agent_server() -> AsyncIterable[TaskWorkerServer]:
    from meadowrun.run_job_local import TaskWorkerServer

    server = await TaskWorkerServer.start_serving("127.0.0.1")
    yield server
    await server.close()


PATH_TO_TASK_WORKER = (
    Path("src") / "meadowrun" / "func_worker" / "__meadowrun_task_worker.py"
)


@pytest.fixture
@pytest.mark.asyncio
async def task_worker_process_monitor(
    tmp_path: Path, agent_server: TaskWorkerServer
) -> Callable[[str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]]:
    @contextlib.asynccontextmanager
    async def task_worker_monitor_helper(
        module_name: str,
        function_name: str,
    ) -> AsyncGenerator[Tuple[WorkerProcessMonitor, Path], None]:

        try:
            python = Path(sys.executable)
            io_path = tmp_path / "testagent"
            monitor = WorkerProcessMonitor(
                [
                    str(python),
                    str(PATH_TO_TASK_WORKER),
                    "--io-path",
                    str(io_path),
                    "--result-highest-pickle-protocol",
                    str(pickle.HIGHEST_PROTOCOL),
                    "--module-name",
                    module_name,
                    "--function-name",
                    function_name,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(agent_server.port),
                ],
                working_directory=None,
                env_vars={"PYTHONPATH": "tests/example_user_code"},
            )
            await monitor.start_and_tail()
            yield monitor, io_path
        finally:
            await monitor.cleanup()

    return task_worker_monitor_helper


@pytest.fixture
@pytest.mark.asyncio
async def task_worker_container_monitor(
    tmp_path: Path, agent_server: TaskWorkerServer
) -> Callable[[str, str], AsyncContextManager[Tuple[WorkerContainerMonitor, Path]]]:
    @contextlib.asynccontextmanager
    async def task_worker_container_helper(
        module_name: str,
        function_name: str,
    ) -> AsyncGenerator[Tuple[WorkerContainerMonitor, Path], None]:
        io_path = tmp_path / "testagent"  # tmp_path bound in container
        try:
            cmd = [
                "python",
                str(PATH_TO_TASK_WORKER),
                "--io-path",
                "/tmp/testagent",
                "--result-highest-pickle-protocol",
                str(pickle.HIGHEST_PROTOCOL),
                "--module-name",
                module_name,
                "--function-name",
                function_name,
                "--host",
                "host.docker.internal",
                "--port",
                str(agent_server.port),
            ]
            monitor = WorkerContainerMonitor(
                client=None,
                image="python:3.9-slim",
                cmd=cmd,  #: Optional[List[str]],
                environment_variables={
                    "PYTHONPATH": "/var/meadowrun/tests/example_user_code"
                },
                working_directory="/var/meadowrun",
                binds=[
                    (
                        str(pathlib.Path(__file__).parent.parent.absolute()),
                        "/var/meadowrun",
                    ),
                    (str(tmp_path), "/tmp"),
                ],
                ports=[],
                extra_hosts=[],
                uses_gpus=False,
            )
            await monitor.start_and_tail()
            yield monitor, io_path
        finally:
            await monitor.stop()
            if monitor.docker_client:
                await monitor.docker_client.close()

    return task_worker_container_helper
