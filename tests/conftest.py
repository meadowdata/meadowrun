from __future__ import annotations

import asyncio
from asyncio import subprocess
import contextlib
from pathlib import Path
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

if TYPE_CHECKING:
    from meadowrun._vendor.aiodocker import Docker
    from meadowrun._vendor.aiodocker.containers import DockerContainer
    from meadowrun.run_job_local import AgentTaskWorkerServer

# See comment in meadowrun/__init__.py
if sys.platform == "win32" and sys.version_info < (3, 8):

    @pytest.fixture
    def event_loop() -> Iterable[asyncio.AbstractEventLoop]:
        loop = asyncio.ProactorEventLoop()
        yield loop
        loop.close()


@pytest.fixture
@pytest.mark.asyncio
async def agent_server() -> AsyncIterable[AgentTaskWorkerServer]:
    from meadowrun.run_job_local import agent_start_serving

    server = await agent_start_serving("127.0.0.1")
    yield server
    await server.close()


PATH_TO_TASK_WORKER = (
    Path("src") / "meadowrun" / "func_worker" / "__meadowrun_task_worker.py"
)


@pytest.fixture
@pytest.mark.asyncio
async def task_worker_process(
    tmp_path: Path, agent_server: AgentTaskWorkerServer
) -> Callable[[str, str], AsyncContextManager[Tuple[subprocess.Process, Path]]]:
    @contextlib.asynccontextmanager
    async def task_worker_process_helper(
        module_name: str,
        function_name: str,
    ) -> AsyncGenerator[Tuple[subprocess.Process, Path], None]:
        python = Path(sys.executable)
        io_path = tmp_path / "testagent"
        try:
            proc = await subprocess.create_subprocess_exec(
                python,
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
                env={"PYTHONPATH": "tests/example_user_code"},
            )
            yield proc, io_path
        finally:
            try:
                proc.terminate()
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass

    return task_worker_process_helper


@pytest.fixture
@pytest.mark.asyncio
async def task_worker_container(
    tmp_path: Path, agent_server: AgentTaskWorkerServer
) -> Callable[[str, str], AsyncContextManager[Tuple[DockerContainer, Docker, Path]]]:

    from meadowrun.docker_controller import run_container

    @contextlib.asynccontextmanager
    async def task_worker_container_helper(
        module_name: str,
        function_name: str,
    ) -> AsyncGenerator[Tuple[DockerContainer, Docker, Path], None]:
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
            container, docker = await run_container(
                client=None,
                image="python:3.9-slim",
                cmd=cmd,  #: Optional[List[str]],
                environment_variables={
                    "PYTHONPATH": "/var/meadowrun/tests/example_user_code"
                },
                working_directory="/var/meadowrun",
                binds=[
                    ("/home/kurts/projects/meadowrun/", "/var/meadowrun"),
                    (str(tmp_path), "/tmp"),
                ],
                ports=[],
                extra_hosts=[],
                all_gpus=False,
            )

            async def tail_log() -> None:
                async for line in container.log(stdout=True, stderr=True, follow=True):
                    print(f"Task worker: {line}", end="")

            tail_log_task = asyncio.create_task(tail_log())

            yield container, docker, io_path
        finally:
            if container:
                await container.stop()
                await container.wait()
            if tail_log_task:
                await tail_log_task
            await docker.close()

    return task_worker_container_helper
