from __future__ import annotations

import asyncio
import pickle
import sys
from typing import TYPE_CHECKING, Any, AsyncContextManager, Callable, Tuple

from meadowrun.run_job_local import restart_worker

if TYPE_CHECKING:
    from meadowrun.run_job_local import TaskWorkerServer
    from pathlib import Path
    from meadowrun.run_job_local import (
        WorkerContainerMonitor,
        WorkerProcessMonitor,
        WorkerMonitor,
    )

import pytest

PATH_TO_TASK_WORKER = "src/meadowrun/func_worker/__meadowrun_task_worker.py"

if sys.platform != "linux":
    pytest.skip("Skipping linux-only tests", allow_module_level=True)


async def wait_for_path(path: Path) -> None:
    while not path.exists():
        await asyncio.sleep(0.1)


async def _assert_agent_result(
    io_path: Path, expected_state: str, expected_result: Any
) -> None:

    state_path = io_path.with_suffix(".state")
    await asyncio.wait_for(wait_for_path(state_path), timeout=2)
    with open(state_path, "r", encoding="utf-8") as f:
        state = f.readline()

    with open(io_path.with_suffix(".result"), "rb") as f:
        result = pickle.load(f)

    assert state == expected_state
    if expected_result:
        assert result == expected_result


async def send_receive(agent_server: TaskWorkerServer) -> None:
    long_string = "loooong" * 10_000
    messages = [
        (
            (("arg1", 12654), {"kw_arg1": 3.1415, "kw_arg2": "kw_arg2"}),
            "SUCCEEDED",
            "arg1126543.1415kw_arg2",
        ),
        (
            (("arg1", 12654), {"kw_arg1": 3.1415, "kw_arg2": long_string}),
            "SUCCEEDED",
            "arg1126543.1415" + long_string,
        ),
        ((("arg1",), {}), "PYTHON_EXCEPTION", None),
        (
            (("arg1", 12), {"kw_arg1": 2.71, "kw_arg2": "kw"}),
            "SUCCEEDED",
            "arg1122.71kw",
        ),
        ((("",), {"noarg": 23}), "PYTHON_EXCEPTION", None),
    ]
    await agent_server.wait_for_task_worker_connection(timeout=2)
    for msg, expected_state, expected_result in messages:

        await agent_server.send_message(pickle.dumps(msg))
        result = await agent_server.receive_message()

        assert expected_state == result[0]
        if expected_result is not None:
            assert expected_result == result[1]


@pytest.mark.asyncio
async def test_call_process(
    agent_server: TaskWorkerServer,
    task_worker_process_monitor: Callable[
        [str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]
    ],
) -> None:
    async with task_worker_process_monitor(
        "example_package.example", "example_function"
    ) as (monitor, io_path):
        await _check_start(agent_server, monitor, io_path)
        assert monitor.pid is not None


async def _check_start(
    agent_server: TaskWorkerServer, monitor: WorkerMonitor, io_path: Path
) -> None:
    await send_receive(agent_server)
    await agent_server.close_task_worker_connection()
    result = await monitor.wait_until_exited()
    assert result == 0
    await _assert_agent_result(io_path, "SUCCEEDED", None)


@pytest.mark.asyncio
async def test_restart_process(
    agent_server: TaskWorkerServer,
    task_worker_process_monitor: Callable[
        [str, str], AsyncContextManager[Tuple[WorkerProcessMonitor, Path]]
    ],
) -> None:
    async with task_worker_process_monitor(
        "example_package.example", "example_function"
    ) as (monitor, io_path):

        await _check_restart(agent_server, monitor, io_path)
        assert monitor.pid is not None


async def _check_restart(
    agent_server: TaskWorkerServer, monitor: WorkerMonitor, io_path: Path
) -> None:
    await send_receive(agent_server)

    await restart_worker(agent_server, monitor)

    await send_receive(agent_server)

    await agent_server.close_task_worker_connection()
    result = await monitor.wait_until_exited()
    assert result == 0
    await _assert_agent_result(io_path, "SUCCEEDED", None)


@pytest.mark.asyncio
async def test_call_container(
    agent_server: TaskWorkerServer,
    task_worker_container_monitor: Callable[
        [str, str], AsyncContextManager[Tuple[WorkerContainerMonitor, Path]]
    ],
) -> None:
    async with task_worker_container_monitor(
        "example_package.example", "example_function"
    ) as (
        monitor,
        io_path,
    ):
        await _check_start(agent_server, monitor, io_path)
        assert monitor.container_id is not None


@pytest.mark.asyncio
async def test_restart_container(
    agent_server: TaskWorkerServer,
    task_worker_container_monitor: Callable[
        [str, str], AsyncContextManager[Tuple[WorkerContainerMonitor, Path]]
    ],
) -> None:
    async with task_worker_container_monitor(
        "example_package.example", "example_function"
    ) as (
        monitor,
        io_path,
    ):

        await _check_restart(agent_server, monitor, io_path)
        assert monitor.container_id is not None
