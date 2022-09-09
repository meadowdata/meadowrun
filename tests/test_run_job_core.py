import asyncio
from dataclasses import dataclass
import pickle
from typing import AsyncIterable, Callable, Tuple

import asyncssh
import pytest
from meadowrun import TaskResult
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import GridJobDriver, TaskException
from meadowrun.shared import pickle_exception


def test_task_result() -> None:
    task_result = TaskResult(1, is_success=True, result="good result")
    assert task_result.result_or_raise() == "good result"

    task_result = TaskResult[str](
        2,
        is_success=False,
        exception=pickle.loads(
            pickle_exception(Exception("test"), pickle.HIGHEST_PROTOCOL)
        ),
    )
    with pytest.raises(TaskException):
        task_result.result_or_raise()

    task_result = TaskResult[None](3, is_success=False)
    with pytest.raises(TaskException):
        task_result.result_or_raise()


@dataclass(frozen=True)
class TestGridJobDriver(GridJobDriver):
    def worker_function(self) -> Callable[[str, int], None]:
        return lambda p, w: None

    async def receive_task_results(
        self,
        *,
        stop_receiving: asyncio.Event,
        workers_done: asyncio.Event,
    ) -> AsyncIterable[Tuple[int, int, ProcessState]]:
        yield (
            1,
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=pickle.dumps("good result"),
            ),
        )
        yield (
            2,
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pickled_result=pickle_exception(
                    Exception("test"), pickle.HIGHEST_PROTOCOL
                ),
            ),
        )
        yield (
            3,
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
                return_code=2,
            ),
        )

    async def retry_task(self, task_id: int, attempts_so_far: int) -> None:
        raise NotImplementedError()


@pytest.mark.asyncio
async def test_grid_job_driver() -> None:
    def f(i: str) -> str:
        ...

    helper = TestGridJobDriver(
        "test_region",
        {"host1": ["worker1", "worker2"], "host2": ["worker3"]},
        "sshname",
        asyncssh.SSHKey(),
        4,
        4,
        f,
    )

    results = []
    async for res in helper.get_results_as_completed(asyncio.Event(), 1):
        results.append(res)

    assert results[0] == TaskResult(1, is_success=True, result="good result")
    assert not results[1].is_success
    assert results[1].exception is not None
    assert results[2] == TaskResult(3, is_success=False)
