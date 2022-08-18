import asyncio
import pickle
from typing import AsyncIterable, Optional, Tuple
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import RunMapHelper, TaskException, TaskResult
from meadowrun.shared import pickle_exception
import pytest
import asyncssh


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


@pytest.mark.asyncio
async def test_run_map_helper() -> None:
    async def test_results_generator(
        workers_done: Optional[asyncio.Event],
    ) -> AsyncIterable[Tuple[int, ProcessState]]:
        yield (
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=pickle.dumps("good result"),
            ),
        )
        yield (
            2,
            ProcessState(
                state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pickled_result=pickle_exception(
                    Exception("test"), pickle.HIGHEST_PROTOCOL
                ),
            ),
        )
        yield (
            3,
            ProcessState(
                state=ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
                return_code=2,
            ),
        )

    helper = RunMapHelper(
        "test_region",
        {"host1": ["worker1", "worker2"], "host2": ["worker3"]},
        lambda address, id: None,
        "sshname",
        asyncssh.SSHKey(),
        4,
        test_results_generator,
    )

    results = []
    async for res in helper.get_results_as_completed(None):
        results.append(res)

    assert results[0] == TaskResult(1, is_success=True, result="good result")
    assert not results[1].is_success
    assert results[1].exception is not None
    assert results[2] == TaskResult(3, is_success=False)
