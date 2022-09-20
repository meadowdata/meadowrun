import pickle

import pytest
from meadowrun import TaskResult
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import TaskException
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


def test_task_result_from_process_state_success() -> None:
    task_result = TaskResult.from_process_state(
        2,
        1,
        ProcessState(
            state=ProcessState.ProcessStateEnum.SUCCEEDED,
            pickled_result=pickle.dumps("OK"),
        ),
    )
    assert task_result.task_id == 2
    assert task_result.attempt == 1
    assert task_result.is_success
    assert task_result.result == "OK"


def test_task_result_from_process_state_exception() -> None:
    task_result = TaskResult.from_process_state(
        2,
        1,
        ProcessState(
            state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
            pickled_result=pickle_exception(Exception("test"), pickle.HIGHEST_PROTOCOL),
        ),
    )
    assert task_result.task_id == 2
    assert task_result.attempt == 1
    assert not task_result.is_success
    assert task_result.exception is not None
