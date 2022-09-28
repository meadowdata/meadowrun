import pickle

import pytest
from meadowrun import TaskResult
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import TaskException, TaskProcessState
from meadowrun.shared import pickle_exception


def test_task_result() -> None:
    task_result = TaskResult(
        1,
        is_success=True,
        state=ProcessState.ProcessStateEnum.Name(
            ProcessState.ProcessStateEnum.SUCCEEDED
        ),
        result="good result",
    )
    assert task_result.result_or_raise() == "good result"

    task_result = TaskResult[str](
        2,
        is_success=False,
        state=ProcessState.ProcessStateEnum.Name(
            ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
        ),
        exception=pickle.loads(
            pickle_exception(Exception("test"), pickle.HIGHEST_PROTOCOL)
        ),
    )
    with pytest.raises(TaskException):
        task_result.result_or_raise()

    task_result = TaskResult[None](
        3,
        is_success=False,
        state=ProcessState.ProcessStateEnum.Name(
            ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
        ),
    )
    with pytest.raises(TaskException):
        task_result.result_or_raise()


def test_task_result_from_process_state_success() -> None:
    task_result = TaskResult.from_process_state(
        TaskProcessState(
            2,
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pickled_result=pickle.dumps("OK"),
            ),
        )
    )
    assert task_result.task_id == 2
    assert task_result.attempt == 1
    assert task_result.is_success
    assert task_result.result == "OK"
    assert task_result.state == "SUCCEEDED"


def test_task_result_from_process_state_exception() -> None:
    task_result = TaskResult.from_process_state(
        TaskProcessState(
            2,
            1,
            ProcessState(
                state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pickled_result=pickle_exception(
                    Exception("test"), pickle.HIGHEST_PROTOCOL
                ),
            ),
        )
    )
    assert task_result.task_id == 2
    assert task_result.attempt == 1
    assert not task_result.is_success
    assert task_result.exception is not None
    assert task_result.state == "PYTHON_EXCEPTION"
