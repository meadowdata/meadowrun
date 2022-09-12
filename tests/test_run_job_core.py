import pickle

import pytest

from meadowrun import TaskResult
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
