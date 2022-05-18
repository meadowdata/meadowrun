from __future__ import annotations

import pickle
import traceback
from typing import Optional, TypeVar

from meadowrun.meadowrun_pb2 import ProcessState


def pickle_exception(e: Exception, pickle_protocol: int) -> bytes:
    """
    We generally don't want to pickle exceptions directly--there's no guarantee that a
    random exception that was thrown can be unpickled in a different process.
    """
    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return pickle.dumps(
        (str(type(e)), str(e), tb),
        protocol=pickle_protocol,
    )


COMPLETED_PROCESS_STATES = {
    ProcessState.ProcessStateEnum.SUCCEEDED,
    ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
    ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
    ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
    ProcessState.ProcessStateEnum.RESOURCES_NOT_AVAILABLE,
    ProcessState.ProcessStateEnum.ERROR_GETTING_STATE,
}


_T = TypeVar("_T")


def assert_is_not_none(resources: Optional[_T]) -> _T:
    """A helper for mypy"""
    assert resources is not None
    return resources
