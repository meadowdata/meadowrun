from __future__ import annotations

import asyncio
import pickle
import shutil
import sys
import traceback
import zipfile
from typing import Optional, Tuple, TypeVar, IO, TYPE_CHECKING


from meadowrun.meadowrun_pb2 import ProcessState

if TYPE_CHECKING:
    from os import PathLike
    from typing_extensions import Literal


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


def unpickle_exception(bs: bytes) -> Tuple[str, str, str]:
    return pickle.loads(bs)


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


def remove_corrupted_environment(path: str) -> None:
    try:
        shutil.rmtree(path, True)
    except asyncio.CancelledError:
        raise
    except BaseException:
        print(
            f"Warning, exception trying to delete {path}, this environment is corrupted"
        )
        traceback.print_exc()


async def cancel_task(task: asyncio.Task) -> None:
    """Cancel a task, wait for it, don't throw CancelledError."""
    try:
        task.cancel()
        await task
    except asyncio.CancelledError:
        pass


def create_zipfile(
    file: str | PathLike[str] | IO[bytes],
    mode: Literal["r", "w", "x", "a"],
    compression: int,
) -> zipfile.ZipFile:
    """
    Equivalent to zipfile.ZipFile(...) but passes strict_timestamps=False if that
    parameter is available in the current version of python
    """
    if sys.version_info < (3, 8):
        # strict_timestamps parameter was addded in 3.8:
        # https://docs.python.org/3/library/zipfile.html#zipfile.ZipFile
        return zipfile.ZipFile(file, mode, compression)
    else:
        return zipfile.ZipFile(file, mode, compression, strict_timestamps=False)
