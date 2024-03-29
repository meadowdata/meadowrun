from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Set,
    Tuple,
    TypeVar,
    Union,
    overload,
)
import botocore.exceptions

if TYPE_CHECKING:
    from typing_extensions import Literal

_T = TypeVar("_T")


@overload
def ignore_boto3_error_code(
    func: Callable[[], _T],
    error_codes: Union[str, Set[str]],
    return_error_code: Literal[False] = False,
) -> Union[Tuple[Literal[True], _T], Tuple[Literal[False], None]]:
    pass


@overload
def ignore_boto3_error_code(
    func: Callable[[], _T],
    error_codes: Union[str, Set[str]],
    return_error_code: Literal[True],
) -> Union[Tuple[Literal[True], _T, None], Tuple[Literal[False], None, str]]:
    pass


def ignore_boto3_error_code(
    func: Callable[[], _T],
    error_codes: Union[str, Set[str]],
    return_error_code: bool = False,
) -> Union[
    Tuple[Literal[True], _T],
    Tuple[Literal[False], None],
    Tuple[Literal[True], _T, None],
    Tuple[Literal[False], None, str],
]:
    """
    Calls func.
    - If func succeeds:
        - And return_error_code is False, returns (True, result of func)
        - And return_error_code is True, returns (True, result of func, None)
    - If func raises a boto3 error with the specified code:
        - And return_error_code is False, returns (False, None)
        - And return_error_code is True, returns (True, None, error code), where error
          code is one of the strings in the error_codes argument
    - If func raises any other type of exception (i.e. a boto3 error with a different
      code or a different type of error altogether), then the exception is raised
      normally.
    """
    if isinstance(error_codes, str):
        error_codes = {error_codes}

    try:
        result = func()
        if return_error_code:
            return True, result, None
        else:
            return True, result
    except botocore.exceptions.ClientError as e:
        if "Error" in e.response:
            error = e.response["Error"]
            if "Code" in error and error["Code"] in error_codes:
                if return_error_code:
                    return False, None, error["Code"]
                else:
                    return False, None

        raise


async def ignore_boto3_error_code_async(
    func: Awaitable[_T],
    error_codes: Union[str, Set[str]],
) -> Union[Tuple[Literal[True], _T, None], Tuple[Literal[False], None, str]]:
    """Same semantics as ignore_boto3_error_code"""
    if isinstance(error_codes, str):
        error_codes = {error_codes}

    try:
        result = await func
        return True, result, None
    except botocore.exceptions.ClientError as e:
        if "Error" in e.response:
            error = e.response["Error"]
            if "Code" in error and error["Code"] in error_codes:
                return False, None, error["Code"]

        raise
