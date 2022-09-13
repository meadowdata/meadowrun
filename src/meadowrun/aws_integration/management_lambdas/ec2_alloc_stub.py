"""
This is code that "should" be part of ec2_instance_allocation.py, but is also needed by
adjust_ec2_instances.

This module contains code that will run in the generic AWS Lambda environment, so it
should not refer to any outside code
"""
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

if TYPE_CHECKING:
    from typing_extensions import Literal

import botocore.exceptions

_T = TypeVar("_T")

# a dynamodb table that holds information about EC2 instances we've created and what has
# been allocated to which instances
_EC2_ALLOC_TABLE_NAME = "meadowrun_ec2_alloc_table"

# names of attributes/keys in the EC2 alloc table
_PUBLIC_ADDRESS = "public_address"
_INSTANCE_ID = "instance_id"
_RESOURCES_AVAILABLE = "resources_available"
_NON_CONSUMABLE_RESOURCES = "non_consumable_resources"
_RESOURCES_ALLOCATED = "resources_allocated"
_ALLOCATED_TIME = "allocated_time"
_LAST_UPDATE_TIME = "last_update_time"
_PREVENT_FURTHER_ALLOCATION = "prevent_further_allocation"
_RUNNING_JOBS = "running_jobs"
_JOB_ID = "job_id"

# A tag for EC2 instances that are created using ec2_alloc
_EC2_ALLOC_TAG = "meadowrun_ec2_alloc"
_EC2_ALLOC_TAG_VALUE = "TRUE"

_MEADOWRUN_GENERATED_DOCKER_REPO = "meadowrun_generated"


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
