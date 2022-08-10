from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Any, Callable, Iterable, Optional, Set, TypeVar, Union

import aiohttp
import aiohttp.client_exceptions
import boto3
import botocore.exceptions

_T = TypeVar("_T")

# the IAM user group that gives users the permissions they need to run Meadowrun jobs
_MEADOWRUN_USER_GROUP_NAME = "meadowrun_user_group"


class MeadowrunNotInstalledError(Exception):
    def __init__(self, missing_resource: str):
        super().__init__(
            f"Meadowrun resource is not available: {missing_resource}. Please run "
            "`meadowrun-manage-ec2 install` as an AWS root/Administrator account"
        )


class MeadowrunAWSAccessError(Exception):
    def __init__(self, resource: str, custom_exception_message: bool = False):
        if custom_exception_message:
            message = resource
        else:
            message = (
                f"Encountered an AWS access exception trying to access {resource}. If "
                f"you are not a member of the {_MEADOWRUN_USER_GROUP_NAME} user group, "
                "please add yourself/ask to to be added to this group with `aws iam "
                f"add-user-to-group --group-name {_MEADOWRUN_USER_GROUP_NAME} "
                "--user-name <username>`. If this group does not exist, please run "
                "`meadowrun-manage-ec2 install` as an AWS root/Administrator account"
            )
        super().__init__(message)


def wrap_access_or_install_errors(
    func: Callable[[], _T],
    resource: str,
    access_error_code: Union[str, Set[str]],
    install_error_code: Union[str, Set[str]],
) -> _T:
    """
    A variation on ignore_boto3_error_code that would otherwise require wrapping func in
    two lambdas. Makes sense for operations that are the first operation a user will
    try, so that we can give them helpful information if Meadowrun hasn't been installed
    yet or they don't have the right permissions.
    """
    if isinstance(access_error_code, str):
        access_error_code = {access_error_code}

    if isinstance(install_error_code, str):
        install_error_code = {install_error_code}

    try:
        return func()
    except botocore.exceptions.ClientError as e:
        if "Error" in e.response:
            error = e.response["Error"]
            if "Code" in error:
                if error["Code"] in access_error_code:
                    raise MeadowrunAWSAccessError(resource) from e
                elif error["Code"] in install_error_code:
                    raise MeadowrunNotInstalledError(resource) from e

        raise


def _boto3_paginate(method: Any, **kwargs: Any) -> Iterable[Any]:
    paginator = method.__self__.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for item in page:
            yield item


# on WSL, the EC2 metadata endpoint hangs instead of immediately failing to connect. We
# have a relatively short timeout here as we should never have network issues with the
# EC2 metadata endpoint
_EC2_METADATA_ENDPOINT_TIMEOUT_SECS = 1


async def _get_ec2_metadata(url_suffix: str) -> Optional[str]:
    """
    Queries the EC2 metadata endpoint, which gives us information about the EC2 instance
    we're currently running on:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    Returns None if the endpoint is not available because e.g. we're not running on an
    EC2 instance.
    """
    try:
        async with aiohttp.request(
            "GET",
            f"http://169.254.169.254/latest/meta-data/{url_suffix}",
            timeout=aiohttp.ClientTimeout(total=_EC2_METADATA_ENDPOINT_TIMEOUT_SECS),
        ) as response:
            response.raise_for_status()
            return await response.text()
    except asyncio.CancelledError:
        raise
    except Exception:
        # the AWS metadata endpoint is not available, probably because we're not on
        # an EC2 instance.
        pass

    return None


async def _get_default_region_name() -> str:
    """
    Tries to get the default region name. E.g. us-east-2. First sees if the AWS CLI is
    set up, and returns the equivalent of `aws configure get region`. Then checks if we
    are running on an EC2 instance in which case we check the AWS metadata endpoint

    TODO this function is overused almost everywhere. Currently, we just always use the
    default region for everything and don't support multi-region deployments, but region
    should be a first-class concept.
    """

    default_session = boto3._get_default_session()
    if default_session is not None and default_session.region_name:
        # equivalent of `aws configure get region`
        return default_session.region_name
    else:
        result = await _get_ec2_metadata("placement/region")
        if result:
            return result
        else:
            raise ValueError(
                "region_name was not specified, we are not logged into AWS locally, and"
                " we're not running on an EC2 instance, so we have no way of picking a "
                "default region."
            )


def _iam_role_exists(iam_client: Any, role_name: str) -> bool:
    """Returns True if the specified IAM role exists, otherwise returns False"""
    try:
        iam_client.get_role(RoleName=role_name)
        return True
    except Exception as e:
        # Unfortunately boto3 appears to have dynamic exception types. So type(e) would
        # be "botocore.errorfactory.NoSuchEntityException", but NoSuchEntityException
        # can't be imported from botocore.errorfactory.
        if type(e).__name__ == "NoSuchEntityException":
            return False

        raise


async def _get_current_ip_for_ssh() -> str:
    """
    Gets an ip address for the current machine that is likely to work for allowing SSH
    into an EC2 instance.
    """
    # if we're already in an EC2 instance, use the EC2 metadata to get our private IP
    private_ip = await _get_ec2_metadata("local-ipv4")
    if private_ip:
        return private_ip

    # otherwise, we'll use checkip.amazonaws.com to figure out how AWS sees our IP
    async with aiohttp.request("GET", "https://checkip.amazonaws.com/") as response:
        response.raise_for_status()
        return (await response.text()).strip()


@lru_cache(maxsize=1)
def _get_account_number() -> str:
    # weird that we have to do this to get the account number to construct the ARN
    return boto3.client("sts").get_caller_identity().get("Account")
