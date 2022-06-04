from __future__ import annotations

from typing import Any, Iterable, Optional

import aiohttp
import aiohttp.client_exceptions
import boto3


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
            return await response.text()
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
        return (await response.text()).strip()


def _get_account_number() -> str:
    # weird that we have to do this to get the account number to construct the ARN
    return boto3.client("sts").get_caller_identity().get("Account")
