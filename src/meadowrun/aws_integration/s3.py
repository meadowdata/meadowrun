import hashlib
from typing import Dict, List, Optional, Tuple

import boto3
import boto3.exceptions
from botocore.exceptions import ClientError

from meadowrun.aws_integration.aws_core import (
    MeadowrunAWSAccessError,
    MeadowrunNotInstalledError,
    _get_account_number,
    _get_default_region_name,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)

BUCKET_PREFIX = "meadowrun"


def _get_bucket_name(region_name: str) -> str:
    # s3 bucket names must be globally unique across all accounts and regions.
    return f"{BUCKET_PREFIX}-{region_name}-{_get_account_number()}"


def ensure_bucket(
    region_name: str,
    expire_days: int = 14,
) -> None:
    """
    Create an S3 bucket in a specified region if it does not exist yet.

    If a region is not specified, the bucket is created in the configured default
    region.

    The bucket is created with a default lifecycle policy of 14 days.

    Since bucket names must be globally unique, the name is bucket_prefix + region +
    account number

    :param bucket_name: Bucket to create
    :param region_name: String region to create bucket in, e.g., 'us-west-2'
    :param expire_days: int number of days after which keys are deleted by lifecycle
        policy.
    :return: the full bucket name
    """

    s3 = boto3.client("s3", region_name=region_name)

    bucket_name = _get_bucket_name(region_name)

    location = {"LocationConstraint": region_name}

    success, _ = ignore_boto3_error_code(
        lambda: s3.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location
        ),
        "BucketAlreadyOwnedByYou",
    )
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=dict(
            Rules=[
                dict(
                    Expiration=dict(
                        Days=expire_days,
                    ),
                    ID="meadowrun-lifecycle-policy",
                    # Filter is mandatory, but we don't want one:
                    Filter=dict(Prefix=""),
                    Status="Enabled",
                )
            ]
        ),
    )


async def ensure_uploaded(
    file_path: str, region_name: Optional[str] = None
) -> Tuple[str, str]:

    if region_name is None:
        region_name = await _get_default_region_name()

    s3 = boto3.client("s3", region_name=region_name)

    hasher = hashlib.blake2b()
    with open(file_path, "rb") as file:
        buf = file.read()
        hasher.update(buf)
    digest = hasher.hexdigest()

    bucket_name = _get_bucket_name(region_name)
    try:
        s3.head_object(Bucket=bucket_name, Key=digest)
        return bucket_name, digest
    except ClientError as error:
        if error.response["Error"]["Code"] == "403":
            # if we don't have permissions to the bucket, throw a helpful error
            raise MeadowrunAWSAccessError("S3 bucket") from error

        # don't raise an error saying the file doesn't exist, we'll just upload it in
        # that case by falling through to the next bit of code
        if not error.response["Error"]["Code"] == "404":
            raise error

    # doesn't exist, need to upload it
    try:
        s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=digest),
    except boto3.exceptions.S3UploadFailedError as e:
        if len(e.args) >= 1 and "NoSuchBucket" in e.args[0]:
            raise MeadowrunNotInstalledError("S3 bucket")
        raise

    return bucket_name, digest


async def download_file(
    bucket_name: str,
    object_name: str,
    file_name: str,
    region_name: Optional[str] = None,
) -> None:
    if region_name is None:
        region_name = await _get_default_region_name()

    s3 = boto3.client("s3", region_name=region_name)

    s3.download_file(bucket_name, object_name, file_name)


def delete_bucket(region_name: str) -> None:
    """Deletes the meadowrun bucket"""
    s3 = boto3.resource("s3", region_name=region_name)

    bucket = s3.Bucket(_get_bucket_name(region_name))
    success, _ = ignore_boto3_error_code(
        lambda: list(bucket.objects.limit(1)),
        "NoSuchBucket",
    )
    if success:
        # S3 doesn't allow deleting a bucket with anything in it, so delete all objects
        # in chunks of up to 1000, which is the maximum allowed.
        key_chunk: List[Dict[str, str]] = []
        for s3object in bucket.objects.all():
            if len(key_chunk) == 1000:
                bucket.delete_objects(Delete=dict(Objects=key_chunk))
                key_chunk.clear()
            key_chunk.append(dict(Key=s3object.key))
        if key_chunk:
            bucket.delete_objects(Delete=dict(Objects=key_chunk))

        bucket.delete()
