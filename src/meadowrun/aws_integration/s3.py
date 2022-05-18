import hashlib
from typing import Dict, List, Optional, Tuple
import uuid

import boto3
from botocore.exceptions import ClientError

from meadowrun.aws_integration.aws_core import _get_default_region_name

BUCKET_PREFIX = "meadowrun"


def ensure_bucket(
    region_name: str,
    expire_days: int = 14,
) -> str:
    """Create an S3 bucket in a specified region if it does not exist yet.

    If a region is not specified, the bucket is created in the configured default
    region.

    The bucket is created with a default lifecycle policy of 14 days.

    Since bucket names must be globally unique, the name is bucket_prefix + region +
    uuid and is returned. If a bucket with the given bucket_prefix already exists, then
    that one is used.

    :param bucket_name: Bucket to create
    :param region_name: String region to create bucket in, e.g., 'us-west-2'
    :param expire_days: int number of days after which keys are deleted by lifecycle
        policy.
    :return: the full bucket name
    """

    s3 = boto3.client("s3", region_name=region_name)

    # s3 bucket names must be globally unique accross all acounts and regions.
    prefix = f"{BUCKET_PREFIX}-{region_name}"
    response = s3.list_buckets()
    for existing_bucket in response["Buckets"]:
        if existing_bucket["Name"].startswith(prefix):
            return existing_bucket["Name"]

    location = {"LocationConstraint": region_name}
    bucket_name = f"{prefix}-{str(uuid.uuid4())}"
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
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
    return bucket_name


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

    bucket_name = ensure_bucket(region_name)
    try:
        s3.head_object(Bucket=bucket_name, Key=digest)
        return bucket_name, digest
    except ClientError as error:
        if not error.response["Error"]["Code"] == "404":
            raise error

    # doesn't exist, need to upload it
    s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=digest)
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


def delete_all_buckets(region_name: str) -> None:
    """Deletes all meadowrun buckets in given region."""
    s3 = boto3.client("s3", region_name=region_name)

    prefix = f"{BUCKET_PREFIX}-{region_name}"
    response = s3.list_buckets()
    for existing_bucket in response["Buckets"]:
        if existing_bucket["Name"].startswith(prefix):
            bucket_name = existing_bucket["Name"]
            break
    else:
        return

    # easier to work with resource now
    s3 = boto3.resource("s3", region_name=region_name)
    bucket = s3.Bucket(bucket_name)

    # S3 doesn't allow deleting a bucket with anything in it, so delete all objects in
    # chunks of up to 1000, which is the maximum allowed.
    key_chunk: List[Dict[str, str]] = []
    for object in bucket.objects.all():
        if len(key_chunk) == 1000:
            bucket.delete_objects(Delete=dict(Objects=key_chunk))
            key_chunk.clear()
        key_chunk.append(dict(Key=object.key))
    bucket.delete_objects(Delete=dict(Objects=key_chunk))

    bucket.delete()
