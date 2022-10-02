from __future__ import annotations

import dataclasses
import hashlib
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import aiobotocore.session
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
from meadowrun.object_storage import ObjectStorage
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

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

    # us-east-1 cannot be specified as a LocationConstraint because it is the default
    # region
    # https://stackoverflow.com/questions/51912072/invalidlocationconstraint-error-while-creating-s3-bucket-when-the-used-command-i
    if region_name == "us-east-1":
        additional_parameters = {}
    else:
        additional_parameters = {
            "CreateBucketConfiguration": {"LocationConstraint": region_name}
        }

    success, _ = ignore_boto3_error_code(
        lambda: s3.create_bucket(Bucket=bucket_name, **additional_parameters),
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


async def ensure_uploaded_by_hash(
    s3_client: S3Client,
    local_file_path: str,
    bucket_name: str,
    key_prefix: str = "",
) -> str:
    """
    Uploads the specified file from the local machine to S3. The file will be uploaded
    to the key "{key_prefix}{hash}", where {hash} is a hash of the contents of the file.
    If the S3 object already exists, it does not need to be re-uploaded.

    key_prefix should usually be "" or end in a "/" like "code/". key_suffix should
    usually be "" or something like ".tar.gz"

    Returns the key of the file in the S3 bucket, e.g. "code/123456789abcdefg"
    """

    hasher = hashlib.blake2b()
    with open(local_file_path, "rb") as file:
        buf = file.read()
        hasher.update(buf)
    s3_key = f"{key_prefix}{hasher.hexdigest()}"

    try:
        await s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return s3_key
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
        await s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buf)
    except boto3.exceptions.S3UploadFailedError as e:
        if len(e.args) >= 1 and "NoSuchBucket" in e.args[0]:
            raise MeadowrunNotInstalledError("S3 bucket")
        raise

    return s3_key


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


def upload(object_name: str, data: bytes, region_name: str) -> None:
    s3 = boto3.client("s3", region_name=region_name)
    bucket_name = _get_bucket_name(region_name)
    with BytesIO(data) as file_obj:
        s3.upload_fileobj(file_obj, bucket_name, object_name)


async def upload_async(
    object_name: str, data: bytes, region_name: str, s3_client: Any
) -> None:
    bucket_name = _get_bucket_name(region_name)
    await s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)


async def list_objects_async(
    s3_client: Any,
    bucket_name: str,
    prefix: str,
    start_after: str,
) -> List[str]:
    """Returns the keys in the meadowrun bucket."""
    paginator = s3_client.get_paginator("list_objects_v2")
    results = []
    async for result in paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, StartAfter=start_after
    ):
        for c in result.get("Contents", []):
            results.append(c["Key"])
    return results


async def download_async(
    s3c: S3Client,
    bucket_name: str,
    object_name: str,
    byte_range: Optional[Tuple[int, int]] = None,
) -> Tuple[str, bytes]:
    if byte_range is None:
        response = await s3c.get_object(Bucket=bucket_name, Key=object_name)
    else:
        response = await s3c.get_object(
            Bucket=bucket_name,
            Key=object_name,
            Range=f"bytes={byte_range[0]}-{byte_range[1]}",
        )
    async with response["Body"] as stream:
        return object_name, await stream.read()


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


@dataclasses.dataclass
class S3ObjectStorage(ObjectStorage):
    region_name: Optional[str] = None

    @classmethod
    def get_url_scheme(cls) -> str:
        return "s3"

    async def _upload(self, file_path: str) -> Tuple[str, str]:
        if self.region_name is None:
            region_name = await _get_default_region_name()
        else:
            region_name = self.region_name

        bucket_name = _get_bucket_name(region_name)

        async with aiobotocore.session.get_session().create_client(
            "s3", region_name=region_name
        ) as s3_client:
            try:
                s3_key = await ensure_uploaded_by_hash(
                    s3_client, file_path, bucket_name, STORAGE_CODE_CACHE_PREFIX
                )
            except ClientError as error:
                if error.response["Error"]["Code"] == "403":
                    # if we don't have permissions to the bucket, throw a helpful error
                    raise MeadowrunAWSAccessError("S3 bucket") from error

                raise

        return bucket_name, s3_key

    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        return await download_file(
            bucket_name, object_name, file_name, self.region_name
        )
