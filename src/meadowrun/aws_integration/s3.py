from __future__ import annotations
import asyncio

import dataclasses
import hashlib
import json
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple

import aiobotocore.session
import boto3
import boto3.exceptions
from botocore.exceptions import ClientError
from meadowrun._vendor.fastcdc.fastcdc_py import fastcdc_py
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


async def _exists(s3_client: S3Client, bucket_name: str, key: str) -> bool:
    try:
        await s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "403":
            # if we don't have permissions to the bucket, throw a helpful error
            raise MeadowrunAWSAccessError("S3 bucket") from error

        # don't raise an error saying the file doesn't exist
        if not error.response["Error"]["Code"] == "404":
            raise error
    return False


async def _put_object(
    s3_client: S3Client, bucket_name: str, key: str, buffer: bytes
) -> None:
    try:
        await s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer)
    except boto3.exceptions.S3UploadFailedError as e:
        if len(e.args) >= 1 and "NoSuchBucket" in e.args[0]:
            raise MeadowrunNotInstalledError("S3 bucket")
        raise


async def _put_object_if_not_exists(
    s3_client: S3Client, bucket_name: str, key: str, data: bytes
) -> None:
    if not await _exists(s3_client, bucket_name, key):
        await _put_object(s3_client, bucket_name, key, data)


# This is ugly, but avoids repeated checking if the same bucket+key exists
# Only used in ensure_uploaded_incremental
_existing_keys: Set[Tuple[str, str]] = set()

_CHUNKS_KEY = "chunks"


async def ensure_uploaded_incremental(
    s3_client: S3Client,
    local_file_path: str,
    bucket_name: str,
    key_prefix: str = "",
    avg_chunk_size: int = 1000_000,
) -> str:
    """
    Uploads the specified file from the local machine to S3, in chunks. Each chunk is
    content-hashed and uploaded to "{key_prefix}{hash}.part", where {hash} is a hash of
    the contents of the file. A JSON file with all the parts is uploaded to the key
    "{key_prefix}{hash}.json", where {hash} is a hash of the contents.

    The file is split using content-based-chunking - this means there's no exact size
    for each chunk, but the size can be controlled by setting avg_chunk_size.

    If any S3 object already exists, it is not re-uploaded.

    key_prefix should usually be "" or end in a "/" like "code/".

    Returns the key of the JSON file in the S3 bucket, e.g. "code/123456789abcdefg.json"
    """

    # TODO: only check for existence of chunks if chunks_json does not exist?

    chunk_upload_tasks = []
    keys = []
    for chunk in fastcdc_py(
        local_file_path, avg_size=avg_chunk_size, fat=True, hf=hashlib.blake2b
    ):
        key = f"{key_prefix}{chunk.hash}.part"
        keys.append(key)
        cache_key = (bucket_name, key)
        if (bucket_name, key) not in _existing_keys:
            chunk_upload_tasks.append(
                asyncio.create_task(
                    _put_object_if_not_exists(s3_client, bucket_name, key, chunk.data)
                )
            )
            _existing_keys.add(cache_key)

    results = await asyncio.gather(*chunk_upload_tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            # on exceptions, we're not sure what the state of the uploaded keys is.
            _existing_keys.clear()
            raise result

    chunks_json = json.dumps({_CHUNKS_KEY: keys}).encode("utf-8")
    chunks_json_key = f"{key_prefix}{hashlib.blake2b(chunks_json).hexdigest()}.json"
    await _put_object_if_not_exists(
        s3_client, bucket_name, chunks_json_key, chunks_json
    )

    return chunks_json_key


async def download_chunked_file(
    s3c: S3Client,
    bucket_name: str,
    object_name: str,
    file_name: str,
) -> None:
    """Download a file that was uploaded in chunks via ensure_uploaded_incremental"""
    response = await s3c.get_object(Bucket=bucket_name, Key=object_name)
    async with response["Body"] as stream:
        chunk_list_bs = await stream.read()
    chunks = json.loads(chunk_list_bs.decode("utf-8"))[_CHUNKS_KEY]

    with tempfile.TemporaryDirectory() as tmp:
        await _download_files(s3c, bucket_name, chunks, tmp)
        _concat_chunks(tmp, chunks, file_name)


def _concat_chunks(
    source_folder: str, chunks: Iterable[str], destination_file: str
) -> None:
    with open(destination_file, "wb") as dest:
        for source_file in chunks:
            with open(
                os.path.join(source_folder, os.path.basename(source_file)), "rb"
            ) as src:
                shutil.copyfileobj(src, dest)


async def _download_file(
    s3c: S3Client, bucket_name: str, key: str, target_file: str
) -> None:
    response = await s3c.get_object(Bucket=bucket_name, Key=key)
    async with response["Body"] as stream:
        with open(target_file, "wb") as tgt:
            # unfortunately, stream.read(length) is not supported
            tgt.write(await stream.read())


async def _download_files(
    s3c: S3Client, bucket_name: str, keys: Iterable[str], target_folder: str
) -> None:
    """Download all given keys in the bucket as separate files to the target folder, in
    parallel. Keys can contain duplicates, they'll be downloaded only once.
    """
    download_tasks = []
    for key in set(keys):
        download_tasks.append(
            asyncio.create_task(
                _download_file(
                    s3c,
                    bucket_name,
                    key,
                    os.path.join(target_folder, os.path.basename(key)),
                )
            )
        )
    await asyncio.gather(*download_tasks)


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

    async def get_region_name(self) -> str:
        if self.region_name is None:
            self.region_name = await _get_default_region_name()
        return self.region_name

    async def _upload(self, file_path: str) -> Tuple[str, str]:
        region_name = await self.get_region_name()
        bucket_name = _get_bucket_name(region_name)

        async with aiobotocore.session.get_session().create_client(
            "s3", region_name=region_name
        ) as s3c:
            try:
                s3_key = await ensure_uploaded_incremental(
                    s3c, file_path, bucket_name, STORAGE_CODE_CACHE_PREFIX
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
        region_name = await self.get_region_name()
        async with aiobotocore.session.get_session().create_client(
            "s3", region_name=region_name
        ) as s3c:
            await download_chunked_file(s3c, bucket_name, object_name, file_name)
