"""
There's no such thing as an "S3 grid job". The code in this module just helps us
implement grid jobs that use an S3-compatible object store to transfer data, e.g. for
EC2 and Kubernetes
"""


from __future__ import annotations

import io
import pickle
import typing

from typing import Optional, Any, TYPE_CHECKING

import aiobotocore.session
import botocore.exceptions

if TYPE_CHECKING:
    import types_aiobotocore_s3


def get_storage_client_from_args(
    storage_endpoint_url: Optional[str],
    storage_access_key_id: Optional[str],
    storage_secret_access_key: Optional[str],
) -> typing.AsyncContextManager[types_aiobotocore_s3.S3Client]:
    kwargs = {}
    if storage_access_key_id is not None:
        kwargs["aws_access_key_id"] = storage_access_key_id
    if storage_secret_access_key is not None:
        kwargs["aws_secret_access_key"] = storage_secret_access_key
    if storage_endpoint_url is not None:
        kwargs["endpoint_url"] = storage_endpoint_url

    # TODO if all the parameters are None then we're implicitly falling back on AWS
    # S3, which we should make explicit
    session = aiobotocore.session.get_session()
    return session.create_client("s3", **kwargs)  # type: ignore


async def read_storage(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
) -> Any:
    response = await storage_client.get_object(
        Bucket=storage_bucket, Key=storage_filename
    )
    async with response["Body"] as stream:
        return await stream.read()


async def write_storage_pickle(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
    obj: Any,
    pickle_protocol: Optional[int],
) -> None:
    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer, protocol=pickle_protocol)
        buffer.seek(0)
        await storage_client.put_object(
            Bucket=storage_bucket,
            Key=storage_filename,
            Body=buffer,
        )


async def write_storage_file(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    local_filename: str,
    storage_filename: str,
) -> None:
    with open(local_filename, "rb") as f:
        await storage_client.put_object(
            Bucket=storage_bucket, Key=storage_filename, Body=f
        )


async def try_get_storage_file(
    storage_client: types_aiobotocore_s3.S3Client,
    storage_bucket: str,
    storage_filename: str,
    local_filename: str,
) -> bool:
    try:
        response = await storage_client.get_object(
            Bucket=storage_bucket, Key=storage_filename
        )
        async with response["Body"] as stream:
            with open(local_filename, "wb") as f:
                f.write(await stream.read())
        return True
    except botocore.exceptions.ClientError as error:
        # don't raise an error saying the file doesn't exist, we'll just upload it
        # in that case by falling through to the next bit of code
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise error

        return False
