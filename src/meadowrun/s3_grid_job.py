"""
There's no such thing as an "S3 grid job". The code in this module just helps us
implement grid jobs that use an S3-compatible object store to transfer data, e.g. for
EC2 and Kubernetes
"""


from __future__ import annotations

import io
import pickle

from typing import Optional, Any

import boto3
import botocore.exceptions


def get_storage_client_from_args(
    storage_endpoint_url: Optional[str],
    storage_access_key_id: Optional[str],
    storage_secret_access_key: Optional[str],
) -> Any:
    session_kwargs = {}
    if storage_access_key_id is not None:
        session_kwargs["aws_access_key_id"] = storage_access_key_id
    if storage_secret_access_key is not None:
        session_kwargs["aws_secret_access_key"] = storage_secret_access_key
    client_kwargs = {}
    if storage_endpoint_url is not None:
        client_kwargs["endpoint_url"] = storage_endpoint_url
    if session_kwargs:
        session = boto3.Session(**session_kwargs)  # type: ignore
        return session.client("s3", **client_kwargs)  # type: ignore
    else:
        # TODO if all the parameters are None then we're implicitly falling back on AWS
        # S3, which we should make explicit
        return boto3.client("s3", **client_kwargs)  # type: ignore


def read_storage_pickle(
    storage_client: Any, storage_bucket: str, storage_filename: str
) -> Any:
    with io.BytesIO() as buffer:
        storage_client.download_fileobj(
            Bucket=storage_bucket, Key=storage_filename, Fileobj=buffer
        )
        buffer.seek(0)
        return pickle.load(buffer)


def read_storage_bytes(
    storage_client: Any, storage_bucket: str, storage_filename: str
) -> bytes:
    with io.BytesIO() as buffer:
        storage_client.download_fileobj(
            Bucket=storage_bucket, Key=storage_filename, Fileobj=buffer
        )
        buffer.seek(0)
        return buffer.read()


def write_storage_pickle(
    storage_client: Any,
    storage_bucket: str,
    storage_filename: str,
    obj: Any,
    pickle_protocol: Optional[int],
) -> None:
    with io.BytesIO() as buffer:
        pickle.dump(obj, buffer, protocol=pickle_protocol)
        buffer.seek(0)
        storage_client.upload_fileobj(
            Fileobj=buffer, Bucket=storage_bucket, Key=storage_filename
        )


def write_storage_bytes(
    storage_client: Any, storage_bucket: str, storage_filename: str, bs: bytes
) -> None:
    with io.BytesIO() as buffer:
        buffer.write(bs)
        buffer.seek(0)
        storage_client.upload_fileobj(
            Fileobj=buffer, Bucket=storage_bucket, Key=storage_filename
        )


def write_storage_file(
    storage_client: Any, storage_bucket: str, local_filename: str, storage_filename: str
) -> None:
    storage_client.upload_file(
        Filename=local_filename, Bucket=storage_bucket, Key=storage_filename
    )


def try_get_storage_file(
    storage_client: Any,
    storage_bucket: str,
    storage_filename: str,
    local_filename: str,
) -> bool:
    try:
        storage_client.download_file(storage_bucket, storage_filename, local_filename)
        return True
    except botocore.exceptions.ClientError as error:
        # don't raise an error saying the file doesn't exist, we'll just upload it
        # in that case by falling through to the next bit of code
        if not error.response["Error"]["Code"] == "404":
            raise error

        return False
