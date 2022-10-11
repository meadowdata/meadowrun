from __future__ import annotations

import dataclasses
from typing import Optional, Tuple

import aiobotocore.session
from botocore.exceptions import ClientError
import boto3.exceptions
from meadowrun.aws_integration.aws_core import (
    MeadowrunAWSAccessError,
    MeadowrunNotInstalledError,
    _get_default_region_name,
    get_bucket_name,
)
from meadowrun.object_storage import ObjectStorage
from meadowrun.storage_grid_job import (
    S3ClientWrapper,
    download_chunked_file,
    ensure_uploaded_incremental,
)
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX


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
        bucket_name = get_bucket_name(region_name)

        async with S3ClientWrapper(
            aiobotocore.session.get_session().create_client(
                "s3", region_name=region_name
            )
        ) as s3_client:
            try:
                s3_key = await ensure_uploaded_incremental(
                    s3_client, file_path, bucket_name, STORAGE_CODE_CACHE_PREFIX
                )
            except boto3.exceptions.S3UploadFailedError as e:
                if len(e.args) >= 1 and "NoSuchBucket" in e.args[0]:
                    raise MeadowrunNotInstalledError("S3 bucket")
                raise
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
        async with S3ClientWrapper(
            aiobotocore.session.get_session().create_client(
                "s3", region_name=region_name
            )
        ) as s3_client:
            await download_chunked_file(s3_client, bucket_name, object_name, file_name)
