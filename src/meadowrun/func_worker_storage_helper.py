"""
This code belongs in either func_worker_storage.py. Unfortunately, if we put it in
func_worker_storage.py, then meadowrun.__init__ will import kubernetes_integration which
will import func_worker_storage, which means that running func_worker_storage will
produce annoying messages like RuntimeWarning: 'meadowrun.func_worker_storage' found in
sys.modules after import of package 'meadowrun', but prior to execution of
'meadowrun.func_worker_storage'; this may result in unpredictable behaviour
warn(RuntimeWarning(msg))
"""
from __future__ import annotations

import dataclasses
import hashlib

from typing import Optional, Any, Tuple

import botocore.exceptions

from meadowrun.run_job_core import S3CompatibleObjectStorage

MEADOWRUN_STORAGE_USERNAME = "MEADOWRUN_STORAGE_USERNAME"
MEADOWRUN_STORAGE_PASSWORD = "MEADOWRUN_STORAGE_PASSWORD"

# This is a global variable that will be updated with the storage client if it's
# available in func_worker_storage
FUNC_WORKER_STORAGE_CLIENT: Optional[Any] = None
FUNC_WORKER_STORAGE_BUCKET: Optional[str] = None


@dataclasses.dataclass
class FuncWorkerClientObjectStorage(S3CompatibleObjectStorage):
    # this really belongs in kubernetes_integration.py but can't put it there because of
    # circular imports
    storage_client: Any = None
    bucket_name: Optional[str] = None

    @classmethod
    def get_url_scheme(cls) -> str:
        return "meadowrunfuncworkerstorage"

    async def _upload(self, file_path: str) -> Tuple[str, str]:
        # TODO these will never get cleaned up

        if self.bucket_name is None:
            raise Exception("Can't use _upload without a bucket_name")

        hasher = hashlib.blake2b()
        with open(file_path, "rb") as file:
            buf = file.read()
            hasher.update(buf)
        digest = hasher.hexdigest()

        try:
            self.storage_client.head_object(Bucket=self.bucket_name, Key=digest)
            return self.bucket_name, digest
        except botocore.exceptions.ClientError as error:
            # don't raise an error saying the file doesn't exist, we'll just upload it
            # in that case by falling through to the next bit of code
            if not error.response["Error"]["Code"] == "404":
                raise error

        # doesn't exist, need to upload it
        self.storage_client.upload_file(
            Filename=file_path, Bucket=self.bucket_name, Key=digest
        ),
        return self.bucket_name, digest

    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        storage_client = FUNC_WORKER_STORAGE_CLIENT
        if storage_client is None:
            raise Exception("FUNC_WORKER_STORAGE_CLIENT is not available")
        storage_client.download_file(bucket_name, object_name, file_name)
