"""
This code belongs in either func_worker_storage.py. Unfortunately, if we put it in
func_worker_storage.py, then meadowrun.__init__ will import k8s_integration.k8s which
will import func_worker_storage, which means that running func_worker_storage will
produce annoying messages like RuntimeWarning: 'meadowrun.func_worker_storage' found in
sys.modules after import of package 'meadowrun', but prior to execution of
'meadowrun.func_worker_storage'; this may result in unpredictable behaviour
warn(RuntimeWarning(msg))
"""
from __future__ import annotations

import dataclasses

from typing import Optional, Tuple, TYPE_CHECKING, Type

from meadowrun.aws_integration.s3 import ensure_uploaded_by_hash
from meadowrun.object_storage import ObjectStorage
from meadowrun.s3_grid_job import read_storage
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX

if TYPE_CHECKING:
    from types import TracebackType
    import types_aiobotocore_s3

MEADOWRUN_STORAGE_USERNAME = "MEADOWRUN_STORAGE_USERNAME"
MEADOWRUN_STORAGE_PASSWORD = "MEADOWRUN_STORAGE_PASSWORD"

# This is a global variable that will be updated with the storage client if it's
# available in func_worker_storage
FUNC_WORKER_STORAGE_CLIENT: Optional[types_aiobotocore_s3.S3Client] = None
FUNC_WORKER_STORAGE_BUCKET: Optional[str] = None


@dataclasses.dataclass
class FuncWorkerClientObjectStorage(ObjectStorage):
    # this really belongs in k8s.py but can't put it there because of
    # circular imports
    storage_client: Optional[types_aiobotocore_s3.S3Client] = None
    bucket_name: Optional[str] = None

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self.storage_client is not None:
            await self.storage_client.__aexit__(exc_typ, exc_val, exc_tb)

    @classmethod
    def get_url_scheme(cls) -> str:
        return "meadowrunfuncworkerstorage"

    async def _upload(self, file_path: str) -> Tuple[str, str]:
        # TODO these will never get cleaned up

        if self.storage_client is None or self.bucket_name is None:
            raise Exception(
                "Can't use _upload without a bucket_name and a storage endpoint"
            )

        s3_key = await ensure_uploaded_by_hash(
            self.storage_client, file_path, self.bucket_name, STORAGE_CODE_CACHE_PREFIX
        )
        return self.bucket_name, s3_key

    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        storage_client = FUNC_WORKER_STORAGE_CLIENT
        if storage_client is None:
            raise Exception("FUNC_WORKER_STORAGE_CLIENT is not available")
        with open(file_name, "wb") as f:
            f.write(await read_storage(storage_client, bucket_name, object_name))
