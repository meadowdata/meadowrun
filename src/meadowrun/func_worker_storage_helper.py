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

from typing import Optional, TYPE_CHECKING, Type

from meadowrun.object_storage import ObjectStorage
from meadowrun.storage_grid_job import (
    AbstractStorageBucket,
    download_chunked_file,
    ensure_uploaded_incremental,
)
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX

if TYPE_CHECKING:
    from types import TracebackType

MEADOWRUN_STORAGE_USERNAME = "MEADOWRUN_STORAGE_USERNAME"
MEADOWRUN_STORAGE_PASSWORD = "MEADOWRUN_STORAGE_PASSWORD"

# This is a global variable that will be updated with the storage client if it's
# available in func_worker_storage
FUNC_WORKER_STORAGE_BUCKET: Optional[AbstractStorageBucket] = None


@dataclasses.dataclass
class FuncWorkerClientObjectStorage(ObjectStorage):
    # this really belongs in k8s.py but can't put it there because of
    # circular imports
    storage_bucket: Optional[AbstractStorageBucket] = None

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.storage_bucket is not None:
            await self.storage_bucket.__aexit__(exc_type, exc_val, exc_tb)

    @classmethod
    def get_url_scheme(cls) -> str:
        return "meadowrunfuncworkerstorage"

    async def _upload(self, file_path: str) -> str:
        # TODO these will never get cleaned up

        if self.storage_bucket is None:
            raise Exception(
                "Can't use _upload without a bucket_name and a storage endpoint"
            )

        return await ensure_uploaded_incremental(
            self.storage_bucket, file_path, STORAGE_CODE_CACHE_PREFIX
        )

    async def _download(self, object_name: str, file_name: str) -> None:
        storage_bucket = FUNC_WORKER_STORAGE_BUCKET
        if storage_bucket is None:
            raise Exception("FUNC_WORKER_STORAGE_CLIENT is not available")
        await download_chunked_file(storage_bucket, object_name, file_name)
