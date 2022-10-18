from __future__ import annotations

from typing import List, Tuple, Optional, Type, Coroutine, Any, TYPE_CHECKING

import aiohttp
import meadowrun._vendor.gcloud.aio.storage as gcloud_storage
from meadowrun.storage_grid_job import AbstractStorageBucket

if TYPE_CHECKING:
    from types import TracebackType


class GoogleStorageBucket(AbstractStorageBucket):
    def __init__(self, storage: gcloud_storage.Storage, bucket: str):
        self._storage = storage
        self._bucket = bucket

    def get_cache_key(self) -> str:
        return f"gcp/{self._bucket}"

    async def __aenter__(self) -> GoogleStorageBucket:
        self._storage = await self._storage.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self._storage.__aexit__(exc_type, exc_val, exc_tb)

    def get_bytes(self, key: str) -> Coroutine[Any, Any, bytes]:
        return self._storage.download(self._bucket, key)

    async def try_get_bytes(self, key: str) -> Optional[bytes]:
        try:
            return await self.get_bytes(key)
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return None
            raise

    def get_byte_range(
        self, key: str, byte_range: Tuple[int, int]
    ) -> Coroutine[Any, Any, bytes]:
        return self._storage.download(
            self._bucket,
            key,
            headers={"Range": f"bytes={byte_range[0]}-{byte_range[1]}"},
        )

    async def write_bytes(self, data: bytes, key: str) -> None:
        await self._storage.upload(self._bucket, key, data)

    async def exists(self, key: str) -> bool:
        try:
            await self._storage.download_metadata(self._bucket, key)
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return False
            raise

    def get_file(self, key: str, local_filename: str) -> Coroutine[Any, Any, None]:
        return self._storage.download_to_filename(self._bucket, key, local_filename)

    async def try_get_file(self, key: str, local_filename: str) -> bool:
        try:
            await self.get_file(key, local_filename)
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return False
            raise

    async def write_file(self, local_filename: str, key: str) -> None:
        await self._storage.upload_from_filename(self._bucket, key, local_filename)

    async def list_objects(self, key_prefix: str) -> List[str]:
        result = await self._storage.list_objects(
            self._bucket, params={"prefix": key_prefix, "fields": "items(name)"}
        )
        return [item["name"] for item in result.get("items", ())]

    async def delete_object(self, key: str) -> None:
        await self._storage.delete(self._bucket, key)


def get_google_storage_bucket(bucket: str) -> GoogleStorageBucket:
    return GoogleStorageBucket(gcloud_storage.Storage(), bucket)
