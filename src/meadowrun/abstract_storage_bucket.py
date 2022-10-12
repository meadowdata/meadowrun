from __future__ import annotations

import abc
from typing import Optional, Type, Tuple, List, TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType


class AbstractStorageBucket(abc.ABC):
    """
    A wrapper around S3, Azure Storage, Google Cloud Storage, or other similar object
    storage systems.

    All of these systems implement an S3-compatible API, so we could theoretically get
    by with just S3ClientWrapper which uses boto3 under the hood, but the big difference
    is authentication. For example, Google Cloud Storage "natively" uses OAuth2, where
    as S3/boto3 use an HMAC-based authentication scheme. So if we want to use e.g. a
    service account's identity-based permissions on a GKE node, we need to use the
    Google Cloud API directly, as S3ClientWrapper/boto3 will only work with a
    username/password.
    """

    async def __aenter__(self) -> AbstractStorageBucket:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    @abc.abstractmethod
    def get_cache_key(self) -> str:
        """
        The cache key uniquely identifies this storage bucket. E.g. we need to be able
        to disambiguate between an AWS S3 bucket called "foo" and a Google Cloud Storage
        bucket called "foo"
        """
        ...

    @abc.abstractmethod
    async def get_bytes(self, key: str) -> bytes:
        ...

    @abc.abstractmethod
    async def try_get_bytes(self, key: str) -> Optional[bytes]:
        ...

    async def get_bytes_and_key(self, key: str) -> Tuple[bytes, str]:
        """
        A thin wrapper around get_bytes that also returns the key. Wouldn't be necessary
        if asyncio was easier to work with
        """
        return await self.get_bytes(key), key

    @abc.abstractmethod
    async def get_byte_range(self, key: str, byte_range: Tuple[int, int]) -> bytes:
        ...

    @abc.abstractmethod
    async def write_bytes(self, data: bytes, key: str) -> None:
        ...

    @abc.abstractmethod
    async def exists(self, key: str) -> bool:
        ...

    async def write_bytes_if_not_exists(self, data: bytes, key: str) -> None:
        if not await self.exists(key):
            await self.write_bytes(data, key)

    @abc.abstractmethod
    async def get_file(self, key: str, local_filename: str) -> None:
        ...

    @abc.abstractmethod
    async def try_get_file(self, key: str, local_filename: str) -> bool:
        ...

    @abc.abstractmethod
    async def write_file(self, local_filename: str, key: str) -> None:
        ...

    @abc.abstractmethod
    async def list_objects(self, key_prefix: str) -> List[str]:
        ...

    @abc.abstractmethod
    async def delete_object(self, key: str) -> None:
        ...
