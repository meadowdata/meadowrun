from abc import ABC, abstractmethod
from typing import Any
import duckdb
import pandas as pd
import os

Key = str


class KeyValueStore(ABC):
    """Abstract key-value store."""

    @abstractmethod
    def exists(self, key: Key) -> bool:
        ...

    @abstractmethod
    def set_pickle(self, key: Key, value: Any) -> None:
        ...

    @abstractmethod
    def get_pickle(self, key: Key) -> Any:
        ...

    @abstractmethod
    def set_parquet(self, key: Key, value: pd.DataFrame) -> None:
        ...

    @abstractmethod
    def get_parquet_duckdb_relation(
        self, key: Key, conn: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyRelation:
        ...


class FileSystemStore(KeyValueStore):
    """
    Values are persisted as files on a file system, under a given root directory.

    Note this means that any path separators in the keys are interpreted as such, which
    means it's trivial to "break out" of the root dir.
    """

    def __init__(self, root_dir: str) -> None:
        super().__init__()
        self._root_dir = root_dir

    def _full_path(self, key: Key) -> str:
        return os.path.join(self._root_dir, key)

    def exists(self, key: Key) -> bool:
        return os.path.exists(self._full_path(key))

    def set_pickle(self, key: Key, value: Any) -> None:
        pd.to_pickle(value, self._full_path(key))

    def get_pickle(self, key: Key) -> Any:
        return pd.read_pickle(self._full_path(key))

    def set_parquet(self, key: Key, df: pd.DataFrame) -> None:
        df.to_parquet(self._full_path(key), index=False)

    def get_parquet_duckdb_relation(
        self, key: Key, conn: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyRelation:
        return conn.from_parquet(self._full_path(key))
