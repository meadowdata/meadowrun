import threading
import os
from typing import List, Tuple, Dict, Optional
import uuid

import pandas as pd

from .readerwriter_shared import TableVersion


class TableVersionsClientLocal:
    """
    A lightweight way to use nextdb from a single process without a versions server.

    In general, behavior is undefined if more than one client is pointed to a single
    data_dir.

    TODO can provide a better guarantee on a file system
    TODO add support for S3-compatible API
    """

    def __init__(self, data_dir: str):
        # The root directory for all data
        self.data_dir = data_dir

        self._lock = threading.RLock()

        if not os.path.exists(self._table_versions_filename()):
            # This is for starting a brand new TableVersionsClientLocal in an empty
            # directory

            # List[(userspace, table_name, table_id)], maps userspace/table_name to
            # table_id. Note that more than one
            # userspace/table_name can map to a single table_id
            self._table_names_history: List[Tuple[str, str, uuid.UUID]] = []
            # Maps table_id to (table_schema_filename, data_list_filename) for a
            # specific version of that table
            self._table_versions_history: List[TableVersion] = []
            # The biggest version number that has not yet been used
            self._version_number: int = 0
        else:
            # This reads existing data created by a TableVersionsClientLocal

            self._table_names_history, self._table_versions_history = pd.read_pickle(
                self._table_versions_filename()
            )
            self._version_number = (
                max(t.version_number for t in self._table_versions_history) + 1
            )

        # Keeps the latest userspace/table_name -> table_id mapping for ease of use
        self._table_names_current: Dict[Tuple[str, str], uuid.UUID] = {
            (userspace, table_name): table_id
            for userspace, table_name, table_id in self._table_names_history
        }

        # Keeps the latest table_id -> TableVersion mapping for ease of use
        self._table_versions_current: Dict[uuid.UUID, TableVersion] = {
            t.table_id: t for t in self._table_versions_history
        }

    def prepend_data_dir(self, filename: str) -> str:
        return os.path.join(self.data_dir, filename)

    def _save_table_versions(self) -> None:
        # TODO save every X seconds? Or on every update? currently this function is not
        #  called
        pd.to_pickle(
            (self._table_names_history, self._table_versions_history),
            self._table_versions_filename(),
        )

    def _table_versions_filename(self) -> str:
        # The path for storing table_versions data (i.e. the metadata that this class
        # manages)
        return os.path.join(self.data_dir, "table_versions.pkl")

    def add_initial_table_version(
        self,
        userspace: str,
        table_name: str,
        table_id: uuid.UUID,
        table_schema_filename: str,
        data_list_filename: str,
    ) -> bool:
        """
        Add a table version, ensure that there is no existing data for that
        userspace/table_name or table_id. Returns False if these checks fail, returns
        True if successful
        """
        with self._lock:
            if (userspace, table_name) in self._table_names_current:
                return False

            if table_id in self._table_versions_current:
                raise ValueError(
                    f"Tried to write table {table_id} (for {userspace}/{table_name}) "
                    f"but it already exists! uuid collision?"
                )

            self._add_table_version(
                TableVersion(
                    table_id,
                    table_schema_filename,
                    data_list_filename,
                    self._version_number,
                )
            )
            self._version_number += 1

            self._add_or_update_table_names(userspace, table_name, table_id)

            return True

    def add_table_version(
        self,
        userspace: str,
        table_name: str,
        table_id: uuid.UUID,
        prev_version_number: int,
        table_schema_filename: str,
        data_list_filename: str,
    ) -> bool:
        """
        Write a new version of userspace/table. Ensures that userspace/table_name maps
        to table_id, and that table_id's latest version is prev_version_number. Returns
        False if these checks fail, returns True if successful.
        """
        with self._lock:
            if self._table_names_current[(userspace, table_name)] != table_id:
                return False

            prev_table_version = self._table_versions_current[table_id]
            if prev_table_version.version_number != prev_version_number:
                return False

            self._add_table_version(
                TableVersion(
                    table_id,
                    table_schema_filename,
                    data_list_filename,
                    self._version_number,
                )
            )
            self._version_number += 1

            return True

    def _add_table_version(self, table_version: TableVersion) -> None:
        with self._lock:
            self._table_versions_history.append(table_version)
            self._table_versions_current[table_version.table_id] = table_version

    def _add_or_update_table_names(
        self, userspace: str, table_name: str, table_id: uuid.UUID
    ) -> None:
        with self._lock:
            self._table_names_history.append((userspace, table_name, table_id))
            self._table_names_current[(userspace, table_name)] = table_id

    def get_current_table_version(
        self, userspace: str, table_name: str
    ) -> Optional[TableVersion]:
        """
        Get the current version for the specified userspace/table_name. Returns None if
        there is no data for that userspace/table_name
        """
        key = (userspace, table_name)
        if key not in self._table_names_current:
            return None

        table_id = self._table_names_current[key]
        if table_id not in self._table_versions_current:
            raise ValueError(
                f"Metadata is in an invalid state. {userspace}/{table_name} points to "
                f"{table_id} but that table id does not exist. This should never "
                f"happen!"
            )
        return self._table_versions_current[table_id]
