import bisect
import threading
import uuid
from typing import Dict, Final, List, Optional, Tuple

from meadowdb.readerwriter_shared import TableName, TableVersion
from meadowdb.storage import FileSystemStore, KeyValueStore


class TableVersionsClientLocal:
    """
    A lightweight way to use meadowdb from a single process without a versions server.

    In general, behavior is undefined if more than one client is pointed to a single
    data_dir.

    TODO can provide a better guarantee on a file system
    TODO add support for S3-compatible API
    """

    TABLE_VERSIONS: Final = "table_versions.pkl"

    def __init__(self, data_dir: str):
        # The root directory for all data
        self.store: KeyValueStore = FileSystemStore(data_dir)
        # TODO deal with this more cleanly - see Connection.key()
        self.key = data_dir

        self._lock = threading.RLock()

        if not self.store.exists(self.TABLE_VERSIONS):
            # This is for starting a brand new TableVersionsClientLocal in an empty
            # directory

            # List[(userspace, table_name, table_id)], maps userspace/table_name to
            # table_id. Note that more than one
            # userspace/table_name can map to a single table_id
            self._table_names_history: List[TableName] = []
            # Maps table_id to (table_schema_filename, data_list_filename) for a
            # specific version of that table
            self._table_versions_history: List[TableVersion] = []
            # The biggest version number that has not yet been used
            self._version_number: int = 0
        else:
            # This reads existing data created by a TableVersionsClientLocal
            (
                self._table_names_history,
                self._table_versions_history,
            ) = self.store.get_pickle(self.TABLE_VERSIONS)

            self._version_number = (
                max(
                    max(t.version_number for t in self._table_versions_history),
                    max(t.version_number for t in self._table_names_history),
                )
                + 1
            )

        # Keeps the latest userspace/table_name -> table_id mapping for ease of use
        self._table_names_current: Dict[Tuple[str, str], TableName] = {
            (t.userspace, t.table_name): t for t in self._table_names_history
        }

        # Keeps the latest table_id -> TableVersion mapping for ease of use
        self._table_versions_current: Dict[uuid.UUID, TableVersion] = {
            t.table_id: t for t in self._table_versions_history
        }

    def _save_table_versions(self) -> None:
        # TODO save every X seconds? Or on every update? currently this function is not
        #  called
        self.store.set_pickle(
            self.TABLE_VERSIONS,
            (self._table_names_history, self._table_versions_history),
        )

    def add_initial_table_version(
        self,
        userspace: str,
        table_name: str,
        table_id: uuid.UUID,
        table_schema_filename: Optional[str],
        data_list_filename: str,
    ) -> Optional[int]:
        """
        Add a table version, ensure that there is no existing data for that
        userspace/table_name or table_id. Returns None if these checks fail, returns
        the version number written if successful.
        """
        with self._lock:
            if (userspace, table_name) in self._table_names_current:
                return None

            if table_id in self._table_versions_current:
                raise ValueError(
                    f"Tried to write table {table_id} (for {userspace}/{table_name}) "
                    f"but it already exists! uuid collision?"
                )

            version_to_write = self._version_number

            self._add_table_version(
                TableVersion(
                    version_to_write,
                    table_id,
                    table_schema_filename,
                    data_list_filename,
                )
            )

            self._add_or_update_table_names(
                TableName(version_to_write, userspace, table_name, table_id)
            )

            self._version_number += 1

            return version_to_write

    def add_table_version(
        self,
        userspace: str,
        table_name: str,
        table_id: uuid.UUID,
        prev_version_number: int,
        table_schema_filename: Optional[str],
        data_list_filename: str,
    ) -> Optional[int]:
        """
        Write a new version of userspace/table. Ensures that userspace/table_name maps
        to table_id, and that table_id's latest version is prev_version_number. Returns
        None if these checks fail, returns the version number written if successful.
        """
        with self._lock:
            if self._table_names_current[(userspace, table_name)].table_id != table_id:
                return None

            prev_table_version = self._table_versions_current[table_id]
            if prev_table_version.version_number != prev_version_number:
                return None

            version_to_write = self._version_number

            self._add_table_version(
                TableVersion(
                    version_to_write,
                    table_id,
                    table_schema_filename,
                    data_list_filename,
                )
            )
            self._version_number += 1

            return version_to_write

    def _add_table_version(self, table_version: TableVersion) -> None:
        with self._lock:
            self._table_versions_history.append(table_version)
            self._table_versions_current[table_version.table_id] = table_version

    def _add_or_update_table_names(self, table_name: TableName) -> None:
        with self._lock:
            self._table_names_history.append(table_name)
            self._table_names_current[
                (table_name.userspace, table_name.table_name)
            ] = table_name

    def get_current_table_version(
        self, userspace: str, table_name: str, max_version_number: Optional[int]
    ) -> Optional[TableVersion]:
        """
        Get the current version for the specified userspace/table_name. If
        max_version_number is specified, ignores any updates that were made after
        max_version_number (max_version_number is inclusive). Use max_version_number =
        None to not restrict at all. Returns None if there is no data for that
        userspace/table_name
        """
        key = (userspace, table_name)
        if key not in self._table_names_current:
            if max_version_number is None:
                return None
            else:
                return None  # TODO this assumes we never delete a name

        current_table_name = self._table_names_current[key]

        if (
            max_version_number is None
            or current_table_name.version_number <= max_version_number
        ):
            table_id: Optional[uuid.UUID] = current_table_name.table_id
        else:
            table_id = None

            # binary search to find most recent record, and just iterate backwards
            # until we find the userspace/table_name we're looking for.
            # TODO probably will need to make this more efficient
            i = bisect.bisect_right(
                self._table_names_history, TableName.dummy(max_version_number)
            )
            while i >= 0:
                el = self._table_names_history[i]
                if el.userspace == userspace and el.table_name == table_name:
                    table_id = el.table_id
                    break
                i -= 1

            if table_id is None:
                return None

        if table_id not in self._table_versions_current:
            raise ValueError(
                f"Metadata is in an invalid state. {userspace}/{table_name} points to "
                f"{table_id} as of version {max_version_number} but that table id does "
                f"not exist. This should never happen!"
            )
        current_table_version = self._table_versions_current[table_id]

        if (
            max_version_number is None
            or current_table_version.version_number <= max_version_number
        ):
            return current_table_version
        else:
            # same idea as above
            # TODO probably will need to make this more efficient
            i = bisect.bisect_right(
                self._table_versions_history, TableVersion.dummy(max_version_number)
            )
            while i >= 0:
                ev = self._table_versions_history[i]
                if ev.table_id == table_id:
                    return ev
                i -= 1

            return None  # if we didn't find anything
