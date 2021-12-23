"""
Types/functions used by both the reader and writer.

Overview of overall data structure layout:

TableVersionsClientLocal:
# filename: 'table_versions'
    table_names_history: List[TableName]
        versioned lookup of userspace and tablename to a stable id
        userspace, table_name, version -> table_id
    table_version_history: List[TableVersion]
        versioned lookup of table id to schema and log
        log determines contents of the table,
        containing writes and deletes
        table_id, version -> table_schema_filename, table_log_filename
# table_schema_filename: 'table_schema.{table_id}.{uuid}'
    table_schema: TableSchema
        columns_names_and_types: NOT IMPLEMENTED,
        deduplication_keys: list of column names to use as the key for
        deduplicating rows
# table_log_filename: 'data_list.{table_id}.{uuid}'
    table_log_entries: List[TableLogEntry]
    TableLogEntry: Write data_filename | Delete data_filename | DeleteAll
# data_filename: '{write|delete}.{table_id}.{uuid}]'
    data: DataFrame

"""

from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass, field
from typing import Final, List, Optional, Tuple


_dummy_uuid = uuid.UUID(int=0)


@dataclass(frozen=True, order=True)
class TableName:
    """A TableName maps a userspace,table name,version to a table id"""

    # This needs to be the first field! We rely on this being the first field and
    # therefore effectively defining the sort order
    version_number: int
    userspace: str
    table_name: str
    table_id: uuid.UUID

    @staticmethod
    def dummy(version_number: int) -> TableName:
        """
        The bisect library doesn't allow for passing in a sort key, so we just make
        TableName sortable based on version_number (which must be the first field in the
        class in order to make dataclass' built-in ordering functions work this way).
        This means we sometimes need dummy TableNames to compare against. This function
        constructs those dummy TableNames.
        """
        return TableName(version_number, "", "", _dummy_uuid)


@dataclass(frozen=True, order=True)
class TableVersion:
    """A TableVersion represents a single version of a table"""

    # This needs to be the first field! We rely on this being the first field and
    # therefore effectively defining the sort order
    version_number: int

    # The table_versions server maps table_ids to a userspace/table name. A single
    # table_id can map to 0, 1, 2 or more userspace/table names.
    # TODO should it be possible to have a table_id map to prod/foo AND user1/bar? Seems
    #  too confusing...
    table_id: uuid.UUID

    # Points to a file which contains a TableSchema. None means first try to fall back
    # on the parent userspace's table's schema, then fall back on the default
    # TableSchema
    table_schema_filename: Optional[str]

    # Points to a file which contains an ordered list of TableLogEntry
    table_log_filename: str

    @staticmethod
    def dummy(version_number: int) -> TableVersion:
        """See TableName.dummy"""
        return TableVersion(version_number, _dummy_uuid, "", "")


@dataclass(frozen=True)
class TableSchema:
    # TODO currently not used
    column_names_and_types: None = None

    # If this list is not empty, rows will automatically be deduplicated (newest rows
    # preserved) based on the columns specified by deduplication_keys. E.g. if
    # deduplication_keys for table t is ['a', 'b'], this means that when you write to t,
    # with a = 1, b = 2, any existing rows in t where a = 1 and b = 2 will be
    # effectively deleted. As a result, when you read from t, every row will have a
    # distinct deduplication key. If deduplication_keys is empty, then new rows are
    # appended normally.
    deduplication_keys: List[str] = field(default_factory=list)


class TableLogEntry(abc.ABC):
    # TODO add some statistics here to make querying faster

    # Can be "write", "delete", or "delete_all". "write" can be interpreted as an append
    # or an upsert depending on the table's deduplication_keys. "delete" is a delete
    # where equal (see Connection.delete_where_equal). "delete_all" means all existing
    # data was deleted at that point. "delete_all" is necessary (instead of just having
    # an empty data_filename) so that a table in a userspace can ignore data in a parent
    # userspace.
    pass


@dataclass(frozen=True)
class WriteLogEntry(TableLogEntry):
    # Points to a parquet file.
    data_filename: str


@dataclass(frozen=True)
class DeleteLogEntry(TableLogEntry):
    # Points to a parquet file.
    data_filename: str


@dataclass(frozen=True)
class DeleteAllLogEntry(TableLogEntry):
    pass


MAIN_USERSPACE_NAME: Final = "main"


@dataclass(frozen=True)
class UserspaceSpec:
    """
    Specify userspace layering.

    The tip userspace is written to, and also read from. Any userspaces in bases are
    read from. The changes from bases and tip are combined according to the given mode.
    """

    tip: str = MAIN_USERSPACE_NAME
    bases: Tuple[str, ...] = tuple()
    # TODO add fields here to specify different ways of reading and writing.
    # e.g. could add "branch" mode where tip starts from certain version in
    # base.
    # e.g. could have a mode where changes from bases and tips are added
    # in version order.
    # e.g. combine specific version ranges from bases and tip
    # Note these could have different constraints (for branches, hard to see
    # how to combine more than two) so subclassing may be a good idea.

    def userspaces_base_to_tip(self) -> Tuple[str, ...]:
        """Returns userspaces in base to tip order."""
        return self.bases + (self.tip,)

    @staticmethod
    def main() -> UserspaceSpec:
        """
        Reads and writes to main userspace only.

        Equivalent to UserspaceSpec().
        """
        return UserspaceSpec()

    @staticmethod
    def main_with_tip(tip_userspace: str) -> UserspaceSpec:
        """
        Writes to a given userspace, overlaying it on the prod userspace.

        Args:
            tip_on_main (str): name of the tip userspace.
        """
        return UserspaceSpec(tip=tip_userspace, bases=(MAIN_USERSPACE_NAME,))
