"""Types/functions used by both the reader and writer"""

from typing import List, Literal, Optional
from dataclasses import dataclass
import uuid


@dataclass(frozen=True)
class TableVersion:
    """A TableVersion represents a single version of a table"""

    # The table_versions server maps table_ids to a userspace/table name. A single
    # table_id can map to 0, 1, 2 or more userspace/table names.
    # TODO should it be possible to have a table_id map to prod/foo AND user1/bar? Seems
    #  too confusing...
    table_id: uuid.UUID

    # Points to a file which contains a TableSchema
    table_schema_filename: str

    # Points to a file which contains an ordered list of DataFileEntry
    data_list_filename: str

    # A globally unique number that represents this version of this table
    version_number: int


@dataclass(frozen=True)
class TableSchema:
    # TODO currently not used
    column_names_and_types: None

    # If this is not None, rows will automatically be deduplicated (newest rows
    # preserved) based on the columns specified by deduplication_keys. E.g. if
    # deduplication_keys for table t is ['a', 'b'], this means that when you write to t,
    # with a = 1, b = 2, any existing rows in t where a = 1 and b = 2 will be
    # effectively deleted. As a result, when you read from t, every row will have a
    # distinct deduplication key. If deduplication_keys is None, then new rows are
    # appended normally.
    deduplication_keys: Optional[List[str]]


@dataclass(frozen=True)
class DataFileEntry:
    # Can be 'write', or 'delete'. 'write' can be interpreted as an append or an upsert
    # depending on the table's deduplication_keys.
    data_file_type: Literal["write", "delete"]

    # Points to a parquet file
    data_file_name: str

    # TODO add some statistics here to make querying faster
