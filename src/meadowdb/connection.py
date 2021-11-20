from __future__ import annotations

import contextlib
import dataclasses
import os
from typing import Generator, Optional, List, Dict, Tuple

import pandas as pd

from meadowdb import writer, reader
from meadowdb.readerwriter_shared import TableSchema
from meadowdb.table_versions_client_local import TableVersionsClientLocal

try:
    # we try to import meadowflow.effects because we want to make sure that
    # _save_effects gets registered in the case where the user code only references
    # meadowdb and not meadowflow.effects
    import meadowflow.effects
except Exception:
    pass


prod_userspace_name = "prod"


_MEADOWDB_DEFAULT_USERSPACE = "MEADOWDB_DEFAULT_USERSPACE"
# Should only be accessed by get_default_userspace and set_default_userspace
_default_userspace_name: Optional[str] = None


def get_default_userspace() -> str:
    """
    Gets the default userspace, which can be set via environment variable or by the
    set_default_userspace function. Whenever userspace is not set, the default userspace
    will be used.

    Thematically, this would make sense in meadowflow.context, but preferable not to
    introduce that dependency.
    """
    global _default_userspace_name
    if _default_userspace_name is None:
        if _MEADOWDB_DEFAULT_USERSPACE in os.environ:
            _default_userspace_name = os.environ[_MEADOWDB_DEFAULT_USERSPACE]
        else:
            _default_userspace_name = prod_userspace_name

    return _default_userspace_name


@contextlib.contextmanager
def set_default_userspace(userspace_name: str) -> Generator[None, None, None]:
    """
    Usage:
    with set_default_userspace("userspace_name"):
        # read and write

    In the with block, the default userspace will be "userspace_name".

    Sets globally across all threads, not threadsafe
    """
    global _default_userspace_name

    prev_default_userspace = _default_userspace_name
    _default_userspace_name = userspace_name
    try:
        yield None
    finally:
        _default_userspace_name = prev_default_userspace


# Keeps track of all connections created.
# TODO connection management--we shouldn't have unnecessary multiple instances of
#  connections
all_connections: List[Connection] = []


@dataclasses.dataclass(frozen=True)
class MeadowdbEffects:
    """
    This should be part of meadowflow.effects, but we want meadowflow to depend on
    meadowdb, not the other way around.
    """

    # Maps from table (userspace, name) -> set(versions read)
    tables_read: Dict[Tuple[str, str], set[int]] = dataclasses.field(
        default_factory=lambda: {}
    )
    # Maps from table (userspace, name) -> set(versions written)
    tables_written: Dict[Tuple[str, str], set[int]] = dataclasses.field(
        default_factory=lambda: {}
    )


# See Connection.key()
ConnectionKey = str


@dataclasses.dataclass(frozen=True)
class Connection:
    """
    A connection object that can be used to read and write data. This is the main class
    that a user will interact with.
    """

    table_versions_client: TableVersionsClientLocal
    # Keeps track of the tables we read/wrote to in this job
    effects: MeadowdbEffects = dataclasses.field(
        default_factory=lambda: MeadowdbEffects()
    )

    def __post_init__(self) -> None:
        all_connections.append(self)

    def key(self) -> ConnectionKey:
        """
        Currently this is used to group Connections that operate on the same underlying
        data files for grouping together effects that happened "on the same data"
        """
        return self.table_versions_client.data_dir

    def reset_effects(self) -> None:
        self.effects.tables_read.clear()
        self.effects.tables_written.clear()

    def create_or_update_table_schema(
        self,
        table_name: str,
        table_schema: TableSchema,
        userspace: str = None,
    ) -> None:
        """Creates or updates the table_schema (TableSchema) for userspace/table_name"""
        if userspace is None:
            userspace = get_default_userspace()

        written_version = writer.create_or_update_table_schema(
            self.table_versions_client, userspace, table_name, table_schema
        )
        self.effects.tables_written.setdefault((userspace, table_name), set()).add(
            written_version
        )

    def write(
        self,
        table_name: str,
        df: Optional[pd.DataFrame],
        userspace: str = None,
        delete_where_equal_df: Optional[pd.DataFrame] = None,
        delete_all: bool = False,
    ) -> None:
        """
        Writes df to userspace/table_name. df should be a pandas dataframe that matches
        the schema (if the schema exists) of table_name.

        If delete_where_equal_df is not None, applies the delete to table_name first,
        then writes df. See delete_where_equal for semantics of delete_where_equal_df

        If delete_all is True, deletes everything in table_name first, then writes df.
        Cannot be used together with delete_where_equal_df. See also
        Connection.delete_all.

        At least one of df and delete_where_equal_df must be not None
        """
        # TODO in the current implementation, would be pretty easy to support arbitrary
        #  WHERE clauses for delete_where_equal_df, is that the right thing to do?

        if userspace is None:
            userspace = get_default_userspace()

        written_version = writer.write(
            self.table_versions_client,
            userspace,
            table_name,
            df,
            delete_where_equal_df,
            delete_all,
        )
        self.effects.tables_written.setdefault((userspace, table_name), set()).add(
            written_version
        )

    def delete_where_equal(
        self,
        table_name: str,
        delete_where_equal_df: pd.DataFrame,
        userspace: str = None,
    ) -> None:
        """
        delete_where_equal_df should also be a pandas dataframe that has a subset of the
        columns in table_name. Any existing rows matching the values in
        delete_where_equal_df will be "deleted". E.g. if delete_where_equal_df is

        a | b
        1 | 2
        3 | 4

        This is equivalent to DELETE FROM table_name WHERE (a = 1 AND b = 2) OR (a = 3
        and b = 4)
        """
        self.write(table_name, None, userspace, delete_where_equal_df)

    def delete_all(self, table_name: str, userspace: str = None) -> None:
        """Deletes all data in the specified table"""
        self.write(table_name, None, userspace, None, True)

    def read(
        self,
        table_name: str,
        userspace: str = None,
        max_version_number: int = None,
    ) -> reader.MdbTable:
        """Returns a MdbTable object that can be used to query userspace/table_name"""

        if userspace is None:
            userspace = get_default_userspace()

        result = reader.read(
            self.table_versions_client, userspace, table_name, max_version_number
        )
        self.effects.tables_read.setdefault((userspace, table_name), set()).add(
            result._version_number
        )
        return result
