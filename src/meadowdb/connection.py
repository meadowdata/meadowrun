from __future__ import annotations

import contextlib
import dataclasses
import os
from typing import Dict, Final, Generator, List, Optional, Tuple

import pandas as pd

from meadowdb import reader, writer
from meadowdb.readerwriter_shared import TableSchema, UserspaceSpec
from meadowdb.table_versions_client_local import TableVersionsClientLocal

try:
    # we try to import meadowflow.effects because we want to make sure that
    # _save_effects gets registered in the case where the user code only references
    # meadowdb and not meadowflow.effects
    import meadowflow.effects  # noqa F401
except Exception:
    pass


_MEADOWDB_DEFAULT_USERSPACE: Final = "MEADOWDB_DEFAULT_USERSPACE"
# Should only be accessed by get_default_userspace and set_default_userspace
_default_userspace_spec: Optional[UserspaceSpec] = None


def get_default_userspace_spec() -> UserspaceSpec:
    """
    Gets the default userspace spec, which can be set via environment variable
    MEADOWDB_DEFAULT_USERSPACE or by the set_default_userspace function. Whenever
    userspace is not set, the default userspace will be used.

    MEADOWDB_DEFAULT_USERSPACE is a comma separated list of userspace names. The last is
    UserspaceSpec.tip, and the first are added to UserspaceSpec.bases in the order
    given. This order is the order in which changes from the given userspace are
    combined on read.

    Thematically, this would make sense in meadowflow.context, but preferable not to
    introduce that dependency.
    """
    global _default_userspace_spec
    if _default_userspace_spec is None:
        if _MEADOWDB_DEFAULT_USERSPACE in os.environ:
            userspace_spec_str = os.environ[_MEADOWDB_DEFAULT_USERSPACE]
            userspaces = userspace_spec_str.split(",")
            if not userspaces:
                raise ValueError(
                    f"The environment variable {_MEADOWDB_DEFAULT_USERSPACE} is set, "
                    "but is not a valid userspace spec. Please set it to a "
                    "comma-separated list of at least one userspace name, for "
                    "example 'main,test-space'."
                )

            _default_userspace_spec = UserspaceSpec(
                tip=userspaces[-1],
                bases=tuple() if len(userspaces) == 1 else tuple(userspaces[:-1]),
            )
        else:
            _default_userspace_spec = UserspaceSpec.main()

    return _default_userspace_spec


@contextlib.contextmanager
def set_default_userspace_spec(
    userspace_spec: UserspaceSpec,
) -> Generator[None, None, None]:
    """
    Usage:
    >>> with set_default_userspace_spec(userspace_spec):
    >>>     # read and write

    In the with block, the default userspace spec will be "userspace_spec". It
    can still be overwritten by giving an explicit userspace_spec argument when
    reading or writing.

    Sets globally across all threads, not threadsafe.
    """
    global _default_userspace_spec

    prev_default_userspace_spec = _default_userspace_spec
    _default_userspace_spec = userspace_spec
    try:
        yield None
    finally:
        _default_userspace_spec = prev_default_userspace_spec


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
    effects: MeadowdbEffects = dataclasses.field(default_factory=MeadowdbEffects)

    def __post_init__(self) -> None:
        all_connections.append(self)

    def key(self) -> ConnectionKey:
        """
        Currently this is used to group Connections that operate on the same underlying
        data files for grouping together effects that happened "on the same data"
        """
        return self.table_versions_client.key

    def reset_effects(self) -> None:
        self.effects.tables_read.clear()
        self.effects.tables_written.clear()

    def create_or_update_table_schema(
        self,
        table_name: str,
        table_schema: TableSchema,
        userspace_spec: Optional[UserspaceSpec] = None,
    ) -> None:
        """Creates or updates the table_schema (TableSchema) for userspace/table_name"""
        if userspace_spec is None:
            userspace_spec = get_default_userspace_spec()

        written_version = writer.create_or_update_table_schema(
            self.table_versions_client, userspace_spec.tip, table_name, table_schema
        )
        self.effects.tables_written.setdefault(
            (userspace_spec.tip, table_name), set()
        ).add(written_version)

    def write(
        self,
        table_name: str,
        df: Optional[pd.DataFrame],
        userspace_spec: Optional[UserspaceSpec] = None,
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

        if userspace_spec is None:
            userspace_spec = get_default_userspace_spec()

        userspace = userspace_spec.tip

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
        userspace_spec: Optional[UserspaceSpec] = None,
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
        self.write(table_name, None, userspace_spec, delete_where_equal_df)

    def delete_all(
        self, table_name: str, userspace_spec: Optional[UserspaceSpec] = None
    ) -> None:
        """Deletes all data in the specified table"""
        self.write(table_name, None, userspace_spec, None, True)

    def read(
        self,
        table_name: str,
        userspace_spec: Optional[UserspaceSpec] = None,
        max_version_number: Optional[int] = None,
    ) -> reader.MdbTable:
        """Returns a MdbTable object that can be used to query userspace/table_name"""

        if userspace_spec is None:
            userspace_spec = get_default_userspace_spec()

        result = reader.read(
            self.table_versions_client, userspace_spec, table_name, max_version_number
        )
        # TODO the effect just records the tip here - need a representation of the spec?
        self.effects.tables_read.setdefault(
            (userspace_spec.tip, table_name), set()
        ).add(result._version_number)
        return result
