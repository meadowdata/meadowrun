from __future__ import annotations

import abc
import collections.abc
import datetime
from dataclasses import dataclass
from typing import Iterable, List, Literal, Optional, Tuple, Union, cast, overload

import duckdb
import pandas as pd

from meadowdb.readerwriter_shared import (
    DeleteAllLogEntry,
    DeleteLogEntry,
    TableLogEntry,
    TableSchema,
    TableVersion,
    UserspaceSpec,
    WriteLogEntry,
)
from meadowdb.storage import KeyValueStore
from meadowdb.table_versions_client_local import TableVersionsClientLocal


def read(
    table_version_client: TableVersionsClientLocal,
    userspace_spec: UserspaceSpec,
    table_name: str,
    max_version_number: Optional[int],
) -> MdbTable:
    """See Connection.read for usage docstring"""

    # TODO currently we only implement userspace specs that replay
    # changes in the order they're specified in the spec,
    # but we plan on supporting other strategies as well -
    # in particular branch like behavior.

    # Populate table_versions.
    # Order is important here, we always want to apply
    # base userspaces' writes first, then tip userspace writes,
    # regardless of the original order of writes, and similarly
    # for schemas.
    table_versions = _get_table_versions_base_to_tip(
        table_version_client, userspace_spec, table_name, max_version_number
    )
    # We are now guaranteed to have at least one table_version, else ValueError
    # was raised.

    table_log_filenames = []
    # Will be max version number
    table_version_number = -1
    # If table_schema is missing in all userspaces, we just use a default schema.
    table_schema = TableSchema()
    for table_version in table_versions:
        if table_version.table_schema_filename:
            table_schema = table_version_client.store.get_pickle(
                table_version.table_schema_filename
            )
        table_log_filenames.append(table_version.table_log_filename)
        table_version_number = max(table_version_number, table_version.version_number)

    return MdbTable(
        table_version_number,
        table_schema,
        table_version_client.store,
        [
            table_log_entry
            for table_log_filename in table_log_filenames
            for table_log_entry in table_version_client.store.get_pickle(
                table_log_filename
            )
        ],
        [],
    )


def _get_table_versions_base_to_tip(
    table_version_client: TableVersionsClientLocal,
    userspace_spec: UserspaceSpec,
    table_name: str,
    max_version_number: Optional[int],
) -> Tuple[TableVersion, ...]:
    table_versions = tuple(
        table_version
        for userspace in userspace_spec.userspaces_base_to_tip()
        for table_version in (
            table_version_client.get_current_table_version(
                userspace, table_name, max_version_number
            ),
        )
        if table_version is not None
    )

    if not table_versions:
        raise ValueError(
            f"Requested table {table_name} does not exist in any userspace: "
            f"{', '.join(userspace_spec.userspaces_base_to_tip())}"
        )

    return table_versions


# Some strings used in internal sql construction
_table_name_placeholder = "[!!__to_be_replaced_table_name__!!]"
# TODO P1 throw an exception if someone tries to use this __mdb_reserved_indicator__
#  reserved column name
_indicator_column_name = "__mdb_reserved_indicator__"


@dataclass(frozen=True)
class _SelectColumnsOp:
    columns_to_select: Iterable[str]


@dataclass(frozen=True)
class _SelectRowsOp:
    filter_column: Union[MdbComputedBoolColumnOpArg, MdbComputedBoolColumnOpColumn]


class MdbTable:
    """
    Represents a meadowdb table. No data gets materialized until to_pd is called. We can
    optionally limit to a subset of rows/columns before materializing. Some illustrative
    examples:

    >>> t[['column1', 'column2']] # limit to a subset of columns
    >>> t[t['column1'].between(100, 200)] # limit to a subset of rows
    """

    def __init__(
        self,
        version_number: int,
        table_schema: TableSchema,
        store: KeyValueStore,
        log_entry_list: List[TableLogEntry],
        ops: List[Union[_SelectColumnsOp, _SelectRowsOp]],
    ):
        # a unique identifier for this version of this table
        self._version_number = version_number
        self._table_schema = table_schema
        self._store = store
        # a list of log entries that we will read when we materialize
        self._log_entry_list = log_entry_list
        # a list of query operations to apply before we materialize
        self._ops = ops

    @overload
    def __getitem__(self, item: str) -> MdbColumn:
        ...

    # Need to specialize Iterable here, because str is an Iterable[str]
    # see https://github.com/python/typing/issues/256
    @overload
    def __getitem__(
        self, item: Union[List[str], Tuple[str, ...], MdbColumn, MdbBoolColumn]
    ) -> MdbTable:
        ...

    def __getitem__(
        self, item: Union[str, Iterable[str], MdbColumn, MdbBoolColumn]
    ) -> Union[MdbColumn, MdbTable]:
        if isinstance(item, str):
            # E.g. t['column1']. Get a single column (usually to apply additional
            # operations to turn it into a bool computed column that can be used to
            # filter rows).
            return MdbColumn(self, item)
        elif isinstance(item, collections.abc.Iterable):
            # E.g. t[['column1', 'column2']]. Filter/reorder columns
            return MdbTable(
                self._version_number,
                self._table_schema,
                self._store,
                self._log_entry_list,
                self._ops + [_SelectColumnsOp(item)],
            )
        elif isinstance(
            item, (MdbComputedBoolColumnOpArg, MdbComputedBoolColumnOpColumn)
        ):
            # Filter rows based on a computed column, e.g.
            # mdb_computed_column = t['column1'] == 3; t[mdb_computed_series]
            return MdbTable(
                self._version_number,
                self._table_schema,
                self._store,
                self._log_entry_list,
                self._ops + [_SelectRowsOp(item)],
            )
        elif isinstance(item, MdbColumn):
            # Filter rows based on a column that's already a bool, e.g.
            # t[t['bool_column']]
            return self[item._interpret_as_bool()]
        else:
            raise ValueError(f"MdbTable[{type(item)}] is not a valid operation")

    @property
    def columns(self) -> list[str]:
        # TODO implement
        raise NotImplementedError()

    @property
    def empty(self) -> bool:
        return not self._log_entry_list
        # TODO add more logic here--just because we have data files doesn't mean the
        #  table isn't empty; we need to actually query the data. Probably head(1)
        #  should be efficient enough.

    def head(self, n: int = 10) -> pd.DataFrame:
        # TODO implement
        raise NotImplementedError("Should be straightforward to implement...")

    def to_pd(self) -> pd.DataFrame:
        """
        Materialize the table into a pandas dataframe.

        The general strategy is to take the query operations that have been specified
        (self._ops) and translate them into a SQL query, and then use duckdb to execute
        that query on the underlying data files (self._log_entry_list).
        """
        conn = duckdb.connect(":memory:")

        select_clause, where_clause = self._construct_sql()

        # stores the materialized pd.DataFrame for each partition
        partition_results: List[pd.DataFrame] = []
        deduplication_keys = self._table_schema.deduplication_keys
        # we'll keep track of the deduplication key values and deletes that we've seen
        # so that we can filter them out of older partitions
        # TODO possibly better to not keep deduplication_keys_seen and deletes in memory
        deduplication_keys_seen = pd.DataFrame()
        deletes = pd.DataFrame()

        # we have to iterate newest partitions first so we know what to filter out of
        # older partitions
        for i, log_entry in enumerate(reversed(self._log_entry_list)):
            if isinstance(log_entry, WriteLogEntry):
                # materialize the write into a pd.DataFrame, applying any filters the
                # user specified (select_clause, where_clause), as well as any
                # deduplication_key-based filters and deletes that we've seen so far
                table_name = f"t{i}"
                table_relation = self._store.get_parquet_duckdb_relation(
                    log_entry.data_filename, conn
                )
                table_relation.create_view(table_name)

                if len(deduplication_keys_seen) == 0:  # or deduplication_keys is None
                    if len(deletes) == 0:
                        # simplest case--no deletes or deduplication_keys to filter out
                        sql = (
                            select_clause.replace(_table_name_placeholder, table_name)
                            + f" FROM {table_name} WHERE "
                            + where_clause.replace(_table_name_placeholder, table_name)
                        )
                    else:
                        # case where we have delete_where_equal
                        conn.register("ds", deletes)
                        sql = (
                            select_clause.replace(_table_name_placeholder, table_name)
                            + f" FROM {table_name} LEFT JOIN ds ON "
                            + " AND ".join(
                                f"{table_name}.{c} = ds.{c}"
                                for c in deletes.columns
                                if c != _indicator_column_name
                            )
                            + f" WHERE ds.{_indicator_column_name} IS NULL AND "
                            + where_clause.replace(_table_name_placeholder, table_name)
                        )
                else:
                    if len(deletes) == 0:
                        # case where we have deduplication_keys (automatically
                        # overwriting rows based on the deduplication_keys)

                        # TODO this could be implemented at write time as just another
                        #  delete, that might be the right thing to do?
                        # TODO check performance--is special antijoin code being hit?
                        #  are statistics being used?
                        # TODO the below line is unnecessary if deduplication_keys_seen
                        #  hasn't been updated between iterations
                        conn.register("pks", deduplication_keys_seen)
                        sql = (
                            select_clause.replace(_table_name_placeholder, table_name)
                            + f" FROM {table_name} LEFT JOIN pks ON "
                            + " AND ".join(
                                f"{table_name}.{c} = pks.{c}"
                                for c in deduplication_keys
                            )
                            + f" WHERE pks.{_indicator_column_name} IS NULL AND "
                            + where_clause.replace(_table_name_placeholder, table_name)
                        )
                    else:
                        # case where we have deduplication_keys AND deletes
                        conn.register("pks", deduplication_keys_seen)
                        conn.register("ds", deletes)
                        sql = (
                            select_clause.replace(_table_name_placeholder, table_name)
                            + f" FROM {table_name} LEFT JOIN ds ON "
                            + " AND ".join(
                                f"{table_name}.{c} = ds.{c}"
                                for c in deletes.columns
                                if c != _indicator_column_name
                            )
                            + " LEFT JOIN pks ON "
                            + " AND ".join(
                                f"{table_name}.{c} = pks.{c}"
                                for c in deduplication_keys
                            )
                            + f" WHERE ds.{_indicator_column_name} IS NULL AND "
                            + f"pks.{_indicator_column_name} IS NULL AND "
                            + where_clause.replace(_table_name_placeholder, table_name)
                        )

                # Uncommenting this line is helpful in debugging the sql generation
                # print(sql)
                conn.execute(sql)
                df = cast(pd.DataFrame, conn.fetchdf())
                partition_results.append(df)

                if deduplication_keys:
                    # add this partition's deduplication keys to the list of
                    # deduplication keys we've seen
                    deduplication_keys_seen = pd.concat(
                        [
                            deduplication_keys_seen,
                            df[deduplication_keys].assign(
                                **{_indicator_column_name: 1}
                            ),
                        ]
                    )
            elif isinstance(log_entry, DeleteLogEntry):
                # Read deletes so we can use them to filter out rows in older partitions

                # pd.read_parquet would be faster, but this way we're always using the
                # same engine
                d = cast(
                    pd.DataFrame,
                    self._store.get_parquet_duckdb_relation(
                        log_entry.data_filename, conn
                    ).to_df(),
                )
                d[_indicator_column_name] = 1

                if len(deletes) > 0 and sorted(deletes.columns) != sorted(d.columns):
                    # TODO P1 this should really throw an error at write time (or we
                    #  should add support for it)
                    raise NotImplementedError(
                        "Deletes on different sets of columns is not supported"
                    )
                deletes = pd.concat([deletes, d])
            elif isinstance(log_entry, DeleteAllLogEntry):
                # delete_all means stop iterating
                break
            else:
                raise ValueError(f"data_file_type {log_entry} is not supported")

        # put the results together
        partition_results.reverse()

        if len(partition_results) == 0:
            # TODO use `columns` when returning an empty dataframe
            return pd.DataFrame()
        else:
            return pd.concat(partition_results, ignore_index=True)

    def _construct_sql(self) -> Tuple[str, str]:
        """
        Returns a select_clause and a where_clause. These clauses reflect the
        user-supplied operations on this MdbTable. Both clauses need to have
        .replace(_table_name_placeholder, table_name) called on them.
        """

        # TODO some weirdness here where you can do t1 = t['a', 'b']; t1[t1['c'] == 3].
        #  This shouldn't work (c has been filtered out), but it will for now

        # filtering columns, aka select_clause
        column_args = [
            op.columns_to_select for op in self._ops if isinstance(op, _SelectColumnsOp)
        ]
        if len(column_args) == 0:
            select_clause = f"SELECT {_table_name_placeholder}.*"
        else:
            curr_columns = column_args[0]
            for next_columns in column_args[1:]:
                columns_not_previously_selected = [
                    a for a in next_columns if a not in curr_columns
                ]
                if len(columns_not_previously_selected) > 0:
                    raise ValueError(
                        f"Tried to select columns "
                        f'{", ".join(columns_not_previously_selected)} after already '
                        f"filtering them out"
                    )
                curr_columns = next_columns
            select_clause = "SELECT " + ", ".join(
                f'{_table_name_placeholder}."{c}"' for c in curr_columns
            )

        # filtering rows, aka where_clause
        row_args = [
            op.filter_column for op in self._ops if isinstance(op, _SelectRowsOp)
        ]
        if len(row_args) == 0:
            where_clause = "TRUE"  # TODO see if this causes performance issues
        else:
            curr_filter_column = row_args[0]
            for next_filter_column in row_args[1:]:
                curr_filter_column = MdbComputedBoolColumnOpColumn(
                    curr_filter_column, next_filter_column, "AND"
                )
            where_clause = row_args[0]._construct_where_clause(self)

        return select_clause, where_clause


# The types of literals that can be used for comparisons.
# TODO add more support and add type checks
COMPARISON_LITERAL_TYPE = Union[str, datetime.datetime, int, float]


class MdbColumn:
    """
    Represents a column from a MdbTable. Supports:
    >>> col == X, col != X, col < X, col <= X, col > X, col >= X
    >>> col.between(X, Y)
    >>> col.isin([X, Y, Z])
    where X, Y, Z are literals to compare against.

    This can also be directly materialized into a pd.Series with to_pd.

    This technically also implements MdbBoolColumn, as it's possible that this is a bool
    column coming directly from the data, but because it requires calling
    _interpret_as_bool first, it's easier to not have it implement MdbBoolColumn.
    """

    def __init__(self, mdb_table: MdbTable, column_name: str):
        self._mdb_table = mdb_table
        self._column_name = column_name

    def __eq__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:  # type: ignore[override] # noqa E501
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, "=", other
        )

    def __ne__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:  # type: ignore[override] # noqa E501
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, "!=", other
        )

    def __gt__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, ">", other
        )

    def __lt__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, "<", other
        )

    def __ge__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, ">=", other
        )

    def __le__(self, other: COMPARISON_LITERAL_TYPE) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, "<=", other
        )

    def between(
        self, a: COMPARISON_LITERAL_TYPE, b: COMPARISON_LITERAL_TYPE
    ) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpTwoArgs(
            self._mdb_table, self._column_name, "BETWEEN", (a, b)
        )

    def isin(self, items: Iterable[COMPARISON_LITERAL_TYPE]) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpManyArgs(
            self._mdb_table, self._column_name, "IN", items
        )

    def head(self, n: int) -> pd.DataFrame:
        raise NotImplementedError()

    def __invert__(self) -> MdbBoolColumn:
        return ~(self._interpret_as_bool())

    def _interpret_as_bool(self) -> MdbBoolColumn:
        # TODO check if this can actually be interpreted as a bool?
        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, "=", "TRUE"
        )

    def __and__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self._interpret_as_bool(), other, "AND")

    def __or__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self._interpret_as_bool(), other, "OR")

    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        return self._interpret_as_bool()._construct_where_clause(mdb_table)

    def to_pd(self) -> pd.Series:
        return self._mdb_table[[self._column_name]].to_pd()[self._column_name]


class MdbBoolColumn(abc.ABC):
    """
    Represents a bool column, supports C & D, C | D, and ~C where C and D are both bool
    columns
    """

    @abc.abstractmethod
    def __invert__(self) -> MdbBoolColumn:
        pass

    @abc.abstractmethod
    def __and__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        pass

    @abc.abstractmethod
    def __or__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        pass

    @abc.abstractmethod
    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        pass


class MdbComputedBoolColumnOpArg(MdbBoolColumn, abc.ABC):
    """
    Represents a bool computed column of the form column `op` arg, e.g. t['column1'] ==
    3
    """

    def __init__(
        self,
        mdb_table: MdbTable,
        column_name: str,
        op: str,
    ):
        self._mdb_table = mdb_table
        self._column_name = column_name
        self._op = op

    def __and__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self, other, "AND")

    def __or__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self, other, "OR")

    def _single_arg_to_string(self, arg: COMPARISON_LITERAL_TYPE) -> str:
        # TODO this needs to be way more sophisticated, will rely on
        #  self._mdb_table.table_schema
        if isinstance(arg, str) or isinstance(arg, datetime.datetime):
            return f"'{arg}'"
        else:
            return str(arg)

    def _assert_table_version_is_same(self, mdb_table: MdbTable) -> None:
        if self._mdb_table._version_number != mdb_table._version_number:
            raise ValueError(
                "Using a series from a different table in a row selector is not "
                "supported"
            )


class MdbComputedBoolColumnOpSingleArg(MdbComputedBoolColumnOpArg):
    """
    Represents a bool computed column of the form column `op` arg, with a single
    argument e.g. t['column1'] == 3
    """

    def __init__(
        self,
        mdb_table: MdbTable,
        column_name: str,
        op: str,
        arg: COMPARISON_LITERAL_TYPE,
    ):
        super().__init__(mdb_table, column_name, op)
        self._arg = arg

    def __invert__(self) -> MdbComputedBoolColumnOpSingleArg:
        if self._op == "=":
            new_op = "!="
        elif self._op == "!=":
            new_op = "="
        elif self._op == ">":
            new_op = "<="
        elif self._op == "<=":
            new_op = ">"
        elif self._op == "<":
            new_op = ">="
        elif self._op == ">=":
            new_op = "<"
        else:
            raise ValueError(f"Programming error: op {self._op} is not covered")

        return MdbComputedBoolColumnOpSingleArg(
            self._mdb_table, self._column_name, new_op, self._arg
        )

    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        self._assert_table_version_is_same(mdb_table)

        return (
            f'({_table_name_placeholder}."{self._column_name}" {self._op} '
            f"{self._single_arg_to_string(self._arg)})"
        )


class MdbComputedBoolColumnOpTwoArgs(MdbComputedBoolColumnOpArg):
    """
    Represents a bool computed column of the form column `op` arg with two arguments,
    e.g. t['column1'],between(0, 3)
    """

    def __init__(
        self,
        mdb_table: MdbTable,
        column_name: str,
        op: str,
        arg: Tuple[COMPARISON_LITERAL_TYPE, COMPARISON_LITERAL_TYPE],
    ):
        super().__init__(mdb_table, column_name, op)
        self._arg = arg

    def __invert__(self) -> MdbComputedBoolColumnOpTwoArgs:
        if self._op == "BETWEEN":
            new_op = "NOT BETWEEN"
        elif self._op == "NOT BETWEEN":
            new_op = "BETWEEN"
        else:
            raise ValueError(f"Programming error: op {self._op} is not covered")

        return MdbComputedBoolColumnOpTwoArgs(
            self._mdb_table, self._column_name, new_op, self._arg
        )

    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        self._assert_table_version_is_same(mdb_table)

        return (
            f'({_table_name_placeholder}."{self._column_name}" {self._op} '
            f"{self._single_arg_to_string(self._arg[0])} AND "
            f"{self._single_arg_to_string(self._arg[1])})"
        )


class MdbComputedBoolColumnOpManyArgs(MdbComputedBoolColumnOpArg):
    """
    Represents a bool computed column of the form column `op` arg, with many arguments
    e.g. t['column1'].isin(1,2,3)
    """

    def __init__(
        self,
        mdb_table: MdbTable,
        column_name: str,
        op: str,
        arg: Iterable[COMPARISON_LITERAL_TYPE],
    ):
        super().__init__(mdb_table, column_name, op)
        self._arg = arg

    def __invert__(self) -> MdbComputedBoolColumnOpManyArgs:
        if self._op == "IN":
            new_op = "NOT IN"
        elif self._op == "NOT IN":
            new_op = "IN"
        else:
            raise ValueError(f"Programming error: op {self._op} is not covered")

        return MdbComputedBoolColumnOpManyArgs(
            self._mdb_table, self._column_name, new_op, self._arg
        )

    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        self._assert_table_version_is_same(mdb_table)

        return (
            f'({_table_name_placeholder}."{self._column_name}" {self._op} '
            f'({", ".join(self._single_arg_to_string(arg) for arg in self._arg)}))'
        )


class MdbComputedBoolColumnOpColumn(MdbBoolColumn):
    """
    Represents a bool computed column of the form column `op` column, e.g.
    >>> (t['column1'] == 3) & (t['column2'] < 10)
    """

    def __init__(
        self, series_a: MdbBoolColumn, series_b: MdbBoolColumn, op: Literal["AND", "OR"]
    ):
        self._series_a = series_a
        self._series_b = series_b
        self._op = op

    def __and__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self, other, "AND")

    def __or__(self, other: MdbBoolColumn) -> MdbBoolColumn:
        return MdbComputedBoolColumnOpColumn(self, other, "OR")

    def __invert__(self) -> MdbBoolColumn:
        if self._op == "AND":
            return MdbComputedBoolColumnOpColumn(~self._series_a, ~self._series_b, "OR")
        elif self._op == "OR":
            return MdbComputedBoolColumnOpColumn(
                ~self._series_a, ~self._series_b, "AND"
            )
        else:
            raise ValueError(f"Programming error: self._op cannot be {self._op}")

    def _construct_where_clause(self, mdb_table: MdbTable) -> str:
        return (
            f"({self._series_a._construct_where_clause(mdb_table)} {self._op} "
            f"{self._series_b._construct_where_clause(mdb_table)})"
        )
