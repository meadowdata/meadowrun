from typing import Optional

import pandas as pd

from . import writer, reader
from .readerwriter_shared import TableSchema
from .table_versions_client_local import TableVersionsClientLocal


prod_userspace_name = "prod"


class Connection:
    """
    A connection object that can be used to read and write data. This is the main class
    that a user will interact with.
    """

    def __init__(self, table_versions_client: TableVersionsClientLocal):
        self._table_versions_client = table_versions_client

    def create_or_update_table_schema(
        self,
        table_name: str,
        table_schema: TableSchema,
        userspace: str = prod_userspace_name,
    ) -> None:
        """Creates or updates the table_schema (TableSchema) for userspace/table_name"""
        writer.create_or_update_table_schema(
            self._table_versions_client, userspace, table_name, table_schema
        )

    def write(
        self,
        table_name: str,
        df: Optional[pd.DataFrame],
        userspace: str = prod_userspace_name,
        delete_where_equal_df: Optional[pd.DataFrame] = None,
    ) -> None:
        """
        Writes df to userspace/table_name. df should be a pandas dataframe that matches
        the schema (if the schema exists) of table_name.

        If delete_where_equal_df is not None, applies the delete to table_name first,
        then writes df. See delete_where_equal for semantics of delete_where_equal_df

        At least one of df and delete_where_equal_df must be not None
        """
        # TODO in the current implementation, would be pretty easy to support arbitrary
        #  WHERE clauses for deletes, is that the right thing to do?
        writer.write(
            self._table_versions_client,
            userspace,
            table_name,
            df,
            delete_where_equal_df,
        )

    def delete_where_equal(
        self,
        table_name: str,
        delete_where_equal_df: pd.DataFrame,
        userspace: str = prod_userspace_name,
    ) -> None:
        """
        delete_where_equal_df should also be a pandas dataframe that has a subset of the
        columns in table_name. Any existing rows matching the values in deletes will be
        "deleted". E.g. if deletes is

        a | b
        1 | 2
        3 | 4

        This is equivalent to DELETE FROM table_name WHERE (a = 1 AND b = 2) OR (a = 3
        and b = 4)
        """
        self.write(table_name, None, userspace, delete_where_equal_df)

    def read(
        self,
        table_name: str,
        userspace: str = prod_userspace_name,
        max_version_number: int = None,
    ) -> reader.NdbTable:
        """
        Returns a NdbTable object that can be used to query userspace/table_name
        """
        return reader.read(
            self._table_versions_client, userspace, table_name, max_version_number
        )
