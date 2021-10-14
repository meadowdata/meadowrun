"""
This file is like a database migration tool for meadowdb. Because meadowdb schema
definitions are done just by running some python code, we don't need any specialized
tools beyond source control and meadowflow(?)
"""

# TODO this should get run via meadowflow

from covid_data.mdb import mdb_test
from meadowdb import TableSchema


def define_schemas():
    mdb_test().create_or_update_table_schema(
        "cdc_covid_data",
        TableSchema(
            column_names_and_types=None, deduplication_keys=["submission_date", "state"]
        ),
    )

    mdb_test().create_or_update_table_schema(
        "cdc_covid_data_smoothed",
        TableSchema(
            column_names_and_types=None, deduplication_keys=["submission_date", "state"]
        ),
    )

    mdb_test().table_versions_client._save_table_versions()
