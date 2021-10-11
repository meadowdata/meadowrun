"""
This file is like a database migration tool for nextdb. Because nextdb schema
definitions are done just by running some python code, we don't need any specialized
tools beyond source control and nextbeat(?)
"""

# TODO this should get run via nextbeat

from covid_data.ndb import ndb_test
from nextdb import TableSchema


def define_schemas():
    ndb_test().create_or_update_table_schema(
        "cdc_covid_data",
        TableSchema(
            column_names_and_types=None, deduplication_keys=["submission_date", "state"]
        ),
    )

    ndb_test().create_or_update_table_schema(
        "cdc_covid_data_smoothed",
        TableSchema(
            column_names_and_types=None, deduplication_keys=["submission_date", "state"]
        ),
    )

    ndb_test().table_versions_client._save_table_versions()
