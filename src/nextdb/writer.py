import uuid
from typing import Optional

import pandas as pd

from .table_versions_client_local import TableVersionsClientLocal
from .readerwriter_shared import DataFileEntry, TableSchema


def write(
    table_versions_client: TableVersionsClientLocal,
    userspace: str,
    table_name: str,
    df: Optional[pd.DataFrame],
    deletes: Optional[pd.DataFrame],
) -> None:
    """See docstring on connection.Connection"""
    # TODO add support for writing multiple tables at the same time (transaction?)

    # Get the table_id for this userspace/table_name, make a new one if it doesn't exist
    prev_table_version = table_versions_client.get_current_table_version(
        userspace, table_name
    )
    if prev_table_version is None:
        table_id = uuid.uuid4()
    else:
        table_id = prev_table_version.table_id

    # Write data files for writes and deletes and construct DataFileEntry
    new_data_file_entries = []

    if df is not None:
        # TODO there probably needs to be some sort of segmentation so that every file
        #  doesn't end up in the same directory
        write_data_filename = f"write.{table_id}.{uuid.uuid4()}.parquet"
        df.to_parquet(
            table_versions_client.prepend_data_dir(write_data_filename), index=False
        )
        new_data_file_entries.append(DataFileEntry("write", write_data_filename))

    if deletes is not None:
        delete_data_filename = f"delete.{table_id}.{uuid.uuid4()}.parquet"
        deletes.to_parquet(
            table_versions_client.prepend_data_dir(delete_data_filename), index=False
        )
        new_data_file_entries.append(DataFileEntry("delete", delete_data_filename))

    if len(new_data_file_entries) == 0:
        raise ValueError("At least one of df and deletes needs to be not None")

    data_list_filename = f"data_list.{table_id}.{uuid.uuid4()}.pkl"

    # Write the data list file, schema file if this is a brand new table, and update the
    # table versions server
    if prev_table_version is None:
        # if this is a brand new table, we need to write a schema as well
        table_schema_filename = f"table_schema.{table_id}.{uuid.uuid4()}.pkl"
        # TODO use a real serialization format (not pickle)
        pd.to_pickle(
            TableSchema(None, None),
            table_versions_client.prepend_data_dir(table_schema_filename),
        )
        # write new data list
        pd.to_pickle(
            new_data_file_entries,
            table_versions_client.prepend_data_dir(data_list_filename),
        )
        # update table versions server
        success = table_versions_client.add_initial_table_version(
            userspace, table_name, table_id, table_schema_filename, data_list_filename
        )
        # TODO retry on failure, same for `else` clause
        if not success:
            raise ValueError(
                f"Optimistic concurrency check failed for {userspace}/{table_name}"
            )
    else:
        # write new data list
        data_list = pd.read_pickle(
            table_versions_client.prepend_data_dir(
                prev_table_version.data_list_filename
            )
        )
        pd.to_pickle(
            data_list + new_data_file_entries,
            table_versions_client.prepend_data_dir(data_list_filename),
        )
        # update table versions server
        success = table_versions_client.add_table_version(
            userspace,
            table_name,
            table_id,
            prev_table_version.version_number,
            prev_table_version.table_schema_filename,
            data_list_filename,
        )
        if not success:
            raise ValueError(
                f"Optimistic concurrency check failed for {userspace}/{table_name}"
            )


def create_or_update_table_schema(
    table_versions_client: TableVersionsClientLocal,
    userspace: str,
    table_name: str,
    table_schema: TableSchema,
) -> None:
    # TODO check that column_names_and_types and deduplication_keys agree with any
    #  existing data
    # TODO should you be allowed to have deduplication_keys without
    #  column_names_and_types?

    # Get the table_id for this userspace/table_name, make a new one if it doesn't exist
    prev_table_version = table_versions_client.get_current_table_version(
        userspace, table_name
    )
    if prev_table_version is None:
        table_id = uuid.uuid4()
    else:
        table_id = prev_table_version.table_id

    # Write the table_schema file
    table_schema_filename = f"table_schema.{table_id}.{uuid.uuid4()}.pkl"
    pd.to_pickle(
        table_schema, table_versions_client.prepend_data_dir(table_schema_filename)
    )

    # Write an empty data list if this is a brand new table, and update the table
    # versions server
    if prev_table_version is None:
        # write empty data list
        data_list_filename = f"data_list.{table_id}.{uuid.uuid4()}.pkl"
        pd.to_pickle([], table_versions_client.prepend_data_dir(data_list_filename))

        # update table versions server
        success = table_versions_client.add_initial_table_version(
            userspace, table_name, table_id, table_schema_filename, data_list_filename
        )
        # TODO retry on failure, same for `else` clause
        if not success:
            raise ValueError(
                f"Optimistic concurrency check failed for {userspace}/{table_name}"
            )
    else:
        # update table versions server
        success = table_versions_client.add_table_version(
            userspace,
            table_name,
            table_id,
            prev_table_version.version_number,
            table_schema_filename,
            prev_table_version.data_list_filename,
        )
        if not success:
            raise ValueError(
                f"Optimistic concurrency check failed for {userspace}/{table_name}"
            )
