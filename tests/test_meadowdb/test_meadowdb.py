from typing import Callable
import meadowdb
import pandas as pd
import unittest.mock
from meadowdb.connection import (
    _MEADOWDB_DEFAULT_USERSPACE,
    UserspaceSpec,
    set_default_userspace_spec,
)
import pytest


def test_meadowdb(
    mdb_connection: meadowdb.Connection,
    random_df: Callable[..., pd.DataFrame],
):
    mdb = mdb_connection
    # see TODO below
    starting_version = mdb.table_versions_client._version_number
    table = "temp1"
    # generate and write some data
    test_data1 = random_df()
    test_data1.loc[50, "str1"] = "hello"
    test_data1.loc[51, "str1"] = "foobar"
    test_data1.loc[52, "str1"] = "foobar"
    test_data2 = random_df()
    test_data3 = random_df()
    mdb.write(table, test_data1)
    mdb.write(table, test_data2)
    mdb.write(table, test_data3)

    test_data_combined = pd.concat(
        [test_data1, test_data2, test_data3], ignore_index=True
    )
    t = mdb.read(table)

    # test some queries

    assert t.to_pd().equals(test_data_combined)
    assert (
        t[t["int1"].between(80, 120) & (t["int2"] > 500)][["int1", "int2", "str1"]]
        .to_pd()
        .equals(
            test_data_combined[
                test_data_combined["int1"].between(80, 120)
                & (test_data_combined["int2"] > 500)
            ][["int1", "int2", "str1"]].reset_index(drop=True)
        )
    )
    assert (
        t[t["str1"] == "hello"]
        .to_pd()
        .equals(
            test_data_combined[test_data_combined["str1"] == "hello"].reset_index(
                drop=True
            )
        )
    )
    timestamp_filter_result = test_data_combined[
        test_data_combined["timestamp1"].between("2011-01-01", "2011-02-01")
    ].reset_index(drop=True)
    assert (
        t[t["timestamp1"].between("2011-01-01", "2011-02-01")]
        .to_pd()
        .equals(timestamp_filter_result)
    )
    assert (
        t[
            t["timestamp1"].between(
                pd.Timestamp("2011-01-01"), pd.Timestamp("2011-02-01")
            )
        ]
        .to_pd()
        .equals(timestamp_filter_result)
    )

    # test simple delete_where_equal

    mdb.delete_where_equal("temp1", pd.DataFrame({"str1": ["hello"]}))

    t = mdb.read("temp1")
    assert t.to_pd().equals(
        test_data_combined[test_data_combined["str1"] != "hello"].reset_index(drop=True)
    )

    # test delete_where_equal and write at the same time, make sure the write gets
    # applied after the delete

    test_data4 = random_df(1)
    test_data4.loc[0, "str1"] = "foobar"
    mdb.write(
        "temp1", test_data4, delete_where_equal_df=pd.DataFrame({"str1": ["foobar"]})
    )

    test_data_combined_4 = pd.concat(
        [
            test_data_combined[
                ~test_data_combined["str1"].isin({"hello", "foobar"})
            ].reset_index(drop=True),
            test_data4,
        ],
        ignore_index=True,
    )
    t = mdb.read("temp1")
    t.to_pd().equals(test_data_combined_4)

    # now delete everything

    mdb.delete_all("temp1")
    t = mdb.read("temp1")
    assert len(t.to_pd()) == 0

    # test reading old versions

    # TODO we should have a better way of querying the version numbers
    assert (
        mdb.read("temp1", max_version_number=starting_version)
        .to_pd()
        .equals(test_data1)
    )
    assert (
        mdb.read("temp1", max_version_number=starting_version + 1)
        .to_pd()
        .equals(pd.concat([test_data1, test_data2], ignore_index=True))
    )
    assert (
        mdb.read("temp1", max_version_number=starting_version + 2)
        .to_pd()
        .equals(pd.concat([test_data1, test_data2, test_data3], ignore_index=True))
    )
    assert (
        mdb.read("temp1", max_version_number=starting_version + 3)
        .to_pd()
        .equals(
            test_data_combined[test_data_combined["str1"] != "hello"].reset_index(
                drop=True
            )
        )
    )
    assert (
        mdb.read("temp1", max_version_number=starting_version + 4)
        .to_pd()
        .equals(test_data_combined_4)
    )
    assert len(mdb.read("temp1", max_version_number=starting_version + 5).to_pd()) == 0


def test_meadowdb_duplication_keys(random_df, mdb_connection: meadowdb.Connection):

    mdb = mdb_connection
    test_data1 = random_df().drop_duplicates(["int1", "str1"])
    test_data1.loc[50, "str1"] = "hello"
    test_data1.loc[51, "str1"] = "foobar"
    test_data1.loc[52, "str1"] = "foobar"
    test_data2 = random_df().drop_duplicates(["int1", "str1"])
    test_data2.loc[0:50, ["int1", "str1"]] = test_data1.loc[0:50, ["int1", "str1"]]
    test_data3 = random_df().drop_duplicates(["int1", "str1"])
    test_data3.loc[25:75, ["int1", "str1"]] = test_data1.loc[25:75, ["int1", "str1"]]
    test_data_combined = pd.concat(
        [test_data1.loc[76:], test_data2[0:25], test_data2[51:], test_data3],
        ignore_index=True,
    )

    mdb.create_or_update_table_schema(
        "temp2", meadowdb.TableSchema(None, ["int1", "str1"])
    )

    t = mdb.read("temp2")
    t.to_pd()

    mdb.write("temp2", test_data1)
    mdb.write("temp2", test_data2)
    mdb.write("temp2", test_data3)

    t = mdb.read("temp2")
    assert t.to_pd().equals(test_data_combined)
    assert (
        t[t["int1"] < 250]
        .to_pd()
        .equals(
            test_data_combined[test_data_combined["int1"] < 250].reset_index(drop=True)
        )
    )

    mdb.delete_where_equal("temp2", pd.DataFrame({"str1": ["hello"]}))

    t = mdb.read("temp2")
    assert t.to_pd().equals(
        test_data_combined[test_data_combined["str1"] != "hello"].reset_index(drop=True)
    )

    test_data4 = random_df(1)
    test_data4.loc[0, "str1"] = "foobar"
    mdb.write(
        "temp2", test_data4, delete_where_equal_df=pd.DataFrame({"str1": ["hello"]})
    )

    test_data_combined_4 = pd.concat(
        [
            test_data_combined[
                ~test_data_combined["str1"].isin({"hello", "foobar"})
            ].reset_index(drop=True),
            test_data4,
        ],
        ignore_index=True,
    )
    t = mdb.read("temp2")
    t.to_pd().equals(test_data_combined_4)

    mdb.delete_all("temp2")
    t = mdb.read("temp2")
    assert len(t.to_pd()) == 0


def test_meadowdb_userspace(
    random_df: Callable[[], pd.DataFrame], mdb_connection: meadowdb.Connection
):
    mdb = mdb_connection
    table_name = "temp3"
    prod_data = random_df()
    prod_data.loc[50, "str1"] = "hello"
    us_data1 = random_df()
    us_data1.loc[50, "str1"] = "hello"
    us_data2 = random_df()

    u1_on_main = UserspaceSpec.main_with_tip("U1")
    u2_on_main = UserspaceSpec.main_with_tip("U2")

    # first, delete_where_equal in U1 without any data in the prod table
    mdb.delete_where_equal(table_name, pd.DataFrame({"str1": ["hello"]}), u1_on_main)
    assert len(mdb.read(table_name, u1_on_main).to_pd()) == 0

    # next write some data in the prod table, note that the deletes always get applied
    # on top of prod because this userspace is in read_committed mode (the only
    # currently available mode)
    mdb.write(table_name, prod_data)
    assert mdb.read(table_name).to_pd().equals(prod_data)

    prod_data_filtered = prod_data[prod_data["str1"] != "hello"].reset_index(drop=True)
    with set_default_userspace_spec(u2_on_main):
        # now read from a different (empty) userspace
        assert mdb.read(table_name).to_pd().equals(prod_data)
        # override the in-context U2 with explicitly specified U1
        assert mdb.read(table_name, u1_on_main).to_pd().equals(prod_data_filtered)

        # next, write some actual data into U1 (again override U2 by explicitly
        # specifying U1)
        mdb.write(table_name, us_data1, u1_on_main)
        assert mdb.read(table_name, UserspaceSpec.main()).to_pd().equals(prod_data)
        assert mdb.read(table_name, UserspaceSpec(tip="U1")).to_pd().equals(us_data1)

    expected = pd.concat([prod_data_filtered, us_data1], ignore_index=True)
    with set_default_userspace_spec(u1_on_main):
        assert mdb.read(table_name).to_pd().equals(expected)

        # next, wipe everything and write new data
        mdb.write(table_name, us_data2, delete_all=True)

    assert mdb.read(table_name).to_pd().equals(prod_data)
    assert mdb.read(table_name, u1_on_main).to_pd().equals(us_data2)

    # TODO none of the table schema interactions are tested


def test_single_userspace_spec_via_env_var(
    random_df: Callable[[], pd.DataFrame], mdb_connection: meadowdb.Connection
):
    # Setup - write df1 to main, then write to various test userspaces.
    mdb = mdb_connection
    main_data, df1, df2 = random_df(), random_df(), random_df()
    table_name = "table"
    mdb.write(table_name, main_data)

    # trick to make the tests work: we assume (reasonably) that env variables
    # are set for the lifetime of the process, so the mdb.write above sets the
    # default userspace to main, and then any changes to env variables are no longer
    # picked up. We set it manually to None again to make sure that following env var
    # settings are registered.
    meadowdb.connection._default_userspace_spec = None

    with unittest.mock.patch.dict("os.environ", {_MEADOWDB_DEFAULT_USERSPACE: "test1"}):
        # nothing written to test1, so table does not exist
        with pytest.raises(ValueError):
            mdb.read(table_name)

        # write something, should only show up in test1
        mdb.write(table_name, df1)

        assert mdb.read(table_name).to_pd().equals(df1)
        assert mdb.read(table_name, UserspaceSpec.main()).to_pd().equals(main_data)

    meadowdb.connection._default_userspace_spec = None

    with unittest.mock.patch.dict(
        "os.environ", {_MEADOWDB_DEFAULT_USERSPACE: "main,test1"}
    ):
        # if we read from main and test1, we should see both writes
        assert (
            mdb.read(table_name)
            .to_pd()
            .equals(pd.concat([main_data, df1], ignore_index=True))
        )

    meadowdb.connection._default_userspace_spec = None

    with unittest.mock.patch.dict(
        "os.environ", {_MEADOWDB_DEFAULT_USERSPACE: "main,test2"}
    ):
        # write something, should be written to test2
        # but we can see the main data too
        mdb.write(table_name, df2)

        assert (
            mdb.read(table_name)
            .to_pd()
            .equals(pd.concat([main_data, df2], ignore_index=True))
        )
        assert mdb.read(table_name, UserspaceSpec(tip="test2")).to_pd().equals(df2)
        assert mdb.read(table_name, UserspaceSpec.main()).to_pd().equals(main_data)
