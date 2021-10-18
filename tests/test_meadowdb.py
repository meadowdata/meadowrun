import string
import pathlib

import pandas as pd
import numpy as np

import meadowdb


def _random_string(n=5):
    return "".join(
        string.ascii_lowercase[i]
        for i in np.random.randint(len(string.ascii_lowercase), size=n)
    )


def _random_df(n=100):
    return pd.DataFrame(
        {
            "int1": np.random.randint(1000, size=n),
            "int2": np.random.randint(1000, size=n),
            "float1": np.random.randn(n),
            "str1": [_random_string() for _ in range(n)],
            "timestamp1": pd.date_range("2011-01-01", periods=n, freq="D"),
        }
    )


_TEST_DATA_DIR = str(
    (pathlib.Path(__file__).parent.parent / "test_data" / "meadowdb").resolve()
)


def test_meadowdb():
    # set up connection
    print(f"Using {_TEST_DATA_DIR} for test data")
    conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(_TEST_DATA_DIR))

    # generate and write some data

    test_data1 = _random_df()
    test_data1.loc[50, "str1"] = "hello"
    test_data1.loc[51, "str1"] = "foobar"
    test_data1.loc[52, "str1"] = "foobar"
    test_data2 = _random_df()
    test_data3 = _random_df()
    conn.write("temp1", test_data1)
    conn.write("temp1", test_data2)
    conn.write("temp1", test_data3)

    test_data_combined = pd.concat(
        [test_data1, test_data2, test_data3], ignore_index=True
    )
    t = conn.read("temp1")

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

    conn.delete_where_equal("temp1", pd.DataFrame({"str1": ["hello"]}))

    t = conn.read("temp1")
    assert t.to_pd().equals(
        test_data_combined[test_data_combined["str1"] != "hello"].reset_index(drop=True)
    )

    # test delete_where_equal and write at the same time, make sure the write gets
    # applied after the delete

    test_data4 = _random_df(1)
    test_data4.loc[0, "str1"] = "foobar"
    conn.write(
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
    t = conn.read("temp1")
    t.to_pd().equals(test_data_combined_4)

    # now delete everything

    conn.delete_all("temp1")
    t = conn.read("temp1")
    assert len(t.to_pd()) == 0

    # test reading old versions

    # TODO we should have a better way of querying the version numbers
    assert conn.read("temp1", max_version_number=0).to_pd().equals(test_data1)
    assert (
        conn.read("temp1", max_version_number=1)
        .to_pd()
        .equals(pd.concat([test_data1, test_data2], ignore_index=True))
    )
    assert (
        conn.read("temp1", max_version_number=2)
        .to_pd()
        .equals(pd.concat([test_data1, test_data2, test_data3], ignore_index=True))
    )
    assert (
        conn.read("temp1", max_version_number=3)
        .to_pd()
        .equals(
            test_data_combined[test_data_combined["str1"] != "hello"].reset_index(
                drop=True
            )
        )
    )
    assert conn.read("temp1", max_version_number=4).to_pd().equals(test_data_combined_4)
    assert len(conn.read("temp1", max_version_number=5).to_pd()) == 0


def test_meadowdb_duplication_keys():
    """Test deduplication_keys"""

    conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(_TEST_DATA_DIR))

    test_data1 = _random_df().drop_duplicates(["int1", "str1"])
    test_data1.loc[50, "str1"] = "hello"
    test_data1.loc[51, "str1"] = "foobar"
    test_data1.loc[52, "str1"] = "foobar"
    test_data2 = _random_df().drop_duplicates(["int1", "str1"])
    test_data2.loc[0:50, ["int1", "str1"]] = test_data1.loc[0:50, ["int1", "str1"]]
    test_data3 = _random_df().drop_duplicates(["int1", "str1"])
    test_data3.loc[25:75, ["int1", "str1"]] = test_data1.loc[25:75, ["int1", "str1"]]
    test_data_combined = pd.concat(
        [test_data1.loc[76:], test_data2[0:25], test_data2[51:], test_data3],
        ignore_index=True,
    )

    conn.create_or_update_table_schema(
        "temp2", meadowdb.TableSchema(None, ["int1", "str1"])
    )

    t = conn.read("temp2")
    t.to_pd()

    conn.write("temp2", test_data1)
    conn.write("temp2", test_data2)
    conn.write("temp2", test_data3)

    t = conn.read("temp2")
    assert t.to_pd().equals(test_data_combined)
    assert (
        t[t["int1"] < 250]
        .to_pd()
        .equals(
            test_data_combined[test_data_combined["int1"] < 250].reset_index(drop=True)
        )
    )

    conn.delete_where_equal("temp2", pd.DataFrame({"str1": ["hello"]}))

    t = conn.read("temp2")
    assert t.to_pd().equals(
        test_data_combined[test_data_combined["str1"] != "hello"].reset_index(drop=True)
    )

    test_data4 = _random_df(1)
    test_data4.loc[0, "str1"] = "foobar"
    conn.write(
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
    t = conn.read("temp2")
    t.to_pd().equals(test_data_combined_4)

    conn.delete_all("temp2")
    t = conn.read("temp2")
    assert len(t.to_pd()) == 0
