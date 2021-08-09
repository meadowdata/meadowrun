import string
import pathlib

import pandas as pd
import numpy as np

import nextdb


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


def test_nextdb():
    test_data_dir = (
        pathlib.Path(__file__).parent.parent / "test_data" / "nextdb"
    ).resolve()
    print(f"Using {test_data_dir} for test data")
    conn = nextdb.Connection(nextdb.TableVersionsClientLocal(test_data_dir))

    test_data1 = _random_df()
    test_data1.loc[50, "str1"] = "hello"
    test_data2 = _random_df()
    conn.write("temp1", test_data1)
    conn.write("temp1", test_data2)
    test_data_combined = pd.concat([test_data1, test_data2], ignore_index=True)
    # conn._table_versions_client._save_table_versions()

    t = conn.read("temp1")

    # various sample queries
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

    # test deletes

    conn.delete_where_equal("temp1", pd.DataFrame({"str1": ["hello"]}))

    t = conn.read("temp1")
    assert t.to_pd().equals(
        test_data_combined[test_data_combined["str1"] != "hello"].reset_index(drop=True)
    )

    # test deduplication_keys

    test_data3 = test_data1.drop_duplicates(["int1", "str1"])
    test_data4 = test_data2.drop_duplicates(["int1", "str1"])
    test_data4.loc[0:50, ["int1", "str1"]] = test_data3.loc[0:50, ["int1", "str1"]]
    test_data5 = _random_df().drop_duplicates(["int1", "str1"])
    test_data5.loc[25:75, ["int1", "str1"]] = test_data3.loc[25:75, ["int1", "str1"]]
    test_data_combined2 = pd.concat(
        [test_data3.loc[76:], test_data4[0:25], test_data4[51:], test_data5],
        ignore_index=True,
    )

    conn.create_or_update_table_schema(
        "temp2", nextdb.TableSchema(None, ["int1", "str1"])
    )

    t = conn.read("temp2")
    t.to_pd()

    conn.write("temp2", test_data3)
    conn.write("temp2", test_data4)
    conn.write("temp2", test_data5)

    t = conn.read("temp2")
    assert t.to_pd().equals(test_data_combined2)
    assert (
        t[t["int1"] < 250]
        .to_pd()
        .equals(
            test_data_combined2[test_data_combined2["int1"] < 250].reset_index(
                drop=True
            )
        )
    )

    conn.delete_where_equal("temp2", pd.DataFrame({"str1": ["hello"]}))

    t = conn.read("temp2")
    assert t.to_pd().equals(
        test_data_combined2[test_data_combined2["str1"] != "hello"].reset_index(
            drop=True
        )
    )
