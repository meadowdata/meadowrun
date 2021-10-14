import pathlib

import pandas as pd

import meadowdb


def main():
    test_data_dir = str(
        (
            pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowdb"
        ).resolve()
    )

    conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(test_data_dir))
    conn.write("A", pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}))
    conn.read("A").to_pd()


if __name__ == "__main__":
    main()
