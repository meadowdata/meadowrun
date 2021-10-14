"""This file defines connections to meadowdb"""

import pathlib
from typing import Optional

import meadowdb

_TEST_DATA_DIR = (
    pathlib.Path(__file__).parent.parent.parent.parent.parent / "test_data" / "meadowdb"
).resolve()


_mdb_test: Optional[meadowdb.Connection] = None


def mdb_test() -> meadowdb.Connection:
    global _mdb_test
    if _mdb_test is None:
        print(f"Using {_TEST_DATA_DIR} for test data")
        _mdb_test = meadowdb.Connection(
            meadowdb.TableVersionsClientLocal(str(_TEST_DATA_DIR))
        )
    return _mdb_test
