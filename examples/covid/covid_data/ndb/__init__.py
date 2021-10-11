"""This file defines connections to nextdb"""

import pathlib
from typing import Optional

import nextdb

_TEST_DATA_DIR = (
    pathlib.Path(__file__).parent.parent.parent.parent.parent / "test_data" / "nextdb"
).resolve()


_ndb_test: Optional[nextdb.Connection] = None


def ndb_test() -> nextdb.Connection:
    global _ndb_test
    if _ndb_test is None:
        print(f"Using {_TEST_DATA_DIR} for test data")
        _ndb_test = nextdb.Connection(
            nextdb.TableVersionsClientLocal(str(_TEST_DATA_DIR))
        )
    return _ndb_test
