"""This file defines connections to meadowdb"""

import os
from typing import Optional

import meadowdb

import covid_data

_TEST_DATA_DIR = os.path.join(covid_data.ROOT_DIR, "test_data", "meadowdb")


_mdb_test: Optional[meadowdb.Connection] = None


def mdb_test(reset: bool = False) -> meadowdb.Connection:
    global _mdb_test
    if _mdb_test is None or reset:
        print(f"Using {_TEST_DATA_DIR} for test data")
        _mdb_test = meadowdb.Connection(
            meadowdb.TableVersionsClientLocal(str(_TEST_DATA_DIR))
        )
    return _mdb_test
