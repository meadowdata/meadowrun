import pathlib
import string
import tempfile
from typing import Callable

import meadowdb
import numpy as np
import pandas as pd
import pytest


def _random_string(n=5):
    return "".join(
        string.ascii_lowercase[i]
        for i in np.random.randint(len(string.ascii_lowercase), size=n)
    )


@pytest.fixture
def random_df() -> Callable[[int], pd.DataFrame]:
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

    return _random_df


@pytest.fixture()
def mdb_connection(mdb_data_dir: str) -> meadowdb.Connection:
    return meadowdb.Connection(meadowdb.TableVersionsClientLocal(mdb_data_dir))
