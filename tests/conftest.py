import pathlib
import tempfile
from typing import Generator

import pytest


@pytest.fixture()
def mdb_data_dir() -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory() as tmp_path:
        tmp_meadowdb = pathlib.Path(tmp_path) / "meadowdb"
        tmp_meadowdb.mkdir()
        print(f"Using {tmp_meadowdb} for test data")
        yield str(tmp_meadowdb)
