from __future__ import annotations

import os
import urllib.parse
import zipfile
from typing import TYPE_CHECKING

import pytest

from automated.test_local_automated import LocalHost
from meadowrun.deployment_manager import _get_zip_file_code_paths
from meadowrun.meadowrun_pb2 import CodeZipFile
from meadowrun.run_job import _prepare_code_deployment
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.asyncio
async def test_ensure_uploaded_incremental(tmp_path: Path) -> None:
    # prepare a CodeZipFile with a file:// url. This is equivalent to what mirror_local
    # will produce

    # the parent directory of temp.zip will get deleted by _prepare_code_deployment, so
    # we need to create a sub-directory of tmp_path
    os.makedirs(tmp_path / "staging", exist_ok=True)
    zip_file_path = tmp_path / "staging" / "temp.zip"
    with zipfile.ZipFile(zip_file_path, mode="x") as zip_file:
        zip_file.writestr("fake_file", "fake_data")
    url = urllib.parse.urlunparse(("file", "", str(zip_file_path), "", "", ""))
    code_zip_file = CodeZipFile(url=url)

    # now call _prepare_code_deployment on our code_zip_file

    local_host = LocalHost(tmp_path)
    await _prepare_code_deployment(code_zip_file, local_host)
    assert code_zip_file.url.startswith(f"mdrstorage://_/{STORAGE_CODE_CACHE_PREFIX}")

    # now simulate the "remote worker" that unpacks the code_zip_file

    async with await local_host.get_storage_bucket() as storage_bucket:
        (
            code_paths,
            interpreter_spec_path,
            current_working_directory,
        ) = await _get_zip_file_code_paths(str(tmp_path), code_zip_file, storage_bucket)
    print(code_paths, interpreter_spec_path, current_working_directory)
    assert os.path.exists(current_working_directory)
    assert os.path.exists(os.path.join(current_working_directory, "fake_file"))
