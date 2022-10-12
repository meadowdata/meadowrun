from __future__ import annotations

import asyncio
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from meadowrun.object_storage import ObjectStorage

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


class MockObjectStorage(ObjectStorage):
    @classmethod
    def get_url_scheme(cls) -> str:
        return "mock"

    async def _upload(self, file_path: str) -> str:
        raise NotImplementedError()

    async def _download(self, object_name: str, file_name: str) -> None:
        raise NotImplementedError()


def make_future(result: Any) -> asyncio.Future:
    future: asyncio.Future = asyncio.Future()
    future.set_result(result)
    return future


@pytest.mark.asyncio
async def test_upload(mocker: MockerFixture) -> None:
    os = MockObjectStorage()
    future = make_future(("bucket", "object"))
    os._upload = MagicMock(return_value=future)  # type: ignore

    actual = await os.upload_from_file_url("file:///a/b/c/def.zip")
    assert actual == "mock://bucket/object", actual


@pytest.mark.asyncio
async def test_download(tmp_path: Path) -> None:
    with zipfile.ZipFile(tmp_path / "object.zip", mode="x") as zip_file:
        zip_file.writestr("fake_file", "fake_data")

    os = MockObjectStorage()
    os._download = MagicMock(return_value=make_future(None))  # type: ignore

    folder = Path(await os.download_and_unzip("mock://bucket/object", str(tmp_path)))
    assert folder.exists()
    assert (folder / "fake_file").exists()
