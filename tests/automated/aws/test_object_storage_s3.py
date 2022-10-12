from __future__ import annotations

import filecmp
import json
from typing import TYPE_CHECKING

import pytest
from botocore.exceptions import ClientError
from meadowrun.storage_grid_job import (
    S3Bucket,
    _existing_keys,
    download_chunked_file,
    ensure_uploaded_incremental,
    get_aws_s3_bucket,
)
from types_aiobotocore_s3.client import S3Client

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture


@pytest.mark.asyncio
async def test_incremental_fresh_upload(tmp_path: Path, mocker: MockerFixture) -> None:
    _existing_keys.clear()

    s3_client_mock = mocker.create_autospec(S3Client)
    bucket_name = "test_bucket"
    s3_client = S3Bucket(s3_client_mock, bucket_name, f"mock/{bucket_name}")
    # head_object is used to check if an object exists. 404 means it doesn't exist.
    s3_client_mock.head_object = mocker.AsyncMock(
        side_effect=ClientError(
            error_response={"Error": {"Code": "404"}}, operation_name="head_object"
        )
    )
    file_size = 8000
    local_file_path = _make_big_file(tmp_path, size_bytes=file_size)
    assert local_file_path.stat().st_size == file_size
    key_prefix = "key_prefix/"
    await ensure_uploaded_incremental(
        s3_client, str(local_file_path), key_prefix, avg_chunk_size=1000
    )

    # even though all chunks need to be uploaded, some chunks are repeated.
    # De-duping makes the call count for head and put is less than the number of chunks.
    expected_call_count = 4
    assert s3_client_mock.head_object.call_count == expected_call_count
    assert s3_client_mock.head_object.await_count == expected_call_count
    for head_object_call in s3_client_mock.head_object.call_args_list:
        assert head_object_call.kwargs["Bucket"] == bucket_name
        assert head_object_call.kwargs["Key"].startswith(key_prefix)

    assert s3_client_mock.put_object.call_count == expected_call_count
    assert s3_client_mock.put_object.await_count == expected_call_count

    # keep the length of each chunk here for later.
    key_len = {}
    for put_object_call in s3_client_mock.put_object.call_args_list[:-1]:
        assert put_object_call.kwargs["Bucket"] == bucket_name
        assert put_object_call.kwargs["Key"].startswith(key_prefix)
        key_len[put_object_call.kwargs["Key"]] = len(put_object_call.kwargs["Body"])

    last_put = s3_client_mock.put_object.call_args_list[-1]
    assert last_put.kwargs["Bucket"] == bucket_name
    assert last_put.kwargs["Key"].startswith(key_prefix)
    assert last_put.kwargs["Key"].endswith("json")
    chunks_json = json.loads(last_put.kwargs["Body"].decode("utf-8"))
    assert len(chunks_json["chunks"]) == 20

    # check that all chunks put together gives us the same length as the original file.
    total_len = 0
    for chunk in chunks_json["chunks"]:
        total_len += key_len[chunk]
    assert total_len == file_size


@pytest.mark.asyncio
async def test_incremental_no_upload(tmp_path: Path, mocker: MockerFixture) -> None:
    _existing_keys.clear()
    s3_client_mock = mocker.create_autospec(S3Client)
    bucket_name = "test_bucket"
    s3_client = S3Bucket(s3_client_mock, bucket_name, f"mock/{bucket_name}")
    # no side_effect means all chunks will appear as "already existing"
    s3_client_mock.head_object = mocker.AsyncMock()
    file_size = 8000
    local_file_path = _make_big_file(tmp_path, size_bytes=file_size)
    assert local_file_path.stat().st_size == file_size
    key_prefix = "key_prefix/"
    await ensure_uploaded_incremental(
        s3_client, str(local_file_path), key_prefix, avg_chunk_size=1000
    )

    expected_call_count = 4
    assert s3_client_mock.head_object.call_count == expected_call_count
    assert s3_client_mock.head_object.await_count == expected_call_count

    assert s3_client_mock.put_object.call_count == 0
    assert s3_client_mock.put_object.await_count == 0


@pytest.mark.asyncio
async def test_incremental_roundtrip(tmp_path: Path) -> None:
    # this test actually uploads to S3

    _existing_keys.clear()
    region_name = "us-east-2"
    source_file = _make_big_file(tmp_path, size_bytes=8000)
    async with get_aws_s3_bucket(region_name) as s3_client:
        chunks_json_key = await ensure_uploaded_incremental(
            s3_client,
            str(source_file),
            avg_chunk_size=1000,
            key_prefix="test/",
        )

        target_file = tmp_path / "target"
        await download_chunked_file(s3_client, chunks_json_key, str(target_file))

        assert target_file.exists()
        assert filecmp.cmp(source_file, target_file, shallow=False)


def _make_big_file(tmp_path: Path, size_bytes: int = 8000) -> Path:
    local_file_path = tmp_path / "file"
    chars = "0123456789abcdefghijklmnopqrst"
    with open(local_file_path, "wb") as f:
        for i in range(size_bytes // 10):
            f.write(chars[i % 20 : i % 20 + 10].encode("utf-8"))
    return local_file_path
