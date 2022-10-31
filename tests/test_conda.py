from __future__ import annotations

import json
import os
import sys
from typing import TYPE_CHECKING
import pytest

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
from meadowrun.deployment import conda

pytestmark = pytest.mark.skipif(
    "sys.version_info < (3, 8)",
    reason="patch() was only updated for patching async functions in python 3.8",
)

_CONDA_RUN_NAME = f"meadowrun.deployment.conda.{conda._run.__qualname__}"


@pytest.mark.asyncio
async def test_conda_env_export_base(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.side_effect = [
        (json.dumps(dict(envs=["/home/user/anaconda"])), ""),
        ("list explicit output", ""),
        (_SOME_YAML, ""),
    ]

    result = await conda.env_export("base")
    assert _run_conda.call_count == 3
    assert result == "list explicit output"


@pytest.mark.asyncio
async def test_conda_env_export_no_envs(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = ((json.dumps(dict(envs=[])), ""),)

    with pytest.raises(ValueError):
        await conda.env_export("base")


@pytest.mark.asyncio
async def test_conda_env_export_name(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.side_effect = [
        (
            json.dumps(
                dict(
                    envs=[
                        "/home/user/anaconda",
                        "/home/user/anaconda/envs/abc",
                        "/home/user/anaconda/envs/def",
                    ]
                )
            ),
            "",
        ),
        ("list explicit output", ""),
        (_SOME_YAML, ""),
    ]
    result = await conda.env_export("abc")
    assert _run_conda.call_count == 3
    assert result == "list explicit output"


@pytest.mark.asyncio
async def test_conda_env_export_name_does_not_exist(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = (
        (
            json.dumps(
                dict(
                    envs=[
                        "/home/user/anaconda",
                        "/home/user/anaconda/envs/abc",
                        "/home/user/anaconda/envs/def",
                    ]
                )
            ),
            "",
        ),
    )

    with pytest.raises(ValueError):
        await conda.env_export("whereami")


@pytest.mark.asyncio
async def test_conda_env_export_path(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.side_effect = [
        (
            json.dumps(
                dict(
                    envs=[
                        "/home/user/anaconda",
                        "/home/user/anaconda/envs/abc",
                        "/home/user/anaconda/envs/def",
                        "/put/it/somewhere/else",
                    ]
                )
            ),
            "",
        ),
        ("list explicit output", ""),
        (_SOME_YAML, ""),
    ]
    result = await conda.env_export("/put/it/somewhere/else")
    assert _run_conda.call_count == 3
    assert result == "list explicit output"


@pytest.mark.asyncio
async def test_conda_env_export_path_does_not_exist(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = (
        (
            json.dumps(
                dict(
                    envs=[
                        "/home/user/anaconda",
                        "/home/user/anaconda/envs/abc",
                        "/home/user/anaconda/envs/def",
                    ]
                )
            ),
            "",
        ),
    )

    with pytest.raises(ValueError):
        await conda.env_export("/where/am/i")


_SOME_YAML = "foo:\n- some yaml"


@pytest.mark.asyncio
async def test_conda_env_export_activated(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = (_SOME_YAML, "")
    mocker.patch.dict(
        os.environ, {"CONDA_PREFIX": "/home/user/anaconda/envs/abc"}, clear=True
    )
    mocker.patch.object(sys, "executable", "/home/user/anaconda/envs/abc/bin/python")
    result = await conda.try_get_current_conda_env()
    assert _run_conda.call_count == 2
    assert result == _SOME_YAML


@pytest.mark.asyncio
async def test_conda_env_export_activated_but_lower_priority(
    mocker: MockerFixture,
) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = (_SOME_YAML, "")
    mocker.patch.dict(
        os.environ, {"CONDA_PREFIX": "/home/user/anaconda/envs/abc"}, clear=True
    )
    mocker.patch.object(sys, "executable", "/home/user/some_other/venv/python")
    result = await conda.try_get_current_conda_env()
    _run_conda.assert_not_called()
    assert result is None


@pytest.mark.asyncio
async def test_conda_env_export_not_activated(mocker: MockerFixture) -> None:
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    mocker.patch.dict(os.environ, {}, clear=True)
    assert await conda.try_get_current_conda_env() is None
    _run_conda.assert_not_called()
