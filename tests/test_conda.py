import json
import os
import pytest
from pytest_mock import MockerFixture
from meadowrun import conda


_CONDA_RUN_NAME = f"meadowrun.conda.{conda._run.__qualname__}"


@pytest.mark.asyncio
async def test_conda_env_export_base(mocker: MockerFixture):
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.side_effect = [
        (json.dumps(dict(envs=["/home/user/anaconda"])), ""),
        ("some yaml", ""),
    ]

    result = await conda.env_export("base")
    assert _run_conda.call_count == 2
    assert result == "some yaml"


@pytest.mark.asyncio
async def test_conda_env_export_no_envs(mocker: MockerFixture):
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = ((json.dumps(dict(envs=[])), ""),)

    with pytest.raises(ValueError):
        await conda.env_export("base")


@pytest.mark.asyncio
async def test_conda_env_export_name(mocker: MockerFixture):
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
        ("some yaml", ""),
    ]
    result = await conda.env_export("abc")
    assert _run_conda.call_count == 2
    assert result == "some yaml"


@pytest.mark.asyncio
async def test_conda_env_export_name_does_not_exist(mocker: MockerFixture):
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
async def test_conda_env_export_path(mocker: MockerFixture):
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
        ("some yaml", ""),
    ]
    result = await conda.env_export("/put/it/somewhere/else")
    assert _run_conda.call_count == 2
    assert result == "some yaml"


@pytest.mark.asyncio
async def test_conda_env_export_path_does_not_exist(mocker: MockerFixture):
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


@pytest.mark.asyncio
async def test_conda_env_export_activated(mocker: MockerFixture):
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    _run_conda.return_value = ("some yaml", "")
    mocker.patch.dict(
        os.environ, {"CONDA_PREFIX": "/home/user/anaconda/envs/abc"}, clear=True
    )
    result = await conda.try_get_current_conda_env()
    _run_conda.assert_called_once()
    assert result == "some yaml"


@pytest.mark.asyncio
async def test_conda_env_export_not_activated(mocker: MockerFixture):
    _run_conda = mocker.patch(_CONDA_RUN_NAME)
    mocker.patch.dict(os.environ, {}, clear=True)
    assert await conda.try_get_current_conda_env() is None
    _run_conda.assert_not_called()
