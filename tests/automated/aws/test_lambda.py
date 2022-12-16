import os
from pathlib import Path
from typing import Awaitable, Callable, Tuple, TypeVar, Union

import pytest

from meadowrun import (
    MEADOWRUN_INTERPRETER,
    AllocLambda,
    Deployment,
    Resources,
    run_function,
)
from meadowrun.deployment_spec import LocalPipInterpreter, PipRequirementsFile
from meadowrun.run_job import run_command, run_map, run_map_as_completed

_T = TypeVar("_T")
_U = TypeVar("_U")


async def _test_all_entry_points(
    function: Callable[[_T], _U],
    args: Tuple[_T, ...],
    expected_results: Tuple[_U, ...],
    resources: Resources,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map_as_completed: bool = False,
) -> None:

    print("##### run_function #####")
    function_result = await run_function(
        lambda: function(args[0]),
        AllocLambda(),
        resources,
        deployment,
    )
    assert (
        function_result == expected_results[0]
    ), f"results={function_result} expected={expected_results[0]}"

    print("##### run_command #####")
    command_result = await run_command(
        "python -m pip --version",
        AllocLambda(),
        resources,
        deployment,
    )
    assert command_result.return_code == 200
    assert "pip" in command_result.log_file_name

    # map_results = await run_map(
    #     function,
    #     args,
    #     AllocLambda(),
    #     resources,
    #     deployment,
    # )
    # assert map_results is not None
    # for actual_result, expected_result in zip(map_results, expected_results):
    #     assert actual_result == expected_result

    # if include_run_map_as_completed:
    #     actual = []
    #     async for result in await run_map_as_completed(
    #         function,
    #         args,
    #         AllocLambda(),
    #         resources,
    #         deployment,
    #     ):
    #         actual.append(result.result_or_raise())

    #     assert set(actual) == set(expected_results)


@pytest.mark.asyncio
async def test_server_available_interpreter() -> None:
    await _test_all_entry_points(
        lambda x: x**x,
        (5,),
        (5**5,),
        Resources(memory_gb=0.25),
        Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
    )


async def _test_libraries_and_code_available(
    versions: Tuple[str, str],
    resources: Resources,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map_as_completed: bool = False,
) -> None:
    def remote_function(arg: str) -> Tuple[Tuple[str, str], str]:
        import importlib

        # we could just do import requests, but that messes with mypy
        pd = importlib.import_module("pandas")
        requests = importlib.import_module("requests")
        example = importlib.import_module("example")
        return (
            (requests.__version__, pd.__version__),
            example.join_strings("hello", arg),
        )

    args = ("foo", "bar", "baz")
    await _test_all_entry_points(
        remote_function,
        args,
        tuple((versions, f"hello, {arg}") for arg in args),
        resources,
        deployment,
        include_run_map_as_completed,
    )


def _path_from_here(path: str) -> str:
    """
    Combines the specified path with the directory this file is in (tests). So e.g.
    _relative_path_from_folder("../") is the root of this git repo
    """
    result = os.path.join(os.path.dirname(__file__), path)
    if not os.path.exists(result):
        raise ValueError(f"path {result} does not exist")
    return result


_TEST_REPO_PATH = Path(_path_from_here("../../../../test_repo"))


@pytest.mark.asyncio
async def test_mirror_local_pip() -> None:
    # this requires creating a virtualenv in this git repo's parent directory.
    venv_interpreter = _TEST_REPO_PATH / "env" / "bin" / "python"

    await _test_libraries_and_code_available(
        ("2.28.1", "1.5.0"),
        Resources(memory_gb=0.75, ephemeral_storage_gb=1),
        await Deployment.mirror_local(
            interpreter=LocalPipInterpreter(str(venv_interpreter)),
            additional_sys_paths=[str(_TEST_REPO_PATH / "example_package")],
        ),
    )


@pytest.mark.asyncio
async def test_mirror_local_pip_file() -> None:
    requirements = _TEST_REPO_PATH / "requirements.txt"

    await _test_libraries_and_code_available(
        ("2.28.1", "1.4.2"),
        Resources(memory_gb=0.75, ephemeral_storage_gb=1),
        await Deployment.mirror_local(
            interpreter=PipRequirementsFile(str(requirements), "3.9"),
            additional_sys_paths=[str(_TEST_REPO_PATH / "example_package")],
        ),
    )
