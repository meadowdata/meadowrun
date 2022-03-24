import pathlib
from typing import Union

import pytest

import meadowrun.docker_controller
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.deployment import (
    CodeDeployment,
    InterpreterDeployment,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
    get_latest_interpreter_version,
)
from meadowrun.meadowrun_pb2 import (
    ContainerAtTag,
    GitRepoBranch,
    GitRepoCommit,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
    ContainerAtDigest,
)
from meadowrun.run_job import run_function, LocalHost, Deployment, run_command

EXAMPLE_CODE = str(
    (pathlib.Path(__file__).parent.parent / "example_user_code").resolve()
)
MEADOWRUN_CODE = str((pathlib.Path(__file__).parent.parent.parent / "src").resolve())


@pytest.mark.asyncio
async def test_meadowrun_server_available_folder():
    await _test_meadowrun(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
    )


@pytest.mark.asyncio
async def test_meadowrun_server_available_folder_container_digest():
    await _test_meadowrun(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        await get_latest_interpreter_version(
            ContainerAtTag(repository="python", tag="3.9.8-slim-buster"), {}
        ),
    )


@pytest.mark.asyncio
async def test_meadowrun_server_available_folder_container_tag():
    await _test_meadowrun(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
    )


TEST_REPO = str(
    (pathlib.Path(__file__).parent.parent.parent.parent / "test_repo").resolve()
)

TEST_WORKING_FOLDER = str(
    (pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowrun").resolve()
)


@pytest.mark.asyncio
async def test_meadowrun_git_repo_commit():
    """
    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowrun repo.
    """
    await _test_meadowrun(
        GitRepoCommit(
            repo_url=TEST_REPO, commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6"
        ),
        ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
    )


@pytest.mark.asyncio
async def test_meadowrun_git_repo_branch():
    await _test_meadowrun(
        GitRepoBranch(repo_url=TEST_REPO, branch="main"),
        ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
    )


@pytest.mark.asyncio
async def test_meadowrun_git_repo_commit_container():
    """
    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowrun repo.
    """
    # TODO first make sure the image we're looking for is NOT already cached on this
    #  system, then run it again after it has been cached, as this works different code
    #  paths
    await _test_meadowrun(
        GitRepoCommit(
            repo_url=TEST_REPO, commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6"
        ),
        ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
    )


async def _test_meadowrun(
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
):

    results: str = await run_function(
        "example_package.example.example_runner",
        LocalHost(),
        Deployment(interpreter_deployment, code_deployment),
        args=["foo"],
    )
    assert results == "hello foo"

    job_completion = await run_command(
        "pip --version",
        LocalHost(),
        Deployment(interpreter_deployment, code_deployment),
    )

    with open(job_completion.log_file_name, "r", encoding="utf-8") as log_file:
        text = log_file.read()
    assert "pip" in text


@pytest.mark.asyncio
async def test_meadowrun_path_in_git_repo():
    """
    Tests GitRepoCommit.path_in_repo

    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowrun repo.
    """

    results: str = await run_function(
        "example.example_runner",
        LocalHost(),
        Deployment(
            code=GitRepoCommit(
                repo_url=TEST_REPO,
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                path_in_repo="example_package",
            )
        ),
        args=["foo"],
    )
    assert results == "hello foo"


@pytest.mark.asyncio
async def test_meadowrun_command_context_variables():
    """
    Runs example_script twice (in parallel), once with no context variables, and once
    with context variables. Makes sure the output is the same in both cases.
    """
    job_completion1 = await run_command(
        "python example_script.py",
        LocalHost(),
        Deployment(
            code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
        ),
    )
    job_completion2 = await run_command(
        "python example_script.py",
        LocalHost(),
        Deployment(
            code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
        ),
        {"foo": "bar"},
    )

    with open(job_completion1.log_file_name, "r", encoding="utf-8") as log_file:
        text = log_file.read()
    assert "hello there: no_data" in text

    with open(job_completion2.log_file_name, "r", encoding="utf-8") as log_file:
        text = log_file.read()
    assert "hello there: bar" in text


@pytest.mark.asyncio
async def test_meadowrun_containers():
    """
    Basic test on running with containers, checks that different images behave as
    expected
    """
    for version in ["3.9.8", "3.8.12"]:
        digest = await (
            meadowrun.docker_controller.get_latest_digest_from_registry(
                "python", f"{version}-slim-buster", None
            )
        )

        result = await run_command(
            "python --version",
            LocalHost(),
            Deployment(ContainerAtDigest(repository="python", digest=digest)),
        )

        with open(result.log_file_name, "r", encoding="utf-8") as log_file:
            text = log_file.read()
        assert text.startswith(f"Python {version}")
