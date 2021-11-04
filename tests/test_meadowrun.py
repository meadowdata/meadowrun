import pickle
import sys
import asyncio
import pathlib
import time
from typing import Sequence

import pip

import meadowrun.coordinator
import meadowrun.coordinator_main
import meadowrun.job_worker_main
from meadowrun.coordinator_client import (
    MeadowRunCoordinatorClientAsync,
    ProcessStateEnum,
)
from meadowrun.deployed_function import (
    MeadowRunFunction,
    Deployment,
    MeadowRunDeployedCommand,
    MeadowRunDeployedFunction,
)
from meadowrun.grid import grid_map
from meadowrun.meadowrun_pb2 import ServerAvailableFolder, GitRepoCommit, ProcessState


EXAMPLE_CODE = str((pathlib.Path(__file__).parent / "example_user_code").resolve())
MEADOWDATA_CODE = str((pathlib.Path(__file__).parent.parent / "src").resolve())


def test_meadowrun_server_available_folder():

    _test_meadowrun(
        ServerAvailableFolder(
            code_paths=[EXAMPLE_CODE], interpreter_path=sys.executable
        )
    )


TEST_REPO = str((pathlib.Path(__file__).parent.parent.parent / "test_repo").resolve())


# For all of these processes, to debug the coordinator and job_worker, just run them
# separately and the child processes we start in these tests will just silently fail.


def test_meadowrun_server_git_repo_commit():
    """
    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """
    _test_meadowrun(
        GitRepoCommit(
            repo_url=TEST_REPO,
            commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
            interpreter_path=sys.executable,
        )
    )


async def _wait_for_process(
    client: MeadowRunCoordinatorClientAsync, job_id: str
) -> Sequence[ProcessState]:
    """wait (no more than ~10s) for the remote process to finish"""
    i = 0
    results = None
    while (
        i == 0
        or results[0].state
        in (
            ProcessStateEnum.RUN_REQUESTED,
            ProcessStateEnum.ASSIGNED,
            ProcessStateEnum.RUNNING,
        )
        and i < 100
    ):
        print("Waiting for remote process to finish")
        time.sleep(0.1)
        results = await client.get_simple_job_states([job_id])
        assert len(results) == 1
        i += 1
    return results


def assert_successful(state: ProcessState) -> None:
    """
    Equivalent to `assert state == ProcessStateEnum.SUCCEEDED but provides a richer
    error message
    """
    if state.state != ProcessStateEnum.SUCCEEDED:
        if state.state in (
            ProcessStateEnum.PYTHON_EXCEPTION,
            ProcessStateEnum.RUN_REQUEST_FAILED,
        ):
            exception = pickle.loads(state.pickled_result)[2]
        else:
            exception = ""
        raise AssertionError(
            f"ProcessState was not SUCCEEDED: {state.state}. {exception}"
        )


def _test_meadowrun(deployment: Deployment):
    with (
        meadowrun.coordinator_main.main_in_child_process(),
        meadowrun.job_worker_main.main_in_child_process(),
    ):

        async def run():
            async with MeadowRunCoordinatorClientAsync() as client:

                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                add_job_state = await client.add_py_func_job(
                    request_id,
                    "example_runner",
                    MeadowRunDeployedFunction(
                        deployment,
                        MeadowRunFunction.from_name(
                            "example_package.example", "example_runner", arguments
                        ),
                    ),
                )
                assert add_job_state == "ADDED"

                # try running a duplicate
                duplicate_add_job_state = await client.add_py_func_job(
                    request_id,
                    "baz",
                    MeadowRunDeployedFunction(
                        ServerAvailableFolder(
                            code_paths=["foo"],
                            interpreter_path=sys.executable,
                        ),
                        MeadowRunFunction.from_name("foo.bar", "baz", ["foo"]),
                    ),
                )
                assert duplicate_add_job_state == "IS_DUPLICATE"

                # confirm that it's still running
                results = await client.get_simple_job_states([request_id])
                assert len(results) == 1
                assert results[0].state in (
                    ProcessStateEnum.RUN_REQUESTED,
                    ProcessStateEnum.ASSIGNED,
                    ProcessStateEnum.RUNNING,
                )

                results = await _wait_for_process(client, request_id)

                # confirm that it completed successfully
                assert_successful(results[0])
                assert results[0].pid > 0
                assert (
                    pickle.loads(results[0].pickled_result)[0]
                    == f"hello {arguments[0]}"
                )
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert f"example_runner called with {arguments[0]}" in text

                # test that requesting a different result works as expected
                results = await client.get_simple_job_states(["hey"])
                assert len(results) == 1
                assert results[0].state == ProcessStateEnum.UNKNOWN

                # test running a command rather than a function. pip should be available
                # in the Scripts folder of the current python interpreter
                request_id = "request2"
                add_job_state = await client.add_py_command_job(
                    request_id,
                    "pip",
                    MeadowRunDeployedCommand(deployment, ["pip", "--version"]),
                )
                assert add_job_state == "ADDED"

                results = await _wait_for_process(client, request_id)
                assert_successful(results[0])
                assert results[0].pid > 0
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert f"pip {pip.__version__}" in text

        asyncio.run(run())


def test_meadowrun_server_path_in_repo():
    """
    Tests GitRepoCommit.path_in_repo

    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """

    with (
        meadowrun.coordinator_main.main_in_child_process(),
        meadowrun.job_worker_main.main_in_child_process(),
    ):

        async def run():
            async with MeadowRunCoordinatorClientAsync() as client:

                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                await client.add_py_func_job(
                    request_id,
                    "example_runner",
                    MeadowRunDeployedFunction(
                        GitRepoCommit(
                            repo_url=TEST_REPO,
                            commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                            interpreter_path=sys.executable,
                            path_in_repo="example_package",
                        ),
                        MeadowRunFunction.from_name(
                            "example", "example_runner", arguments
                        ),
                    ),
                )

                results = await _wait_for_process(client, request_id)
                assert_successful(results[0])

                assert (
                    pickle.loads(results[0].pickled_result)[0]
                    == f"hello {arguments[0]}"
                )

        asyncio.run(run())


def test_meadowrun_command_context_variables():
    """
    Runs example_script twice (in parallel), once with no context variables, and once
    with context variables. Makes sure the output is the same in both cases.
    """

    deployment = ServerAvailableFolder(
        code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE], interpreter_path=sys.executable
    )

    with (
        meadowrun.coordinator_main.main_in_child_process(),
        meadowrun.job_worker_main.main_in_child_process(),
    ):

        async def run():
            async with MeadowRunCoordinatorClientAsync() as client:
                request_id3 = "request3"
                request_id4 = "request4"
                await client.add_py_command_job(
                    request_id3,
                    "example_script",
                    MeadowRunDeployedCommand(
                        deployment, ["python", "example_script.py"]
                    ),
                )
                await client.add_py_command_job(
                    request_id4,
                    "example_script",
                    MeadowRunDeployedCommand(
                        deployment, ["python", "example_script.py"], {"foo": "bar"}
                    ),
                )

                results = await _wait_for_process(client, request_id3)
                assert_successful(results[0])
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert "hello there: no_data" in text

                results = await _wait_for_process(client, request_id4)
                assert_successful(results[0])
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert "hello there: bar" in text

        asyncio.run(run())


def test_meadowrun_grid_job():
    with (
        meadowrun.coordinator_main.main_in_child_process(),
        meadowrun.job_worker_main.main_in_child_process(),
    ):
        results = grid_map(
            lambda s: f"hello {s}",
            ["abc", "def", "ghi"],
            ServerAvailableFolder(
                code_paths=[EXAMPLE_CODE], interpreter_path=sys.executable
            ),
        )

        assert results == ["hello abc", "hello def", "hello ghi"]
