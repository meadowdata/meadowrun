import pickle
import sys
import asyncio
import pathlib
import time
from typing import List

import pip

import meadowrun.server_main
from meadowrun.client import (
    MeadowRunClientAsync,
    ProcessStateEnum,
    MeadowRunDeployedFunction,
)
from meadowrun.deployed_function import (
    MeadowRunFunction,
    Deployment,
    MeadowRunDeployedCommand,
)
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
    client: MeadowRunClientAsync, request_id: str
) -> List[ProcessState]:
    """wait (no more than ~10s) for the remote process to finish"""
    i = 0
    results = None
    while i == 0 or results[0].state == ProcessStateEnum.RUNNING and i < 100:
        print("Waiting for remote process to finish")
        time.sleep(0.1)
        results = await client.get_process_states([request_id])
        assert len(results) == 1
        i += 1
    return results


def _test_meadowrun(deployment: Deployment):
    with meadowrun.server_main.main_in_child_process():

        async def run():
            async with MeadowRunClientAsync() as client:

                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                run_request_result = await client.run_py_func(
                    request_id,
                    "example_runner",
                    MeadowRunDeployedFunction(
                        deployment,
                        MeadowRunFunction(
                            "example_package.example", "example_runner", arguments
                        ),
                    ),
                )
                assert run_request_result.state == ProcessStateEnum.RUNNING
                assert run_request_result.pid > 0
                log_file_name = run_request_result.log_file_name

                # try running a duplicate
                duplicate_request_result = await client.run_py_func(
                    request_id,
                    "baz",
                    MeadowRunDeployedFunction(
                        ServerAvailableFolder(
                            code_paths=["foo"],
                            interpreter_path=sys.executable,
                        ),
                        MeadowRunFunction("foo.bar", "baz", ["foo"]),
                    ),
                )
                assert (
                    duplicate_request_result.state
                    == ProcessStateEnum.REQUEST_IS_DUPLICATE
                )

                # confirm that it's still running
                results = await client.get_process_states([request_id])
                assert len(results) == 1
                assert results[0].state == ProcessStateEnum.RUNNING
                assert results[0].pid == run_request_result.pid
                assert log_file_name == run_request_result.log_file_name

                results = await _wait_for_process(client, request_id)

                # confirm that it completed successfully
                assert results[0].state == ProcessStateEnum.SUCCEEDED
                assert results[0].pid == run_request_result.pid
                assert (
                    pickle.loads(results[0].pickled_result)[0]
                    == f"hello {arguments[0]}"
                )
                assert log_file_name == results[0].log_file_name
                with open(log_file_name, "r") as log_file:
                    text = log_file.read()
                assert f"example_runner called with {arguments[0]}" in text

                # test that requesting a different result works as expected
                results = await client.get_process_states(["hey"])
                assert len(results) == 1
                assert results[0].state == meadowrun.client.ProcessStateEnum.UNKNOWN

                # test running a command rather than a function. pip should be available
                # in the Scripts folder of the current python interpreter
                request_id = "request2"
                run_request_result = await client.run_py_command(
                    request_id,
                    "pip",
                    MeadowRunDeployedCommand(deployment, ["pip", "--version"]),
                )
                assert run_request_result.state == ProcessStateEnum.RUNNING
                assert run_request_result.pid > 0
                log_file_name = run_request_result.log_file_name

                results = await _wait_for_process(client, request_id)
                assert results[0].state == ProcessStateEnum.SUCCEEDED
                assert results[0].pid == run_request_result.pid
                assert log_file_name == results[0].log_file_name
                with open(log_file_name, "r") as log_file:
                    text = log_file.read()
                assert f"pip {pip.__version__}" in text

        asyncio.run(run())


def test_meadowrun_command_context_variables():
    """
    Runs example_script twice (in parallel), once with no context variables, and once
    with context variables. Makes sure the output is the same in both cases.
    """

    deployment = ServerAvailableFolder(
        code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE], interpreter_path=sys.executable
    )

    with meadowrun.server_main.main_in_child_process():

        async def run():
            async with MeadowRunClientAsync() as client:
                request_id3 = "request3"
                request_id4 = "request4"
                await client.run_py_command(
                    request_id3,
                    "example_script",
                    MeadowRunDeployedCommand(
                        deployment, ["python", "example_script.py"]
                    ),
                )
                await client.run_py_command(
                    request_id4,
                    "example_script",
                    MeadowRunDeployedCommand(
                        deployment, ["python", "example_script.py"], {"foo": "bar"}
                    ),
                )

                results = await _wait_for_process(client, request_id3)
                assert results[0].state == ProcessStateEnum.SUCCEEDED
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert "hello there: no_data" in text

                results = await _wait_for_process(client, request_id4)
                assert results[0].state == ProcessStateEnum.SUCCEEDED
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert "hello there: bar" in text

        asyncio.run(run())
