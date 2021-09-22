import pickle
import sys
import asyncio
import pathlib
import time

import nextrun.server_main
from nextrun.client import (
    NextRunClientAsync,
    ProcessStateEnum,
    NextRunDeployedFunction,
)
from nextrun.deployed_function import NextRunFunction, Deployment
from nextrun.nextrun_pb2 import ServerAvailableFolder, GitRepoCommit


def test_nextrun_server_available_folder():
    example_code = str((pathlib.Path(__file__).parent / "example_user_code").resolve())

    _test_nextrun(
        ServerAvailableFolder(
            code_paths=[example_code], interpreter_path=sys.executable
        )
    )


def test_nextrun_server_git_repo_commit():
    """
    Running this requires cloning https://github.com/nextdataplatform/test_repo next
    to the nextdataplatform repo.
    """

    test_repo = str(
        (pathlib.Path(__file__).parent.parent.parent / "test_repo").resolve()
    )

    _test_nextrun(
        GitRepoCommit(
            repo_url=test_repo,
            commit="ec1908ead5c4e8cec2c19262d8a423225aafb396",
            interpreter_path=sys.executable,
        )
    )


def _test_nextrun(deployment: Deployment):
    with nextrun.server_main.main_in_child_process():

        async def run():
            async with NextRunClientAsync() as client:
                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                run_request_result = await client.run_py_func(
                    request_id,
                    NextRunDeployedFunction(
                        deployment,
                        NextRunFunction(
                            "example_package.example", "example_runner", arguments
                        ),
                    ),
                )
                assert run_request_result.state == ProcessStateEnum.RUNNING
                assert run_request_result.pid > 0

                # try running a duplicate
                duplicate_request_result = await client.run_py_func(
                    request_id,
                    NextRunDeployedFunction(
                        ServerAvailableFolder(
                            code_paths=["foo"],
                            interpreter_path=sys.executable,
                        ),
                        NextRunFunction("foo.bar", "baz", ["foo"]),
                    ),
                )
                assert (
                    duplicate_request_result.state
                    == ProcessStateEnum.REQUEST_IS_DUPLICATE
                )

                # confirm that it's still running
                results = await client.get_process_states(["request1"])
                assert len(results) == 1
                assert results[0].state == ProcessStateEnum.RUNNING
                assert results[0].pid == run_request_result.pid

                # wait (no more than ~10s) for the remote process to finish
                i = 0
                while results[0].state == ProcessStateEnum.RUNNING and i < 10:
                    print("Waiting for remote process to finish")
                    time.sleep(0.1)
                    results = await client.get_process_states(["request1"])
                    assert len(results) == 1
                    i += 1

                # confirm that it completed successfully
                assert results[0].state == ProcessStateEnum.SUCCEEDED
                assert results[0].pid == run_request_result.pid
                assert (
                    pickle.loads(results[0].pickled_result)[0]
                    == f"hello {arguments[0]}"
                )

                # test that requesting a different result works as expected
                results = await client.get_process_states(["hey"])
                assert len(results) == 1
                assert results[0].state == nextrun.client.ProcessStateEnum.UNKNOWN

        asyncio.run(run())
