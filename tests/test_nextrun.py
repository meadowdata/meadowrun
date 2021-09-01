import pickle
import sys
import asyncio
import pathlib
import time

import nextrun.server_main
from nextrun.client import (
    NextRunClientAsync,
    ProcessStateEnum,
    JobRunSpecDeployedFunction,
)
from nextrun.job_run_spec import current_version_tuple


def test_nextrun():
    with nextrun.server_main.main_in_child_process():

        async def run():
            async with NextRunClientAsync() as client:
                # run a remote process
                arguments = ["foo"]
                example_code = str(
                    (pathlib.Path(__file__).parent / "example_user_code").resolve()
                )
                request_id = "request1"
                run_request_result = await client.run_py_func(
                    request_id,
                    JobRunSpecDeployedFunction(
                        sys.executable,
                        current_version_tuple(),
                        [example_code],
                        "example_package.example",
                        "example_runner",
                        arguments,
                    ),
                )
                assert run_request_result.state == ProcessStateEnum.RUNNING
                assert run_request_result.pid > 0

                # try running a duplicate
                duplicate_request_result = await client.run_py_func(
                    request_id,
                    JobRunSpecDeployedFunction(
                        sys.executable,
                        current_version_tuple(),
                        ["foo"],
                        "foo.bar",
                        "baz",
                        ["foo"],
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
                    pickle.loads(results[0].pickled_result) == f"hello"
                    f" {arguments[0]}"
                )

                # test that requesting a different result works as expected
                results = await client.get_process_states(["hey"])
                assert len(results) == 1
                assert results[0].state == nextrun.client.ProcessStateEnum.UNKNOWN

        asyncio.run(run())


if __name__ == "__main__":
    test_nextrun()
