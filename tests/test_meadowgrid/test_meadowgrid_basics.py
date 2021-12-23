import asyncio
import pathlib
import pickle
import time
from typing import Sequence, Union

import pip

import meadowgrid.coordinator
import meadowgrid.coordinator_main
import meadowgrid.docker_controller
import meadowgrid.job_worker_main
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.coordinator_client import (
    MeadowGridCoordinatorClientAsync,
    ProcessStateEnum,
)
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridCommand,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
    MeadowGridVersionedDeployedRunnable,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
    get_latest_interpreter_version,
)
from meadowgrid.grid import grid_map, grid_map_async
from meadowgrid.meadowgrid_pb2 import (
    ContainerAtDigest,
    ContainerAtTag,
    GitRepoBranch,
    GitRepoCommit,
    ProcessState,
    ServerAvailableContainer,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)

EXAMPLE_CODE = str(
    (pathlib.Path(__file__).parent.parent / "example_user_code").resolve()
)
MEADOWDATA_CODE = str((pathlib.Path(__file__).parent.parent.parent / "src").resolve())


def test_meadowgrid_server_available_folder():
    _test_meadowgrid(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
    )


def test_meadowgrid_server_available_folder_container_digest():
    _test_meadowgrid(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        asyncio.run(
            get_latest_interpreter_version(
                ContainerAtTag(repository="python", tag="3.9.8-slim-buster"), {}
            )
        ),
    )


def test_meadowgrid_server_available_folder_container_tag():
    _test_meadowgrid(
        ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
        ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
    )


TEST_REPO = str(
    (pathlib.Path(__file__).parent.parent.parent.parent / "test_repo").resolve()
)

TEST_WORKING_FOLDER = str(
    (pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowgrid").resolve()
)


# For all of these processes, to debug the coordinator and job_worker, just run them
# separately and the child processes we start in these tests will just silently fail.


def test_meadowgrid_server_git_repo_commit():
    """
    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """
    _test_meadowgrid(
        GitRepoCommit(
            repo_url=TEST_REPO, commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6"
        ),
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
    )


def test_meadowgrid_server_git_repo_branch():
    _test_meadowgrid(
        GitRepoBranch(repo_url=TEST_REPO, branch="main"),
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
    )


def test_meadowgrid_server_git_repo_commit_container():
    """
    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """
    # TODO first make sure the image we're looking for is NOT already cached on this
    #  system, then run it again after it has been cached, as this works different code
    #  paths
    _test_meadowgrid(
        GitRepoCommit(
            repo_url=TEST_REPO, commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6"
        ),
        ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
    )


async def _wait_for_process(
    client: MeadowGridCoordinatorClientAsync, job_id: str, seconds_to_wait: float = 10
) -> Sequence[ProcessState]:
    """wait (no more than ~10s) for the remote process to finish"""
    t0 = time.time()
    i = 0
    results: Sequence[ProcessState]
    while (
        i == 0
        or results[0].state  # noqa F821
        in (
            ProcessStateEnum.RUN_REQUESTED,
            ProcessStateEnum.ASSIGNED,
            ProcessStateEnum.RUNNING,
        )
        and (time.time() - t0) < seconds_to_wait
    ):
        if i == 0:
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


def _test_meadowgrid(
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
):
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):

        async def run():
            async with MeadowGridCoordinatorClientAsync() as client:

                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                add_job_state = await client.add_py_runnable_job(
                    request_id,
                    "example_runner",
                    MeadowGridVersionedDeployedRunnable(
                        code_deployment,
                        interpreter_deployment,
                        MeadowGridFunction.from_name(
                            "example_package.example", "example_runner", arguments
                        ),
                    ),
                )
                assert add_job_state == "ADDED"

                # try running a duplicate
                duplicate_add_job_state = await client.add_py_runnable_job(
                    request_id,
                    "baz",
                    MeadowGridVersionedDeployedRunnable(
                        ServerAvailableFolder(code_paths=["foo"]),
                        interpreter_deployment,
                        MeadowGridFunction.from_name("foo.bar", "baz", ["foo"]),
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

                # we need to wait longer if we need to pull a container image
                results = await _wait_for_process(client, request_id, 600)

                # confirm that it completed successfully
                assert_successful(results[0])
                assert bool(results[0].pid > 0) ^ bool(results[0].container_id)
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
                add_job_state = await client.add_py_runnable_job(
                    request_id,
                    "pip",
                    MeadowGridDeployedRunnable(
                        code_deployment,
                        interpreter_deployment,
                        MeadowGridCommand(["pip", "--version"]),
                    ),
                )
                assert add_job_state == "ADDED"

                results = await _wait_for_process(client, request_id)
                assert_successful(results[0])
                assert bool(results[0].pid > 0) ^ bool(results[0].container_id)
                with open(results[0].log_file_name, "r") as log_file:
                    text = log_file.read()
                assert f"pip {pip.__version__}" in text

        asyncio.run(run())


def test_meadowgrid_server_path_in_repo():
    """
    Tests GitRepoCommit.path_in_repo

    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """

    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):

        async def run():
            async with MeadowGridCoordinatorClientAsync() as client:

                # run a remote process
                arguments = ["foo"]
                request_id = "request1"
                await client.add_py_runnable_job(
                    request_id,
                    "example_runner",
                    MeadowGridDeployedRunnable(
                        GitRepoCommit(
                            repo_url=TEST_REPO,
                            commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                            path_in_repo="example_package",
                        ),
                        ServerAvailableInterpreter(
                            interpreter_path=MEADOWGRID_INTERPRETER,
                        ),
                        MeadowGridFunction.from_name(
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


def test_meadowgrid_command_context_variables():
    """
    Runs example_script twice (in parallel), once with no context variables, and once
    with context variables. Makes sure the output is the same in both cases.
    """

    code = ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE])
    interpreter = ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER)

    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):

        async def run():
            async with MeadowGridCoordinatorClientAsync() as client:
                request_id3 = "request3"
                request_id4 = "request4"
                await client.add_py_runnable_job(
                    request_id3,
                    "example_script",
                    MeadowGridDeployedRunnable(
                        code,
                        interpreter,
                        MeadowGridCommand(["python", "example_script.py"]),
                    ),
                )
                await client.add_py_runnable_job(
                    request_id4,
                    "example_script",
                    MeadowGridDeployedRunnable(
                        code,
                        interpreter,
                        MeadowGridCommand(
                            ["python", "example_script.py"], {"foo": "bar"}
                        ),
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


def test_meadowgrid_containers():
    """
    Basic test on running with containers, checks that different images behave as
    expected
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):

        async def run():
            async with MeadowGridCoordinatorClientAsync() as client:
                for version in ["3.9.8", "3.8.12"]:
                    digest = await (
                        meadowgrid.docker_controller.get_latest_digest_from_registry(
                            "python", f"{version}-slim-buster", None
                        )
                    )

                    request_id = f"request-{version}"
                    add_job_state = await client.add_py_runnable_job(
                        request_id,
                        f"python_version_check-{version}",
                        MeadowGridDeployedRunnable(
                            ServerAvailableFolder(),
                            ContainerAtDigest(repository="python", digest=digest),
                            MeadowGridCommand(["python", "--version"]),
                        ),
                    )
                    assert add_job_state == "ADDED"
                    results = await _wait_for_process(client, request_id, 600)
                    assert_successful(results[0])
                    with open(results[0].log_file_name, "r") as log_file:
                        text = log_file.read()
                    assert text.startswith(f"Python {version}")

        asyncio.run(run())


def test_meadowgrid_grid_job():
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        interpreters = [
            ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
            # We need an image that has meadowdata in it. This can be produced by
            # running build_docker_image.bat
            ServerAvailableContainer(image_name="meadowdata:latest"),
        ]
        for interpreter in interpreters:
            results = grid_map(
                lambda s: f"hello {s}",
                ["abc", "def", "ghi"],
                interpreter,
                ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE]),
            )

            assert results == ["hello abc", "hello def", "hello ghi"]


def test_meadowgrid_grid_map_async():
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):

        async def run():
            tasks = await grid_map_async(
                lambda s: f"hello {s}",
                ["abc", "def", "ghi"],
                ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
                ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE]),
            )

            results = await asyncio.gather(*tasks)
            assert results == ["hello abc", "hello def", "hello ghi"]

        asyncio.run(run())
