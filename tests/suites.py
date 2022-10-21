from __future__ import annotations

import abc
import asyncio
import importlib
import os
import pickle
import subprocess
import sys
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import pytest

import meadowrun.docker_controller
from meadowrun import (
    CondaEnvironmentYmlFile,
    ContainerInterpreter,
    Deployment,
    LocalCondaInterpreter,
    LocalPipInterpreter,
    PipRequirementsFile,
    PoetryProjectPath,
    PreinstalledInterpreter,
    RunMapTasksFailedException,
    TaskResult,
    run_command,
    run_function,
    run_map,
    run_map_as_completed,
)
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.meadowrun_pb2 import (
    ProcessState,
)
from meadowrun.run_job_core import Host, JobCompletion, MeadowrunException, Resources


class HostProvider(abc.ABC):
    """
    The way we set up our tests is a little complicated. We have multiple "test suites",
    like BasicsSuite, ErrorSuite, which are abstract classes. We also have multiple
    "HostProviders" like AwsHostProvider, LocalHostProvider. So e.g. class
    TestBasicsAws(AwsHostProvider, BasicsSuite), runs the "Basics" test suite on AWS
    hosts.
    """

    @abc.abstractmethod
    def get_resources_required(self) -> Optional[Resources]:
        pass

    @abc.abstractmethod
    def get_host(self) -> Host:
        pass

    @abc.abstractmethod
    def get_test_repo_url(self) -> str:
        pass

    def can_get_log_file(self) -> bool:
        return True

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        pass

    def get_num_concurrent_tasks(self) -> int:
        return 4


def _path_from_here(path: str) -> str:
    """
    Combines the specified path with the directory this file is in (tests). So e.g.
    _relative_path_from_folder("../") is the root of this git repo
    """
    return os.path.join(os.path.dirname(__file__), path)


async def _test_code_available(
    example_package_path: str,
    host_provider: HostProvider,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map: bool = True,
) -> None:
    """Makes sure the example package is available at the example_package_path"""

    # doesn't use _test_all_entry_points so we can test referencing functions with a
    # string
    results: str = await run_function(
        f"{example_package_path}.example_runner",
        host_provider.get_host(),
        host_provider.get_resources_required(),
        deployment,
        args=["foo"],
    )
    assert results == "hello foo"

    job_completion = await run_command(
        "pip --version",
        host_provider.get_host(),
        host_provider.get_resources_required(),
        deployment,
    )

    if host_provider.can_get_log_file():
        assert "pip" in await host_provider.get_log_file_text(job_completion)
    else:
        print("Warning get_log_file_text is not implemented")

    def remote_function(arg: str) -> str:
        # could do import example_package.example, but mypy will complain
        example = importlib.import_module(example_package_path)
        return example.example_runner(arg)

    if include_run_map:
        await run_map(
            remote_function,
            ["foo", "bar", "baz"],
            host_provider.get_host(),
            host_provider.get_resources_required(),
            deployment,
        )


_T = TypeVar("_T")
_U = TypeVar("_U")


async def _test_all_entry_points(
    function: Callable[[_T], _U],
    args: List[_T],
    expected_results: List[_U],
    host_provider: HostProvider,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map_as_completed: bool = False,
) -> None:
    results = await run_function(
        lambda: function(args[0]),
        host_provider.get_host(),
        host_provider.get_resources_required(),
        deployment,
    )
    assert results == expected_results[0]

    job_completion = await run_command(
        "pip --version",
        host_provider.get_host(),
        host_provider.get_resources_required(),
        deployment,
    )
    if host_provider.can_get_log_file():
        assert "pip" in await host_provider.get_log_file_text(job_completion)
    else:
        print("Warning get_log_file_text is not implemented")

    map_results = await run_map(
        function,
        args,
        host_provider.get_host(),
        host_provider.get_resources_required(),
        deployment,
    )
    assert map_results is not None
    for actual_result, expected_result in zip(map_results, expected_results):
        assert actual_result == expected_result

    if include_run_map_as_completed:
        actual = []
        async for result in await run_map_as_completed(
            function,
            args,
            host_provider.get_host(),
            host_provider.get_resources_required(),
            deployment,
        ):
            actual.append(result.result_or_raise())

        assert set(actual) == set(expected_results)


async def _test_libraries_available(
    versions: Tuple[str, str],
    host_provider: HostProvider,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map_as_completed: bool = False,
) -> None:
    def remote_function(arg: str) -> Tuple[str, str]:
        import importlib

        # we could just do import requests, but that messes with mypy
        pd = importlib.import_module("pandas")  # from myenv.yml
        requests = importlib.import_module("requests")  # from myenv.yml
        return requests.__version__, pd.__version__

    args = ["foo", "bar", "baz"]
    await _test_all_entry_points(
        remote_function,
        args,
        [versions for _ in args],
        host_provider,
        deployment,
        include_run_map_as_completed,
    )


async def _test_libraries_and_code_available(
    versions: Tuple[str, str],
    host_provider: HostProvider,
    deployment: Union[Deployment, Awaitable[Deployment]],
    include_run_map_as_completed: bool = False,
) -> None:
    def remote_function(arg: str) -> Tuple[Tuple[str, str], str]:
        import importlib

        # we could just do import requests, but that messes with mypy
        pd = importlib.import_module("pandas")  # from myenv.yml
        requests = importlib.import_module("requests")  # from myenv.yml
        example = importlib.import_module("example")  # from example_package
        return (
            (requests.__version__, pd.__version__),
            example.join_strings("hello", arg),
        )

    args = ["foo", "bar", "baz"]
    await _test_all_entry_points(
        remote_function,
        args,
        [(versions, f"hello, {arg}") for arg in args],
        host_provider,
        deployment,
        include_run_map_as_completed,
    )


async def _test_data_file_and_machine_cache(
    host_provider: HostProvider, deployment: Union[Deployment, Awaitable[Deployment]]
) -> None:
    # TODO upgrade the Meadowrun referenced in requirements.txt and move this inside
    # remote_function
    machine_cache_folder = meadowrun.MACHINE_CACHE_FOLDER

    def remote_function(_: Any) -> str:
        # make sure the machine cache folder is writable
        with open(
            os.path.join(machine_cache_folder, "foo"), "w", encoding="utf-8"
        ) as f:
            f.write("test")

        with open("example_package/test.txt", encoding="utf-8") as f:
            return f.read()

    await _test_all_entry_points(
        remote_function, [None] * 3, ["Hello world!"] * 3, host_provider, deployment
    )


_E = TypeVar("_E", bound=BaseException)


class DeploymentSuite(HostProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_git_repo_commit_path_to_source(self) -> None:
        """Tests GitRepoCommit.path_to_source and commit"""

        await _test_code_available(
            "example",
            self,
            Deployment.git_repo(
                self.get_test_repo_url(),
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                interpreter=PreinstalledInterpreter(MEADOWRUN_INTERPRETER),
                path_to_source="example_package",
            ),
        )

    # git_repo with various interpreters

    @pytest.mark.asyncio
    async def test_git_repo_pip(self) -> None:
        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PipRequirementsFile("requirements.txt", "3.9"),
            ),
            True,
        )

    @pytest.mark.asyncio
    async def test_git_repo_conda(self) -> None:
        await _test_libraries_and_code_available(
            ("2.27.1", "1.4.2"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=CondaEnvironmentYmlFile("myenv.yml"),
            ),
        )

    @pytest.mark.asyncio
    async def test_git_repo_poetry(self) -> None:
        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PoetryProjectPath("", "3.9"),
            ),
        )

    @pytest.mark.asyncio
    async def test_git_repo_pip_with_git_dependency(self) -> None:
        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PipRequirementsFile("requirements_with_git.txt", "3.9"),
            ),
        )

    # TODO add test_git_repo_conda_with_git_dependency

    @pytest.mark.asyncio
    async def test_git_repo_poetry_with_git_dependency(self) -> None:
        await _test_libraries_and_code_available(
            ("2.28.0", "1.5.0"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PoetryProjectPath("poetry_with_git", "3.9"),
            ),
        )

    @pytest.mark.asyncio
    async def test_git_repo_container(self) -> None:
        await _test_libraries_available(
            ("2.28.1", "1.5.0"),
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=ContainerInterpreter("meadowrun/meadowrun_test_env"),
            ),
            True,
        )

    @pytest.mark.asyncio
    async def test_git_repo_data_file_and_machine_cache(self) -> None:
        await _test_data_file_and_machine_cache(
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                interpreter=PipRequirementsFile("requirements.txt", "3.9"),
            ),
        )

    # mirror_local with various interpreters

    @pytest.mark.asyncio
    async def test_mirror_local_conda(self) -> None:
        # this currently needs a conda environment created from the test repo:
        # conda env create -n test_repo_conda_env -f myenv.yml
        exception_raised = False
        try:
            await _test_libraries_and_code_available(
                ("2.27.1", "1.4.3"),
                self,
                await Deployment.mirror_local(
                    interpreter=LocalCondaInterpreter("test_repo_conda_env"),
                    additional_python_paths=[
                        _path_from_here("../../test_repo/example_package")
                    ],
                ),
            )
        except ValueError:
            if sys.platform == "win32":
                exception_raised = True
            else:
                raise

        if sys.platform == "win32":
            assert exception_raised

    @pytest.mark.asyncio
    async def test_mirror_local_conda_file(self) -> None:
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        await _test_libraries_and_code_available(
            ("2.27.1", "1.4.2"),
            self,
            await Deployment.mirror_local(
                interpreter=CondaEnvironmentYmlFile(
                    _path_from_here("../../test_repo/myenv.yml")
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )

    @pytest.mark.asyncio
    async def test_mirror_local_pip(self) -> None:
        # this requires creating a virtualenv in this git repo's parent directory.
        # For Windows:
        # > python -m virtualenv test_venv_windows
        # > test_venv_windows/Scripts/activate.bat
        # > pip install -r test_repo/requirements.txt
        # For Linux:
        # > python -m virtualenv test_venv_linux
        # > source test_venv_linux/bin/activate
        # > pip install -r test_repo/requirements.txt
        if sys.platform == "win32":
            test_venv_interpreter = _path_from_here(
                "../../test_venv_windows/Scripts/python.exe"
            )
        else:
            test_venv_interpreter = _path_from_here("../../test_venv_linux/bin/python")

        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            await Deployment.mirror_local(
                interpreter=LocalPipInterpreter(test_venv_interpreter),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )

    @pytest.mark.asyncio
    async def test_mirror_local_pip_file(self) -> None:
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            await Deployment.mirror_local(
                interpreter=PipRequirementsFile(
                    _path_from_here("../../test_repo/requirements.txt"), "3.9"
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )

    @pytest.mark.asyncio
    async def test_mirror_local_poetry(self) -> None:
        await _test_libraries_and_code_available(
            ("2.28.0", "1.4.2"),
            self,
            await Deployment.mirror_local(
                interpreter=PoetryProjectPath(
                    _path_from_here("../../test_repo/"), "3.9"
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )

    @pytest.mark.asyncio
    async def test_mirror_local_data_file(self) -> None:
        working_dir = os.getcwd()
        try:
            os.chdir(_path_from_here("../../test_repo"))

            await _test_data_file_and_machine_cache(
                self,
                await Deployment.mirror_local(
                    interpreter=PipRequirementsFile(
                        _path_from_here("../../test_repo/requirements.txt"), "3.9"
                    ),
                    working_directory_globs="**/*.txt",
                ),
            )
        finally:
            os.chdir(working_dir)

    @pytest.mark.asyncio
    async def test_mirror_local_container(self) -> None:
        await _test_libraries_available(
            ("2.28.1", "1.5.0"),
            self,
            await Deployment.mirror_local(
                interpreter=ContainerInterpreter("meadowrun/meadowrun_test_env"),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )

    @pytest.mark.asyncio
    async def test_container(self) -> None:
        await _test_libraries_available(
            ("2.28.1", "1.5.0"),
            self,
            Deployment.container_image("meadowrun/meadowrun_test_env"),
        )

    @pytest.mark.asyncio
    async def test_container_without_meadowrun(self) -> None:
        for version in ["3.9.8", "3.8.12"]:
            digest = await (
                meadowrun.docker_controller.get_latest_digest_from_registry(
                    "python", f"{version}-slim-buster", None
                )
            )

            result = await run_command(
                "python --version",
                self.get_host(),
                self.get_resources_required(),
                Deployment.container_image_at_digest("python", digest),
            )

            if self.can_get_log_file():
                actual = await self.get_log_file_text(result)
                assert f"Python" f" {version}" in actual, actual
            else:
                print("Warning get_log_file_text is not implemented")


async def _test_curl_available(
    host_provider: HostProvider, deployment: Union[Deployment, Awaitable[Deployment]]
) -> None:
    def remote_function(_: Any) -> int:
        return subprocess.run(["curl", "--help"]).returncode

    await _test_all_entry_points(
        remote_function, [None] * 3, [0] * 3, host_provider, deployment
    )


class DeploymentSuite2(HostProvider, abc.ABC):
    """These tests currently do not work on Kubernetes for various reasons"""

    # Kubernetes doesn't support (and may never support) an environment spec with an apt
    # dependency at the same time

    @pytest.mark.asyncio
    async def test_git_repo_pip_apt(self) -> None:
        def remote_function(_: Any) -> str:
            import importlib

            # cv2 will only work correctly if libgl1 and libglib2.0-0 are installed
            cv2 = importlib.import_module("cv2")
            return cv2.__version__

        await _test_all_entry_points(
            remote_function,
            [None] * 3,
            ["4.6.0"] * 3,
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                path_to_source="example_package",
                interpreter=PipRequirementsFile(
                    "requirements_with_cv2.txt", "3.9", ["libgl1", "libglib2.0-0"]
                ),
            ),
        )

    @pytest.mark.asyncio
    async def test_git_repo_conda_apt(self) -> None:
        await _test_curl_available(
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=CondaEnvironmentYmlFile(
                    "myenv.yml", additional_software=["curl"]
                ),
            ),
        )

    @pytest.mark.asyncio
    async def test_git_repo_poetry_apt(self) -> None:
        await _test_curl_available(
            self,
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                path_to_source="example_package",
                interpreter=PoetryProjectPath("", "3.9", additional_software=["curl"]),
            ),
        )

    # We have not yet implemented sidecar containers on Kubernetes

    @pytest.mark.asyncio
    async def test_git_sidecar_containers(self) -> None:
        def remote_function() -> str:
            import requests

            return requests.get("http://sidecar-container-0").text

        results = await run_function(
            remote_function,
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                path_to_source="example_package",
                interpreter=PipRequirementsFile("requirements.txt", "3.9"),
            ),
            # this is just a random example of a container with a service in it
            sidecar_containers=ContainerInterpreter("okteto/sample-app"),
        )
        assert results.startswith("<h3>Hello okteto!</h3>")

    # this test uses a vanilla python container (without meadowrun installed). This
    # isn't supported on Kubernetes

    @pytest.mark.asyncio
    async def test_git_repo_container_without_meadowrun(self) -> None:
        # TODO first make sure the image we're looking for is NOT already cached on this
        # system, then run it again after it has been cached, as this works different
        # code paths
        await _test_code_available(
            "example_package.example",
            self,
            Deployment.git_repo(
                self.get_test_repo_url(),
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                interpreter=ContainerInterpreter("python", "3.9.8-slim-buster"),
            ),
            # run_map does not work with this deployment
            False,
        )


class EdgeCasesSuite(HostProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_run_request_failed(self) -> None:
        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                lambda: "hello",
                self.get_host(),
                self.get_resources_required(),
                Deployment.container_image("does-not-exist"),
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED
        )

    @pytest.mark.asyncio
    async def test_non_zero_return_code(self) -> None:
        def exit_immediately() -> None:
            sys.exit(101)

        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                exit_immediately,
                self.get_host(),
                self.get_resources_required(),
                Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
        )
        assert exc_info.value.process_state.return_code == 101

    @pytest.mark.asyncio
    async def test_result_cannot_be_pickled(self) -> None:
        def remote_func(x: Any) -> Any:
            class UnpicklableClass:
                pass

            return UnpicklableClass()

        with pytest.raises(RunMapTasksFailedException) as exc_info:
            await run_map(
                remote_func,
                [1],
                self.get_host(),
                self.get_resources_required(),
                Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
            )

        assert exc_info.value.failed_tasks[0].state == "PYTHON_EXCEPTION"
        assert (
            exc_info.value.failed_tasks[0].exception is not None
            and "Can't pickle" in exc_info.value.failed_tasks[0].exception[1]
        )

        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                lambda: remote_func(1),
                self.get_host(),
                self.get_resources_required(),
                Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
        )
        assert (
            "Can't pickle"
            in pickle.loads(exc_info.value.process_state.pickled_result)[1]
        )

    @pytest.mark.asyncio
    async def test_result_cannot_be_unpickled(self) -> None:
        """
        Runs a "real" run_map in a container, returning a result that is not a meadowrun
        dependency. This is to avoid a regression - meadowrun was trying to unpickle the
        result from the worker in the agent, which fails if the result has a dependency
        on e.g. numpy. This also checks that despite that the result can't be unpickled
        on the client, the tasks still succeed, and have an appropriate error message.
        """

        def remote_function(i: int) -> Any:
            import importlib

            # we could just do import numpy, but that messes with mypy
            np = importlib.import_module("numpy")  # from myenv.yml

            array = np.array([i + 1, i + 2, i + 3], dtype=np.int32)
            return array

        with pytest.raises(RunMapTasksFailedException) as exc_info:
            await run_map(
                remote_function,
                [1, 2, 3, 4],
                self.get_host(),
                self.get_resources_required(),
                num_concurrent_tasks=self.get_num_concurrent_tasks(),
                deployment=Deployment.git_repo(
                    repo_url=self.get_test_repo_url(),
                    branch="main",
                    path_to_source="example_package",
                    interpreter=PoetryProjectPath("poetry_with_git", "3.9"),
                ),
            )

        assert len(exc_info.value.failed_tasks) == 4
        assert all(
            failed_task.state == "RESULT_CANNOT_BE_UNPICKLED"
            for failed_task in exc_info.value.failed_tasks
        )

    @pytest.mark.asyncio
    async def test_asyncio_run(self) -> None:
        async def remote_async_function(i: int) -> int:
            await asyncio.sleep(0.1)
            return i * 2

        def remote_function(i: int = 1) -> int:
            return asyncio.run(remote_async_function(i))

        await run_function(
            remote_function,
            self.get_host(),
            self.get_resources_required(),
            Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
        )

        await run_map(
            remote_function,
            [1, 2],
            self.get_host(),
            self.get_resources_required(),
            Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
        )


class MapRetriesSuite(HostProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_run_map_as_completed_with_retries(self) -> None:
        actual: List[TaskResult[None]] = []

        def fail(x: int) -> None:
            raise Exception("intentional failure")

        async for result in await run_map_as_completed(
            fail,
            [1, 2, 3, 4],
            self.get_host(),
            self.get_resources_required(),
            deployment=Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
            num_concurrent_tasks=self.get_num_concurrent_tasks(),
            max_num_task_attempts=3,
        ):
            actual.append(result)

        assert len(actual) == 4
        for result in actual:
            assert not result.is_success
            assert result.exception is not None
            assert result.attempt == 3

    @pytest.mark.asyncio
    async def test_run_map_as_completed_in_container_with_retries(self) -> None:
        def fail(x: int) -> None:
            raise Exception("intentional failure")

        actual: List[TaskResult[None]] = []
        async for result in await run_map_as_completed(
            fail,
            [1, 2, 3, 4],
            self.get_host(),
            self.get_resources_required(),
            num_concurrent_tasks=self.get_num_concurrent_tasks(),
            max_num_task_attempts=3,
            deployment=Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PoetryProjectPath("poetry_with_git", "3.9"),
            ),
        ):
            actual.append(result)

        assert len(actual) == 4
        for result in actual:
            assert not result.is_success
            assert result.exception is not None
            assert result.attempt == 3

    @pytest.mark.asyncio
    async def test_run_map_as_completed_unexpected_exit(self) -> None:
        def unexpected_exit(_: int) -> None:
            sys.exit(123)

        actual: List[TaskResult[None]] = []
        async for result in await run_map_as_completed(
            unexpected_exit,
            [1, 2],
            self.get_host(),
            self.get_resources_required(),
            deployment=Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
            num_concurrent_tasks=self.get_num_concurrent_tasks(),
            max_num_task_attempts=3,
        ):
            actual.append(result)

        assert len(actual) == 2
        for result in actual:
            assert not result.is_success
            assert result.exception is None
            assert result.attempt == 3

    @pytest.mark.asyncio
    async def test_run_map_unexpected_exit(self) -> None:
        def unexpected_exit(x: int) -> None:
            sys.exit(123)

        with pytest.raises(RunMapTasksFailedException) as exc:
            _ = await run_map(
                unexpected_exit,
                [1, 2, 3, 4],
                self.get_host(),
                self.get_resources_required(),
                deployment=Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
                num_concurrent_tasks=self.get_num_concurrent_tasks(),
            )
        assert "UNEXPECTED_WORKER_EXIT" in str(exc)
