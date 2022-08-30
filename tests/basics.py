from __future__ import annotations

import abc
import os
import sys
from typing import TYPE_CHECKING, Union, Callable, Tuple

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
    run_command,
    run_function,
    run_map,
)
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.run_job import run_map_as_completed

if TYPE_CHECKING:
    from meadowrun.deployment_internal_types import (
        CodeDeployment,
        InterpreterDeployment,
        VersionedCodeDeployment,
        VersionedInterpreterDeployment,
    )
from meadowrun.meadowrun_pb2 import (
    ContainerAtDigest,
    ContainerAtTag,
    GitRepoBranch,
    GitRepoCommit,
    ServerAvailableInterpreter,
    ServerAvailableContainer,
    ProcessState,
)
from meadowrun.run_job_core import (
    Host,
    JobCompletion,
    MeadowrunException,
    Resources,
)


class HostProvider(abc.ABC):
    """
    The way we set up our tests is a little complicated. We have multiple "test suites",
    like BasicsSuite, ErrorSuite, which are abstract classes. We also have multiple
    "HostProviders" like AwsHostProvider, LocalHostProvider. So e.g. class
    TestBasicsAws(AwsHostProvider, BasicsSuite), runs the "Basics" test suite on AWS
    hosts.
    """

    @abc.abstractmethod
    def get_resources_required(self) -> Resources:
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


def _path_from_here(path: str) -> str:
    """
    Combines the specified path with the directory this file is in (tests). So e.g.
    _relative_path_from_folder("../") is the root of this git repo
    """
    return os.path.join(os.path.dirname(__file__), path)


class BasicsSuite(HostProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_commit(self) -> None:
        await self._test_meadowrun(
            GitRepoCommit(
                repo_url=self.get_test_repo_url(),
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
            ),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_branch(self) -> None:
        await self._test_meadowrun(
            GitRepoBranch(repo_url=self.get_test_repo_url(), branch="main"),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_commit_container(self) -> None:
        # TODO first make sure the image we're looking for is NOT already cached on this
        # system, then run it again after it has been cached, as this works different
        # code paths
        await self._test_meadowrun(
            GitRepoCommit(
                repo_url=self.get_test_repo_url(),
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
            ),
            ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
        )

    async def _test_meadowrun(
        self,
        code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
        interpreter_deployment: Union[
            InterpreterDeployment, VersionedInterpreterDeployment
        ],
    ) -> None:
        results: str = await run_function(
            "example_package.example.example_runner",
            self.get_host(),
            self.get_resources_required(),
            Deployment(interpreter_deployment, code_deployment),
            args=["foo"],
        )
        assert results == "hello foo"

        job_completion = await run_command(
            "pip --version",
            self.get_host(),
            self.get_resources_required(),
            Deployment(interpreter_deployment, code_deployment),
        )

        if self.can_get_log_file():
            assert "pip" in await self.get_log_file_text(job_completion)
        else:
            print("Warning get_log_file_text is not implemented")

    @pytest.mark.asyncio
    async def test_meadowrun_path_in_git_repo(self) -> None:
        """Tests GitRepoCommit.path_to_source"""

        results: str = await run_function(
            "example.example_runner",
            self.get_host(),
            self.get_resources_required(),
            Deployment(
                code=GitRepoCommit(
                    repo_url=self.get_test_repo_url(),
                    commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
                    path_to_source="example_package",
                )
            ),
            args=["foo"],
        )
        assert results == "hello foo"

    @pytest.mark.asyncio
    async def test_meadowrun_containers(self) -> None:
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
                self.get_host(),
                self.get_resources_required(),
                Deployment(ContainerAtDigest(repository="python", digest=digest)),
            )

            if self.can_get_log_file():
                assert (await self.get_log_file_text(result)).startswith(
                    f"Python" f" {version}"
                )
            else:
                print("Warning get_log_file_text is not implemented")

    @pytest.mark.skipif(
        "sys.version_info < (3, 8)",
        reason="cloudpickle issue that prevents lambdas serialized on 3.7 running on "
        "3.8. Assuming here that this extends to all lambdas serialized on <=3.7 "
        "running on >=3.8",
    )
    @pytest.mark.asyncio
    async def test_conda_file_in_git_repo(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=CondaEnvironmentYmlFile("myenv.yml"),
            ),
        )
        assert results == ("2.27.1", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PipRequirementsFile("requirements.txt", "3.9"),
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_git_dependency(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PipRequirementsFile("requirements_with_git.txt", "3.9"),
            ),
        )
        # the version number will keep changing, but we know it will be > 2.28.0
        assert [int(part) for part in results[0].split(".")] > [2, 28, 0]
        assert results[1:] == ("1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_apt_dependency(self) -> None:
        def remote_function() -> str:
            import importlib

            cv2 = importlib.import_module("cv2")
            return cv2.__version__

        results = await run_function(
            remote_function,
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                path_to_source="example_package",
                interpreter=PipRequirementsFile(
                    "requirements_with_cv2.txt", "3.9", ["libgl1", "libglib2.0-0"]
                ),
            ),
        )
        assert results == "4.6.0"

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_sidecar_container(self) -> None:
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

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_data_file(self) -> None:
        """This test is doing double-duty, also checking for the machine_cache folder"""

        def remote_function() -> Tuple[bool, str]:
            with open("example_package/test.txt", encoding="utf-8") as f:
                return os.path.isdir("/meadowrun/machine_cache"), f.read()

        results = await run_function(
            remote_function,
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                interpreter=PipRequirementsFile("requirements.txt", "3.9"),
            ),
        )
        assert results == (True, "Hello world!")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_poetry_project_in_git_repo(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PoetryProjectPath("", "3.9"),
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_poetry_project_in_git_repo_with_git_dependency(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=PoetryProjectPath("poetry_with_git", "3.9"),
            ),
        )
        # the version number will keep changing, but we know it will be > 2.28.0
        assert [int(part) for part in results[0].split(".")] > [2, 28, 0]
        print(results)
        assert results[1:] == ("1.4.3", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_git_repo_with_container(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            Deployment.git_repo(
                repo_url=self.get_test_repo_url(),
                branch="main",
                path_to_source="example_package",
                interpreter=ContainerInterpreter("hrichardlee/meadowrun_test_env"),
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_conda_interpreter(self) -> None:
        # this currently needs a conda environment created from the test repo:
        # conda env create -n test_repo_conda_env -f myenv.yml
        exception_raised = False
        try:
            results = await run_function(
                self._get_remote_function_for_deployment(),
                self.get_host(),
                self.get_resources_required(),
                await Deployment.mirror_local(
                    interpreter=LocalCondaInterpreter("test_repo_conda_env"),
                    additional_python_paths=[
                        _path_from_here("../../test_repo/example_package")
                    ],
                ),
            )
            assert results == ("2.27.1", "1.4.2", "a, b")
        except ValueError:
            if sys.platform == "win32":
                exception_raised = True
            else:
                raise

        if sys.platform == "win32":
            assert exception_raised

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_pip_interpreter(self) -> None:
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
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            await Deployment.mirror_local(
                interpreter=LocalPipInterpreter(test_venv_interpreter, "3.9"),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_conda_file(self) -> None:
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            await Deployment.mirror_local(
                interpreter=CondaEnvironmentYmlFile(
                    _path_from_here("../../test_repo/myenv.yml")
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )
        assert results == ("2.27.1", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_pip_file(self) -> None:
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            await Deployment.mirror_local(
                interpreter=PipRequirementsFile(
                    _path_from_here("../../test_repo/requirements.txt"), "3.9"
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_pip_file_with_data_file(self) -> None:
        def remote_function() -> str:
            print(os.getcwd())
            print(os.listdir("."))

            with open("example_package/test.txt", encoding="utf-8") as f:
                return f.read()

        working_dir = os.getcwd()
        try:
            os.chdir(_path_from_here("../../test_repo"))

            results = await run_function(
                remote_function,
                self.get_host(),
                self.get_resources_required(),
                await Deployment.mirror_local(
                    interpreter=PipRequirementsFile(
                        _path_from_here("../../test_repo/requirements.txt"), "3.9"
                    ),
                    working_directory_globs="**/*.txt",
                ),
            )
            assert results == "Hello world!"
        finally:
            os.chdir(working_dir)

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_poetry_project(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            await Deployment.mirror_local(
                interpreter=PoetryProjectPath(
                    _path_from_here("../../test_repo/"), "3.9"
                ),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_code_with_container(self) -> None:
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            self.get_resources_required(),
            await Deployment.mirror_local(
                interpreter=ContainerInterpreter("hrichardlee/meadowrun_test_env"),
                additional_python_paths=[
                    _path_from_here("../../test_repo/example_package")
                ],
            ),
        )
        assert results == ("2.28.1", "1.4.3", "a, b")

    def _get_remote_function_for_deployment(self) -> Callable[[], Tuple[str, str, str]]:
        # we have a wrapper around this so that the function gets pickled as a lambda
        def remote_function() -> Tuple[str, str, str]:
            import importlib

            # we could just do import requests, but that messes with mypy
            pd = importlib.import_module("pandas")  # from myenv.yml
            requests = importlib.import_module("requests")  # from myenv.yml
            example = importlib.import_module("example")  # from example_package
            return (
                requests.__version__,
                pd.__version__,
                example.join_strings("a", "b"),
            )

        return remote_function


class ErrorsSuite(HostProvider, abc.ABC):
    @pytest.mark.asyncio
    async def test_run_request_failed(self) -> None:
        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                lambda: "hello",
                self.get_host(),
                self.get_resources_required(),
                Deployment(ServerAvailableContainer(image_name="does-not-exist")),
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED
        )

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_non_zero_return_code(self) -> None:
        def exit_immediately() -> None:
            sys.exit(101)

        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                exit_immediately, self.get_host(), self.get_resources_required()
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
        )
        assert exc_info.value.process_state.return_code == 101


class MapSuite(HostProvider, abc.ABC):
    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_run_map(self) -> None:
        """Runs a "real" run_map"""
        results = await run_map(
            lambda x: x**x,
            [1, 2, 3, 4],
            self.get_host(),
            self.get_resources_required(),
            num_concurrent_tasks=4,
        )

        assert results == [1, 4, 27, 256], str(results)

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_run_map_as_completed(self) -> None:
        actual = []
        async for result in await run_map_as_completed(
            lambda x: x**x,
            [1, 2, 3, 4],
            self.get_host(),
            self.get_resources_required(),
            num_concurrent_tasks=2,
        ):
            actual.append(result.result_or_raise())

        assert set(actual) == set([1, 4, 27, 256])
