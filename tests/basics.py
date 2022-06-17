import abc
import os
import sys
from typing import Union, Callable, Tuple

import pytest

import meadowrun.docker_controller
from meadowrun import (
    AllocCloudInstances,
    CondaEnvironmentYmlFile,
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
from meadowrun.deployment import (
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
    CloudProviderType,
    Host,
    JobCompletion,
    MeadowrunException,
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
    def get_host(self) -> Host:
        pass

    @abc.abstractmethod
    def get_test_repo_url(self) -> str:
        pass

    @abc.abstractmethod
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
    async def test_meadowrun_git_repo_commit(self):
        await self._test_meadowrun(
            GitRepoCommit(
                repo_url=self.get_test_repo_url(),
                commit="cb277fa1d35bfb775ed1613b639e6f5a7d2f5bb6",
            ),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_branch(self):
        await self._test_meadowrun(
            GitRepoBranch(repo_url=self.get_test_repo_url(), branch="main"),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_commit_container(self):
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
    ):
        results: str = await run_function(
            "example_package.example.example_runner",
            self.get_host(),
            Deployment(interpreter_deployment, code_deployment),
            args=["foo"],
        )
        assert results == "hello foo"

        job_completion = await run_command(
            "pip --version",
            self.get_host(),
            Deployment(interpreter_deployment, code_deployment),
        )

        assert "pip" in await self.get_log_file_text(job_completion)

    @pytest.mark.asyncio
    async def test_meadowrun_path_in_git_repo(self):
        """Tests GitRepoCommit.path_to_source"""

        results: str = await run_function(
            "example.example_runner",
            self.get_host(),
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
    async def test_meadowrun_containers(self):
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
                Deployment(ContainerAtDigest(repository="python", digest=digest)),
            )

            assert (await self.get_log_file_text(result)).startswith(
                f"Python" f" {version}"
            )

    @pytest.mark.skipif(
        "sys.version_info < (3, 8)",
        reason="cloudpickle issue that prevents lambdas serialized on 3.7 running on "
        "3.8. Assuming here that this extends to all lambdas serialized on <=3.7 "
        "running on >=3.8",
    )
    @pytest.mark.asyncio
    async def test_conda_file_in_git_repo(self):
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
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
    async def test_pip_file_in_git_repo(self):
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
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
    async def test_poetry_project_in_git_repo(self):
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
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
    async def test_local_conda_interpreter(self):
        # this currently needs a conda environment created from the test repo:
        # conda env create -n test_repo_conda_env -f myenv.yml
        exception_raised = False
        try:
            results = await run_function(
                self._get_remote_function_for_deployment(),
                self.get_host(),
                await Deployment.mirror_local(
                    interpreter=LocalCondaInterpreter("test_repo_conda_env"),
                    additional_paths=[
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
    async def test_local_pip_interpreter(self):
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
            await Deployment.mirror_local(
                interpreter=LocalPipInterpreter(test_venv_interpreter, "3.9"),
                additional_paths=[_path_from_here("../../test_repo/example_package")],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_conda_file(self):
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            await Deployment.mirror_local(
                interpreter=CondaEnvironmentYmlFile(
                    _path_from_here("../../test_repo/myenv.yml")
                ),
                additional_paths=[_path_from_here("../../test_repo/example_package")],
            ),
        )
        assert results == ("2.27.1", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_pip_file(self):
        # this requires creating a virtualenv in this git repo's parent directory called
        # test_venv with the following steps:
        # - virtualenv test_venv
        # - test_venv/Scripts/activate.bat OR source test_venv/Scripts/activate,
        # - pip install -r test_repo/requirements.txt
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            await Deployment.mirror_local(
                interpreter=PipRequirementsFile(
                    _path_from_here("../../test_repo/requirements.txt"), "3.9"
                ),
                additional_paths=[_path_from_here("../../test_repo/example_package")],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_local_poetry_project(self):
        results = await run_function(
            self._get_remote_function_for_deployment(),
            self.get_host(),
            await Deployment.mirror_local(
                interpreter=PoetryProjectPath(
                    _path_from_here("../../test_repo/"), "3.9"
                ),
                additional_paths=[_path_from_here("../../test_repo/example_package")],
            ),
        )
        assert results == ("2.28.0", "1.4.2", "a, b")

    def _get_remote_function_for_deployment(self) -> Callable[[], Tuple[str, str, str]]:
        # we have a wrapper around this so that the function gets pickled as a lambda
        def remote_function():
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
    async def test_run_request_failed(self):
        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(
                lambda: "hello",
                self.get_host(),
                Deployment(ServerAvailableContainer(image_name="does-not-exist")),
            )

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED
        )

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_non_zero_return_code(self):
        def exit_immediately():
            sys.exit(101)

        with pytest.raises(MeadowrunException) as exc_info:
            await run_function(exit_immediately, self.get_host())

        assert (
            exc_info.value.process_state.state
            == ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
        )
        assert exc_info.value.process_state.return_code == 101


class MapSuite(abc.ABC):
    @abc.abstractmethod
    def cloud_provider(self) -> CloudProviderType:
        pass

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_run_map(self):
        """Runs a "real" run_map"""
        results = await run_map(
            lambda x: x**x,
            [1, 2, 3, 4],
            AllocCloudInstances(1, 1, 15, self.cloud_provider(), 3),
        )

        assert results == [1, 4, 27, 256]
