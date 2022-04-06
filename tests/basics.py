import abc
import sys
from typing import Union

import pytest

import meadowrun.docker_controller
from meadowrun import (
    ServerAvailableInterpreter,
    ContainerAtTag,
    GitRepoCommit,
    GitRepoBranch,
    run_function,
    Deployment,
    run_command,
    ContainerAtDigest,
)
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.deployment import (
    CodeDeployment,
    VersionedCodeDeployment,
    InterpreterDeployment,
    VersionedInterpreterDeployment,
)
from meadowrun.meadowrun_pb2 import EnvironmentSpecInCode
from meadowrun.meadowrun_pb2 import ServerAvailableContainer, ProcessState
from meadowrun.run_job import Host, JobCompletion
from meadowrun.run_job import MeadowrunException


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
    def get_log_file_text(self, job_completion: JobCompletion) -> str:
        pass


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

        assert "pip" in self.get_log_file_text(job_completion)

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

            assert self.get_log_file_text(result).startswith(f"Python {version}")

    @pytest.mark.asyncio
    async def test_meadowrun_environment_in_spec(self):
        def remote_function():
            import importlib

            # we could just do import requests, but that messes with mypy
            pd = importlib.import_module("pandas")  # from myenv.yml
            requests = importlib.import_module("requests")  # from myenv.yml
            example = importlib.import_module("example")  # from example_package
            return requests.__version__, pd.__version__, example.join_strings("a", "b")

        results = await run_function(
            remote_function,
            self.get_host(),
            Deployment(
                EnvironmentSpecInCode(
                    environment_type=EnvironmentSpecInCode.EnvironmentType.CONDA,
                    path_to_spec="myenv.yml",
                ),
                GitRepoCommit(
                    repo_url=self.get_test_repo_url(),
                    commit="a249fc16",
                    path_to_source="example_package",
                ),
            ),
        )
        assert results == ("2.27.1", "1.4.1", "a, b")


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