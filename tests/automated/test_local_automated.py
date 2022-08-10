"""
These tests should (mostly) not require an internet connection and not require any
manual intervention beyond some initial setup.
"""
from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import pytest

from basics import BasicsSuite, HostProvider, ErrorsSuite
from meadowrun import Resources, Deployment, run_command
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.deployment_internal_types import get_latest_interpreter_version
from meadowrun.meadowrun_pb2 import (
    ContainerAtTag,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host, JobCompletion
from meadowrun.run_job_local import LocalHost

EXAMPLE_CODE = str(
    (pathlib.Path(__file__).parent.parent / "example_user_code").resolve()
)
MEADOWRUN_CODE = str((pathlib.Path(__file__).parent.parent.parent / "src").resolve())


class LocalHostProvider(HostProvider):
    def get_resources_required(self) -> Resources:
        return Resources(1, 1, 100)

    def get_host(self) -> Host:
        return LocalHost()

    def get_test_repo_url(self) -> str:
        # We want to use a local copy so that we don't need to go out to the internet
        # for this local test. This means running these tests requires cloning
        # https://github.com/meadowdata/test_repo next to the meadowrun repo.
        return str(
            (pathlib.Path(__file__).parent.parent.parent.parent / "test_repo").resolve()
        )

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        with open(job_completion.log_file_name, "r", encoding="utf-8") as log_file:
            return log_file.read()


class TestBasicsLocal(LocalHostProvider, BasicsSuite):
    # TODO we should move these tests that use ServerAvailable into BasicsSuite and make
    # them work for TestBasicsAws. I think the best way to do that is to add
    # support for mounting an EBS volume, create one with the code in EXAMPLE_CODE and
    # attach it for these tests.

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder(self) -> None:
        await self._test_meadowrun(
            ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder_container_digest(self) -> None:
        await self._test_meadowrun(
            ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
            await get_latest_interpreter_version(
                ContainerAtTag(repository="python", tag="3.9.8-slim-buster"), {}
            ),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder_container_tag(self) -> None:
        await self._test_meadowrun(
            ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
            ContainerAtTag(repository="python", tag="3.9.8-slim-buster"),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_command_context_variables(self) -> None:
        """
        Runs example_script twice (in parallel), once with no context variables, and
        once with context variables. Makes sure the output is the same in both cases.
        """
        job_completion1 = await run_command(
            "python example_script.py",
            self.get_host(),
            self.get_resources_required(),
            Deployment(
                code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
            ),
        )
        job_completion2 = await run_command(
            "python example_script.py",
            self.get_host(),
            self.get_resources_required(),
            Deployment(
                code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
            ),
            {"foo": "bar"},
        )

        assert "hello there: no_data" in await self.get_log_file_text(job_completion1)
        assert "hello there: bar" in await self.get_log_file_text(job_completion2)


class TestErrorsLocal(LocalHostProvider, ErrorsSuite):
    pass
