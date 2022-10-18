"""
These tests should (mostly) not require an internet connection and not require any
manual intervention beyond some initial setup.
"""
from __future__ import annotations

import os
import pathlib
import pickle
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import pytest

from suites import (
    DeploymentSuite,
    EdgeCasesSuite,
    HostProvider,
    DeploymentSuite2,
    _test_meadowrun,
)
from meadowrun import Deployment, Resources, TaskResult, run_command
from meadowrun.abstract_storage_bucket import AbstractStorageBucket
from meadowrun.config import MEADOWRUN_INTERPRETER
from meadowrun.deployment_internal_types import get_latest_interpreter_version
from meadowrun.meadowrun_pb2 import (
    ContainerAtTag,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)
from meadowrun.run_job_core import (
    Host,
    Job,
    JobCompletion,
    MeadowrunException,
    ProcessState,
    ResourcesInternal,
    WaitOption,
)
from meadowrun.run_job_local import _set_up_working_folder, run_local

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

EXAMPLE_CODE = str(
    (pathlib.Path(__file__).parent.parent / "example_user_code").resolve()
)
MEADOWRUN_CODE = str((pathlib.Path(__file__).parent.parent.parent / "src").resolve())

_T = TypeVar("_T")
_U = TypeVar("_U")


class LocalFileBucket(AbstractStorageBucket):
    """
    Currently nothing on this class is implemented. If we want to be able to test e.g.
    mirror_local we will need to implement methods on this class.
    """

    def __init__(self, tmp_path: pathlib.Path) -> None:
        self.tmp_path = tmp_path

    def get_cache_key(self) -> str:
        return "localfile"

    async def get_bytes(self, key: str) -> bytes:
        with open(self.tmp_path / key, "rb") as f:
            return f.read()

    async def try_get_bytes(self, key: str) -> Optional[bytes]:
        raise NotImplementedError()

    async def get_byte_range(self, key: str, byte_range: Tuple[int, int]) -> bytes:
        raise NotImplementedError()

    async def write_bytes(self, data: bytes, key: str) -> None:
        path = self.tmp_path / key
        os.makedirs(path.parent, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    async def exists(self, key: str) -> bool:
        return os.path.exists(self.tmp_path / key)

    async def get_file(self, key: str, local_filename: str) -> None:
        data = await self.get_bytes(key)
        with open(local_filename, "wb") as f:
            f.write(data)

    async def try_get_file(self, key: str, local_filename: str) -> bool:
        raise NotImplementedError()

    async def write_file(self, local_filename: str, key: str) -> None:
        raise NotImplementedError()

    async def list_objects(self, key_prefix: str) -> List[str]:
        raise NotImplementedError()

    async def delete_object(self, key: str) -> None:
        raise NotImplementedError()


class LocalHost(Host):
    def __init__(self, tmp_path: Optional[pathlib.Path] = None):
        self.tmp_path = tmp_path

    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        if wait_for_result != WaitOption.WAIT_AND_TAIL_STDOUT:
            raise NotImplementedError(
                f"{wait_for_result} is not supported for LocalHost yet"
            )

        if resources_required is not None:
            raise ValueError("Specifying Resources for LocalHost is not supported")

        initial_update, continuation = await run_local(job)
        if (
            initial_update.state != ProcessState.ProcessStateEnum.RUNNING
            or continuation is None
        ):
            result = initial_update
        else:
            result = await continuation

        if result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
            job_spec_type = job.WhichOneof("job_spec")
            # we must have a result from functions, in other cases we can optionally
            # have a result
            if job_spec_type == "py_function" or result.pickled_result:
                unpickled_result = pickle.loads(result.pickled_result)
            else:
                unpickled_result = None

            return JobCompletion(
                unpickled_result,
                result.state,
                result.log_file_name,
                result.return_code,
                "localhost",
            )
        else:
            raise MeadowrunException(result)

    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_tasks_attempts: int,
        retry_with_more_memory: bool,
    ) -> Optional[Sequence[_U]]:
        raise NotImplementedError("run_map on LocalHost is not implemented")

    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_tasks_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult[_U]]:
        raise NotImplementedError(
            "run_map_as_completed is not implemented for LocalHost"
        )

    async def get_storage_bucket(self) -> AbstractStorageBucket:
        if self.tmp_path is None:
            raise ValueError(
                "In order to use get_storage_bucket, LocalHost must be constructed with"
                " a path"
            )
        return LocalFileBucket(self.tmp_path)


class LocalHostProvider(HostProvider):
    def get_resources_required(self) -> Optional[Resources]:
        return None

    def get_host(self) -> Host:
        return LocalHost()

    def get_test_repo_url(self) -> str:
        # We want to use a local copy so that we don't need to go out to the internet
        # for this local test. This means running these tests requires cloning
        # https://github.com/meadowdata/test_repo next to the meadowrun repo.
        return str(
            (pathlib.Path(__file__).parent.parent.parent.parent / "test_repo").resolve()
        )

    def can_get_log_file(self) -> bool:
        return False

    # async def get_log_file_text(self, job_completion: JobCompletion) -> str:
    #     with open(job_completion.log_file_name, "r", encoding="utf-8") as log_file:
    #         return log_file.read()


def _set_up_working_folder_for_tests(
    working_folder: str,
) -> Tuple[str, str, str, str, str]:
    io_folder = os.path.join(working_folder, "io")
    job_logs_folder = os.path.join(working_folder, "job_logs")
    git_repos_folder = os.path.join(working_folder, "git_repos")
    local_copies_folder = os.path.join(working_folder, "local_copies")
    misc_folder = os.path.join(working_folder, "misc")

    result = (
        io_folder,
        job_logs_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    )
    for path in result:
        pathlib.Path(path).mkdir(exist_ok=True)
    return result


_SET_UP_WORKING_FOLDER_NAME = (
    f"meadowrun.run_job_local.{_set_up_working_folder.__qualname__}"
)


@pytest.fixture(scope="function")
def patch_working_folder(tmp_path: pathlib.Path, mocker: MockerFixture) -> None:
    _working_folder = mocker.patch(_SET_UP_WORKING_FOLDER_NAME)
    _working_folder.side_effect = lambda: _set_up_working_folder_for_tests(
        str(tmp_path)
    )


@pytest.mark.usefixtures(patch_working_folder.__qualname__)
class TestDeploymentsLocal(LocalHostProvider, DeploymentSuite):
    # TODO we should move these tests that use ServerAvailable into BasicsSuite and make
    # them work for TestBasicsAws. I think the best way to do that is to add
    # support for mounting an EBS volume, create one with the code in EXAMPLE_CODE and
    # attach it for these tests.

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder(self) -> None:
        await _test_meadowrun(
            self,
            ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder_container_digest(self) -> None:
        await _test_meadowrun(
            self,
            ServerAvailableFolder(code_paths=[EXAMPLE_CODE]),
            await get_latest_interpreter_version(
                ContainerAtTag(repository="python", tag="3.9.8-slim-buster"), {}
            ),
        )

    @pytest.mark.asyncio
    async def test_meadowrun_server_available_folder_container_tag(self) -> None:
        await _test_meadowrun(
            self,
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
            "python -m example_script",
            self.get_host(),
            self.get_resources_required(),
            Deployment(
                code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
            ),
        )
        job_completion2 = await run_command(
            "python -m example_script",
            self.get_host(),
            self.get_resources_required(),
            Deployment(
                code=ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWRUN_CODE])
            ),
            {"foo": "bar"},
        )
        if self.can_get_log_file():
            assert "hello there: no_data" in await self.get_log_file_text(
                job_completion1
            )
            assert "hello there: bar" in await self.get_log_file_text(job_completion2)


@pytest.mark.usefixtures(patch_working_folder.__qualname__)
class TestDeployments2Local(LocalHostProvider, DeploymentSuite2):
    pass


class TestEdgeCasesLocal(LocalHostProvider, EdgeCasesSuite):
    pass
