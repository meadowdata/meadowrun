"""
This code belongs in run_job.py, but this is split out to avoid circular dependencies
"""
from __future__ import annotations

import abc
import asyncio
import dataclasses
import enum
import os
import os.path
import pickle
import shutil
import sys
import urllib.parse
import zipfile
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import asyncssh
import cloudpickle
import filelock
from typing_extensions import Literal

import meadowrun.ssh as ssh
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import Job, ProcessState, PyFunctionJob
from meadowrun.shared import unpickle_exception

if TYPE_CHECKING:
    from meadowrun.credentials import UsernamePassword


_T = TypeVar("_T")
_U = TypeVar("_U")


CloudProvider = "EC2", "AzureVM"
CloudProviderType = Literal["EC2", "AzureVM"]


async def _retry(
    function: Callable[[], Awaitable[_T]],
    exception_types: Union[Type, Tuple[Type, ...]],
    max_num_attempts: int = 5,
    delay_seconds: float = 1,
    message: str = "Retrying on error",
) -> _T:
    i = 0
    while True:
        try:
            return await function()
        except exception_types as e:
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(f"{message}: {e}")
                await asyncio.sleep(delay_seconds)


def _needs_cuda(flags_required: Union[Iterable[str], str, None]) -> bool:
    if flags_required is None:
        return False
    if isinstance(flags_required, str):
        return "nvidia" == flags_required
    return "nvidia" in flags_required


@dataclasses.dataclass(frozen=True)
class Resources:
    """
    Specifies the requirements for a job or for each task within a job

    Attributes:
        logical_cpu: Specifies logical CPU (aka vCPU) required. E.g. 2 means we require
            2 logical CPUs
        memory_gb: Specifies RAM required. E.g. 1.5 means we requires 1.5 GB of RAM
        max_eviction_rate: Specifies what eviction rate (aka interruption probability)
            we're okay with as a percent. E.g. `80` means that any instance type with an
            eviction rate less than 80% can be used. Use `0` to indicate that only
            on-demand instance are acceptable (i.e. do not use spot instances)
        gpus: Number of GPUs required. If gpu_memory is set, but this value is not set,
            this is implied to be 1
        gpu_memory: Total GPU memory (aka VRAM) required across all GPUs
        flags_required: E.g. "intel", "avx512", etc.
    """

    logical_cpu: Optional[float] = None
    memory_gb: Optional[float] = None
    max_eviction_rate: float = 80
    gpus: Optional[float] = None
    gpu_memory: Optional[float] = None
    flags: Union[Iterable[str], str, None] = None

    def uses_gpu(self) -> bool:
        return self.gpus is not None or self.gpu_memory is not None

    def needs_cuda(self) -> bool:
        return self.uses_gpu() and _needs_cuda(self.flags)

    def to_internal(self) -> Optional[ResourcesInternal]:
        if self.logical_cpu is None or self.memory_gb is None:
            return None
        else:
            return ResourcesInternal.from_cpu_and_memory(
                self.logical_cpu,
                self.memory_gb,
                self.max_eviction_rate,
                self.gpus,
                self.gpu_memory,
                self.flags,
            )


class ObjectStorage(abc.ABC):
    """An ObjectStorage is a place where you can upload files and download them"""

    @classmethod
    @abc.abstractmethod
    def get_url_scheme(cls) -> str:
        """
        Right now we're using the URL scheme to effectively serialize the ObjectStorage
        object to the job. This works as long as we don't need any additional parameters
        (region name, username/password), but we may need to make this more flexible in
        the future.
        """
        pass

    @abc.abstractmethod
    async def upload_from_file_url(self, file_url: str) -> str:
        """
        file_url will be a file:// url to a file on the local machine. This function
        should upload that file to the object storage, delete the local file, and return
        the URL of the remote file.
        """
        pass

    @abc.abstractmethod
    async def download_and_unzip(
        self, remote_url: str, local_copies_folder: str
    ) -> str:
        """
        remote_url will be the URL of a file in the object storage system as generated
        by upload_from_file_url. This function should download the file and extract it
        to local_copies_folder if it has not already been extracted.
        """
        pass


class LocalObjectStorage(ObjectStorage):
    """
    This is a "pretend" version of ObjectStorage where we assume that we have the same
    file system available on both the client and the server. Mostly for testing.
    """

    @classmethod
    def get_url_scheme(cls) -> str:
        return "file"

    async def upload_from_file_url(self, file_url: str) -> str:
        # TODO maybe assert that this starts with file://
        return file_url

    async def download_and_unzip(
        self, remote_url: str, local_copies_folder: str
    ) -> str:
        decoded_url = urllib.parse.urlparse(remote_url)
        extracted_folder = os.path.join(
            local_copies_folder, os.path.splitext(os.path.basename(decoded_url.path))[0]
        )
        with filelock.FileLock(f"{extracted_folder}.lock", timeout=120):
            if not os.path.exists(extracted_folder):
                with zipfile.ZipFile(decoded_url.path) as zip_file:
                    zip_file.extractall(extracted_folder)

        return extracted_folder


class S3CompatibleObjectStorage(ObjectStorage, abc.ABC):
    """
    Think of this class as what ObjectStorage "should be" if it weren't for
    LocalObjectStorage. Most implementations of ObjectStorage should implement this
    class.
    """

    @abc.abstractmethod
    async def _upload(self, file_path: str) -> Tuple[str, str]:
        pass

    @abc.abstractmethod
    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        pass

    async def upload_from_file_url(self, file_url: str) -> str:
        decoded_url = urllib.parse.urlparse(file_url)
        if decoded_url.scheme != "file":
            raise ValueError(f"Expected file URI: {file_url}")
        if sys.platform == "win32" and decoded_url.path.startswith("/"):
            # on Windows, file:///C:\foo turns into file_url.path = /C:\foo so we need
            # to remove the forward slash at the beginning
            file_path = decoded_url.path[1:]
        else:
            file_path = decoded_url.path
        bucket_name, object_name = await self._upload(file_path)
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
        return urllib.parse.urlunparse(
            (self.get_url_scheme(), bucket_name, object_name, "", "", "")
        )

    async def download_and_unzip(
        self, remote_url: str, local_copies_folder: str
    ) -> str:
        decoded_url = urllib.parse.urlparse(remote_url)
        bucket_name = decoded_url.netloc
        object_name = decoded_url.path.lstrip("/")
        extracted_folder = os.path.join(local_copies_folder, object_name)

        with filelock.FileLock(f"{extracted_folder}.lock", timeout=120):
            if not os.path.exists(extracted_folder):
                zip_file_path = extracted_folder + ".zip"
                await self._download(bucket_name, object_name, zip_file_path)
                with zipfile.ZipFile(zip_file_path) as zip_file:
                    zip_file.extractall(extracted_folder)

        return extracted_folder


class WaitOption(enum.Enum):
    WAIT_AND_TAIL_STDOUT = 1
    WAIT_SILENTLY = 2
    DO_NOT_WAIT = 3


class Host(abc.ABC):
    """
    Host is an abstract class for specifying where to run a job. See implementations
    below.
    """

    async def set_defaults(self) -> None:
        # This function gives derived classes an opportunity to set defaults before any
        # other functions on the object are called.
        pass

    @abc.abstractmethod
    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        pass

    @abc.abstractmethod
    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
    ) -> Optional[Sequence[_U]]:
        # Note for implementors: job_fields will be populated with everything other than
        # job_id and py_function, so the implementation should construct
        # Job(job_id=job_id, py_function=py_function, **job_fields)
        pass

    @abc.abstractmethod
    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: bool,
        max_num_task_attempts: int,
    ) -> AsyncIterable[TaskResult[_U]]:
        pass

    @abc.abstractmethod
    async def get_object_storage(self) -> ObjectStorage:
        pass


class SshHost(Host):
    """
    Tells run_function and related functions to connect to the remote machine over SSH.
    """

    def __init__(
        self,
        address: str,
        username: str,
        private_key: asyncssh.SSHKey,
        cloud_provider: Optional[Tuple[CloudProviderType, str]] = None,
    ) -> None:
        super().__init__()
        self.address = address
        self.username = username
        self.private_key = private_key
        # If this field is populated, it will be a tuple of (cloud provider, region
        # name). Cloud provider will be e.g. "EC2" indicating that we're running on e.g.
        # an EC2 instance allocated via instance_allocation.py, so we need to deallocate
        # the job via the right InstanceRegistrar when we're done. region name indicates
        # where the InstanceRegistrar that we used to allocate this job is.
        self.cloud_provider = cloud_provider
        self._connect_task: Optional[asyncio.Task[asyncssh.SSHClientConnection]] = None

    def _connection_future(self) -> asyncio.Task[asyncssh.SSHClientConnection]:
        # try the connection 20 times.
        if self._connect_task is None:
            self._connect_task = asyncio.create_task(
                _retry(
                    lambda: ssh.connect(
                        self.address,
                        username=self.username,
                        private_key=self.private_key,
                    ),
                    (TimeoutError, ConnectionRefusedError, OSError),
                    max_num_attempts=20,
                )
            )
        return self._connect_task

    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:

        connection = await self._connection_future()

        try:
            job_io_prefix = ""

            # assumes that meadowrun is installed in /var/meadowrun/env as per
            # build_meadowrun_amis.md. Also uses the default working_folder, which
            # should (but doesn't strictly need to) correspond to
            # agent._set_up_working_folder

            home_result = await ssh.run_and_capture(
                connection, "echo $HOME", check=False
            )
            if not home_result.exit_status == 0:
                raise ValueError(
                    "Error getting home directory on remote machine "
                    + str(home_result.stdout)
                )
            home_out = str(home_result.stdout).strip()

            # in_stream is needed otherwise invoke listens to stdin, which
            # pytest doesn't like
            remote_working_folder = f"{home_out}/meadowrun"
            mkdir_result = await ssh.run_and_capture(
                connection, f"mkdir -p {remote_working_folder}/io", check=False
            )
            if not mkdir_result.exit_status == 0:
                raise ValueError(
                    "Error creating meadowrun directory " + str(mkdir_result.stdout)
                )

            # serialize job_to_run and send it to the remote machine
            job_io_prefix = f"{remote_working_folder}/io/{job.job_id}"
            await ssh.write_to_file(
                connection, job.SerializeToString(), f"{job_io_prefix}.job_to_run"
            )

            command_prefixes = []
            command_suffixes = []

            if wait_for_result == WaitOption.DO_NOT_WAIT:
                # reference on nohup: https://github.com/ronf/asyncssh/issues/137
                command_prefixes.append("/usr/bin/nohup")
            if self.cloud_provider is not None:
                command_suffixes.append(
                    f"--cloud {self.cloud_provider[0]} "
                    f"--cloud-region-name {self.cloud_provider[1]}"
                )
            if wait_for_result in (WaitOption.DO_NOT_WAIT, WaitOption.WAIT_SILENTLY):
                command_suffixes.append("< /dev/null > /dev/null 2>&1")
                if wait_for_result == WaitOption.DO_NOT_WAIT:
                    command_suffixes.append("&")

            if command_prefixes:
                command_prefixes.append(" ")
            if command_suffixes:
                command_suffixes.insert(0, " ")

            command = (
                " ".join(command_prefixes) + "/usr/bin/env PYTHONUNBUFFERED=1 "
                "/var/meadowrun/env/bin/python "
                # "-X importtime "
                # "-m cProfile -o remote.prof "
                "-m meadowrun.run_job_local_main "
                f"--job-id {job.job_id} "
                f"--working-folder {remote_working_folder}" + " ".join(command_suffixes)
            )

            print(
                f"Running job on {self.address} "
                f"/var/meadowrun/job_logs/{job.job_friendly_name}.{job.job_id}.log"
            )

            cmd_result = await ssh.run_and_print(connection, command, check=False)

            # TODO consider using result.tail, result.stdout

            # see if we got a normal return code
            if cmd_result.exit_status != 0:
                raise ValueError(f"Process exited {cmd_result.returncode}")

            if wait_for_result == WaitOption.DO_NOT_WAIT:
                return JobCompletion(
                    None, ProcessState.ProcessStateEnum.RUNNING, "", 0, self.address
                )

            process_state = ProcessState()
            process_state.ParseFromString(
                await ssh.read_from_file(connection, f"{job_io_prefix}.process_state")
            )

            if process_state.state == ProcessState.ProcessStateEnum.SUCCEEDED:
                job_spec_type = job.WhichOneof("job_spec")
                # we must have a result from functions, in other cases we can
                # optionally have a result
                if job_spec_type == "py_function" or process_state.pickled_result:
                    result = pickle.loads(process_state.pickled_result)
                else:
                    result = None
                return JobCompletion(
                    result,
                    process_state.state,
                    process_state.log_file_name,
                    process_state.return_code,
                    self.address,
                )
            else:
                raise MeadowrunException(process_state)

        finally:
            # TODO also clean up log files?
            # TODO clean up files for jobs where wait_for_result is DO_NOT_WAIT
            if job_io_prefix and wait_for_result != WaitOption.DO_NOT_WAIT:
                remote_paths = " ".join(
                    [
                        f"{job_io_prefix}.job_to_run",
                        f"{job_io_prefix}.function",
                        f"{job_io_prefix}.arguments",
                        f"{job_io_prefix}.state",
                        f"{job_io_prefix}.result",
                        f"{job_io_prefix}.process_state",
                        f"{job_io_prefix}.initial_process_state",
                    ]
                )
                try:
                    # -f so that we don't throw an error on files that don't
                    # exist
                    await ssh.run_and_capture(
                        connection, f"rm -f {remote_paths}", check=True
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(
                        f"Error cleaning up files on remote machine: {remote_paths} {e}"
                    )

    async def close_connection(self) -> None:
        if self._connect_task is not None:
            connection = await self._connect_task
            connection.close()
            await connection.wait_closed()

    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
    ) -> Optional[Sequence[_U]]:
        raise NotImplementedError("run_map is not implemented for SshHost")

    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: bool,
        max_num_task_attempts: int,
    ) -> AsyncIterable[TaskResult[_U]]:
        raise NotImplementedError("run_map_as_completed is not implemented for SshHost")

    async def get_object_storage(self) -> ObjectStorage:
        raise NotImplementedError("get_object_storage is not implemented for SshHost")


@dataclasses.dataclass
class JobCompletion(Generic[_T]):
    """Information about how a job completed"""

    # TODO both JobCompletion and MeadowrunException should be revisited

    result: _T
    process_state: ProcessState._ProcessStateEnum.ValueType
    log_file_name: str
    return_code: int
    public_address: str


class MeadowrunException(Exception):
    def __init__(self, process_state: ProcessState) -> None:
        super().__init__("Failure while running a meadowrun job: " + str(process_state))
        self.process_state = process_state


class TaskException(Exception):
    """Represents an exception that occurred in a task."""

    pass


@dataclasses.dataclass(frozen=True)
class TaskResult(Generic[_T]):
    """
    The result of a [run_map_as_completed][meadowrun.run_map_as_completed] task.

    Attributes:
        task_id: The index of the task as it was originally passed to
            `run_map_as_completed`.
        is_success: True if the task completed successfully, False if the task raised an
            exception
        result: If `is_success`, the result of the task. Otherwise, None. See also
            `result_or_raise`
        exception: If `not is_success`, a Tuple describing the exception that the task
            raised. Otherwise, None. See also `result_or_raise`.
        attempt: 1-based number indicating which attempt of the task this is. 1 means
            first attempt, 2 means second attempt, etc.
    """

    task_id: int
    is_success: bool
    result: Optional[_T] = None
    exception: Optional[Tuple[str, str, str]] = None
    attempt: int = 1

    def result_or_raise(self) -> _T:
        """Returns a successful task result, or raises a TaskException.

        Raises:
            TaskException: if the task did not finish successfully.

        Returns:
            _T: the unpickled result if the task finished successfully.
        """
        if self.is_success:
            assert self.result is not None
            return self.result
        elif self.exception is not None:
            raise TaskException(*self.exception)
        else:
            # fallback exception
            raise TaskException("Task was not successful")


_EXCEPTION_STATES = (
    ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
    ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
)


# mypy bug/issue, see https://github.com/python/mypy/issues/5374
@dataclasses.dataclass(frozen=True)  # type: ignore[misc]
class GridJobDriver(abc.ABC, Generic[_T, _U]):
    """See run_map. This allows run_map to use EC2 or Azure VMs"""

    region_name: str
    allocated_hosts: Dict[str, List[str]]
    ssh_username: str
    ssh_private_key: asyncssh.SSHKey
    num_tasks: int
    num_workers: int
    function: Callable[[_T], _U]

    # would love to make this worker_function(self, public_address, worker_id) -> None,
    # but worker_function is cloudpickled to the remote and self shouldn't be in the
    # closure.
    @abc.abstractmethod
    def worker_function(self) -> Callable[[str, int], None]:
        ...

    @abc.abstractmethod
    def receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[int, int, ProcessState]]:
        ...

    @abc.abstractmethod
    async def retry_task(self, task_id: int, attempts_so_far: int) -> None:
        ...

    async def get_results_as_completed(
        self,
        workers_done: asyncio.Event,
        max_num_task_attempts: int,
    ) -> AsyncIterable[TaskResult]:
        """Generates TaskResult objects for each task as tasks are completed. Useful for
        overlapping async work with getting task results, or getting the results from
        jobs with partially unsuccesful tasks.
        """
        # done = successful or exhausted retries
        num_tasks_done = 0
        stop_receiving = asyncio.Event()
        if self.num_tasks == num_tasks_done:
            stop_receiving.set()
        async for task_id, attempt, result in self.receive_task_results(
            stop_receiving=stop_receiving, workers_done=workers_done
        ):
            print(
                f"Waiting for task results. Requested: {self.num_tasks}, "
                f"received: {num_tasks_done}"
            )
            if result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
                num_tasks_done += 1
                # TODO try/catch on pickle.loads?
                yield TaskResult(
                    task_id,
                    is_success=True,
                    result=pickle.loads(result.pickled_result),
                    attempt=attempt,
                )
            else:
                if result.state in _EXCEPTION_STATES:
                    exception = unpickle_exception(result.pickled_result)
                    task_result: TaskResult = TaskResult(
                        task_id, is_success=False, exception=exception, attempt=attempt
                    )
                else:
                    task_result = TaskResult(task_id, is_success=False, attempt=attempt)

                if attempt < max_num_task_attempts:
                    print(f"Task {task_id} failed at attempt {attempt}, retrying.")
                    await self.retry_task(task_id, attempt)
                else:
                    print(
                        f"Task {task_id} failed at attempt {attempt}, "
                        f"max attempts is {max_num_task_attempts}, not retrying."
                    )
                    num_tasks_done += 1
                    yield task_result

            if num_tasks_done >= self.num_tasks:
                stop_receiving.set()
            # else:
            #     num_tasks_done += 1
            #     yield TaskResult(task_id, is_success=False, attempt=attempt)
        if num_tasks_done < self.num_tasks:
            print(
                "Gave up retrieving task results. "
                f"Received {num_tasks_done}/{self.num_tasks} task results."
            )
        else:
            print(f"Received all {self.num_tasks} task results.")


@dataclasses.dataclass(frozen=True)
class ContainerRegistryHelper:
    """
    Allows compile_environment_spec_to_container to use either AWS ECR, Azure CR, or
    neither
    """

    should_push: bool
    username_password: Optional[UsernamePassword]
    image_name: str
    does_image_exist: bool


class AllocVM(Host, abc.ABC):
    """
    An abstract class that provides shared implementation for
    [AllocEC2Instance][meadowrun.AllocEC2Instance] and
    [AllocAzureVM][meadowrun.AllocAzureVM]
    """

    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
    ) -> Optional[Sequence[_U]]:
        if resources_required_per_task is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocEC2Instance and AllocAzureVM"
            )

        async_iterator = self.run_map_as_completed(
            function,
            args,
            resources_required_per_task,
            job_fields,
            num_concurrent_tasks,
            pickle_protocol,
            wait_for_result,
            max_num_task_attempts,
        )

        if wait_for_result == WaitOption.DO_NOT_WAIT:
            # we still need to iterate, even though no values are returned, to execute
            # the rest of the code in the iterator.
            async for _ in async_iterator:
                pass
            return None
        else:
            task_results: List[TaskResult] = []

            # TODO - this will wait forever if any tasks are missing
            async for task_result in async_iterator:
                task_results.append(task_result)

            task_results.sort(key=lambda tr: tr.task_id)

            # if tasks were None, we'd have throw already
            failed_tasks = [result for result in task_results if not result.is_success]
            if failed_tasks:
                # TODO better error message
                raise Exception(f"Some tasks failed: {failed_tasks}")

            return [result.result for result in task_results]  # type: ignore[misc]

    async def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: bool,
        max_num_task_attempts: int,
    ) -> AsyncIterable[TaskResult[_U]]:
        if self.get_cloud_provider() == "AzureVM" and max_num_task_attempts != 1:
            raise NotImplementedError(
                "max_num_task_attempts must be 1 for AzureVM. "
                "Task retries are not implemented yet."
            )
        if resources_required_per_task is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocEC2Instance and AllocAzureVM"
            )

        driver = await self._create_grid_job_driver(
            function,
            args,
            resources_required_per_task,
            job_fields["ports"],
            num_concurrent_tasks,
        )

        worker_tasks, ssh_hosts = await self._run_worker_loops(
            driver,
            pickle_protocol,
            job_fields,
            resources_required_per_task,
            wait_for_result=WaitOption.WAIT_SILENTLY,
        )

        async def gather_workers_and_set(
            event: asyncio.Event, worker_tasks: List[asyncio.Task[JobCompletion]]
        ) -> List:
            worker_results = await asyncio.gather(*worker_tasks, return_exceptions=True)
            event.set()
            return worker_results

        try:
            workers_done = asyncio.Event()
            workers_done_future = asyncio.create_task(
                gather_workers_and_set(workers_done, worker_tasks)
            )

            if wait_for_result:
                async for result in driver.get_results_as_completed(
                    workers_done, max_num_task_attempts
                ):
                    yield result

            worker_results = await workers_done_future
            for worker_id, result in enumerate(worker_results):
                if isinstance(result, Exception):
                    print(f"Worker {worker_id} exited with error: {result}")
        finally:
            await asyncio.gather(
                *[ssh_host.close_connection() for ssh_host in ssh_hosts],
                return_exceptions=True,
            )

    @abc.abstractmethod
    async def _create_grid_job_driver(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: ResourcesInternal,
        ports: Sequence[str],
        num_concurrent_tasks: int,
    ) -> GridJobDriver:
        pass

    @abc.abstractmethod
    def get_cloud_provider(self) -> CloudProviderType:
        pass

    async def _run_worker_loops(
        self,
        driver: GridJobDriver,
        pickle_protocol: int,
        job_fields: Dict[str, Any],
        resources_required_per_task: Optional[ResourcesInternal],
        wait_for_result: WaitOption,
    ) -> Tuple[List[asyncio.Task[JobCompletion]], Iterable[SshHost]]:
        # Now we will run worker_loop jobs on the hosts we got:

        pickled_worker_function = cloudpickle.dumps(
            driver.worker_function(), protocol=pickle_protocol
        )

        worker_tasks = []
        worker_id = 0
        address_to_ssh_host: Dict[str, SshHost] = {}
        for public_address, worker_job_ids in driver.allocated_hosts.items():
            ssh_host = address_to_ssh_host.get(public_address)
            if ssh_host is None:
                ssh_host = SshHost(
                    public_address,
                    driver.ssh_username,
                    driver.ssh_private_key,
                    (self.get_cloud_provider(), driver.region_name),
                )
                address_to_ssh_host[public_address] = ssh_host
            for worker_job_id in worker_job_ids:
                job = Job(
                    job_id=worker_job_id,
                    py_function=PyFunctionJob(
                        pickled_function=pickled_worker_function,
                        pickled_function_arguments=pickle.dumps(
                            ([public_address, worker_id], {}), protocol=pickle_protocol
                        ),
                    ),
                    **job_fields,
                )

                worker_tasks.append(
                    asyncio.create_task(
                        ssh_host.run_job(
                            resources_required_per_task, job, wait_for_result
                        )
                    )
                )

                worker_id += 1

        return worker_tasks, address_to_ssh_host.values()

    @abc.abstractmethod
    def get_runtime_resources(self) -> ResourcesInternal:
        # "Runtime resources" are resources that aren't tied to a particular instance
        # type. Instance type resources are things like CPU and memory. Runtime
        # resources are things like an AMI id or subnet id. Runtime resources should NOT
        # be considered when choosing an instance type, but need to be considered when
        # deciding whether an existing instance can run a new job.
        pass
