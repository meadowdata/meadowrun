"""
This code belongs in run_job.py, but this is split out to avoid circular dependencies
"""
from __future__ import annotations

import abc
import asyncio
import dataclasses
import enum
import pickle
import traceback
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
from typing_extensions import Literal

import meadowrun.ssh as ssh
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import Job, ProcessState
from meadowrun.shared import unpickle_exception

if TYPE_CHECKING:
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket
    from meadowrun.credentials import UsernamePassword

_T = TypeVar("_T")
_U = TypeVar("_U")


CloudProvider = "EC2", "AzureVM"
CloudProviderType = Literal["EC2", "AzureVM"]


def get_log_path(job_id: str) -> str:
    return f"/var/meadowrun/job_logs/{job_id}.log"


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
        ephemeral_storage: GB of local storage (aka local disk). Currently only
            supported on Kubernetes
    """

    logical_cpu: Optional[float] = None
    memory_gb: Optional[float] = None
    max_eviction_rate: float = 80
    gpus: Optional[float] = None
    gpu_memory: Optional[float] = None
    flags: Union[Iterable[str], str, None] = None
    ephemeral_storage_gb: Optional[float] = None

    def uses_gpu(self) -> bool:
        return self.gpus is not None or self.gpu_memory is not None

    def needs_cuda(self) -> bool:
        return self.uses_gpu() and _needs_cuda(self.flags)

    def to_internal_or_none(self) -> Optional[ResourcesInternal]:
        if self.logical_cpu is None or self.memory_gb is None:
            return None
        else:
            return self.to_internal()

    def to_internal(self) -> ResourcesInternal:
        return ResourcesInternal.from_cpu_and_memory(
            self.logical_cpu,
            self.memory_gb,
            self.max_eviction_rate,
            self.gpus,
            self.gpu_memory,
            self.flags,
            self.ephemeral_storage_gb,
        )


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
        retry_with_more_memory: bool,
    ) -> Optional[Sequence[_U]]:
        async_iterator = self.run_map_as_completed(
            function,
            args,
            resources_required_per_task,
            job_fields,
            num_concurrent_tasks,
            pickle_protocol,
            wait_for_result,
            max_num_task_attempts,
            retry_with_more_memory,
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
                raise RunMapTasksFailedException(
                    failed_tasks, [args[task.task_id] for task in failed_tasks]
                )

            return [result.result for result in task_results]  # type: ignore[misc]

    @abc.abstractmethod
    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult[_U]]:
        pass

    @abc.abstractmethod
    async def get_storage_bucket(self) -> AbstractStorageBucket:
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
        instance_name: Optional[str] = None,
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
        self.instance_name = instance_name
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
        # TODO we should probably make SshHost not a Host, and if we want to expose
        # SshHost-as-a-Host functionality, create a separate, more pure SshHost class.
        # This SshHost class is fairly tightly coupled to the EC2/AzureVM code.
        return await self.run_cloud_job(job, job.base_job_id, wait_for_result, None)

    async def run_cloud_job(
        self,
        job: Job,
        job_id: str,
        wait_for_result: WaitOption,
        deallocator: Optional[Callable[[], Awaitable[None]]],
    ) -> JobCompletion[Any]:
        job_io_prefix = ""
        deallocation_ran = False

        try:
            connection = await self._connection_future()

            # serialize job_to_run and send it to the remote machine
            job_io_prefix = f"/var/meadowrun/io/{job_id}"
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

            log_file_name = get_log_path(job_id)
            if wait_for_result == WaitOption.WAIT_AND_TAIL_STDOUT:
                command_suffixes.append(
                    f"2>&1 | tee --ignore-interrupts {log_file_name}"
                )
            else:
                command_suffixes.append(f"> {log_file_name} 2>&1")
                if wait_for_result == WaitOption.DO_NOT_WAIT:
                    command_suffixes.append("&")

            if command_prefixes:
                command_prefixes.append(" ")
            if command_suffixes:
                command_suffixes.insert(0, " ")

            command = (
                " ".join(command_prefixes) + "/usr/bin/env PYTHONUNBUFFERED=1 "
                "/var/meadowrun/env/bin/python -m meadowrun.run_job_local_main --job-id"
                f" {job_id} --public-address {self.address}"
                + " ".join(command_suffixes)
            )

            print(f"Running job on {self.address} {log_file_name}")

            # very shortly after this point, we can rely on the deallocation command
            # running on the remote machine
            deallocation_ran = True
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
                raise MeadowrunException(process_state, self.address)

        finally:
            if not deallocation_ran and deallocator:
                await deallocator()

            # TODO also clean up log files?
            # TODO this logic should be moved into the remote machine, a la
            # deallocate_jobs.py. That will also take care of cleaning up these files
            # during DO_NOT_WAIT
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
                    pass
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
        retry_with_more_memory: bool,
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
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult[_U]]:
        raise NotImplementedError("run_map_as_completed is not implemented for SshHost")

    async def get_storage_bucket(self) -> AbstractStorageBucket:
        raise NotImplementedError("get_storage_bucket is not implemented for SshHost")

    async def tail_log(self, log_file_name: str) -> None:
        connection = await self._connection_future()
        cmd_result = await ssh.run_and_print(
            connection, f"tail -F {log_file_name}", check=False
        )
        if cmd_result.exit_status != 0:
            raise ValueError(f"tail command exited with {cmd_result.returncode}")


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
    def __init__(
        self, process_state: ProcessState, address: Optional[str] = None
    ) -> None:
        if process_state.state in (
            ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
            ProcessState.ProcessStateEnum.UNEXPECTED_WORKER_EXIT,
        ):
            return_code = f": {process_state.return_code}"
        else:
            return_code = ""

        message = [
            "Failure while running a Meadowrun job: "
            f"{ProcessState.ProcessStateEnum.Name(process_state.state)}{return_code}"
        ]

        if process_state.log_file_name:
            if address is not None:
                message.append(f"Log file: {address} {process_state.log_file_name}")
            else:
                message.append(f"Log file: {process_state.log_file_name}")
        elif address is not None:
            message.append(f"On host: {address}")

        if (
            process_state.state
            in (
                ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
                ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                ProcessState.ProcessStateEnum.ERROR_GETTING_STATE,
            )
            and process_state.pickled_result
        ):
            remote_exception = unpickle_exception(process_state.pickled_result)
            message.append(remote_exception[2])

        super().__init__("\n".join(message))
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
    state: str
    result: Optional[_T] = None
    exception: Optional[Tuple[str, str, str]] = None
    attempt: int = 1
    log_file_name: str = ""

    @staticmethod
    def from_process_state(task: TaskProcessState) -> TaskResult:
        if task.result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
            try:
                unpickled_result = pickle.loads(task.result.pickled_result)
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                # the task was successful but now the result cannot be unpickled. This
                # is usually because there is a dependency available on the remote side
                # that is not available locally.
                return TaskResult(
                    task.task_id,
                    is_success=False,
                    state="RESULT_CANNOT_BE_UNPICKLED",
                    exception=(str(type(e)), str(e), traceback.format_exc()),
                    result=task.result.pickled_result,
                    attempt=task.attempt,
                    log_file_name=task.result.log_file_name,
                )

            return TaskResult(
                task.task_id,
                is_success=True,
                state=ProcessState.ProcessStateEnum.Name(task.result.state),
                result=unpickled_result,
                attempt=task.attempt,
                log_file_name=task.result.log_file_name,
            )
        elif task.result.state in _EXCEPTION_STATES:
            exception = unpickle_exception(task.result.pickled_result)
            if exception is None and task.result.return_code != 0:
                return_code_message = f"Non-zero return code: {task.result.return_code}"
                exception = ("", return_code_message, return_code_message + "\n")
            return TaskResult(
                task.task_id,
                is_success=False,
                state=ProcessState.ProcessStateEnum.Name(task.result.state),
                exception=exception,
                attempt=task.attempt,
                log_file_name=task.result.log_file_name,
            )
        else:
            if task.result.return_code != 0:
                return_code_message = f"Non-zero return code: {task.result.return_code}"
                exception = ("", return_code_message, return_code_message + "\n")
            else:
                exception = None
            return TaskResult(
                task.task_id,
                is_success=False,
                state=ProcessState.ProcessStateEnum.Name(task.result.state),
                exception=exception,
                attempt=task.attempt,
                log_file_name=task.result.log_file_name,
            )

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
            raise TaskException(f"Task was not successful, state: {self.state}")


_EXCEPTION_STATES = (
    ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
    ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
)


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


class RunMapTasksFailedException(Exception):
    def __init__(self, failed_tasks: List[TaskResult], failed_task_args: List[Any]):
        failed_task_message = [f"{len(failed_tasks)} tasks failed:\n"]
        for task, arg in zip(failed_tasks, failed_task_args):
            failed_task_message.append(
                f"Task #{task.task_id} with arg ({arg}) on "
                f"attempt {task.attempt} failed, log file is {task.log_file_name}, "
                f"final state: {task.state}, "
                f"exception was:\n"
            )
            if task.exception is not None:
                failed_task_message.append(task.exception[2])
            else:
                failed_task_message.append("Exception traceback not available\n")

        super().__init__("".join(failed_task_message))

        self.failed_tasks = failed_tasks
        self.failed_task_args = failed_task_args


@dataclasses.dataclass(frozen=True)
class TaskProcessState:
    task_id: int
    attempt: int
    result: ProcessState


@dataclasses.dataclass(frozen=True)
class WorkerProcessState:
    worker_index: str
    result: ProcessState
