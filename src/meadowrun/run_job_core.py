"""
This code belongs in run_job.py, but this is split out to avoid circular dependencies
"""
from __future__ import annotations

import abc
import asyncio
import collections
import dataclasses
import enum
import itertools
import pickle
import time
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Coroutine,
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
    cast,
)

import asyncssh
import cloudpickle
from typing_extensions import Literal


import meadowrun.ssh as ssh
from meadowrun.config import MEMORY_GB
from meadowrun.instance_allocation import allocate_jobs_to_instances, InstanceRegistrar
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import Job, ProcessState, PyAgentJob
from meadowrun.shared import unpickle_exception

if TYPE_CHECKING:
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket
    from meadowrun.credentials import UsernamePassword
    from meadowrun.run_job_local import TaskWorkerServer, WorkerMonitor
    from types import TracebackType


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
        return await self.run_cloud_job(job, wait_for_result, None)

    async def run_cloud_job(
        self,
        job: Job,
        wait_for_result: WaitOption,
        deallocator: Optional[Callable[[], Awaitable[None]]],
    ) -> JobCompletion[Any]:
        job_io_prefix = ""
        deallocation_ran = False

        try:
            connection = await self._connection_future()

            # serialize job_to_run and send it to the remote machine
            job_io_prefix = f"/var/meadowrun/io/{job.job_id}"
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

            log_file_name = (
                f"/var/meadowrun/job_logs/{job.job_friendly_name}.{job.job_id}.log"
            )
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
                "/var/meadowrun/env/bin/python "
                # "-X importtime "
                # "-m cProfile -o remote.prof "
                "-m meadowrun.run_job_local_main "
                f"--job-id {job.job_id}" + " ".join(command_suffixes)
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


class AllocVM(Host, abc.ABC):
    """
    An abstract class that provides shared implementation for
    [AllocEC2Instance][meadowrun.AllocEC2Instance] and
    [AllocAzureVM][meadowrun.AllocAzureVM]
    """

    async def run_map_as_completed(
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
        if resources_required_per_task is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocEC2Instance and AllocAzureVM"
            )

        async with self._create_grid_job_cloud_interface() as cloud_interface:
            driver = GridJobDriver(
                cloud_interface, num_concurrent_tasks, resources_required_per_task
            )
            run_worker_loops = asyncio.create_task(
                driver.run_worker_functions(
                    self,
                    function,
                    pickle_protocol,
                    job_fields,
                    wait_for_result,
                )
            )
            num_tasks_done = 0
            async for result in driver.add_tasks_and_get_results(
                args, max_num_task_attempts, retry_with_more_memory
            ):
                yield result
                num_tasks_done += 1

            await run_worker_loops

        # this is for extra safety--the only case where we don't get all of our results
        # back should be if run_worker_loops throws an exception because there were
        # worker failures
        if num_tasks_done < len(args):
            raise ValueError(
                "Gave up retrieving task results, most likely due to worker failures. "
                f"Received {num_tasks_done}/{len(args)} task results."
            )

    @abc.abstractmethod
    def _create_grid_job_cloud_interface(self) -> GridJobCloudInterface:
        pass

    @abc.abstractmethod
    def get_cloud_provider(self) -> CloudProviderType:
        pass

    @abc.abstractmethod
    def get_runtime_resources(self) -> ResourcesInternal:
        # "Runtime resources" are resources that aren't tied to a particular instance
        # type. Instance type resources are things like CPU and memory. Runtime
        # resources are things like an AMI id or subnet id. Runtime resources should NOT
        # be considered when choosing an instance type, but need to be considered when
        # deciding whether an existing instance can run a new job.
        pass


@dataclasses.dataclass(frozen=True)
class TaskProcessState:
    task_id: int
    attempt: int
    result: ProcessState


@dataclasses.dataclass(frozen=True)
class WorkerProcessState:
    worker_index: str
    result: ProcessState


class GridJobCloudInterface(abc.ABC, Generic[_T, _U]):
    """
    See also GridJobDriver. The GridJobDriver is a concrete class that handles the
    cloud-independent logic for running a grid job, e.g. replacing workers that exit
    unexpectedly (not implemented yet), retrying vs giving up on tasks. The
    GridJobCloudInterface is an interface that we can implement for different cloud
    providers. The GridJobDriver always has a single GridJobCloudInterface
    implementation that it uses to actually "do things" in the real world like launch
    instances and start workers.
    """

    async def __aenter__(self) -> GridJobCloudInterface:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    @abc.abstractmethod
    def create_instance_registrar(self) -> InstanceRegistrar:
        ...

    @abc.abstractmethod
    def create_queue(self) -> int:
        ...

    @abc.abstractmethod
    async def setup_and_add_tasks(self, tasks: Sequence[_T]) -> None:
        """
        GridJobDriver will always call this exactly once before any other functions on
        this class are called.
        """
        ...

    @abc.abstractmethod
    async def ssh_host_from_address(self, address: str, instance_name: str) -> SshHost:
        ...

    @abc.abstractmethod
    async def shutdown_workers(self, num_workers: int, queue_index: int) -> None:
        ...

    @abc.abstractmethod
    async def get_worker_function(
        self, queue_index: int
    ) -> Callable[
        [str, str, TaskWorkerServer, WorkerMonitor],
        Coroutine[Any, Any, None],
    ]:
        """
        Returns a function that will poll/wait for tasks, and communicate to task worker
        via the given streamreader and -writer. The returned function will also exit in
        response to shutdown_workers.
        """
        ...

    @abc.abstractmethod
    async def receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
        ...

    @abc.abstractmethod
    async def retry_task(
        self, task_id: int, attempts_so_far: int, queue_index: int
    ) -> None:
        ...


_PRINT_RECEIVED_TASKS_SECONDS = 10


@dataclasses.dataclass
class WorkerQueue:
    """
    Keeps track of how many workers we've started/shutdown for each queue. The 0th queue
    is the "normal" one, and subsequent queues are for retrying tasks with more
    resources
    """

    queue_index: int
    num_workers_needed: int
    num_workers_launched: int = 0
    num_worker_shutdown_messages_sent: int = 0
    num_workers_exited_unexpectedly: int = 0
    pickled_worker_function_task: Optional[asyncio.Task[bytes]] = None


@dataclasses.dataclass(frozen=True)
class WorkerTask:
    log_file_info: str
    queue_index: int
    task: asyncio.Task[JobCompletion]


def _memory_gb_for_queue_index(
    queue_index: int, base_resources: ResourcesInternal
) -> float:
    """See WorkerQueue"""
    return base_resources.consumable[MEMORY_GB] * (1 + queue_index)


def _resources_for_queue_index(
    queue_index: int, base_resources: ResourcesInternal
) -> ResourcesInternal:
    """See WorkerQueue"""
    if queue_index == 0:
        return base_resources
    else:
        return base_resources.add(
            ResourcesInternal.from_cpu_and_memory(
                0,
                base_resources.consumable[MEMORY_GB] * queue_index,
            )
        )


class GridJobDriver:
    """
    See GridJobCloudInterface. This class handles the cloud-independent logic for
    running a grid job, e.g. replacing workers that exit unexpectedly (not implemented
    yet), retrying vs giving up on tasks.

    The basic design is that there are two "loops" run_worker_functions and
    add_tasks_and_get_results that run "in parallel" via asyncio. They interact using
    shared asyncio.Event objects.
    """

    def __init__(
        self,
        cloud_interface: GridJobCloudInterface,
        num_concurrent_tasks: int,
        resources_required_per_task: ResourcesInternal,
    ):
        """This constructor must be called on an EventLoop"""
        self._cloud_interface = cloud_interface
        self._resources_required_per_task = resources_required_per_task

        # run_worker_functions will set this to indicate to add_tasks_and_get_results
        # that there all of our workers have either exited unexpectedly (and we have
        # given up trying to restore them), or have been told to shutdown normally
        self._no_workers_available = asyncio.Event()

        self._worker_queues = [WorkerQueue(0, num_concurrent_tasks)]
        self._num_workers_needed_changed = asyncio.Event()

        self._abort_launching_new_workers = asyncio.Event()

    async def run_worker_functions(
        self,
        alloc_cloud_instance: AllocVM,
        user_function: Callable[[_T], _U],
        pickle_protocol: int,
        job_fields: Dict[str, Any],
        wait_for_result: WaitOption,
    ) -> None:
        """
        Allocates cloud instances, runs a worker function on them, sends worker shutdown
        messages when requested by add_tasks_and_get_results, and generally manages
        workers (e.g. replacing workers when they exit unexpectedly, not implemented
        yet).
        """

        address_to_ssh_host: Dict[str, SshHost] = {}
        worker_tasks: List[WorkerTask] = []

        workers_needed_changed_wait_task = asyncio.create_task(
            self._num_workers_needed_changed.wait()
        )
        pickled_worker_function_task: Optional[asyncio.Task[bytes]] = None
        async_cancel_exception = False

        try:
            if (
                job_fields["ports"]
                and alloc_cloud_instance.get_cloud_provider() == "AzureVM"
            ):
                raise NotImplementedError(
                    "Opening ports on Azure is not implemented, please comment on "
                    "https://github.com/meadowdata/meadowrun/issues/126"
                )

            # we create an asyncio.task for this because it requires waiting for
            # _cloud_interface.setup_and_add_tasks to complete. We don't want to just
            # run this sequentially, though, because we want to start launching
            # instances before setup_and_add_tasks is complete.
            async def _get_pickled_worker_function(queue_index: int) -> bytes:
                return cloudpickle.dumps(
                    await self._cloud_interface.get_worker_function(queue_index),
                    protocol=pickle_protocol,
                )

            # initially, we just want the 0th queue
            self._worker_queues[0].pickled_worker_function_task = asyncio.create_task(
                _get_pickled_worker_function(0)
            )

            # "inner_" on the parameter names is just to avoid name collision with outer
            # scope
            async def launch_worker_function(
                inner_ssh_host: SshHost,
                inner_worker_job_id: str,
                log_file_name: str,
                inner_instance_registrar: InstanceRegistrar,
                queue_index: int,
            ) -> JobCompletion:
                inner_worker_queue = self._worker_queues[queue_index]
                if inner_worker_queue.pickled_worker_function_task is None:
                    inner_worker_queue.pickled_worker_function_task = (
                        asyncio.create_task(_get_pickled_worker_function(queue_index))
                    )
                job = Job(
                    job_id=inner_worker_job_id,
                    py_agent=PyAgentJob(
                        pickled_function=cloudpickle.dumps(
                            user_function, protocol=pickle_protocol
                        ),
                        pickled_agent_function=(
                            await inner_worker_queue.pickled_worker_function_task
                        ),
                        pickled_agent_function_arguments=pickle.dumps(
                            ([inner_ssh_host.address, log_file_name], {}),
                            protocol=pickle_protocol,
                        ),
                    ),
                    **job_fields,
                )

                async def deallocator() -> None:
                    if inner_ssh_host.instance_name is not None:
                        await inner_instance_registrar.deallocate_job_from_instance(
                            await inner_instance_registrar.get_registered_instance(
                                inner_ssh_host.instance_name
                            ),
                            inner_worker_job_id,
                        )

                return await inner_ssh_host.run_cloud_job(
                    job, wait_for_result, deallocator
                )

            async with self._cloud_interface.create_instance_registrar() as instance_registrar:  # noqa: E501
                while True:
                    for worker_queue in self._worker_queues:
                        # TODO we should subtract workers_exited_unexpectedly from
                        # workers_launched, this would mean we replace workers that
                        # exited unexpectedly. Implementing this properly means we
                        # should add more code that will tell us why workers exited
                        # unexpectedly (e.g. segfault in user code, spot instance
                        # eviction, vs an issue creating the environment). The main
                        # concern is ending up in an infinite loop where we're
                        # constantly launching workers that fail.
                        new_workers_to_launch = worker_queue.num_workers_needed - (
                            worker_queue.num_workers_launched
                            - worker_queue.num_worker_shutdown_messages_sent
                        )

                        # launch new workers if they're needed
                        if new_workers_to_launch > 0:
                            # TODO pass in an event for if workers_needed goes to 0,
                            # cancel any remaining allocate_jobs_to_instances
                            async for allocated_hosts in allocate_jobs_to_instances(
                                instance_registrar,
                                _resources_for_queue_index(
                                    worker_queue.queue_index,
                                    self._resources_required_per_task,
                                ),
                                new_workers_to_launch,
                                alloc_cloud_instance,
                                job_fields["ports"],
                                self._abort_launching_new_workers,
                            ):
                                for (
                                    (public_address, instance_name),
                                    worker_job_ids,
                                ) in allocated_hosts.items():
                                    ssh_host = address_to_ssh_host.get(public_address)
                                    if ssh_host is None:
                                        ssh_host = await self._cloud_interface.ssh_host_from_address(  # noqa: E501
                                            public_address, instance_name
                                        )
                                        address_to_ssh_host[public_address] = ssh_host
                                    for worker_job_id in worker_job_ids:
                                        log_file_name = (
                                            "/var/meadowrun/job_logs/"
                                            f"{job_fields['job_friendly_name']}."
                                            f"{worker_job_id}.log"
                                        )
                                        worker_tasks.append(
                                            WorkerTask(
                                                f"{public_address} {log_file_name}",
                                                worker_queue.queue_index,
                                                asyncio.create_task(
                                                    launch_worker_function(
                                                        ssh_host,
                                                        worker_job_id,
                                                        log_file_name,
                                                        instance_registrar,
                                                        worker_queue.queue_index,
                                                    )
                                                ),
                                            )
                                        )
                                        worker_queue.num_workers_launched += 1

                        # shutdown workers if they're no longer needed
                        # once we have the lost worker replacement logic this should
                        # just be -workers_to_launch
                        workers_to_shutdown = (
                            worker_queue.num_workers_launched
                            - worker_queue.num_worker_shutdown_messages_sent
                            - worker_queue.num_workers_exited_unexpectedly
                        ) - (worker_queue.num_workers_needed)
                        if workers_to_shutdown > 0:
                            await self._cloud_interface.shutdown_workers(
                                workers_to_shutdown, worker_queue.queue_index
                            )
                            worker_queue.num_worker_shutdown_messages_sent += (
                                workers_to_shutdown
                            )

                    # now we wait until either add_tasks_and_get_results tells us to
                    # shutdown some workers, or workers exit
                    await asyncio.wait(
                        itertools.chain(
                            cast(
                                Iterable[asyncio.Task],
                                (task.task for task in worker_tasks),
                            ),
                            (workers_needed_changed_wait_task,),
                        ),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    new_worker_tasks = []
                    for worker_task in worker_tasks:
                        if worker_task.task.done():
                            exception = worker_task.task.exception()
                            if exception is not None:
                                print(
                                    # TODO also include queue index here?
                                    f"Error running worker {worker_task.log_file_info}:"
                                    f"\n"
                                    + "".join(
                                        traceback.format_exception(
                                            type(exception),
                                            exception,
                                            exception.__traceback__,
                                        )
                                    )
                                )
                                self._worker_queues[
                                    worker_task.queue_index
                                ].num_workers_exited_unexpectedly += 1
                                # TODO ideally we would tell the receive_results loop to
                                # reschedule whatever task the worker was working on
                            #  TODO do something with worker_task.result()
                        else:
                            new_worker_tasks.append(worker_task)
                    worker_tasks = new_worker_tasks

                    if workers_needed_changed_wait_task.done():
                        self._num_workers_needed_changed.clear()
                        workers_needed_changed_wait_task = asyncio.create_task(
                            self._num_workers_needed_changed.wait()
                        )

                    # this means all workers are either done or shutting down
                    if all(
                        worker_queue.num_workers_launched
                        - worker_queue.num_worker_shutdown_messages_sent
                        - worker_queue.num_workers_exited_unexpectedly
                        <= 0
                        for worker_queue in self._worker_queues
                    ):
                        break

        except asyncio.CancelledError:
            # if we're being cancelled, then most likely the worker_tasks are being
            # cancelled as well (because someone pressed Ctrl+C), which means it's
            # unhelpful to cancel them again, we want them to finish running their
            # except/finally clauses
            async_cancel_exception = True
            raise
        finally:
            print("Shutting down workers")
            # setting this is very critical--otherwise, add_tasks_and_get_results will
            # hang forever, not knowing that it has no hope of workers working on any of
            # its tasks
            self._no_workers_available.set()

            if not async_cancel_exception:
                # cancel any outstanding workers. Even if there haven't been any
                # exception and we've sent worker shutdown messages for all the workers,
                # a worker might be in the middle of building/pulling an environment and
                # we don't want to wait for that to complete
                for worker_task in worker_tasks:
                    worker_task.task.cancel()

            # we still want the tasks to complete their except/finally blocks, after
            # which they should reraise asyncio.CancelledError, which we can safely
            # ignore
            await asyncio.gather(
                *(task.task for task in worker_tasks), return_exceptions=True
            )

            await asyncio.gather(
                *[
                    ssh_host.close_connection()
                    for ssh_host in address_to_ssh_host.values()
                ],
                return_exceptions=True,
            )
            workers_needed_changed_wait_task.cancel()
            if pickled_worker_function_task is not None:
                pickled_worker_function_task.cancel()

    async def add_tasks_and_get_results(
        self,
        args: Sequence[_T],
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult]:
        """
        Adds the specified tasks to the "queue", and retries tasks as needed. Yields
        TaskResult objects as soon as tasks complete.
        """

        # this keeps track of which queue each arg is assigned to. Initially they are
        # all assigned to the 0th queue, and if they fail because of suspected lack of
        # memory and retry_with_more_memory is set, then we will increase their queue
        # index. -1 indicates that the arg is done so it is no longer assigned to any
        # queue
        arg_to_queue_index = [0] * len(args)
        await self._cloud_interface.setup_and_add_tasks(args)

        # done = successful or exhausted retries
        num_tasks_done = 0
        # stop_receiving tells _cloud_interface.receive_task_results that there are no
        # more results to get
        stop_receiving = asyncio.Event()
        if len(args) == num_tasks_done:
            stop_receiving.set()
        last_printed_update = time.time()
        print(
            f"Waiting for task results. Requested: {len(args)}, "
            f"Done: {num_tasks_done}"
        )
        async for task_batch, worker_batch in await self._cloud_interface.receive_task_results(  # noqa: E501
            stop_receiving=stop_receiving, workers_done=self._no_workers_available
        ):
            # TODO right now we ignore worker_batch because we get worker failures
            # through the SSH connection. At some point, we may want to process worker
            # failures here.

            for task in task_batch:
                task_result = TaskResult.from_process_state(task)
                if task_result.is_success:
                    num_tasks_done += 1
                    arg_to_queue_index[task.task_id] = -1
                    yield task_result
                elif (
                    task.attempt < max_num_task_attempts
                    and task_result.state != "RESULT_CANNOT_BE_UNPICKLED"
                ):
                    prev_queue_index = arg_to_queue_index[task.task_id]
                    prev_memory_requirement = _memory_gb_for_queue_index(
                        prev_queue_index, self._resources_required_per_task
                    )
                    if (
                        retry_with_more_memory
                        and task.result.max_memory_used_gb
                        >= 0.95 * prev_memory_requirement
                    ):
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, "
                            "retrying with more memory (task used "
                            f"{task.result.max_memory_used_gb:.2f}/"
                            f"{prev_memory_requirement}GB requested)"
                        )

                        new_queue_index = arg_to_queue_index[task.task_id] + 1
                        arg_to_queue_index[task.task_id] = new_queue_index
                        if len(self._worker_queues) < new_queue_index + 1:
                            # TODO any new queue gets 1 worker by default. We should
                            # increase this later depending on how the situation changes
                            self._cloud_interface.create_queue()
                            self._worker_queues.append(WorkerQueue(new_queue_index, 1))
                        await self._cloud_interface.retry_task(
                            task.task_id, task.attempt, new_queue_index
                        )
                    else:
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, "
                            "retrying"
                        )
                        await self._cloud_interface.retry_task(
                            task.task_id, task.attempt, prev_queue_index
                        )
                else:
                    if (
                        task.attempt < max_num_task_attempts
                        and task_result.state == "RESULT_CANNOT_BE_UNPICKLED"
                    ):
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, max"
                            f" attempts is {max_num_task_attempts}, but not retrying "
                            "because the failure happened when trying to unpickle the "
                            "result on the client."
                        )
                    else:
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, max"
                            f" attempts is {max_num_task_attempts}, not retrying."
                        )
                    num_tasks_done += 1
                    arg_to_queue_index[task.task_id] = -1
                    yield task_result

            if num_tasks_done >= len(args):
                stop_receiving.set()
            else:
                t0 = time.time()
                if t0 - last_printed_update > _PRINT_RECEIVED_TASKS_SECONDS:
                    print(
                        f"Waiting for task results. Requested: {len(args)}, "
                        f"Done: {num_tasks_done}"
                    )
                    last_printed_update = t0

            # reduce the number of workers needed if we have more workers than
            # outstanding tasks
            num_workers_needed_per_queue = collections.Counter(arg_to_queue_index)
            for worker_queue in self._worker_queues:
                # num_workers_needed = min(outstanding tasks for this queue, current num
                # workers needed for this queue)
                # TODO at some point we might want to increase the number of workers
                # needed?
                num_workers_needed = min(
                    num_workers_needed_per_queue.get(worker_queue.queue_index, 0),
                    worker_queue.num_workers_needed,
                )
                if num_workers_needed < worker_queue.num_workers_needed:
                    worker_queue.num_workers_needed = num_workers_needed
                    self._num_workers_needed_changed.set()

        # We could be more finegrained about aborting launching workers. This is the
        # easiest to implement, but ideally every time num_workers_needed changes we
        # would consider cancelling launching new workers
        self._abort_launching_new_workers.set()

        if num_tasks_done < len(args):
            # It would make sense for this to raise an exception, but it's more helpful
            # to see the actual worker failures, and run_worker_functions should always
            # raise an exception in that case. The caller should still check though that
            # we returned all of the task results we were expecting.
            print(
                "Gave up retrieving task results, most likely due to worker failures. "
                f"Received {num_tasks_done}/{len(args)} task results."
            )
        else:
            print(f"Received all {len(args)} task results.")
