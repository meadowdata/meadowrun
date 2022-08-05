"""
This code belongs in run_job.py, but this is split out to avoid circular dependencies
"""
from __future__ import annotations

import abc
import asyncio
import dataclasses
import pickle
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import asyncssh
from typing_extensions import Literal

import meadowrun.ssh as ssh

if TYPE_CHECKING:
    from meadowrun.credentials import UsernamePassword
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import Job, ProcessState


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
            interruption probability less than 80% can be used. Use `0` to indicate that
            only on-demand instance are acceptable (i.e. do not use spot instances)
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


class Host(abc.ABC):
    """
    Host is an abstract class for specifying where to run a job. See implementations
    below.
    """

    @abc.abstractmethod
    async def run_job(
        self, resources_required: Optional[ResourcesInternal], job: Job
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
    ) -> Sequence[_U]:
        # Note for implementors: job_fields will be populated with everything other than
        # job_id and py_function, so the implementation should construct
        # Job(job_id=job_id, py_function=py_function, **job_fields)
        pass


@dataclasses.dataclass(frozen=True)
class SshHost(Host):
    """
    Tells run_function and related functions to connect to the remote machine over SSH.
    """

    address: str
    username: str
    private_key: asyncssh.SSHKey
    # If this field is populated, it will be a tuple of (cloud provider, region name).
    # Cloud provider will be e.g. "EC2" indicating that we're running on e.g. an EC2
    # instance allocated via instance_allocation.py, so we need to deallocate the job
    # via the right InstanceRegistrar when we're done. region name indicates where
    # the InstanceRegistrar that we used to allocate this job is.
    cloud_provider: Optional[Tuple[CloudProviderType, str]] = None

    async def run_job(
        self, resources_required: Optional[ResourcesInternal], job: Job
    ) -> JobCompletion[Any]:
        # try the connection 20 times.
        connection = await _retry(
            lambda: ssh.connect(
                self.address, username=self.username, private_key=self.private_key
            ),
            (TimeoutError, ConnectionRefusedError, OSError),
            max_num_attempts=20,
        )

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

            command = (
                "/usr/bin/env PYTHONUNBUFFERED=1 "
                "/var/meadowrun/env/bin/python "
                # "-X importtime "
                # "-m cProfile -o remote.prof "
                "-m meadowrun.run_job_local_main "
                f"--job-id {job.job_id} "
                f"--working-folder {remote_working_folder} "
            )
            if self.cloud_provider is not None:
                command += f" --cloud {self.cloud_provider[0]}"
                command += f" --cloud-region-name {self.cloud_provider[1]}"

            print(f"Running {command}")

            cmd_result = await ssh.run_and_print(connection, command, check=False)

            # TODO consider using result.tail, result.stdout

            # see if we got a normal return code
            if cmd_result.exit_status != 0:
                raise ValueError(f"Process exited {cmd_result.returncode}")

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
            if job_io_prefix:
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
                        f"Error cleaning up files on remote machine: "
                        f"{remote_paths} {e}"
                    )
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
    ) -> Sequence[_U]:
        raise NotImplementedError("run_map is not implemented for SshHost")


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


@dataclasses.dataclass(frozen=True)
class RunMapHelper:
    """See run_map. This allows run_map to use EC2 or Azure VMs"""

    region_name: str
    allocated_hosts: Dict[str, List[str]]
    # public_address, worker_id -> None
    worker_function: Callable[[str, int], None]
    ssh_username: str
    ssh_private_key: asyncssh.SSHKey
    results_future: Coroutine[Any, Any, List[Any]]


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
