"""
This code belongs in run_job.py, but this is split out to avoid circular dependencies
"""
from __future__ import annotations

import abc
import asyncio
import dataclasses
import io
import pickle
import threading
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import fabric
import paramiko.ssh_exception

from meadowrun.credentials import UsernamePassword
from meadowrun.meadowrun_pb2 import Job, ProcessState


_T = TypeVar("_T")


CloudProvider = "EC2", "AzureVM"
CloudProviderType = Literal["EC2", "AzureVM"]


async def _retry(
    function: Callable[[], _T],
    exception_types: Union[Exception, Tuple[Exception, ...]],
    max_num_attempts: int = 5,
    delay_seconds: float = 1,
) -> _T:
    i = 0
    while True:
        try:
            return function()
        except exception_types as e:  # type: ignore
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(f"Retrying on error: {e}")
                await asyncio.sleep(delay_seconds)


class Host(abc.ABC):
    @abc.abstractmethod
    async def run_job(self, job: Job) -> JobCompletion[Any]:
        pass


@dataclasses.dataclass(frozen=True)
class SshHost(Host):
    """
    Tells run_function and related functions to connect to the remote machine over SSH
    via the fabric library https://www.fabfile.org/ fabric_kwargs are passed directly to
    fabric.Connection().
    """

    address: str
    # these options are forwarded directly to Fabric
    fabric_kwargs: Optional[Dict[str, Any]] = None
    # If this field is populated, it will be a tuple of (cloud provider, region name).
    # Cloud provider will be e.g. "EC2" indicating that we're running on e.g. an EC2
    # instance allocated via instance_allocation.py, so we need to deallocate the job
    # via the right InstanceRegistrar when we're done. region name indicates where
    # the InstanceRegistrar that we used to allocate this job is.
    cloud_provider: Optional[Tuple[CloudProviderType, str]] = None

    async def run_job(self, job: Job) -> JobCompletion[Any]:
        with fabric.Connection(
            self.address, **(self.fabric_kwargs or {})
        ) as connection:
            job_io_prefix = ""

            try:
                # assumes that meadowrun is installed in /var/meadowrun/env as per
                # build_meadowrun_amis.md. Also uses the default working_folder, which
                # should (but doesn't strictly need to) correspond to
                # agent._set_up_working_folder

                # try the first command 20 times, as this is when we actually try to
                # connect to the remote machine.
                home_result = await _retry(
                    lambda: connection.run("echo $HOME", hide=True, in_stream=False),
                    (
                        cast(Exception, paramiko.ssh_exception.NoValidConnectionsError),
                        cast(Exception, TimeoutError),
                    ),
                    max_num_attempts=20,
                )
                if not home_result.ok:
                    raise ValueError(
                        "Error getting home directory on remote machine "
                        + home_result.stdout
                    )

                # in_stream is needed otherwise invoke listens to stdin, which pytest
                # doesn't like
                remote_working_folder = f"{home_result.stdout.strip()}/meadowrun"
                mkdir_result = connection.run(
                    f"mkdir -p {remote_working_folder}/io", in_stream=False
                )
                if not mkdir_result.ok:
                    raise ValueError(
                        "Error creating meadowrun directory " + mkdir_result.stdout
                    )

                job_io_prefix = f"{remote_working_folder}/io/{job.job_id}"

                # serialize job_to_run and send it to the remote machine
                with io.BytesIO(job.SerializeToString()) as job_to_run_serialized:
                    connection.put(
                        job_to_run_serialized, remote=f"{job_io_prefix}.job_to_run"
                    )

                # fabric doesn't have any async APIs, which means that in order to run
                # more than one fabric command at the same time, we need to have a
                # thread per fabric command. We use an asyncio.Future here to make the
                # API async, so from the user perspective, it feels like this function
                # is async

                # fabric is supposedly not threadsafe, but it seems to work as long as
                # more than one connection is not being opened at the same time:
                # https://github.com/fabric/fabric/pull/2010/files
                result_future: asyncio.Future = asyncio.Future()
                event_loop = asyncio.get_running_loop()

                command = (
                    f"/var/meadowrun/env/bin/meadowrun-local --job-id {job.job_id} "
                    f"--working-folder {remote_working_folder}"
                )
                if self.cloud_provider is not None:
                    command += f" --cloud {self.cloud_provider[0]}"
                    command += f" --cloud-region-name {self.cloud_provider[1]}"

                print(f"Running {command}")

                def run_and_wait() -> None:
                    try:
                        # use meadowrun to run the job
                        returned_result = connection.run(command, in_stream=False)
                        event_loop.call_soon_threadsafe(
                            lambda r=returned_result: result_future.set_result(r)
                        )
                    except Exception as e2:
                        event_loop.call_soon_threadsafe(
                            lambda e2=e2: result_future.set_exception(e2)
                        )

                threading.Thread(target=run_and_wait).start()

                result = await result_future

                # TODO consider using result.tail, result.stdout

                # see if we got a normal return code
                if result.return_code != 0:
                    raise ValueError(f"Process exited {result.return_code}")

                with io.BytesIO() as result_buffer:
                    connection.get(f"{job_io_prefix}.process_state", result_buffer)
                    result_buffer.seek(0)
                    process_state = ProcessState()
                    process_state.ParseFromString(result_buffer.read())

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
                if job_io_prefix:
                    remote_paths = " ".join(
                        [
                            f"{job_io_prefix}.job_to_run",
                            f"{job_io_prefix}.state",
                            f"{job_io_prefix}.result",
                            f"{job_io_prefix}.process_state",
                            f"{job_io_prefix}.initial_process_state",
                        ]
                    )
                    try:
                        # -f so that we don't throw an error on files that don't
                        # exist
                        connection.run(f"rm -f {remote_paths}", in_stream=False)
                    except Exception as e:
                        print(
                            f"Error cleaning up files on remote machine: "
                            f"{remote_paths} {e}"
                        )

                    # TODO also clean up log files?


@dataclasses.dataclass(frozen=True)
class AllocCloudInstancesInternal:
    """Identical to AllocCloudInstances but all values must be set"""

    logical_cpu_required_per_task: int
    memory_gb_required_per_task: float
    interruption_probability_threshold: float
    num_concurrent_tasks: int
    region_name: str


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
    fabric_kwargs: Dict[str, Any]
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
