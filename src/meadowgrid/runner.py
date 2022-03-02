from __future__ import annotations

import abc
import asyncio
import dataclasses
import io
import os.path
import pickle
import shlex
import threading
import uuid
from typing import Callable, TypeVar, Union, Any, Dict, Optional, Sequence, cast, Tuple

import fabric
import paramiko.ssh_exception

from meadowgrid import ServerAvailableFolder
from meadowgrid.agent import run_one_job
from meadowgrid.aws_integration import _get_default_region_name
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.coordinator_client import (
    _add_deployments_to_job,
    _create_py_function,
    _make_valid_job_id,
    _pickle_protocol_for_deployed_interpreter,
    _string_pairs_from_dict,
)
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridFunction,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.ec2_alloc import allocate_ec2_instances
from meadowgrid.grid import _get_id_name_function
from meadowgrid.meadowgrid_pb2 import (
    Job,
    JobToRun,
    ProcessState,
    PyCommandJob,
    ServerAvailableInterpreter,
)
from meadowgrid.resource_allocation import Resources

_T = TypeVar("_T")


async def _retry(
    function: Callable[[], _T],
    exception_types: Exception,
    max_num_attempts: int = 3,
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


@dataclasses.dataclass(frozen=True)
class Deployment:
    interpreter: Union[InterpreterDeployment, VersionedInterpreterDeployment]
    code: Union[CodeDeployment, VersionedCodeDeployment, None] = None
    environment_variables: Optional[Dict[str, str]] = None


def _add_defaults_to_deployment(
    deployment: Optional[Deployment],
) -> Tuple[
    Union[InterpreterDeployment, VersionedInterpreterDeployment],
    Union[CodeDeployment, VersionedCodeDeployment],
    Dict[str, str],
]:
    if deployment is None:
        return (
            ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
            ServerAvailableFolder(),
            {},
        )

    return (
        deployment.interpreter,
        deployment.code or ServerAvailableFolder(),
        deployment.environment_variables or {},
    )


class Host(abc.ABC):
    @abc.abstractmethod
    async def run_job(self, job_to_run: JobToRun) -> Any:
        pass


@dataclasses.dataclass(frozen=True)
class LocalHost(Host):
    async def run_job(self, job_to_run: JobToRun) -> Any:
        initial_update, continuation = await run_one_job(job_to_run)
        if (
            initial_update.process_state.state != ProcessState.ProcessStateEnum.RUNNING
            or continuation is None
        ):
            result = initial_update.process_state
        else:
            result = (await continuation).process_state

        if result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
            return pickle.loads(result.pickled_result)
        else:
            # TODO make better error messages
            raise ValueError(f"Error: {result.state}")


@dataclasses.dataclass(frozen=True)
class SshHost(Host):
    address: str
    # these options are forwarded directly to Fabric
    fabric_kwargs: Optional[Dict[str, Any]] = None

    async def run_job(self, job_to_run: JobToRun) -> Any:
        with fabric.Connection(
            self.address, **(self.fabric_kwargs or {})
        ) as connection:
            job_io_prefix = ""

            try:
                # assumes that meadowgrid is installed in /meadowgrid/env as per
                # build_meadowgrid_amis.md. Also uses the default working_folder, which
                # should (but doesn't strictly need to) correspond to
                # agent._set_up_working_folder

                # try the first command 3 times, as this is when we actually try to
                # connect to the remote machine.
                home_result = await _retry(
                    lambda: connection.run("echo $HOME"),
                    cast(Exception, paramiko.ssh_exception.NoValidConnectionsError),
                )
                if not home_result.ok:
                    raise ValueError(
                        "Error getting home directory on remote machine "
                        + home_result.stdout
                    )

                remote_working_folder = f"{home_result.stdout.strip()}/meadowgrid"
                mkdir_result = connection.run(f"mkdir -p {remote_working_folder}/io")
                if not mkdir_result.ok:
                    raise ValueError(
                        "Error creating meadowgrid directory " + mkdir_result.stdout
                    )

                job_io_prefix = f"{remote_working_folder}/io/{job_to_run.job.job_id}"

                # serialize job_to_run and send it to the remote machine
                with io.BytesIO(
                    job_to_run.SerializeToString()
                ) as job_to_run_serialized:
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

                def run_and_wait() -> None:
                    try:
                        # use meadowrun to run the job
                        returned_result = connection.run(
                            "/meadowgrid/env/bin/meadowrun "
                            f"--job-id {job_to_run.job.job_id} "
                            f"--working-folder {remote_working_folder} "
                            # TODO this flag should only be passed in if we were
                            # originally using an EC2AllocHost
                            f"--needs-deallocation"
                        )
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
                    job_spec_type = job_to_run.job.WhichOneof("job_spec")
                    # we must have a result from functions, in other cases we can
                    # optionally have a result
                    if job_spec_type == "py_function" or process_state.pickled_result:
                        return pickle.loads(process_state.pickled_result)
                    else:
                        return None
                else:
                    # TODO we should throw a better exception
                    raise ValueError(f"Running remotely failed: {process_state}")
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
                        connection.run(f"rm -f {remote_paths}")
                    except Exception as e:
                        print(
                            f"Error cleaning up files on remote machine: "
                            f"{remote_paths} {e}"
                        )

                    # TODO also clean up log file?s


@dataclasses.dataclass(frozen=True)
class EC2AllocHost(Host):
    """A placeholder for a host that will be allocated/created by ec2_alloc.py"""

    logical_cpu_required: int
    memory_gb_required: float
    interruption_probability_threshold: float
    region_name: Optional[str] = None
    private_key_filename: Optional[str] = None

    async def run_job(self, job_to_run: JobToRun) -> Any:
        hosts = await allocate_ec2_instances(
            Resources(self.memory_gb_required, self.logical_cpu_required, {}),
            1,
            self.interruption_probability_threshold,
            self.region_name or await _get_default_region_name(),
        )

        fabric_kwargs: Dict[str, Any] = {"user": "ubuntu"}
        if self.private_key_filename:
            fabric_kwargs["connect_kwargs"] = {
                "key_filename": self.private_key_filename
            }

        if len(hosts) != 1:
            raise ValueError(f"Asked for one host, but got back {len(hosts)}")
        for host, job_ids in hosts.items():
            if len(job_ids) != 1:
                raise ValueError(f"Asked for one job allocation but got {len(job_ids)}")

            # Kind of weird that we're changing the job_id here, but okay as long as
            # job_id remains mostly an internal concept
            job_to_run.job.job_id = job_ids[0]

            return await SshHost(host, fabric_kwargs).run_job(job_to_run)


async def run_function(
    function: Callable[..., _T],
    host: Host,
    deployment: Optional[Deployment] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    """
    Same as run_function_async, but runs on a remote machine, specified by "host".
    Connects to the remote machine over SSH via the fabric library
    https://www.fabfile.org/ fabric_kwargs are passed directly to fabric.Connection().

    The remote machine must have meadowgrid installed as per build_meadowgrid_amis.md
    """

    job_id, friendly_name, pickled_function = _get_id_name_function(function)

    interpreter, code, environment_variables = _add_defaults_to_deployment(deployment)

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(friendly_name),
        environment_variables=_string_pairs_from_dict(environment_variables),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_function=_create_py_function(
            MeadowGridFunction.from_pickled(pickled_function, args, kwargs),
            pickle_protocol,
        ),
    )
    _add_deployments_to_job(job, code, interpreter)

    # TODO figure out what to do about the [0], which is there for dropping effects
    return (await host.run_job(JobToRun(job=job)))[0]


async def run_command(
    args: Union[str, Sequence[str]],
    host: Host,
    deployment: Optional[Deployment] = None,
) -> None:
    """
    Runs the specified command on a remote machine. See run_function_remote for more
    details on requirements for the remote host.
    """

    job_id = str(uuid.uuid4())
    if isinstance(args, str):
        args = shlex.split(args)
    # this is kind of a silly way to get a friendly name--treat the first three
    # elements of args as if they're paths and take the last part of each path
    friendly_name = "-".join(os.path.basename(arg) for arg in args[:3])

    interpreter, code, environment_variables = _add_defaults_to_deployment(deployment)

    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(friendly_name),
        environment_variables=_string_pairs_from_dict(environment_variables),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_command=PyCommandJob(command_line=args),
    )
    _add_deployments_to_job(job, code, interpreter)

    await host.run_job(JobToRun(job=job))
