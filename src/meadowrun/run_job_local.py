from __future__ import annotations

import abc
import asyncio
import asyncio.subprocess
import dataclasses
import itertools
import os
import os.path
import pathlib
import pickle
import shutil
import struct
import sys
import traceback
from decimal import InvalidOperation
from pathlib import PurePath
from typing import (
    TYPE_CHECKING,
    Any,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import psutil

from meadowrun.aws_integration.ecr import get_ecr_username_password
from meadowrun.azure_integration.acr import get_acr_username_password
from meadowrun.config import (
    MEADOWRUN_AGENT_PID,
    MEADOWRUN_CODE_MOUNT_LINUX,
    MEADOWRUN_INTERPRETER,
    MEADOWRUN_IO_MOUNT_LINUX,
)
from meadowrun.credentials import (
    CredentialsDict,
    RawCredentials,
    get_docker_credentials,
    get_matching_credentials,
)
from meadowrun.deployment_manager import (
    compile_environment_spec_locally,
    compile_environment_spec_to_container,
    get_code_paths,
)
from meadowrun.docker_controller import (
    get_image_environment_variables_and_working_dir,
    get_registry_domain,
    pull_image,
    remove_container,
    run_container,
)
from meadowrun.meadowrun_pb2 import (
    Credentials,
    Job,
    ProcessState,
    PyAgentJob,
    PyFunctionJob,
    StringPair,
)
from meadowrun.shared import cancel_task, pickle_exception

if TYPE_CHECKING:
    from typing_extensions import Literal

    JobSpecType = Literal["py_command", "py_function", "py_agent"]

    from meadowrun._vendor import aiodocker
    from meadowrun._vendor.aiodocker.containers import DockerContainer
    from meadowrun.deployment_manager import StorageBucketFactoryType
    from meadowrun.run_job_core import CloudProviderType

ProcessStateEnum = ProcessState.ProcessStateEnum

_T = TypeVar("_T")
_U = TypeVar("_U")


_MEADOWRUN_CONTEXT_VARIABLES = "MEADOWRUN_CONTEXT_VARIABLES"
_MEADOWRUN_RESULT_FILE = "MEADOWRUN_RESULT_FILE"
_MEADOWRUN_RESULT_PICKLE_PROTOCOL = "MEADOWRUN_RESULT_PICKLE_PROTOCOL"
MACHINE_CACHE_FOLDER = "/var/meadowrun/machine_cache"


def _string_pairs_to_dict(pairs: Iterable[StringPair]) -> Dict[str, str]:
    """
    Opposite of _string_pairs_from_dict in coordinator_client.py. Helper for dicts in
    protobuf.
    """
    result = {}
    for pair in pairs:
        result[pair.key] = pair.value
    return result


@dataclasses.dataclass
class _JobSpecTransformed:
    """
    To be able to reuse some code, _prepare_py_command and _prepare_py_function compile
    their respective Job.job_specs into a command line that we can run, plus a bit of
    additional information on how to run the command line.
    """

    # the command line to run
    command_line: List[str]

    # This only gets used if we are running in a container. Specifies docker binds to
    # expose files on the host machine for input/output with the container.
    container_binds: List[Tuple[str, str]]

    environment_variables: Dict[str, str] = dataclasses.field(
        default_factory=lambda: {}
    )
    server: Optional[TaskWorkerServer] = None


def _io_file_container_binds(
    io_folder: str, io_files: Iterable[str]
) -> List[Tuple[str, str]]:
    """
    A little helper function. io_folder is a path on the host, io_files are file names
    in that folder, and this function returns binds for docker that map those files into
    the conventional mounts (for meadowrun) within the container.
    """
    return [
        (os.path.join(io_folder, io_file), f"{MEADOWRUN_IO_MOUNT_LINUX}/{io_file}")
        for io_file in io_files
    ]


def _prepare_py_command(
    job: Job, io_folder: str, is_container: bool
) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use, and returns the
    _JobSpecTransformed
    """

    environment = {}
    io_files = []

    # request the results file
    result_path = os.path.join(io_folder, job.job_id + ".result")
    if is_container:
        result_path_container = f"{MEADOWRUN_IO_MOUNT_LINUX}/{job.job_id}.result"
        # we create an empty file here so that we can expose it to the docker container,
        # docker does not let us bind non-existent files
        open(result_path, "w", encoding="utf-8").close()
    else:
        result_path_container = result_path
    io_files.append(job.job_id + ".result")
    environment[_MEADOWRUN_RESULT_FILE] = result_path_container
    environment[_MEADOWRUN_RESULT_PICKLE_PROTOCOL] = str(
        job.result_highest_pickle_protocol
    )

    # write context variables to file
    if job.py_command.pickled_context_variables:
        context_variables_path = os.path.join(
            io_folder, job.job_id + ".context_variables"
        )
        if is_container:
            context_variables_path_container = (
                f"{MEADOWRUN_IO_MOUNT_LINUX}/{job.job_id}.context_variables"
            )
        else:
            context_variables_path_container = context_variables_path
        with open(context_variables_path, "wb") as f:
            f.write(job.py_command.pickled_context_variables)
        io_files.append(job.job_id + ".context_variables")
        # we can't communicate "directly" with the arbitrary command that the
        # user is running so we'll use environment variables
        environment[_MEADOWRUN_CONTEXT_VARIABLES] = context_variables_path_container

    # get the command line
    if not job.py_command.command_line:
        raise ValueError("command_line must have at least one string")

    return _JobSpecTransformed(
        # we need a list, not a protobuf fake list
        list(job.py_command.command_line),
        _io_file_container_binds(io_folder, io_files),
        environment,
    )


_FUNC_WORKER_PATH = str(
    (
        pathlib.Path(__file__).parent / "func_worker" / "__meadowrun_func_worker.py"
    ).resolve()
)


def _prepare_function(
    job_id: str, function: Union[PyFunctionJob, PyAgentJob], io_folder: str
) -> Tuple[Sequence[str], Sequence[str]]:
    """
    Creates files in io_folder for the child process to use and returns (command line
    arguments, io_files). Compatible with what grid_worker and __meadowrun_func_worker
    expect.
    """
    function_spec = function.WhichOneof("function_spec")
    if function_spec == "qualified_function_name":
        return (
            [
                "--module-name",
                function.qualified_function_name.module_name,
                "--function-name",
                function.qualified_function_name.function_name,
            ],
            [],
        )
    elif function_spec == "pickled_function":
        if function.pickled_function is None:
            raise ValueError("argument cannot be None")
        pickled_function_path = os.path.join(io_folder, job_id + ".function")
        with open(pickled_function_path, "wb") as f:
            f.write(function.pickled_function)
        return ["--has-pickled-function"], [job_id + ".function"]
    else:
        raise ValueError(f"Unknown function_spec {function_spec}")


def _prepare_function_arguments(
    job_id: str, pickled_function_arguments: Optional[bytes], io_folder: str
) -> Tuple[Sequence[str], Sequence[str]]:
    """
    Creates files in io_folder for the child process to use and returns (command line
    arguments, io_files). Compatible with what grid_worker and __meadowrun_func_worker
    expect.
    """

    if pickled_function_arguments:
        pickled_arguments_path = os.path.join(io_folder, job_id + ".arguments")
        with open(pickled_arguments_path, "wb") as f:
            f.write(pickled_function_arguments)
        return ["--has-pickled-arguments"], [job_id + ".arguments"]
    else:
        return [], []


def _prepare_py_function(
    job: Job, io_folder: str, is_container: bool
) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use and returns
    _JobSpecTransformed. We use __meadowrun_func_worker to start the function in the
    child process.
    """

    io_files = [
        # these line up with __meadowrun_func_worker
        os.path.join(job.job_id + ".state"),
        os.path.join(job.job_id + ".result"),
    ]

    if not is_container:
        func_worker_path = _FUNC_WORKER_PATH
        io_path_container = os.path.join(io_folder, job.job_id)
    else:
        func_worker_path = (
            f"{MEADOWRUN_CODE_MOUNT_LINUX}{os.path.basename(_FUNC_WORKER_PATH)}"
        )
        io_path_container = f"{MEADOWRUN_IO_MOUNT_LINUX}/{job.job_id}"
        for io_file in io_files:
            open(os.path.join(io_folder, io_file), "w", encoding="utf-8").close()

    command_line = [
        "python",
        func_worker_path,
        "--result-highest-pickle-protocol",
        str(job.result_highest_pickle_protocol),
        "--io-path",
        io_path_container,
    ]

    command_line_for_function, io_files_for_function = _prepare_function(
        job.job_id, job.py_function, io_folder
    )

    command_line_for_arguments, io_files_for_arguments = _prepare_function_arguments(
        job.job_id, job.py_function.pickled_function_arguments, io_folder
    )

    return _JobSpecTransformed(
        list(
            itertools.chain(
                command_line, command_line_for_function, command_line_for_arguments
            )
        ),
        _io_file_container_binds(
            io_folder,
            itertools.chain(io_files, io_files_for_function, io_files_for_arguments),
        )
        + [(_FUNC_WORKER_PATH, func_worker_path)],
    )


_TASK_WORKER_PATH = str(
    (
        pathlib.Path(__file__).parent / "func_worker" / "__meadowrun_task_worker.py"
    ).resolve()
)


async def _prepare_py_agent(
    job: Job, io_folder: str, is_container: bool
) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use and returns
    _JobSpecTransformed. We use __meadowrun_task_worker to start the function in the
    child process.
    """

    io_files = [
        # these line up with __meadowrun_task_worker
        os.path.join(job.job_id + ".state"),
        os.path.join(job.job_id + ".result"),
    ]

    if not is_container:
        worker_path = _TASK_WORKER_PATH
        io_path_container = os.path.join(io_folder, job.job_id)
    else:
        worker_path = (
            f"{MEADOWRUN_CODE_MOUNT_LINUX}{os.path.basename(_TASK_WORKER_PATH)}"
        )
        io_path_container = f"{MEADOWRUN_IO_MOUNT_LINUX}/{job.job_id}"
        for io_file in io_files:
            open(os.path.join(io_folder, io_file), "w", encoding="utf-8").close()

    server = await TaskWorkerServer.start_serving(
        "0.0.0.0" if is_container else "127.0.0.1"
    )
    command_line = [
        "python",
        worker_path,
        "--result-highest-pickle-protocol",
        str(job.result_highest_pickle_protocol),
        "--io-path",
        io_path_container,
        "--host",
        "host.docker.internal" if is_container else "127.0.0.1",
        "--port",
        str(server.port),
    ]

    command_line_for_function, io_files_for_function = _prepare_function(
        job.job_id, job.py_agent, io_folder
    )

    return _JobSpecTransformed(
        list(itertools.chain(command_line, command_line_for_function)),
        _io_file_container_binds(
            io_folder,
            itertools.chain(io_files, io_files_for_function),
        )
        + [(_TASK_WORKER_PATH, worker_path)],
        server=server,
    )


class TaskWorkerServer:
    """This class opens a port in the agent process for the task worker to connect to.
    It allows waiting for connections from task workers, and sending and receiving
    messages.
    """

    def __init__(self) -> None:
        self.server: Optional[asyncio.AbstractServer] = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.have_connection: asyncio.Event = asyncio.Event()
        self.port: Optional[int] = None

    @staticmethod
    async def start_serving(
        host: str,
    ) -> TaskWorkerServer:
        """Create a TaskWorkerServer, open a free port, and return the server."""
        server = TaskWorkerServer()
        await server._start_serving(host)
        return server

    async def _start_serving(
        self,
        host: str,
    ) -> None:
        server = await asyncio.start_server(self._handle_connection, host, 0)
        (socket,) = server.sockets
        self.port = socket.getsockname()[1]

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.have_connection.set()

    async def wait_for_task_worker_connection(
        self, timeout: float = 30
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Wait until a task worker process connects, with an optional timeout."""
        await asyncio.wait_for(self.have_connection.wait(), timeout=timeout)
        assert self.reader is not None
        assert self.writer is not None
        return self.reader, self.writer

    async def send_message(self, bs: bytes) -> None:
        """Send the given bytes as a message to the task worker. If no task worker is
        connected yet, waits for a connection."""
        if not self.have_connection.is_set():
            await self.wait_for_task_worker_connection()
        assert self.writer is not None
        msg_len = struct.pack(">i", len(bs))
        self.writer.write(msg_len)
        self.writer.write(bs)
        await self.writer.drain()

    async def receive_message(self) -> Any:
        """Receives a message from the task worker. If no task worker is
        connected yet, waits for a connection. The message is unpickled."""
        if not self.have_connection.is_set():
            await self.wait_for_task_worker_connection()
        assert self.reader is not None
        (result_len,) = struct.unpack(">i", await self.reader.read(4))
        result_bs = bytearray()
        while len(result_bs) < result_len:
            result_bs.extend(await self.reader.read(result_len - len(result_bs)))
        return pickle.loads(result_bs)

    async def close_task_worker_connection(self) -> None:
        """Cleanly close the connection to the task worker."""
        if self.writer is not None:
            self.writer.close()
            await self.writer.wait_closed()
            self.have_connection.clear()

    async def close(self) -> None:
        """Cleanly close the connection to the task worker, and stop serving."""
        await self.close_task_worker_connection()
        if self.server:
            self.server.close()
            await self.server.wait_closed()


@dataclasses.dataclass(frozen=True)
class Stats:
    # TODO add cpu usage
    memory_in_use_gb: float


@dataclasses.dataclass
class StatsAccumulator:
    max_memory_used_gb: float = 0

    def accumulate(self, stats: Stats) -> None:
        if stats.memory_in_use_gb > self.max_memory_used_gb:
            self.max_memory_used_gb = stats.memory_in_use_gb

    def snapshot(self) -> Stats:
        return Stats(memory_in_use_gb=self.max_memory_used_gb)


class WorkerMonitor(abc.ABC):
    """Monitors a task worker."""

    def __init__(self) -> None:
        super().__init__()
        self._stats_accumulator = StatsAccumulator()
        self._stats_task: Optional[asyncio.Task[None]] = None

    @abc.abstractmethod
    async def wait_until_exited(self) -> int:
        ...

    @abc.abstractmethod
    async def try_get_return_code(self) -> Optional[int]:
        """
        Returns the exit code of the process if it has already exited, otherwise returns
        None
        """
        ...

    @abc.abstractmethod
    async def restart(self) -> None:
        """Restart the task worker."""
        ...

    @abc.abstractmethod
    async def get_stats(self) -> Stats:
        """Get some stats (memory, cpu...) about the task worker."""
        ...

    async def _start_stats(self) -> None:
        """Subclasses should start this as a task to accumulate statistics,
        and cancel the task when done."""
        self._stats_accumulator = StatsAccumulator()
        while True:
            try:
                stats = await self.get_stats()
                self._stats_accumulator.accumulate(stats)
            except asyncio.CancelledError:
                raise
            except Exception:
                print("Error updating stats:\n" + traceback.format_exc())

            await asyncio.sleep(1)

    def start_stats(self) -> None:
        self._stats_task = asyncio.create_task(self._start_stats())

    async def stop_stats(self) -> StatsAccumulator:
        if self._stats_task is not None:
            await cancel_task(self._stats_task)
        return self._stats_accumulator


class WorkerProcessMonitor(WorkerMonitor):
    """Monitors a task worker that runs as a process."""

    def __init__(
        self,
        command_line: Iterable[str],
        working_directory: Optional[str],
        env_vars: Dict[str, str],
    ):
        super().__init__()
        self.command_line = tuple(command_line)
        self.working_directory = working_directory
        self.env_vars = env_vars
        self._process: Optional[asyncio.subprocess.Process] = None
        self._tail_task: Optional[asyncio.Task[None]] = None

    @property
    def process(self) -> asyncio.subprocess.Process:
        if self._process is None:
            raise InvalidOperation(
                "Worker process not started. Call start_and_tail first."
            )
        return self._process

    @property
    def pid(self) -> int:
        return self.process.pid

    async def start_and_tail(self) -> None:
        self._process = await asyncio.subprocess.create_subprocess_exec(
            *self.command_line,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=self.working_directory,
            env=self.env_vars,
        )
        self._tail_task = asyncio.create_task(self._tail_log())

    async def stop(self) -> None:
        """Safely stop the process."""
        await self.stop_stats()
        try:
            if self._process is not None:
                self._process.terminate()
                self._process.kill()
        except ProcessLookupError:
            # if it's already gone, we're done here
            pass

    async def wait_until_exited(self) -> int:
        returncode = await self.process.wait()
        if self._tail_task is not None:
            await self._tail_task
        self._tail_task = None
        return returncode

    async def try_get_return_code(self) -> Optional[int]:
        # give the child process 0.5 second to exit, as it's helpful if we can get the
        # return code
        try:
            await asyncio.wait_for(self.wait_until_exited(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        return self.process.returncode

    async def cleanup(self) -> None:
        if self._process is not None:
            await self.stop()
            await self.wait_until_exited()
        if self._tail_task is not None:
            self._tail_task.cancel()
            try:
                await self._tail_task
            except asyncio.CancelledError:
                pass
        self._tail_task = None

    async def _tail_log(self) -> None:
        process = self.process
        assert process.stdout is not None
        async for line in process.stdout:
            sys.stdout.buffer.write(line)

    async def restart(self) -> None:
        await self.stop()
        await self.wait_until_exited()
        await self.start_and_tail()

    async def get_stats(self) -> Stats:
        # TODO cache the psutil process
        process = psutil.Process(self.process.pid)
        # RSS is in bytes: https://man7.org/linux/man-pages/man1/ps.1.html
        return Stats(process.memory_info().rss / (1024**3))


class WorkerContainerMonitor(WorkerMonitor):
    """Monitors a task worker that runs as a container."""

    def __init__(
        self,
        client: Optional[aiodocker.Docker],
        image: str,
        cmd: Optional[List[str]],
        environment_variables: Dict[str, str],
        working_directory: Optional[str],
        binds: List[Tuple[str, str]],
        ports: Iterable[str],
        extra_hosts: List[Tuple[str, str]],
        uses_gpus: bool,
    ):
        super().__init__()
        self.docker_client = client
        self.image = image
        self.command_line = cmd
        self.env_vars = environment_variables
        self.working_directory = working_directory
        self.binds = binds
        self.ports = list(ports)
        self.extra_hosts = extra_hosts
        self.uses_gpus = uses_gpus
        self._container: Optional[DockerContainer] = None
        self._tail_task: Optional[asyncio.Task[None]] = None

    @property
    def container(self) -> DockerContainer:
        if self._container is None:
            raise InvalidOperation(
                "Container not started yet. Call start_and_tail first."
            )
        return self._container

    @property
    def container_id(self) -> str:
        return self.container.id

    async def start_and_tail(self) -> None:
        self._container, self.docker_client = await run_container(
            self.docker_client,
            self.image,
            self.command_line,
            self.env_vars,
            self.working_directory,
            self.binds,
            self.ports,
            self.extra_hosts,
            self.uses_gpus,
        )
        self._tail_task = asyncio.create_task(self._tail_log())

    async def stop(self) -> None:
        if self._container is not None:
            await self.container.stop()

    async def wait_until_exited(self) -> int:
        wait_result = await self.container.wait()
        if self._tail_task is not None:
            await self._tail_task
        self._tail_task = None
        # as per https://docs.docker.com/engine/api/v1.41/#operation/ContainerWait we
        # can get the return code from the result
        return wait_result["StatusCode"]

    async def try_get_return_code(self) -> Optional[int]:
        # give the container 0.5 second to exit, as it's helpful if we can get the
        # return code
        try:
            await self.container.wait(timeout=0.5)
        except asyncio.TimeoutError:
            pass
        container_status = (await self.container.show()).get("State", {})
        if container_status.get("Status", "") == "running":
            # container_status will have ExitCode == 0 while the process is running
            return None
        else:
            return container_status.get("ExitCode", None)

    async def _tail_log(self) -> None:
        # 1. Docker appears to have an objection to having a log driver that can
        # produce plain text files (https://github.com/moby/moby/issues/17020) so we
        # implement that in a hacky way here.
        # TODO figure out overall strategy for logging, maybe eventually implement our
        # own plain text/whatever log driver for docker.
        async for line in self.container.log(stdout=True, stderr=True, follow=True):
            print(f"Task worker: {line}", end="")

    async def restart(self) -> None:
        await self.stop()
        await self.wait_until_exited()
        await self.start_and_tail()

    async def get_stats(self) -> Stats:
        stats = await self.container.stats(stream=False)
        if not stats:
            return Stats(0)
        # according to
        # https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerStats
        # "memory used" in the docker CLI corresponds to this formula:
        return Stats(
            (
                stats[0]["memory_stats"]["usage"]
                - stats[0]["memory_stats"]["stats"]["cache"]
            )
            / (1024**3)
        )


async def restart_worker(
    server: TaskWorkerServer, worker_monitor: WorkerMonitor
) -> None:
    await server.close_task_worker_connection()
    await worker_monitor.restart()
    await server.wait_for_task_worker_connection()


async def _launch_non_container_job(
    job_spec_type: JobSpecType,
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
    cwd_path: Optional[str],
    log_file_name: str,
    job: Job,
    io_folder: str,
) -> Tuple[int, Coroutine[Any, Any, ProcessState]]:
    """
    Contains logic specific to launching jobs that run using
    server_available_interpreter. Only separated from _launch_job for readability.
    Assumes that job.server_available_interpreter is populated.

    Returns (pid, continuation), see _launch_job for how to use the continuation.
    """

    # Note that we don't use command_line.io_files here, we only need those for
    # container jobs

    # (1) get the interpreter path:

    interpreter_path = job.server_available_interpreter.interpreter_path
    if interpreter_path == MEADOWRUN_INTERPRETER:
        # replace placeholder
        interpreter_path = sys.executable

    # (2) construct the environment variables dictionary

    env_vars = os.environ.copy()

    # we intentionally overwrite any existing PYTHONPATH--if for some reason we need the
    # current server process' code for the child process, the user needs to include it
    # directly
    env_vars["PYTHONPATH"] = os.pathsep.join(code_paths)

    # On Windows, we believe that interpreter_path can be one of two formats,
    # python_or_venv_dir/python or python_or_venv_dir/Scripts/python. We need to add the
    # scripts directory to the path so that we can run executables as if we're "in the
    # python environment". On Linux, there is no "Scripts" directory and everything is
    # always available in the same directory as the python executable.
    interpreter_path = pathlib.Path(interpreter_path)
    if interpreter_path.parent.name == "Scripts" or os.name != "nt":
        scripts_dir = str(interpreter_path.parent.resolve())
    else:
        scripts_dir = str((interpreter_path.parent / "Scripts").resolve())
    env_vars["PATH"] = scripts_dir + os.pathsep + env_vars["PATH"]

    # Next, merge in env_vars_to_add, computed in the caller. These should take
    # precedence.
    # TODO consider warning if we're overwriting PYTHONPATH or PATH here
    env_vars.update(**job_spec_transformed.environment_variables)

    # (3) get the working directory and fix the command line

    paths_to_search = env_vars["PATH"]
    if cwd_path:
        working_directory = cwd_path
        paths_to_search = f"{working_directory}{os.pathsep}{paths_to_search}"
    else:
        # TODO probably cleanest to allocate a new working directory for each job
        #  instead of just using the default
        working_directory = None

    # Popen uses cwd and env to search for the specified command on Linux but not on
    # Windows according to the docs:
    # https://docs.python.org/3/library/subprocess.html#subprocess.Popen We can use
    # shutil to make the behavior reasonable on both platforms
    new_first_command_line = shutil.which(
        job_spec_transformed.command_line[0], path=paths_to_search
    )
    if new_first_command_line:
        # noinspection PyTypeChecker
        job_spec_transformed.command_line = [
            new_first_command_line
        ] + job_spec_transformed.command_line[1:]

    # (4) run the process

    print(
        f"Running process ({job_spec_type}): "
        f"{' '.join(job_spec_transformed.command_line)}; cwd={working_directory}; "
        f"PYTHONPATH={env_vars['PYTHONPATH']}; log_file_name={log_file_name}; "
        f"code paths={','.join(code_paths)}"
    )

    worker = WorkerProcessMonitor(
        job_spec_transformed.command_line, working_directory, env_vars
    )
    await worker.start_and_tail()

    # (5) return the pid and continuation
    return worker.pid, _non_container_job_continuation(
        worker,
        job_spec_type,
        job_spec_transformed,
        job,
        io_folder,
        log_file_name,
    )


async def _non_container_job_continuation(
    worker: WorkerProcessMonitor,
    job_spec_type: JobSpecType,
    job_spec_transformed: _JobSpecTransformed,
    job: Job,
    io_folder: str,
    log_file_name: str,
) -> ProcessState:
    """
    Takes an asyncio.subprocess.Process, waits for it to finish, gets results from
    io_folder from the child process if necessary, and then returns an appropriate
    ProcessState indicating how the child process completed.
    """

    try:
        if job_spec_type == "py_agent":
            await _run_agent(job_spec_transformed, job, worker)
            await worker.stop()
            await worker.wait_until_exited()
            return ProcessState(
                state=ProcessStateEnum.SUCCEEDED,
                pid=worker.pid or 0,
                log_file_name=log_file_name,
                return_code=0,
            )

        returncode = await worker.wait_until_exited()
        return _completed_job_state(
            job_spec_type,
            job.job_id,
            io_folder,
            log_file_name,
            returncode,
            worker.pid,
            None,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(
            "There was an exception while trying to get the final ProcessState:\n"
            + traceback.format_exc()
        )
        return ProcessState(
            state=ProcessStateEnum.ERROR_GETTING_STATE,
            pickled_result=pickle_exception(e, job.result_highest_pickle_protocol),
        )
    finally:
        # TODO this is not 100% bulletproof--there will be a tiny sliver of time between
        # when we create the process and we enter this try/finally block
        try:
            await worker.stop()
        except BaseException:
            print("Exception trying to kill process: " + traceback.format_exc())


async def _launch_container_job(
    job_spec_type: JobSpecType,
    container_image_name: str,
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
    cwd_path: Optional[str],
    log_file_name: str,
    job: Job,
    io_folder: str,
    sidecar_container_images: List[str],
) -> Tuple[str, Coroutine[Any, Any, ProcessState]]:
    """
    Contains logic specific to launching jobs that run in a container. Only separated
    from _launch_job for readability.

    job_spec_transformed.environment_variables will take precedence over any environment
    variables specified in the container.

    Assumes that the container image has been pulled to this machine already.

    Returns (container_id, continuation), see _launch_job for how to use the
    continuation.
    """

    binds: List[Tuple[str, str]] = [(MACHINE_CACHE_FOLDER, MACHINE_CACHE_FOLDER)]

    if cwd_path is not None:
        unique_code_paths = list(dict.fromkeys(itertools.chain([cwd_path], code_paths)))
    else:
        unique_code_paths = list(code_paths)

    # populate binds with code paths we need
    # TODO this isn't exactly right--some code paths "overlap", like foo/a and foo/a/b.
    # We should just bind foo/a once as e.g. /meadowrun/code0, and then add both
    # /meadowrun/code0 and /meadowrun/code0/b to the PYTHONPATH. Instead, right now, we
    # mount /foo/a as /meadowrun/code0 and /foo/a/b as /meadowrun/code1, which might
    # lead to unexpected behavior.
    for i, path_on_host in enumerate(unique_code_paths):
        mounted_code_path = f"{MEADOWRUN_CODE_MOUNT_LINUX}{i}"
        binds.append((path_on_host, mounted_code_path))

    # get the image's environment variables and working directory
    (
        image_environment_variables,
        working_dir,
    ) = await get_image_environment_variables_and_working_dir(container_image_name)

    # If the container image has a meaningful working directory set, then we want to
    # leave that as it is. But if the working directory is "/tmp/", we use that as a
    # placeholder to mean that we don't care about the container image's working
    # directory. In that case, we set it to cwd_path, if given.
    if (
        PurePath(working_dir)
        in (PurePath("/tmp"), PurePath("/tmp/__meadowrun_marker__"))
        and cwd_path is not None
    ):
        (working_dir,) = tuple(
            mounted_path for (host_path, mounted_path) in binds if host_path == cwd_path
        )

    # If we've exposed any code paths, add them to PYTHONPATH. The normal behavior for
    # environment variables is that if they're specified in job_spec_transformed, those
    # override (rather than append to) what's defined in the container. If they aren't
    # specified in job_spec_transformed, we use whatever is specified in the container
    # image. We need to replicate that logic here so that we just add mounted_code_paths
    # to whatever PYTHONPATH would have "normally" become.
    if len(binds) > 1:
        existing_python_path = []
        if "PYTHONPATH" in job_spec_transformed.environment_variables:
            existing_python_path = [
                job_spec_transformed.environment_variables["PYTHONPATH"]
            ]
        else:
            if image_environment_variables:
                for image_env_var in image_environment_variables:
                    if image_env_var.startswith("PYTHONPATH="):
                        existing_python_path = [image_env_var[len("PYTHONPATH=") :]]
                        # just take the first PYTHONPATH we see, not worth worrying
                        # about pathological case where there are multiple PYTHONPATH
                        # environment variables
                        break

        code_paths = tuple(
            mounted_path
            for (host_path, mounted_path) in binds
            if host_path in code_paths
        )
        job_spec_transformed.environment_variables["PYTHONPATH"] = os.pathsep.join(
            itertools.chain(code_paths, existing_python_path)
        )

    # now, expose any files we need for communication with the container
    binds.extend(job_spec_transformed.container_binds)

    # now run any sidecar_containers that were specified
    docker_client = None

    sidecar_container_ips = []
    sidecar_containers = []
    for sidecar_container_image in sidecar_container_images:
        sidecar_container, docker_client = await run_container(
            docker_client,
            sidecar_container_image,
            None,
            {},
            None,
            [],
            [],
            [],
            job.uses_gpu,
        )
        sidecar_containers.append(sidecar_container)
        sidecar_container_ips.append(
            (await sidecar_container.show())["NetworkSettings"]["IPAddress"]
        )

    # finally, run the container
    print(
        f"Running container ({job_spec_type}): "
        f"{' '.join(job_spec_transformed.command_line)}; "
        f"container image={container_image_name}; "
        f"PYTHONPATH={job_spec_transformed.environment_variables.get('PYTHONPATH')} "
        f"log_file_name={log_file_name}; code paths={','.join(code_paths)} "
        f"ports={','.join(port for port in job.ports)}"
    )

    worker = WorkerContainerMonitor(
        docker_client,
        container_image_name,
        # json serializer needs a real list, not a protobuf fake list
        job_spec_transformed.command_line,
        job_spec_transformed.environment_variables,
        working_dir,
        binds,
        list(job.ports),
        [(f"sidecar-container-{i}", ip) for i, ip in enumerate(sidecar_container_ips)],
        job.uses_gpu,
    )
    await worker.start_and_tail()

    return worker.container_id, _container_job_continuation(
        worker,
        job_spec_type,
        job_spec_transformed,
        job,
        io_folder,
        log_file_name,
        sidecar_containers,
    )


async def _container_job_continuation(
    worker: WorkerContainerMonitor,
    job_spec_type: JobSpecType,
    job_spec_transformed: _JobSpecTransformed,
    job: Job,
    io_folder: str,
    log_file_name: str,
    sidecar_containers: List[DockerContainer],
) -> ProcessState:
    """
    Writes the container's logs to log_file_name, waits for the container to finish, and
    then returns a ProcessState indicating the state of this container when it
    finished.

    docker_client just needs to be closed when the container process has completed.
    """
    try:
        if job_spec_type == "py_agent":
            await _run_agent(job_spec_transformed, job, worker)
            await worker.stop()
            await worker.wait_until_exited()
            return ProcessState(
                state=ProcessStateEnum.SUCCEEDED,
                container_id=worker.container_id or "",
                log_file_name=log_file_name,
                return_code=0,
            )

        return_code = await worker.wait_until_exited()
        return _completed_job_state(
            job_spec_type,
            job.job_id,
            io_folder,
            log_file_name,
            return_code,
            None,
            worker.container_id,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(
            "There was an exception while trying to get the final ProcessState:\n"
            + traceback.format_exc()
        )
        return ProcessState(
            state=ProcessStateEnum.ERROR_GETTING_STATE,
            pickled_result=pickle_exception(e, job.result_highest_pickle_protocol),
        )
    finally:
        # TODO this is not 100% bulletproof--there will be a tiny sliver of time between
        # when we create the container and we enter this try/finally block
        try:
            await asyncio.gather(
                remove_container(worker.container),
                *(remove_container(c) for c in sidecar_containers),
            )
        except BaseException:
            print("Warning, unable to remove container: " + traceback.format_exc())
        assert worker.docker_client is not None
        await worker.docker_client.__aexit__(None, None, None)


async def _run_agent(
    job_spec_transformed: _JobSpecTransformed, job: Job, worker: WorkerMonitor
) -> None:
    # run the agent function. The agent function connects to another
    # process, the task worker, which runs the actual user function.
    assert job_spec_transformed.server is not None
    agent_func = pickle.loads(job.py_agent.pickled_agent_function)
    agent_func_args, agent_func_kwargs = pickle.loads(
        job.py_agent.pickled_agent_function_arguments
    )
    await agent_func(
        *agent_func_args,
        worker_server=job_spec_transformed.server,
        worker_monitor=worker,
        **agent_func_kwargs,
    )
    await job_spec_transformed.server.close()


def _completed_job_state(
    job_spec_type: JobSpecType,
    job_id: str,
    io_folder: str,
    log_file_name: str,
    return_code: int,
    pid: Optional[int],
    container_id: Optional[str],
) -> ProcessState:
    """
    This creates an appropriate ProcessState for a job that has completed (regardless of
    whether it ran in a process or container).
    """

    # see if we got a normal return code
    if return_code != 0:
        return ProcessState(
            state=ProcessStateEnum.NON_ZERO_RETURN_CODE,
            # TODO some other number? we should have either pid or container_id.
            pid=pid or 0,
            container_id=container_id or "",
            log_file_name=log_file_name,
            return_code=return_code,
        )

    # if we returned normally

    # for py_funcs or py_agents, get the state
    if job_spec_type in ["py_function", "py_agent"]:
        state_file = os.path.join(io_folder, job_id + ".state")
        with open(state_file, "r", encoding="utf-8") as state_file_text_reader:
            state_string = state_file_text_reader.read()
        if state_string == "SUCCEEDED":
            state = ProcessStateEnum.SUCCEEDED
        elif state_string == "PYTHON_EXCEPTION":
            state = ProcessStateEnum.PYTHON_EXCEPTION
        else:
            raise ValueError(f"Unknown state string: {state_string}")
    elif job_spec_type == "py_command":
        state = ProcessStateEnum.SUCCEEDED
    else:
        raise ValueError(f"job_spec was not recognized {job_spec_type}")

    # Next get the result. The result file is optional for py_commands because we don't
    # have full control over the process and there's no way to guarantee that "our code"
    # gets executed
    result_file = os.path.join(io_folder, job_id + ".result")
    if job_spec_type != "py_command" or os.path.exists(result_file):
        with open(result_file, "rb") as result_file_reader:
            result = result_file_reader.read()
    else:
        result = b""

    # TODO clean up files in io_folder for this process

    # return the ProcessState
    return ProcessState(
        state=state,
        pid=pid or 0,
        container_id=container_id or "",
        log_file_name=log_file_name,
        return_code=0,
        pickled_result=result,
    )


def _set_up_working_folder() -> Tuple[str, str, str, str, str]:
    """
    Returns conventional io, job_logs, git_repos, local_copies, and misc path names.
    It's assumed that these paths already exist.
    """

    working_folder = "/var/meadowrun"

    # this holds files for transferring data to and from this server process and the
    # child processes
    io_folder = os.path.join(working_folder, "io")
    # holds the logs for the functions/commands that this server runs
    job_logs_folder = os.path.join(working_folder, "job_logs")
    # see CodeDeploymentManager
    git_repos_folder = os.path.join(working_folder, "git_repos")
    # see CodeDeploymentManager
    local_copies_folder = os.path.join(working_folder, "local_copies")
    # misc folder for e.g. storing environment export files sent from local machine
    misc_folder = os.path.join(working_folder, "misc")

    return (
        io_folder,
        job_logs_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    )


def _get_credentials_sources(job: Job) -> CredentialsDict:
    credentials_sources: CredentialsDict = {}
    for credentials_source in job.credentials_sources:
        source = credentials_source.WhichOneof("source")
        if source is None:
            raise ValueError(
                "CredentialsSourceMessage should have a source set: "
                f"{credentials_source}"
            )
        credentials_sources.setdefault(credentials_source.service, []).append(
            (credentials_source.service_url, getattr(credentials_source, source))
        )

    return credentials_sources


async def _get_credentials_for_docker(
    repository: str,
    credentials_sources: CredentialsDict,
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> Optional[RawCredentials]:
    """
    Tries to get the credentials for a docker repository from credentials_sources. If
    that doesn't succeed, try to get a username/password for ECR/ACR based on the
    current role.
    """
    result = None
    try:
        result = await get_docker_credentials(repository, credentials_sources)
    except asyncio.CancelledError:
        raise
    except Exception:
        print("Error trying to turn credentials source into actual credentials")
        traceback.print_exc()

    if result is None and cloud is not None:
        try:
            registry_domain, _ = get_registry_domain(repository)
            if cloud[0] == "EC2":
                result = get_ecr_username_password(registry_domain)
            elif cloud[0] == "AzureVM":
                result = await get_acr_username_password(registry_domain)
            else:
                raise ValueError("Unexpected value for CloudProviderType: {cloud[0]}")
        except asyncio.CancelledError:
            raise
        except Exception:
            print(f"Error trying to get cloud credentials for {repository}")
            traceback.print_exc()

    return result


async def _get_credentials_for_job(
    job: Job,
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> Tuple[
    Optional[RawCredentials], Optional[RawCredentials], List[Optional[RawCredentials]]
]:
    """
    Returns (credentials for job.code_deployment, credentials for
    job.interpreter_deployment, credentials for job.sidecar_containers). Credentials for
    sidecar_containers will be a list of the same length as sidecar_containers, and
    should be used like zip(job.sidecar_containers, returned_value).

    TODO: This code seems a little convoluted because on the client side, the user
    specifies which credentials are for which deployment/sidecar container. We should
    change this code to reflect the user's specifications, but the code we have here is
    also useful if we want to support registering "global" credentials that are stored
    outside of the context of a single job.
    """

    # first, get all available credentials sources from the JobToRun
    credentials_sources = _get_credentials_sources(job)

    # now, get any matching credentials sources and turn them into credentials
    code_deployment_credentials, interpreter_deployment_credentials = None, None

    code_deployment_type = job.WhichOneof("code_deployment")
    if code_deployment_type in ("git_repo_commit", "git_repo_branch"):
        if code_deployment_type == "git_repo_commit":
            repo_url = job.git_repo_commit.repo_url
        else:
            repo_url = job.git_repo_branch.repo_url

        try:
            code_deployment_credentials = await get_matching_credentials(
                Credentials.Service.GIT, repo_url, credentials_sources
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # TODO ideally this would make it back to an error message for job if it
            #  eventually fails (and maybe even if it doesn't)
            print("Error trying to turn credentials source into actual credentials")
            traceback.print_exc()

    interpreter_deployment_type = job.WhichOneof("interpreter_deployment")
    if interpreter_deployment_type in ("container_at_digest", "container_at_tag"):
        if interpreter_deployment_type == "container_at_digest":
            repository = job.container_at_digest.repository
        else:
            repository = job.container_at_tag.repository

        interpreter_deployment_credentials = await _get_credentials_for_docker(
            repository, credentials_sources, cloud
        )

    sidecar_container_credentials: List[Optional[RawCredentials]] = []
    for sidecar_container in job.sidecar_containers:
        sidecar_container_image = sidecar_container.WhichOneof("container_image")

        if sidecar_container_image in (
            "container_image_at_digest",
            "container_image_at_tag",
        ):
            if sidecar_container_image == "container_image_at_digest":
                repository = sidecar_container.container_image_at_digest.repository
            else:
                repository = sidecar_container.container_image_at_tag.repository

            sidecar_container_credentials.append(
                await _get_credentials_for_docker(
                    repository, credentials_sources, cloud
                )
            )
        else:
            sidecar_container_credentials.append(None)

    return (
        code_deployment_credentials,
        interpreter_deployment_credentials,
        sidecar_container_credentials,
    )


async def run_local(
    job: Job,
    cloud: Optional[Tuple[CloudProviderType, str]] = None,
    storage_bucket_factory: StorageBucketFactoryType = None,
    compile_environment_in_container: bool = True,
) -> Tuple[ProcessState, Optional[asyncio.Task[ProcessState]]]:
    """
    Runs a job locally using the specified working_folder (or uses the default). Meant
    to be called on the "server" where the client is calling e.g. run_function.

    Returns a tuple of (initial job state, continuation).

    The initial ProcessState will either be RUNNING or RUN_REQUEST_FAILED. If the
    initial job state is RUNNING, this should be reported back to the client so that the
    user knows the pid and log_file_name of the child process for that job.

    If the initial job state is RUNNING, then continuation will be an asyncio Task that
    will complete once the child process has completed, and then return another
    ProcessState that indicates how the job completed, e.g. SUCCEEDED, PYTHON_EXCEPTION,
    NON_ZERO_RETURN_CODE.

    If the initial job state is RUN_REQUEST_FAILED, the continuation will be None.

    compile_environment_in_container controls the behavior when job has an environment
    spec for the interpreter. If compile_environment_in_container is true, environment
    specs get compiled into container images. If it's false, environment specs are
    turned into environments in the local environment
    TODO compile_environment_in_container should probably be part of the Job object.
    """

    (
        io_folder,
        job_logs_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    ) = _set_up_working_folder()

    # the logging actually happens via stdout redirection in the run_job_local_main
    # caller
    log_file_name = os.path.join(
        job_logs_folder,
        f"{job.job_friendly_name}.{job.job_id}.log",
    )

    try:
        # unpickle credentials if necessary
        (
            code_deployment_credentials,
            interpreter_deployment_credentials,
            all_sidecar_container_credentials,
        ) = await _get_credentials_for_job(job, cloud)

        # first, get the code paths
        code_paths, interpreter_spec_path, cwd_path = await get_code_paths(
            git_repos_folder,
            local_copies_folder,
            job,
            code_deployment_credentials,
            storage_bucket_factory,
        )

        # next, if we have a environment_spec_in_code, turn into a container

        interpreter_deployment = job.WhichOneof("interpreter_deployment")

        if interpreter_deployment == "environment_spec_in_code":
            if interpreter_spec_path is None:
                raise ValueError(
                    "Cannot specify environment_spec_in_code and not provide any code "
                    "paths"
                )
            if compile_environment_in_container:
                job.server_available_container.CopyFrom(
                    await compile_environment_spec_to_container(
                        job.environment_spec_in_code, interpreter_spec_path, cloud
                    )
                )
                interpreter_deployment = "server_available_container"
            else:
                job.server_available_interpreter.CopyFrom(
                    await compile_environment_spec_locally(
                        job.environment_spec_in_code, interpreter_spec_path, misc_folder
                    )
                )
                interpreter_deployment = "server_available_interpreter"

        if interpreter_deployment == "environment_spec":
            if compile_environment_in_container:
                job.server_available_container.CopyFrom(
                    await compile_environment_spec_to_container(
                        job.environment_spec, misc_folder, cloud
                    )
                )
                interpreter_deployment = "server_available_container"
            else:
                job.server_available_interpreter.CopyFrom(
                    await compile_environment_spec_locally(
                        job.environment_spec, misc_folder, misc_folder
                    )
                )
                interpreter_deployment = "server_available_interpreter"

        # then decide if we're running in a container or not

        is_container = interpreter_deployment in (
            "container_at_digest",
            "container_at_tag",
            "server_available_container",
        )

        # next, transform job_spec into _JobSpecTransformed. _JobSpecTransformed can all
        # be run the same way, i.e. we no longer have to worry about the differences in
        # the job_specs after this section

        job_spec_type = job.WhichOneof("job_spec")

        if job_spec_type == "py_command":
            job_spec_transformed = _prepare_py_command(job, io_folder, is_container)
        elif job_spec_type == "py_function":
            job_spec_transformed = _prepare_py_function(job, io_folder, is_container)
        elif job_spec_type == "py_agent":
            job_spec_transformed = await _prepare_py_agent(job, io_folder, is_container)
        else:
            raise ValueError(f"Unknown job_spec {job_spec_type}")

        # next, prepare a few other things

        # add the agent pid, but don't modify if it already exists somehow
        if MEADOWRUN_AGENT_PID not in job_spec_transformed.environment_variables:
            job_spec_transformed.environment_variables[MEADOWRUN_AGENT_PID] = str(
                os.getpid()
            )

        # add PYTHONUNBUFFERED=1, but don't modify if it already exists
        if "PYTHONUNBUFFERED" not in job_spec_transformed.environment_variables:
            job_spec_transformed.environment_variables["PYTHONUNBUFFERED"] = "1"

        # Merge in the user specified environment, variables, these should always take
        # precedence. A little sloppy to modify in place, but should be fine
        # TODO consider warning if we're overwriting any variables that already exist
        job_spec_transformed.environment_variables.update(
            **_string_pairs_to_dict(job.environment_variables)
        )

        # next we need to launch the job depending on how we've specified the
        # interpreter

        if interpreter_deployment == "server_available_interpreter":
            if job.sidecar_containers:
                raise ValueError(
                    "Cannot specify sidecar_containers with "
                    "server_available_interpreter"
                )
            pid, continuation = await _launch_non_container_job(
                job_spec_type,
                job_spec_transformed,
                code_paths,
                cwd_path,
                log_file_name,
                job,
                io_folder,
            )
            # due to the way protobuf works, this is equivalent to None
            container_id = ""
        elif is_container:
            if interpreter_deployment == "container_at_digest":
                container_image_name = (
                    f"{job.container_at_digest.repository}"
                    f"@{job.container_at_digest.digest}"
                )
                await pull_image(
                    container_image_name, interpreter_deployment_credentials
                )
            elif interpreter_deployment == "container_at_tag":
                # warning this is not reproducible!!! should ideally be resolved on the
                # client
                container_image_name = (
                    f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
                )
                await pull_image(
                    container_image_name, interpreter_deployment_credentials
                )
            elif interpreter_deployment == "server_available_container":
                container_image_name = job.server_available_container.image_name
                # server_available_container assumes that we do not need to pull, and it
                # may not be possible to pull it (i.e. it only exists locally)
            else:
                raise ValueError(
                    f"Unexpected interpreter_deployment: {interpreter_deployment}"
                )

            sidecar_containers = []
            for sidecar_container, sidecar_container_credentials in zip(
                job.sidecar_containers, all_sidecar_container_credentials
            ):
                sidecar_container_image_type = sidecar_container.WhichOneof(
                    "container_image"
                )
                if sidecar_container_image_type == "container_image_at_digest":
                    sidecar_containers.append(
                        f"{sidecar_container.container_image_at_digest.repository}"
                        f"@{sidecar_container.container_image_at_digest.digest}"
                    )
                    await pull_image(
                        sidecar_containers[-1], sidecar_container_credentials
                    )
                elif sidecar_container_image_type == "container_image_at_tag":
                    sidecar_containers.append(
                        f"{sidecar_container.container_image_at_tag.repository}"
                        f":{sidecar_container.container_image_at_tag.tag}"
                    )
                    await pull_image(
                        sidecar_containers[-1], sidecar_container_credentials
                    )
                elif sidecar_container_image_type == "server_available_container_image":
                    sidecar_containers.append(
                        sidecar_container.server_available_container_image.image_name
                    )
                else:
                    raise ValueError(
                        "Unexpected sidecar container image: "
                        f"{sidecar_container_image_type}"
                    )

            container_id, continuation = await _launch_container_job(
                job_spec_type,
                container_image_name,
                job_spec_transformed,
                code_paths,
                cwd_path,
                log_file_name,
                job,
                io_folder,
                sidecar_containers,
            )
            # due to the way protobuf works, this is equivalent to None
            pid = 0
        else:
            raise ValueError(
                f"Did not recognize interpreter_deployment {interpreter_deployment}"
            )

        # launching the process succeeded, return the RUNNING state and create the
        # continuation
        return (
            ProcessState(
                state=ProcessStateEnum.RUNNING,
                pid=pid,
                container_id=container_id,
                log_file_name=log_file_name,
            ),
            asyncio.create_task(continuation),
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        traceback.print_exc()

        # we failed to launch the process
        return (
            ProcessState(
                state=ProcessStateEnum.RUN_REQUEST_FAILED,
                pickled_result=pickle_exception(e, job.result_highest_pickle_protocol),
                log_file_name=log_file_name,
            ),
            None,
        )
