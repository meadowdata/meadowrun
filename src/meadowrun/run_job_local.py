from __future__ import annotations

import asyncio
import asyncio.subprocess
import dataclasses
import itertools
import os
import os.path
import pathlib
import pickle
import shutil
import sys
import traceback
from typing import (
    Any,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

from typing_extensions import Literal

from meadowrun._vendor import aiodocker
from meadowrun._vendor.aiodocker import containers as aiodocker_containers
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
    compile_environment_spec_to_container,
    get_code_paths,
)
from meadowrun.docker_controller import (
    run_container,
    get_image_environment_variables,
    pull_image,
)
from meadowrun.meadowrun_pb2 import (
    Credentials,
    Job,
    ProcessState,
    PyFunctionJob,
    StringPair,
)
from meadowrun.run_job_core import (
    CloudProviderType,
    Host,
    JobCompletion,
    MeadowrunException,
)
from meadowrun.shared import pickle_exception

ProcessStateEnum = ProcessState.ProcessStateEnum


_MEADOWRUN_CONTEXT_VARIABLES = "MEADOWRUN_CONTEXT_VARIABLES"
_MEADOWRUN_RESULT_FILE = "MEADOWRUN_RESULT_FILE"
_MEADOWRUN_RESULT_PICKLE_PROTOCOL = "MEADOWRUN_RESULT_PICKLE_PROTOCOL"


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
        open(result_path, "w").close()
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
    job_id: str, function: PyFunctionJob, io_folder: str
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
            open(os.path.join(io_folder, io_file), "w").close()

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


async def _launch_non_container_job(
    job_spec_type: Literal["py_command", "py_function"],
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
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
    if code_paths:
        working_directory = code_paths[0]
        paths_to_search = f"{working_directory};{paths_to_search}"
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
        f"PYTHONPATH={env_vars['PYTHONPATH']}; log_file_name={log_file_name}"
    )

    process = await asyncio.subprocess.create_subprocess_exec(
        *job_spec_transformed.command_line,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=working_directory,
        env=env_vars,
    )

    # (5) return the pid and continuation
    return process.pid, _non_container_job_continuation(
        process,
        job_spec_type,
        job.job_id,
        io_folder,
        job.result_highest_pickle_protocol,
        log_file_name,
    )


async def _non_container_job_continuation(
    process: asyncio.subprocess.Process,
    job_spec_type: Literal["py_command", "py_function"],
    job_id: str,
    io_folder: str,
    result_highest_pickle_protocol: int,
    log_file_name: str,
) -> ProcessState:
    """
    Takes an asyncio.subprocess.Process, waits for it to finish, gets results from
    io_folder from the child process if necessary, and then returns an appropriate
    ProcessState indicating how the child process completed.
    """

    try:
        # wait for the process to finish
        # TODO add an optional timeout

        with open(log_file_name, "wb") as log_file:
            async for line in process.stdout:  # type: ignore
                log_file.write(line)
                sys.stdout.buffer.write(line)
        returncode = await process.wait()
        return _completed_job_state(
            job_spec_type,
            job_id,
            io_folder,
            log_file_name,
            returncode,
            process.pid,
            None,
        )
    except Exception as e:
        # there was an exception while trying to get the final ProcessState
        return ProcessState(
            state=ProcessStateEnum.ERROR_GETTING_STATE,
            pickled_result=pickle_exception(e, result_highest_pickle_protocol),
        )


async def _launch_container_job(
    job_spec_type: Literal["py_command", "py_function"],
    container_image_name: str,
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
    log_file_name: str,
    job: Job,
    io_folder: str,
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

    binds: List[Tuple[str, str]] = []
    mounted_code_paths: List[str] = []

    # populate binds with code paths we need
    for i, path_on_host in enumerate(code_paths):
        mounted_code_path = f"{MEADOWRUN_CODE_MOUNT_LINUX}{i}"
        mounted_code_paths.append(mounted_code_path)
        binds.append((path_on_host, mounted_code_path))

    # If we've exposed any code paths, add them to PYTHONPATH. The normal behavior for
    # environment variables is that if they're specified in job_spec_transformed, those
    # override (rather than append to) what's defined in the container. If they aren't
    # specified in job_spec_transformed, we use whatever is specified in the container
    # image. We need to replicate that logic here so that we just add mounted_code_paths
    # to whatever PYTHONPATH would have "normally" become.
    if mounted_code_paths:
        existing_python_path = []
        if "PYTHONPATH" in job_spec_transformed.environment_variables:
            existing_python_path = [
                job_spec_transformed.environment_variables["PYTHONPATH"]
            ]
        else:
            image_environment_variables = await get_image_environment_variables(
                container_image_name
            )
            if image_environment_variables:
                for image_env_var in image_environment_variables:
                    if image_env_var.startswith("PYTHONPATH="):
                        existing_python_path = [image_env_var[len("PYTHONPATH=") :]]
                        # just take the first PYTHONPATH we see, not worth worrying
                        # about pathological case where there are multiple PYTHONPATH
                        # environment variables
                        break

        # TODO we need to use ":" for Linux and ";" for Windows containers
        job_spec_transformed.environment_variables["PYTHONPATH"] = ":".join(
            itertools.chain(mounted_code_paths, existing_python_path)
        )

    # now, expose any files we need for communication with the container
    binds.extend(job_spec_transformed.container_binds)

    # finally, run the container
    print(
        f"Running container ({job_spec_type}): "
        f"{' '.join(job_spec_transformed.command_line)}; "
        f"container image={container_image_name}; "
        f"PYTHONPATH={job_spec_transformed.environment_variables.get('PYTHONPATH')} "
        f"log_file_name={log_file_name}"
    )

    container, docker_client = await run_container(
        container_image_name,
        # json serializer needs a real list, not a protobuf fake list
        job_spec_transformed.command_line,
        job_spec_transformed.environment_variables,
        binds,
    )
    return container.id, _container_job_continuation(
        container,
        docker_client,
        job_spec_type,
        job.job_id,
        io_folder,
        job.result_highest_pickle_protocol,
        log_file_name,
    )


async def _container_job_continuation(
    container: aiodocker_containers.DockerContainer,
    docker_client: aiodocker.Docker,
    job_spec_type: Literal["py_command", "py_function"],
    job_id: str,
    io_folder: str,
    result_highest_pickle_protocol: int,
    log_file_name: str,
) -> ProcessState:
    """
    Writes the container's logs to log_file_name, waits for the container to finish, and
    then returns a ProcessState indicating the state of this container when it
    finished.

    docker_client just needs to be closed when the container process has completed.
    """
    try:
        # Docker appears to have an objection to having a log driver that can produce
        # plain text files (https://github.com/moby/moby/issues/17020) so we implement
        # that in a hacky way here.
        # TODO figure out overall strategy for logging, maybe eventually implement our
        #  own plain text/whatever log driver for docker.
        with open(log_file_name, "w", encoding="utf-8") as f:
            async for line in container.log(stdout=True, stderr=True, follow=True):
                print(line, end="")
                f.write(line)

        wait_result = await container.wait()
        # as per https://docs.docker.com/engine/api/v1.41/#operation/ContainerWait we
        # can get the return code from the result
        return_code = wait_result["StatusCode"]

        return _completed_job_state(
            job_spec_type,
            job_id,
            io_folder,
            log_file_name,
            return_code,
            None,
            container.id,
        )

        # TODO delete the container now that we're done with it, but doesn't need to be
        #  on the critical path

    except Exception as e:
        # there was an exception while trying to get the final ProcessState
        return ProcessState(
            state=ProcessStateEnum.ERROR_GETTING_STATE,
            pickled_result=pickle_exception(e, result_highest_pickle_protocol),
        )
    finally:
        await docker_client.__aexit__(None, None, None)


def _completed_job_state(
    job_spec_type: Literal["py_command", "py_function"],
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

    # for py_funcs, get the state
    if job_spec_type == "py_function":
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


def _get_default_working_folder() -> str:
    # figure out the default working_folder based on the OS
    if os.name == "nt":
        return os.path.join(os.environ["USERPROFILE"], "meadowrun")
    elif os.name == "posix":
        return os.path.join(os.environ["HOME"], "meadowrun")
    else:
        raise ValueError(f"Unexpected os.name {os.name}")


def _set_up_working_folder(
    working_folder: Optional[str],
) -> Tuple[str, str, str, str, str]:
    """
    Sets the working_folder to a default if it's not set, creates the necessary
    subfolders, gets a machine-wide lock on the working folder, then returns io_folder,
    job_logs_folder, git_repos_folder, local_copies_folder
    """

    if not working_folder:
        working_folder = _get_default_working_folder()
        os.makedirs(working_folder, exist_ok=True)

    # first, create the directories that we need

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

    os.makedirs(io_folder, exist_ok=True)
    os.makedirs(job_logs_folder, exist_ok=True)
    os.makedirs(git_repos_folder, exist_ok=True)
    os.makedirs(local_copies_folder, exist_ok=True)
    os.makedirs(misc_folder, exist_ok=True)

    return (
        io_folder,
        job_logs_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    )


async def _get_credentials_for_job(
    job: Job,
) -> Tuple[Optional[RawCredentials], Optional[RawCredentials]]:
    """
    Gets the credentials for the code_deployment, interpreter_deployment for job_to_run.
    This is a little silly because we could just have the user specify the
    code_deployment credentials and the interpreter_deployment credentials explicitly.
    We should make that change at the same time as we make it possible to register
    credentials outside of the context of a single job, as we'll need this logic for
    that use case.
    """

    # first, get all available credentials sources from the JobToRun
    credentials_sources: CredentialsDict = {}
    for credentials_source in job.credentials_sources:
        source = credentials_source.WhichOneof("source")
        if source is None:
            raise ValueError(
                "AddCredentialsRequest request should have a source set: "
                f"{credentials_source}"
            )
        credentials_sources.setdefault(credentials_source.service, []).append(
            (credentials_source.service_url, getattr(credentials_source, source))
        )

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

        try:
            interpreter_deployment_credentials = await get_docker_credentials(
                repository, credentials_sources
            )
        except Exception:
            print("Error trying to turn credentials source into actual credentials")
            traceback.print_exc()

    return code_deployment_credentials, interpreter_deployment_credentials


async def run_local(
    job: Job,
    working_folder: Optional[str] = None,
    cloud: Optional[Tuple[CloudProviderType, str]] = None,
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
    """

    (
        io_folder,
        job_logs_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    ) = _set_up_working_folder(working_folder)

    # unpickle credentials if necessary
    (
        code_deployment_credentials,
        interpreter_deployment_credentials,
    ) = await _get_credentials_for_job(job)

    try:
        # first, get the code paths
        code_paths, interpreter_spec_path = await get_code_paths(
            git_repos_folder, local_copies_folder, job, code_deployment_credentials
        )

        # next, if we have a environment_spec_in_code, turn into a container

        interpreter_deployment = job.WhichOneof("interpreter_deployment")

        if interpreter_deployment == "environment_spec_in_code":
            if interpreter_spec_path is None:
                raise ValueError(
                    "Cannot specify environment_spec_in_code and not provide any code "
                    "paths"
                )
            job.server_available_container.CopyFrom(
                await compile_environment_spec_to_container(
                    job.environment_spec_in_code, interpreter_spec_path, cloud
                )
            )
            interpreter_deployment = "server_available_container"

        if interpreter_deployment == "environment_spec":

            job.server_available_container.CopyFrom(
                await compile_environment_spec_to_container(
                    job.environment_spec, misc_folder, cloud
                )
            )
            interpreter_deployment = "server_available_container"

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
        log_file_name = os.path.join(
            job_logs_folder,
            f"{job.job_friendly_name}.{job.job_id}.log",
        )

        # next we need to launch the job depending on how we've specified the
        # interpreter

        if interpreter_deployment == "server_available_interpreter":
            pid, continuation = await _launch_non_container_job(
                job_spec_type,
                job_spec_transformed,
                code_paths,
                log_file_name,
                job,
                io_folder,
            )
            # due to the way protobuf works, this is equivalent to None
            container_id = ""
        elif is_container:
            if interpreter_deployment == "container_at_digest":
                container_image_name = f"{job.container_at_digest.repository}@{job.container_at_digest.digest}"  # noqa: E501
                await pull_image(
                    container_image_name, interpreter_deployment_credentials
                )
            elif interpreter_deployment == "container_at_tag":
                # warning this is not reproducible!!! should ideally be resolved on the
                # client
                container_image_name = f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"  # noqa: E501
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

            container_id, continuation = await _launch_container_job(
                job_spec_type,
                container_image_name,
                job_spec_transformed,
                code_paths,
                log_file_name,
                job,
                io_folder,
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
    except Exception as e:
        # we failed to launch the process
        return (
            ProcessState(
                state=ProcessStateEnum.RUN_REQUEST_FAILED,
                pickled_result=pickle_exception(e, job.result_highest_pickle_protocol),
            ),
            None,
        )


@dataclasses.dataclass(frozen=True)
class LocalHost(Host):
    async def run_job(self, job: Job) -> JobCompletion[Any]:
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
