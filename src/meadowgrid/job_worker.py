import asyncio.subprocess
import dataclasses
import itertools
import os
import os.path
import pathlib
import pickle
import shutil
import sys
import time
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import aiodocker.containers
import psutil

import meadowflow.context
from meadowgrid.code_deployment import CodeDeploymentManager
from meadowgrid.config import (
    LOGICAL_CPU,
    LOG_AVAILABLE_RESOURCES_INTERVAL_SECS,
    MEADOWGRID_CODE_MOUNT_LINUX,
    MEADOWGRID_INTERPRETER,
    MEADOWGRID_IO_MOUNT_LINUX,
    MEADOWGRID_JOB_WORKER_PID,
    MEMORY_GB,
)
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientForWorkersAsync
from meadowgrid.credentials import RawCredentials
from meadowgrid.docker_controller import (
    run_container,
    get_image_environment_variables,
    pull_image,
)
from meadowgrid.exclusive_file_lock import exclusive_file_lock
from meadowgrid.meadowgrid_pb2 import (
    Job,
    JobStateUpdate,
    ProcessState,
    PyFunctionJob,
    StringPair,
)
from meadowgrid.shared import pickle_exception

ProcessStateEnum = ProcessState.ProcessStateEnum


def _string_pairs_to_dict(pairs: List[StringPair]) -> Dict[str, str]:
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
    To be able to reuse some code, _prepare_py_command, _prepare_py_function, and
    _prepare_py_grid compile their respective Job.job_specs into a command line that we
    can run, plus a bit of additional information on how to run the command line.
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
    the conventional mounts (for meadowgrid) within the container.
    """
    return [
        (os.path.join(io_folder, io_file), f"{MEADOWGRID_IO_MOUNT_LINUX}/{io_file}")
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
        result_path_container = f"{MEADOWGRID_IO_MOUNT_LINUX}/{job.job_id}.result"
    else:
        result_path_container = result_path
    # we create an empty file here so that we can expose it to the docker container,
    # docker does not let us bind non-existent files
    open(result_path, "w").close()
    io_files.append(job.job_id + ".result")
    environment[meadowflow.context._MEADOWGRID_RESULT_FILE] = result_path_container
    environment[meadowflow.context._MEADOWGRID_RESULT_PICKLE_PROTOCOL] = str(
        job.result_highest_pickle_protocol
    )

    # write context variables to file
    if job.py_command.pickled_context_variables:
        context_variables_path = os.path.join(
            io_folder, job.job_id + ".context_variables"
        )
        if is_container:
            context_variables_path_container = (
                f"{MEADOWGRID_IO_MOUNT_LINUX}/{job.job_id}.context_variables"
            )
        else:
            context_variables_path_container = context_variables_path
        with open(context_variables_path, "wb") as f:
            f.write(job.py_command.pickled_context_variables)
        io_files.append(job.job_id + ".context_variables")
        # we can't communicate "directly" with the arbitrary command that the
        # user is running so we'll use environment variables
        environment[
            meadowflow.context._MEADOWGRID_CONTEXT_VARIABLES
        ] = context_variables_path_container

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
        pathlib.Path(__file__).parent / "func_worker" / "__meadowgrid_func_worker.py"
    ).resolve()
)


def _prepare_function(
    job_id: str, function: PyFunctionJob, io_folder: str
) -> Tuple[Sequence[str], Sequence[str]]:
    """
    Creates files in io_folder for the child process to use and returns (command line
    arguments, io_files). Compatible with what grid_worker and __meadowgrid_func_worker
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
    arguments, io_files). Compatible with what grid_worker and __meadowgrid_func_worker
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
    _JobSpecTransformed. We use __meadowgrid_func_worker to start the function in the
    child process.
    """

    if not is_container:
        func_worker_path = _FUNC_WORKER_PATH
        io_path_container = os.path.join(io_folder, job.job_id)
    else:
        func_worker_path = (
            f"{MEADOWGRID_CODE_MOUNT_LINUX}{os.path.basename(_FUNC_WORKER_PATH)}"
        )
        io_path_container = f"{MEADOWGRID_IO_MOUNT_LINUX}/{job.job_id}"

    command_line = [
        "python",
        func_worker_path,
        "--result-highest-pickle-protocol",
        str(job.result_highest_pickle_protocol),
        "--io-path",
        io_path_container,
    ]

    io_files = [
        # these line up with __meadowgrid_func_worker
        os.path.join(job.job_id + ".state"),
        os.path.join(job.job_id + ".result"),
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


def _prepare_py_grid(
    job: Job, io_folder: str, coordinator_address: str, is_container: bool
) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use and returns a
    _JobSpecTransformed. We use grid_worker to get tasks and run them in the child
    process
    """

    if not is_container:
        io_path_container = os.path.join(io_folder, job.job_id)
    else:
        io_path_container = f"{MEADOWGRID_IO_MOUNT_LINUX}/{job.job_id}"
        # If the coordinator_address uses localhost, we replace localhost with
        # host.docker.internal so that things "just work". This is mostly for testing.
        # See discussion:
        # https://stackoverflow.com/questions/48546124/what-is-linux-equivalent-of-host-docker-internal/61001152
        # https://stackoverflow.com/questions/31324981/how-to-access-host-port-from-docker-container/43541732#43541732
        if coordinator_address.startswith("localhost"):
            coordinator_address = (
                "host.docker.internal" + coordinator_address[len("localhost") :]
            )

    command_line = [
        "python",
        "-m",
        "meadowgrid.grid_worker",
        "--coordinator-address",
        coordinator_address,
        "--job-id",
        job.job_id,
        "--io-path",
        io_path_container,
    ]

    io_files = []

    command_line_for_function, io_files_for_function = _prepare_function(
        job.job_id, job.py_grid.function, io_folder
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
        ),
    )


async def _launch_job(
    job: Job,
    io_folder: str,
    job_logs_folder: str,
    coordinator_address: str,
    deployment_manager: CodeDeploymentManager,
    free_resources: Callable[[], None],
    code_deployment_credentials: Optional[RawCredentials],
    interpreter_deployment_credentials: Optional[RawCredentials],
) -> Tuple[JobStateUpdate, Optional[asyncio.Task[Optional[JobStateUpdate]]]]:
    """
    Gets the deployment needed and launches a child process to run the specified job.

    Returns a tuple of (initial job state, continuation).

    The initial job state will either be RUNNING or RUN_REQUEST_FAILED. If the initial
    job state is RUNNING, this should be reported back to the coordinator so that the
    coordinator/end-user knows the pid and log_file_name of the child process for that
    job.

    If the initial job state is RUNNING, then continuation will be an asyncio Task that
    will complete once the child process has completed, and then return another
    JobStateUpdate that indicates how the job completed, e.g. SUCCEEDED,
    PYTHON_EXCEPTION, NON_ZERO_RETURN_CODE. The returned JobState could also be None
    which means that the child process itself already took care of updating the
    coordinator.

    If the initial job state is RUN_REQUEST_FAILED, the continuation will be None, as
    there is no need for additional updates on the state of this job.

    free_resources is a function that needs to be called whenever the function "ends",
    regardless of how it ends
    """

    try:
        # first, just get whether we're running in a container or not

        interpreter_deployment = job.WhichOneof("interpreter_deployment")
        is_container = interpreter_deployment in (
            "container_at_digest",
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
        elif job_spec_type == "py_grid":
            job_spec_transformed = _prepare_py_grid(
                job, io_folder, coordinator_address, is_container
            )
        else:
            raise ValueError(f"Unknown job_spec {job_spec_type}")

        # next, prepare a few other things

        # add the job worker pid, but don't modify if it already exists somehow
        if MEADOWGRID_JOB_WORKER_PID not in job_spec_transformed.environment_variables:
            job_spec_transformed.environment_variables[MEADOWGRID_JOB_WORKER_PID] = str(
                os.getpid()
            )

        # Merge in the user specified environment, variables, these should always take
        # precedence. A little sloppy to modify in place, but should be fine
        # TODO consider warning if we're overwriting any variables that already exist
        job_spec_transformed.environment_variables.update(
            **_string_pairs_to_dict(job.environment_variables)
        )
        code_paths = await deployment_manager.get_code_paths(
            job, code_deployment_credentials
        )
        log_file_name = os.path.join(
            job_logs_folder, f"{job.job_friendly_name}.{job.job_id}.log"
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
                free_resources,
            )
            # due to the way protobuf works, this is equivalent to None
            container_id = ""
        elif is_container:
            if interpreter_deployment == "container_at_digest":
                container_image_name = f"{job.container_at_digest.repository}@{job.container_at_digest.digest}"  # noqa E501
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
                free_resources,
            )
            # due to the way protobuf works, this is equivalent to None
            pid = 0
        elif interpreter_deployment == "container_at_tag":
            raise ValueError(
                "Programming error, container_at_tag should have been resolved in the "
                "coordinator"
            )
        else:
            raise ValueError(
                f"Did not recognize interpreter_deployment {interpreter_deployment}"
            )

        # launching the process succeeded, return the RUNNING state and create the
        # continuation
        return (
            JobStateUpdate(
                job_id=job.job_id,
                process_state=ProcessState(
                    state=ProcessStateEnum.RUNNING,
                    pid=pid,
                    container_id=container_id,
                    log_file_name=log_file_name,
                ),
            ),
            asyncio.create_task(continuation),
        )
    except Exception as e:
        # we failed to launch the process
        free_resources()

        return (
            JobStateUpdate(
                job_id=job.job_id,
                process_state=ProcessState(
                    state=ProcessStateEnum.RUN_REQUEST_FAILED,
                    pickled_result=pickle_exception(
                        e, job.result_highest_pickle_protocol
                    ),
                ),
            ),
            None,
        )


async def _launch_non_container_job(
    job_spec_type: Literal["py_command", "py_function", "py_grid"],
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
    log_file_name: str,
    job: Job,
    io_folder: str,
    free_resources: Callable[[], None],
) -> Tuple[int, Coroutine[Any, Any, Optional[JobStateUpdate]]]:
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
    if interpreter_path == MEADOWGRID_INTERPRETER:
        # replace placeholder
        interpreter_path = sys.executable

    # (2) construct the environment variables dictionary

    env_vars = os.environ.copy()

    # we intentionally overwrite any existing PYTHONPATH--if for some reason we need the
    # current server process' code for the child process, the user needs to include it
    # directly
    env_vars["PYTHONPATH"] = os.pathsep.join(code_paths)

    # We believe that interpreter_path can be one of two formats,
    # python_or_venv_dir/python or python_or_venv_dir/Scripts/python. We need to add the
    # scripts directory to the path so that we can run executables as if we're "in the
    # python environment".
    interpreter_path = pathlib.Path(interpreter_path)
    if interpreter_path.parent.name == "Scripts":
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
    new_first_command_line: str = shutil.which(
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

    with open(log_file_name, "w", encoding="utf-8") as log_file:
        process = await asyncio.subprocess.create_subprocess_exec(
            *job_spec_transformed.command_line,
            stdout=log_file,
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
        free_resources,
    )


async def _non_container_job_continuation(
    process: asyncio.subprocess.Process,
    job_spec_type: Literal["py_command", "py_function", "py_grid"],
    job_id: str,
    io_folder: str,
    result_highest_pickle_protocol: int,
    log_file_name: str,
    free_resources: Callable[[], None],
) -> Optional[JobStateUpdate]:
    """
    Takes an asyncio.subprocess.Process, waits for it to finish, gets results from
    io_folder from the child process if necessary, and then returns an appropriate
    JobStateUpdate indicating how the child process completed. The returned
    JobStateUpdate could also be None which means that the child process itself already
    took care of updating the coordinator.
    """

    try:
        # wait for the process to finish
        # TODO add an optional timeout
        await process.wait()
        return await _completed_job_state(
            job_spec_type,
            job_id,
            io_folder,
            log_file_name,
            process.returncode,
            process.pid,
            None,
        )
    except Exception as e:
        # there was an exception while trying to get the final JobStateUpdate
        return JobStateUpdate(
            job_id=job_id,
            process_state=ProcessState(
                state=ProcessStateEnum.ERROR_GETTING_STATE,
                pickled_result=pickle_exception(e, result_highest_pickle_protocol),
            ),
        )
    finally:
        free_resources()


async def _launch_container_job(
    job_spec_type: Literal["py_command", "py_function", "py_grid"],
    container_image_name: str,
    job_spec_transformed: _JobSpecTransformed,
    code_paths: Sequence[str],
    log_file_name: str,
    job: Job,
    io_folder: str,
    free_resources: Callable[[], None],
) -> Tuple[str, Coroutine[Any, Any, Optional[JobStateUpdate]]]:
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
        mounted_code_path = f"{MEADOWGRID_CODE_MOUNT_LINUX}{i}"
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

    container = await run_container(
        container_image_name,
        # json serializer needs a real list, not a protobuf fake list
        job_spec_transformed.command_line,
        job_spec_transformed.environment_variables,
        binds,
    )
    return container.id, _container_job_continuation(
        container,
        job_spec_type,
        job.job_id,
        io_folder,
        job.result_highest_pickle_protocol,
        log_file_name,
        free_resources,
    )


async def _container_job_continuation(
    container: aiodocker.containers.DockerContainer,
    job_spec_type: Literal["py_command", "py_function", "py_grid"],
    job_id: str,
    io_folder: str,
    result_highest_pickle_protocol: int,
    log_file_name: str,
    free_resources: Callable[[], None],
) -> Optional[JobStateUpdate]:
    """
    Writes the container's logs to log_file_name, waits for the container to finish, and
    then returns a JobStateUpdate indicating the state of this container when it
    finished
    """
    try:
        # Docker appears to have an objection to having a log driver that can produce
        # plain text files (https://github.com/moby/moby/issues/17020) so we implement
        # that in a hacky way here.
        # TODO figure out overall strategy for logging, maybe eventually implement our
        #  own plain text/whatever log driver for docker.
        with open(log_file_name, "w") as f:
            async for line in container.log(stdout=True, stderr=True, follow=True):
                f.write(line)

        wait_result = await container.wait()
        # as per https://docs.docker.com/engine/api/v1.41/#operation/ContainerWait we
        # can get the return code from the result
        return_code = wait_result["StatusCode"]

        return await _completed_job_state(
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
        # there was an exception while trying to get the final JobStateUpdate
        return JobStateUpdate(
            job_id=job_id,
            process_state=ProcessState(
                state=ProcessStateEnum.ERROR_GETTING_STATE,
                pickled_result=pickle_exception(e, result_highest_pickle_protocol),
            ),
        )
    finally:
        free_resources()


async def _completed_job_state(
    job_spec_type: Literal["py_command", "py_function", "py_grid"],
    job_id: str,
    io_folder: str,
    log_file_name: str,
    return_code: int,
    pid: Optional[int],
    container_id: Optional[str],
) -> Optional[JobStateUpdate]:
    """
    This creates an appropriate JobStateUpdate for a job that has completed (regardless
    of whether it ran in a process or container).
    """

    # see if we got a normal return code
    if return_code != 0:
        return JobStateUpdate(
            job_id=job_id,
            process_state=ProcessState(
                state=ProcessStateEnum.NON_ZERO_RETURN_CODE,
                pid=pid,
                container_id=container_id,
                log_file_name=log_file_name,
                return_code=return_code,
            ),
        )

    # if we returned normally

    # for py_grid, we can stop here--if the grid_worker exited cleanly, it has already
    # taken care of any needed communication with the coordinator
    if job_spec_type == "py_grid":
        return None

    # for py_funcs, get the state
    if job_spec_type == "py_function":
        state_file = os.path.join(io_folder, job_id + ".state")
        with open(state_file, "r", encoding="utf-8") as f:
            state_string = f.read()
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
        with open(result_file, "rb") as f:
            result = f.read()
    else:
        result = None

    # TODO clean up files in io_folder for this process

    # return the JobStateUpdate
    return JobStateUpdate(
        job_id=job_id,
        process_state=ProcessState(
            state=state,
            pid=pid,
            container_id=container_id,
            log_file_name=log_file_name,
            return_code=0,
            pickled_result=result,
        ),
    )


async def job_worker_main_loop(
    working_folder: str, available_resources: Dict[str, float], coordinator_address: str
) -> None:
    """The main loop for the job_worker"""

    # TODO make this restartable if it crashes

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

    os.makedirs(io_folder, exist_ok=True)
    os.makedirs(job_logs_folder, exist_ok=True)
    os.makedirs(git_repos_folder, exist_ok=True)
    os.makedirs(local_copies_folder, exist_ok=True)

    # Next, exit if there's already an instance running with the same working_folder, as
    # we're assuming we are the only ones managing working_folder Ideally we would
    # prevent any other instance from running on this machine (regardless of whether it
    # has the same working_folder or not), because we're also assuming that the entire
    # machine's resources are ours to manage.

    exclusive_file_lock(lock_file=os.path.join(working_folder, "worker.lock"))

    # initialize some more state

    client = MeadowGridCoordinatorClientForWorkersAsync(coordinator_address)

    deployment_manager = CodeDeploymentManager(git_repos_folder, local_copies_folder)

    # represents jobs where we are preparing to launch the child process
    launching_jobs: Set[
        asyncio.Task[
            Tuple[JobStateUpdate, Optional[asyncio.Task[Optional[JobStateUpdate]]]]
        ]
    ] = set()
    # represents jobs where the child process has been launched and is running
    running_jobs: Set[asyncio.Task[Optional[JobStateUpdate]]] = set()

    # assume we can use the entire machine's resources
    # TODO we should maybe also keep track of the actual resources available? And
    #  potentially limit child processes using cgroups etc? Also account for system
    #  processes/the job worker itself using some CPU/memory?
    if MEMORY_GB not in available_resources:
        available_resources[MEMORY_GB] = psutil.virtual_memory().total / (
            1024 * 1024 * 1024
        )
    if LOGICAL_CPU not in available_resources:
        available_resources[LOGICAL_CPU] = psutil.cpu_count(logical=True)

    last_available_resources_update = time.time()
    print(f"Available resources: {available_resources}")

    while True:
        # TODO a lot of try/catches needed here...

        # first, check if any job launches have completed, get their state update, and
        # move them from launching_jobs to running_jobs

        job_state_updates: List[JobStateUpdate] = []

        if len(launching_jobs) > 0:
            # a little subtle that we are reassigning launching_jobs here to just the
            # ones that are still not complete
            launched_jobs, launching_jobs = await asyncio.wait(
                launching_jobs, timeout=0
            )
            for launched_job in launched_jobs:
                # this corresponds to the return type of _launch_job, see docstring
                job_state_update, running_job = launched_job.result()

                if running_job is not None:
                    # if we successfully launched the process, we will add it to
                    # running_jobs unless it has already completed, in which case we can
                    # skip that step as we just need to update the coordinator
                    if running_job.done():
                        job_state_update = running_job.result()
                    else:
                        running_jobs.add(running_job)

                job_state_updates.append(job_state_update)

        # next, check if any processes are done. If they are, get their state update,
        # and remove them from running_jobs

        if len(running_jobs) > 0:
            # again, subtle reassignment of running_jobs
            done_jobs, running_jobs = await asyncio.wait(running_jobs, timeout=0)
            for done_job in done_jobs:
                job_state_update = done_job.result()
                if job_state_update is not None:
                    job_state_updates.append(job_state_update)

        # now update the coordinator with the state updates we collected

        if job_state_updates:
            await client.update_job_states(job_state_updates)

        # now get another job if we can with our remaining resources

        if (
            time.time() - last_available_resources_update
            > LOG_AVAILABLE_RESOURCES_INTERVAL_SECS
        ):
            last_available_resources_update = time.time()
            print(f"Available resources: {available_resources}")
        # a bit of a hack, but no reason to make requests if we have no more memory or
        # CPU to give out
        if available_resources[MEMORY_GB] > 0 and available_resources[LOGICAL_CPU] > 0:
            next_job_response = await client.get_next_job(available_resources)
            job = next_job_response.job
            if job.job_id:
                for resource in job.resources_required:
                    # TODO this should never happen, but consider doing something if the
                    #  resource does not exist on this worker or if this takes us below
                    #  zero?
                    if resource.name in available_resources:
                        available_resources[resource.name] -= resource.value

                def free_resources(resources_to_free=job.resources_required):
                    for resource in resources_to_free:
                        if resource.name in available_resources:
                            available_resources[resource.name] += resource.value

                # unpickle credentials if necessary
                if next_job_response.code_deployment_credentials.credentials:
                    code_deployment_credentials = pickle.loads(
                        next_job_response.code_deployment_credentials.credentials
                    )
                else:
                    code_deployment_credentials = None

                if next_job_response.interpreter_deployment_credentials.credentials:
                    interpreter_deployment_credentials = pickle.loads(
                        next_job_response.interpreter_deployment_credentials.credentials
                    )
                else:
                    interpreter_deployment_credentials = None

                launching_jobs.add(
                    asyncio.create_task(
                        _launch_job(
                            job,
                            io_folder,
                            job_logs_folder,
                            coordinator_address,
                            deployment_manager,
                            free_resources,
                            code_deployment_credentials,
                            interpreter_deployment_credentials,
                        )
                    )
                )

        # TODO this should be way more sophisticated. Depending on the last time we
        #  polled our processes (really should use async.wait to see if anything has
        #  changed), when we last asked for a job and whether we got one or not, and the
        #  reason we didn't get one (not enough resources here vs not a enough jobs),
        #  how many other agents there are, we should retry/wait
        await asyncio.sleep(0.1)
