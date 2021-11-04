import asyncio.subprocess
import os
import os.path
import pathlib
import shutil
from typing import Dict, Optional, Literal, Iterable, Sequence, Set, Tuple, List

import meadowflow.context
import meadowflow.server.client
from meadowrun.coordinator_client import MeadowRunCoordinatorClientForWorkersAsync
from meadowrun.deployment_manager import DeploymentManager
from meadowrun.exclusive_file_lock import exclusive_file_lock
from meadowrun.meadowrun_pb2 import (
    Job,
    JobStateUpdate,
    ProcessState,
    PyFunctionJob,
    StringPair,
)
from meadowrun.shared import pickle_exception

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


async def _get_environment(
    deployment_manager: DeploymentManager, job: Job
) -> Tuple[str, Dict[str, str]]:
    """
    Returns working directory (having created it if necessary), environment variables
    (which include PATH and PYTHONPATH variables for the deployment specified in job)
    """

    interpreter_path, code_paths = await deployment_manager.get_interpreter_and_code(
        job
    )
    working_directory = code_paths[0]

    environment = os.environ.copy()
    # we intentionally overwrite any existing PYTHONPATH--if for some reason we
    # need the current server process' code for the child process, the user
    # needs to include it directly
    environment["PYTHONPATH"] = ";".join(code_paths)
    environment.update(**_string_pairs_to_dict(job.environment_variables))

    # We believe that interpreter_path can be one of two formats,
    # python_or_venv_dir/python or python_or_venv_dir/Scripts/python. We need to
    # add the scripts directory to the path so that we can run executables as if
    # we're "in the python environment".
    interpreter_path = pathlib.Path(interpreter_path)
    if interpreter_path.parent.name == "Scripts":
        scripts_dir = str(interpreter_path.parent.resolve())
    else:
        scripts_dir = str((interpreter_path.parent / "Scripts").resolve())
    environment["PATH"] = scripts_dir + ";" + environment["PATH"]

    return working_directory, environment


def _prepare_py_command(
    job: Job, io_folder: str, environment: Dict[str, str]
) -> Sequence[str]:
    """
    Creates files in io_folder for the child process to use, modifies environment in
    place(!!), and returns the command line to run.
    """

    # request the results file
    environment[meadowflow.context._MEADOWRUN_RESULT_FILE] = os.path.join(
        io_folder, job.job_id + ".result"
    )
    environment[meadowflow.context._MEADOWRUN_RESULT_PICKLE_PROTOCOL] = str(
        job.result_highest_pickle_protocol
    )

    # write context variables to file
    if job.py_command.pickled_context_variables:
        context_variables_path = os.path.join(
            io_folder, job.job_id + ".context_variables"
        )
        with open(context_variables_path, "wb") as f:
            f.write(job.py_command.pickled_context_variables)
        # we can't communicate "directly" with the arbitrary command that the
        # user is running so we'll use environment variables
        environment[
            meadowflow.context._MEADOWRUN_CONTEXT_VARIABLES
        ] = context_variables_path

    # get the command line
    if not job.py_command.command_line:
        raise ValueError("command_line must have at least one string")

    return job.py_command.command_line


_FUNC_WORKER_PATH = str(
    (
        pathlib.Path(__file__).parent / "func_worker" / "__meadowrun_func_worker.py"
    ).resolve()
)


def _prepare_function(
    job_id: str, function: PyFunctionJob, io_folder: str
) -> Iterable[str]:
    """
    Creates files in io_folder for the child process to use and returns command line
    arguments that are compatible with grid_worker and __meadowrun_func_worker.
    """
    function_spec = function.WhichOneof("function_spec")
    if function_spec == "qualified_function_name":
        return [
            "--module-name",
            function.qualified_function_name.module_name,
            "--function-name",
            function.qualified_function_name.function_name,
        ]
    elif function_spec == "pickled_function":
        if function.pickled_function is None:
            raise ValueError("argument cannot be None")
        pickled_function_path = os.path.join(io_folder, job_id + ".function")
        with open(pickled_function_path, "wb") as f:
            f.write(function.pickled_function)
        return ["--has-pickled-function"]
    else:
        raise ValueError(f"Unknown function_spec {function_spec}")


def _prepare_function_arguments(
    job_id: str, pickled_function_arguments: Optional[bytes], io_folder: str
) -> Iterable[str]:
    """
    Creates files in io_folder for the child process to use and returns command line
    arguments that are compatible with grid_worker and __meadowrun_func_worker.
    """

    if pickled_function_arguments:
        pickled_arguments_path = os.path.join(io_folder, job_id + ".arguments")
        with open(pickled_arguments_path, "wb") as f:
            f.write(pickled_function_arguments)
        return ["--has-pickled-arguments"]
    else:
        return []


def _prepare_py_function(job: Job, io_folder: str) -> Sequence[str]:
    """
    Creates files in io_folder for the child process to use and returns the command line
    to run for this function. We use __meadowrun_func_worker to start the function in
    the child process.
    """

    command_line = [
        "python",
        _FUNC_WORKER_PATH,
        "--result-highest-pickle-protocol",
        str(job.result_highest_pickle_protocol),
        "--io-path",
        os.path.join(io_folder, job.job_id),
    ]

    command_line.extend(_prepare_function(job.job_id, job.py_function, io_folder))
    command_line.extend(
        _prepare_function_arguments(
            job.job_id, job.py_function.pickled_function_arguments, io_folder
        )
    )

    return command_line


def _prepare_py_grid(
    job: Job, io_folder: str, coordinator_address: str
) -> Sequence[str]:
    """
    Creates files in io_folder for the child process to use and returns the command line
    to run for this grid job. We use grid_worker to get tasks and run them in the child
    process
    """

    command_line = [
        "python",
        "-m",
        "meadowrun.grid_worker",
        "--coordinator-address",
        coordinator_address,
        "--job-id",
        job.job_id,
        "--io-path",
        os.path.join(io_folder, job.job_id),
    ]

    command_line.extend(_prepare_function(job.job_id, job.py_grid.function, io_folder))
    command_line.extend(
        _prepare_function_arguments(
            job.job_id, job.py_grid.function.pickled_function_arguments, io_folder
        )
    )

    return command_line


async def _launch_job(
    job: Job,
    io_folder: str,
    job_logs_folder: str,
    coordinator_address: str,
    deployment_manager: DeploymentManager,
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
    """

    try:
        # set up the deployment, get the working directory, environment, and
        # log_file_name
        working_directory, environment = await _get_environment(deployment_manager, job)

        log_file_name = os.path.join(
            job_logs_folder, f"{job.job_friendly_name}.{job.job_id}.log"
        )

        # write files to io_folder for child process communication, modify environment
        # if necessary, and get the command line to run
        job_spec = job.WhichOneof("job_spec")

        if job_spec == "py_command":
            command_line = _prepare_py_command(job, io_folder, environment)
        elif job_spec == "py_function":
            command_line = _prepare_py_function(job, io_folder)
        elif job_spec == "py_grid":
            command_line = _prepare_py_grid(job, io_folder, coordinator_address)
        else:
            raise ValueError(f"Unknown job_spec {job_spec}")

        # Fix the command line: Popen uses cwd and env to search for the specified
        # command on Linux but not on Windows according to the docs:
        # https://docs.python.org/3/library/subprocess.html#subprocess.Popen We can use
        # shutil to make the behavior more similar on both platforms
        new_first_command_line: str = shutil.which(
            command_line[0],
            path=f"{working_directory};{environment['PATH']}",
        )
        if new_first_command_line:
            # noinspection PyTypeChecker
            command_line = [new_first_command_line] + command_line[1:]

        # run the process
        print(
            f"Running process ({job_spec}): {' '.join(command_line)}; "
            f"cwd={working_directory}; PYTHONPATH={environment['PYTHONPATH']}; "
            f"log_file_name={log_file_name}"
        )

        with open(log_file_name, "w", encoding="utf-8") as log_file:
            process = await asyncio.subprocess.create_subprocess_exec(
                *command_line,
                stdout=log_file,
                stderr=asyncio.subprocess.STDOUT,
                cwd=working_directory,
                env=environment,
            )
    except Exception as e:
        # we failed to launch the process
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
    else:
        # launching the process succeeded, return the RUNNING state and create the
        # continuation
        return JobStateUpdate(
            job_id=job.job_id,
            process_state=ProcessState(
                state=ProcessStateEnum.RUNNING,
                pid=process.pid,
                log_file_name=log_file_name,
            ),
        ), asyncio.create_task(
            _on_process_complete(
                process,
                job_spec,
                job.job_id,
                io_folder,
                job.result_highest_pickle_protocol,
                log_file_name,
            )
        )


async def _on_process_complete(
    process: asyncio.subprocess.Process,
    job_spec: Literal["py_command", "py_function", "py_grid"],
    job_id: str,
    io_folder: str,
    result_highest_pickle_protocol: int,
    log_file_name: str,
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

        # see if we got a normal return code
        if process.returncode != 0:
            return JobStateUpdate(
                job_id=job_id,
                process_state=ProcessState(
                    state=ProcessStateEnum.NON_ZERO_RETURN_CODE,
                    pid=process.pid,
                    log_file_name=log_file_name,
                    return_code=process.returncode,
                ),
            )

        # if we returned normally

        # for py_grid, we can stop here--if the grid_worker exited cleanly, it has
        # already taken care of any needed communication with the coordinator
        if job_spec == "py_grid":
            return None

        # for py_funcs, get the state
        if job_spec == "py_function":
            state_file = os.path.join(io_folder, job_id + ".state")
            with open(state_file, "r", encoding="utf-8") as f:
                state_string = f.read()
            if state_string == "SUCCEEDED":
                state = ProcessStateEnum.SUCCEEDED
            elif state_string == "PYTHON_EXCEPTION":
                state = ProcessStateEnum.PYTHON_EXCEPTION
            else:
                raise ValueError(f"Unknown state string: {state_string}")
        elif job_spec == "py_command":
            state = ProcessStateEnum.SUCCEEDED
        else:
            raise ValueError(f"job_spec was not recognized {job_spec}")

        # Next get the result. The result file is optional for py_commands because we
        # don't have full control over the process and there's no way to guarantee that
        # "our code" gets executed
        result_file = os.path.join(io_folder, job_id + ".result")
        if job_spec != "py_command" or os.path.exists(result_file):
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
                pid=process.pid,
                log_file_name=log_file_name,
                return_code=0,
                pickled_result=result,
            ),
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


async def job_worker_main_loop(working_folder: str, coordinator_address: str) -> None:
    """The main loop for the job_worker"""

    # TODO make this restartable if it crashes

    # first, create the directories that we need

    # this holds files for transferring data to and from this server process and the
    # child processes
    io_folder = os.path.join(working_folder, "io")
    # holds the logs for the functions/commands that this server runs
    job_logs_folder = os.path.join(working_folder, "job_logs")
    # see DeploymentManager
    git_repos_folder = os.path.join(working_folder, "git_repos")
    # see DeploymentManager
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

    client = MeadowRunCoordinatorClientForWorkersAsync(coordinator_address)

    deployment_manager = DeploymentManager(git_repos_folder, local_copies_folder)

    # represents jobs where we are preparing to launch the child process
    launching_jobs: Set[
        asyncio.Task[
            Tuple[JobStateUpdate, Optional[asyncio.Task[Optional[JobStateUpdate]]]]
        ]
    ] = set()
    # represents jobs where the child process has been launched and is running
    running_jobs: Set[asyncio.Task[Optional[JobStateUpdate]]] = set()

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

        await client.update_job_states(job_state_updates)

        # now get another job if we can with our remaining resources

        # TODO "resource management" is extremely simple now, just run 4 jobs at a time
        if len(launching_jobs) + len(running_jobs) < 4:
            job = await client.get_next_job()
            if job.job_id:
                launching_jobs.add(
                    asyncio.create_task(
                        _launch_job(
                            job,
                            io_folder,
                            job_logs_folder,
                            coordinator_address,
                            deployment_manager,
                        )
                    )
                )

        # TODO this should be way more sophisticated. Depending on the last time we
        #  polled our processes (really should use async.wait to see if anything has
        #  changed), when we last asked for a job and whether we got one or not, and the
        #  reason we didn't get one (not enough resources here vs not a enough jobs),
        #  how many other agents there are, we should retry/wait
        await asyncio.sleep(0.1)
