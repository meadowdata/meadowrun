"""
Runs a single job in a lambda.
"""

from __future__ import annotations

import asyncio
import base64
import itertools
import os
from pathlib import Path
import traceback
from typing import Any, Dict, Optional, Tuple
from meadowrun.deployment_manager import (
    compile_environment_spec_locally,
    get_code_paths,
)
from meadowrun.storage_grid_job import get_aws_s3_bucket
from meadowrun.run_job_local import (
    _FUNC_WORKER_PATH,
    _MEADOWRUN_CONTEXT_VARIABLES,
    _MEADOWRUN_RESULT_FILE,
    _MEADOWRUN_RESULT_PICKLE_PROTOCOL,
    _JobSpecTransformed,
    ProcessStateEnum,
    _launch_non_container_job,
    _prepare_function,
    _prepare_function_arguments,
    _string_pairs_to_dict,
)
from meadowrun.shared import pickle_exception
import meadowrun.func_worker_storage_helper

from meadowrun.meadowrun_pb2 import ProcessState, Job


def _set_up_working_folder() -> Tuple[str, str, str, str]:
    """
    Returns conventional path names.
    Creates them if they do not exist.
    """

    working_folder = Path("/tmp/meadowrun")

    result = []
    for folder_name in ("io_folder", "git_repos", "local_copies", "misc"):
        folder = working_folder / folder_name
        folder.mkdir(parents=True, exist_ok=True)
        result.append(str(folder))

    return tuple(result)  # type: ignore


def _prepare_py_command(job: Job, job_id: str, io_folder: str) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use, and returns the
    _JobSpecTransformed
    """

    environment = {}
    io_files = []

    # request the results file
    result_path = os.path.join(io_folder, job_id + ".result")

    io_files.append(job_id + ".result")
    environment[_MEADOWRUN_RESULT_FILE] = result_path
    environment[_MEADOWRUN_RESULT_PICKLE_PROTOCOL] = str(
        job.result_highest_pickle_protocol
    )

    # write context variables to file
    if job.py_command.pickled_context_variables:
        context_variables_path = os.path.join(io_folder, job_id + ".context_variables")

        context_variables_path_container = context_variables_path
        with open(context_variables_path, "wb") as f:
            f.write(job.py_command.pickled_context_variables)
        io_files.append(job_id + ".context_variables")
        # we can't communicate "directly" with the arbitrary command that the
        # user is running so we'll use environment variables
        environment[_MEADOWRUN_CONTEXT_VARIABLES] = context_variables_path_container

    # get the command line
    if not job.py_command.command_line:
        raise ValueError("command_line must have at least one string")

    return _JobSpecTransformed(
        # we need a list, not a protobuf fake list
        list(job.py_command.command_line),
        [],
        environment,
    )


def _prepare_py_function(job: Job, job_id: str, io_folder: str) -> _JobSpecTransformed:
    """
    Creates files in io_folder for the child process to use and returns
    _JobSpecTransformed. We use __meadowrun_func_worker to start the function in the
    child process.
    """

    func_worker_path = _FUNC_WORKER_PATH
    io_path = os.path.join(io_folder, job_id)

    command_line = [
        "python",
        func_worker_path,
        "--result-highest-pickle-protocol",
        str(job.result_highest_pickle_protocol),
        "--io-path",
        io_path,
    ]

    command_line_for_function, _ = _prepare_function(job_id, job.py_function, io_folder)

    command_line_for_arguments, _ = _prepare_function_arguments(
        job_id, job.py_function.pickled_function_arguments, io_folder
    )

    return _JobSpecTransformed(
        list(
            itertools.chain(
                command_line, command_line_for_function, command_line_for_arguments
            )
        ),
        [],
    )


async def run_local(
    region_name: str,
    job: Job,
    job_id: str,
) -> Tuple[ProcessState, Optional[asyncio.Task[ProcessState]]]:
    """ """

    (
        io_folder,
        git_repos_folder,
        local_copies_folder,
        misc_folder,
    ) = _set_up_working_folder()

    storage_bucket = get_aws_s3_bucket(region_name)
    async with storage_bucket:
        meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET = storage_bucket

        try:
            # first, get the code paths
            code_paths, interpreter_spec_path, cwd_path = await get_code_paths(
                git_repos_folder,
                local_copies_folder,
                job,
                None,
                storage_bucket,
            )

            # next, if we have a environment_spec_in_code, turn into a container

            interpreter_deployment = job.WhichOneof("interpreter_deployment")

            if interpreter_deployment == "environment_spec_in_code":
                if interpreter_spec_path is None:
                    raise ValueError(
                        "Cannot specify environment_spec_in_code and not provide any "
                        "code paths"
                    )

                job.server_available_interpreter.CopyFrom(
                    await compile_environment_spec_locally(
                        job.environment_spec_in_code, interpreter_spec_path, misc_folder
                    )
                )
                interpreter_deployment = "server_available_interpreter"

            if interpreter_deployment == "environment_spec":

                job.server_available_interpreter.CopyFrom(
                    await compile_environment_spec_locally(
                        job.environment_spec, misc_folder, misc_folder
                    )
                )
                interpreter_deployment = "server_available_interpreter"

            # next, transform job_spec into _JobSpecTransformed. _JobSpecTransformed can
            # all be run the same way, i.e. we no longer have to worry about the
            # differences in the job_specs after this section

            job_spec_type = job.WhichOneof("job_spec")

            if job_spec_type == "py_command":
                job_spec_transformed = _prepare_py_command(job, job_id, io_folder)
            elif job_spec_type == "py_function":
                job_spec_transformed = _prepare_py_function(job, job_id, io_folder)
            else:
                raise ValueError(f"Unsupported job_spec for lambda {job_spec_type}")

            # next, prepare a few other things

            # add PYTHONUNBUFFERED=1, but don't modify if it already exists
            if "PYTHONUNBUFFERED" not in job_spec_transformed.environment_variables:
                job_spec_transformed.environment_variables["PYTHONUNBUFFERED"] = "1"

            # Merge in the user specified environment, variables, these should always
            # take precedence. A little sloppy to modify in place, but should be fine
            # TODO consider warning if we're overwriting any variables that already
            # exist
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
                    # TODO this is a big hack, but I can't find any other way
                    # to put meadowrun's lambda layer on the path
                    # What doesn't work:
                    # - not overwriting the PYTHONPATH in launch_non_container_job
                    # - adding LAMBDA_TASK_ROOT (this is /var/task)
                    # - adding sys.prefix and sys.base_prefix
                    list(code_paths) + ["/opt/python/lib/python3.9/site-packages"],
                    cwd_path,
                    "",
                    job,
                    job_id,
                    io_folder,
                )
                # due to the way protobuf works, this is equivalent to None
                container_id = ""

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
                    log_file_name="",
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
                    pickled_result=pickle_exception(
                        e, job.result_highest_pickle_protocol
                    ),
                    log_file_name="",
                ),
                None,
            )


async def main_async(region_name: str, job: Job, job_id: str) -> bytes:

    # storage_bucket: Optional[AbstractStorageBucket] = None
    # storage_bucket_factory: Union[
    #     StorageBucketFactoryType, AbstractStorageBucket, None
    # ] = None

    first_state, continuation = await run_local(region_name, job, job_id)

    if (
        first_state.state != ProcessState.ProcessStateEnum.RUNNING
        or continuation is None
    ):
        final_process_state = first_state
    else:
        final_process_state = await continuation

    final_process_state_ser = final_process_state.SerializeToString()
    return final_process_state_ser


def lambda_handler(event: Dict[str, Any], content: Any) -> Dict[str, Any]:
    if "job" not in event:
        raise ValueError("Must specify 'job'")

    region_name = os.environ["AWS_REGION"]

    job = Job.FromString(base64.b64decode(event["job"]))
    # job_id = event["job_id"]

    process_state_bytes = asyncio.run(main_async(region_name, job, job.base_job_id))
    process_state_string = base64.b64encode(process_state_bytes).decode("ascii")

    body = {"process_state": process_state_string}
    return {"statusCode": 200, "body": body}
