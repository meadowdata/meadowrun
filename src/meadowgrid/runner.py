import io
import pickle
from typing import Callable, TypeVar, Union, Any, Dict, Optional, Sequence

import fabric

from meadowgrid import ServerAvailableFolder
from meadowgrid.agent import run_one_job
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
from meadowgrid.grid import _get_id_name_function
from meadowgrid.meadowgrid_pb2 import JobToRun, Job, ProcessState


_T = TypeVar("_T")


def _construct_job_to_run_function(
    function: Callable[..., _T],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    environment_variables: Optional[Dict[str, str]] = None,
) -> JobToRun:
    """
    Basically a "user-friendly" JobToRun constructor for functions.

    Kind of a combination of meadowgrid.grid.grid_map and
    meadowgrid.coordinator_client._create_py_runnable_job
    """

    job_id, friendly_name, pickled_function = _get_id_name_function(function)

    if code_deployment is None:
        code_deployment = ServerAvailableFolder()

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
    _add_deployments_to_job(job, code_deployment, interpreter_deployment)
    return JobToRun(job=job)


async def run_function_async(
    function: Callable[..., _T],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    environment_variables: Optional[Dict[str, str]] = None,
) -> _T:
    """
    Runs the specified function on the local machine with the specified deployment(s)
    """

    result = await run_one_job(
        _construct_job_to_run_function(
            function,
            interpreter_deployment,
            code_deployment,
            args,
            kwargs,
            environment_variables,
        )
    )

    if result.state == ProcessState.ProcessStateEnum.SUCCEEDED:
        # TODO figure out what to do about the [0], which is there for dropping effects
        return pickle.loads(result.pickled_result)[0]
    else:
        # TODO make better error messages
        raise ValueError(f"Error: {result.state}")


def run_function_remote(
    function: Callable[..., _T],
    host: str,
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    environment_variables: Optional[Dict[str, str]] = None,
    # these options are forwarded directly to Fabric
    fabric_kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    """
    Same as run_function_async, but runs on a remote machine, specified by "host".
    Connects to the remote machine over SSH via the fabric library
    https://www.fabfile.org/ fabric_kwargs are passed directly to fabric.Connection().

    The remote machine must have meadowgrid installed as per build_meadowgrid_amis.md
    """

    job_to_run = _construct_job_to_run_function(
        function,
        interpreter_deployment,
        code_deployment,
        args,
        kwargs,
        environment_variables,
    )

    if fabric_kwargs is None:
        fabric_kwargs = {}

    with fabric.Connection(host, **fabric_kwargs) as connection:
        # assumes that meadowgrid is installed in /meadowgrid/env as per
        # build_meadowgrid_amis.md. Also uses the default working_folder, which should
        # (but doesn't strictly need to) correspond to agent._set_up_working_folder
        home_result = connection.run("echo $HOME")
        if not home_result.ok:
            raise ValueError(
                "Error getting home directory on remote machine " + home_result.stderr
            )

        remote_working_folder = f"{home_result.stdout.strip()}/meadowgrid"
        job_io_prefix = f"{remote_working_folder}/io/{job_to_run.job.job_id}"
        remote_job_to_run_path = f"{job_io_prefix}.job_to_run"

        # serialize job_to_run and send it to the remote machine
        with io.BytesIO(job_to_run.SerializeToString()) as job_to_run_serialized:
            connection.put(job_to_run_serialized, remote=remote_job_to_run_path)

        # use meadowrun to run the job
        result = connection.run(
            "/meadowgrid/env/bin/meadowrun "
            f"--serialized-job-to-run-path {remote_job_to_run_path} "
            f"--working-folder {remote_working_folder}"
        )

        # TODO consider using result.tail, result.stdout, result.stderr

        # copied/adapted from meadowgrid.agent._completed_job_state

        # see if we got a normal return code
        if result.return_code != 0:
            raise ValueError(f"Process exited {result.return_code}")

        # get the state
        with io.BytesIO() as state_buffer:
            connection.get(f"{job_io_prefix}.state", state_buffer)
            state_buffer.seek(0)
            with io.TextIOWrapper(state_buffer, encoding="utf-8") as wrapper:
                state_string = wrapper.read()

        # Next get the result. The result file is optional for py_commands because we
        # don't have full control over the process and there's no way to guarantee that
        # "our code" gets executed
        with io.BytesIO() as result_buffer:
            connection.get(f"{job_io_prefix}.result", result_buffer)
            result_buffer.seek(0)
            result = pickle.load(result_buffer)

        if state_string == "SUCCEEDED":
            return result[0]  # drop effects
        elif state_string == "PYTHON_EXCEPTION":
            raise ValueError("Python exception in remote process") from result
        else:
            raise ValueError(f"Unknown state string: {state_string}")
