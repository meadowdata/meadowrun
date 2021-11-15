import asyncio
import pickle
import time
import uuid
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)

import cloudpickle

from meadowgrid.config import DEFAULT_COORDINATOR_HOST, DEFAULT_COORDINATOR_PORT
from meadowgrid.coordinator_client import (
    MeadowGridCoordinatorClientAsync,
    MeadowGridCoordinatorClientSync,
)
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.meadowgrid_pb2 import ProcessState, ServerAvailableFolder

_T = TypeVar("_T")
_U = TypeVar("_U")


# Caches of all coordinator clients
_coordinator_clients_async: Dict[str, MeadowGridCoordinatorClientAsync] = {}
_coordinator_clients_sync: Dict[str, MeadowGridCoordinatorClientSync] = {}


def _get_coordinator_client_async(address: str) -> MeadowGridCoordinatorClientAsync:
    """Get the cached coordinator client"""
    if address in _coordinator_clients_async:
        client = _coordinator_clients_async[address]
    else:
        client = MeadowGridCoordinatorClientAsync(address)
        _coordinator_clients_async[address] = client
    return client


def _get_coordinator_client_sync(address: str) -> MeadowGridCoordinatorClientSync:
    """Get the cached coordinator client"""
    if address in _coordinator_clients_sync:
        client = _coordinator_clients_sync[address]
    else:
        client = MeadowGridCoordinatorClientSync(address)
        _coordinator_clients_sync[address] = client
    return client


# TODO This should live elsewhere...
_COMPLETED_PROCESS_STATES = {
    ProcessState.ProcessStateEnum.SUCCEEDED,
    ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
    ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
    ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
    ProcessState.ProcessStateEnum.CANCELLED,
    ProcessState.ProcessStateEnum.ERROR_GETTING_STATE,
    ProcessState.ProcessStateEnum.UNKNOWN,
}


def grid_map(
    function: Callable[[_T], _U],
    args: Iterable[_T],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    coordinator_host: str = DEFAULT_COORDINATOR_HOST,
    coordinator_port: int = DEFAULT_COORDINATOR_PORT,
) -> Sequence[_U]:
    """
    The equivalent of map(function, args) but runs distributed on meadowgrid
    """

    # TODO the grid_map API should be significantly more sophisticated, supporting
    #  asyncio and running tasks even while other tasks are being generated, as well as
    #  returning Future-like objects that give more detailed information about failures

    client = _get_coordinator_client_sync(f"{coordinator_host}:{coordinator_port}")

    # prepare the job

    job_id = str(uuid.uuid4())

    friendly_name = getattr(function, "__name__", "")
    if not friendly_name:
        friendly_name = "lambda"

    pickled_function = cloudpickle.dumps(function)
    # TODO larger functions should get copied to S3/filesystem instead of sent directly
    print(f"Size of pickled function is {len(pickled_function)}")

    # add_py_grid_job expects (task_id, args, kwargs)
    args = [(i, (arg,), {}) for i, arg in enumerate(args)]

    # TODO potentially add sync interfaces for these functions
    if isinstance(code_deployment, VersionedCodeDeployment):
        code_deployment = asyncio.run(code_deployment.get_latest())
    elif code_deployment is None:
        code_deployment = ServerAvailableFolder()

    if isinstance(interpreter_deployment, VersionedInterpreterDeployment):
        interpreter_deployment = asyncio.run(interpreter_deployment.get_latest())

    # add the grid job to the meadowgrid coordinator

    # TODO if the current process is itself a Job, we should pass through everything
    #  that was specified when this was launched, i.e. environment variables, priority,
    #  deployment, etc. Also think about what to pass through even if this is not itself
    #  a Job
    client.add_py_grid_job(
        job_id,
        friendly_name,
        MeadowGridDeployedRunnable(
            code_deployment,
            interpreter_deployment,
            MeadowGridFunction.from_pickled(pickled_function),
        ),
        args,
        True,
    )

    # poll the coordinator until all of the tasks are done

    # we constructed the task_ids to be contiguous integers, so task_states[task_id]
    # will store that task_id's result
    task_states: List[Optional[ProcessState]] = [None] * len(args)
    task_ids_with_results: Set[int] = set()

    while len(task_ids_with_results) < len(task_states):
        # TODO we could be more sophisticated about how often we poll to reduce load
        #  on the coordinator
        time.sleep(0.2)

        curr_task_states = client.get_grid_task_states(
            job_id, list(task_ids_with_results)
        )
        for task in curr_task_states:
            if task.process_state.state in _COMPLETED_PROCESS_STATES:
                task_states[task.task_id] = task.process_state
                task_ids_with_results.add(task.task_id)

        # TODO at some point we should time out

    failed_tasks = [
        ts for ts in task_states if ts.state != ProcessState.ProcessStateEnum.SUCCEEDED
    ]
    if len(failed_tasks) > 0:
        # TODO create a new exception type
        raise ValueError(
            f"{len(failed_tasks)} failed: " + "; ".join(str(t) for t in failed_tasks)
        )

    curr_task_states = []
    effects = []

    for task in task_states:
        r, e = pickle.loads(task.pickled_result)
        curr_task_states.append(r)
        effects.append(e)

    # TODO need to merge effects into the meadowflow.effects

    return curr_task_states
