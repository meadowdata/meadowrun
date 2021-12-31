import asyncio
import pickle
import time
import uuid
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import cloudpickle

from meadowgrid.config import (
    DEFAULT_COORDINATOR_HOST,
    DEFAULT_COORDINATOR_PORT,
    DEFAULT_PRIORITY,
)
from meadowgrid.coordinator_client import (
    MeadowGridCoordinatorClientAsync,
    MeadowGridCoordinatorClientSync,
)
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridFunction,
    MeadowGridVersionedDeployedRunnable,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.meadowgrid_pb2 import ProcessState, ServerAvailableFolder

_T = TypeVar("_T")
_U = TypeVar("_U")


# Caches of all coordinator clients
_coordinator_clients_async: Dict[
    Tuple[str, asyncio.AbstractEventLoop], MeadowGridCoordinatorClientAsync
] = {}
_coordinator_clients_sync: Dict[str, MeadowGridCoordinatorClientSync] = {}


def _get_coordinator_client_async(address: str) -> MeadowGridCoordinatorClientAsync:
    """Get the cached coordinator client"""

    event_loop = asyncio.get_event_loop()
    # grpc doesn't actually call asyncio.get_event_loop(), but I assume the behavior is
    # similar...
    # https://github.com/grpc/grpc/blob/60028a82a9ec546141ef98e92655cf0dfb35180e/src/python/grpcio/grpc/aio/_channel.py#L287
    if (address, event_loop) in _coordinator_clients_async:
        client = _coordinator_clients_async[(address, event_loop)]
    else:
        client = MeadowGridCoordinatorClientAsync(address)
        _coordinator_clients_async[(address, event_loop)] = client
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


def _get_id_name_function(function: Callable[[_T], _U]) -> Tuple[str, str, bytes]:
    """Returns job_id, friendly_name, pickled_function"""
    job_id = str(uuid.uuid4())

    friendly_name = getattr(function, "__name__", "")
    if not friendly_name:
        friendly_name = "lambda"

    pickled_function = cloudpickle.dumps(function)
    # TODO larger functions should get copied to S3/filesystem instead of sent directly
    print(f"Size of pickled function is {len(pickled_function)}")

    return job_id, friendly_name, pickled_function


def grid_map(
    function: Callable[[_T], _U],
    args: Iterable[_T],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None,
    resources_required_per_task: Optional[Dict[str, float]] = None,
    priority: float = DEFAULT_PRIORITY,
    coordinator_host: str = DEFAULT_COORDINATOR_HOST,
    coordinator_port: int = DEFAULT_COORDINATOR_PORT,
) -> Sequence[_U]:
    """
    The equivalent of map(function, args) but runs distributed on meadowgrid
    """

    client = _get_coordinator_client_sync(f"{coordinator_host}:{coordinator_port}")

    # prepare the job

    job_id, friendly_name, pickled_function = _get_id_name_function(function)

    # add_py_grid_job expects (task_id, args, kwargs)
    arg_tuples: List[Tuple[int, Tuple, Dict[str, Any]]] = [
        (i, (arg,), {}) for i, arg in enumerate(args)
    ]

    # ServerAvailableFolder with no code_paths is the easiest way to express "no code
    # deployment"
    if code_deployment is None:
        code_deployment = ServerAvailableFolder()

    # add the grid job to the meadowgrid coordinator

    # TODO if the current process is itself a Job, we should pass through everything
    #  that was specified when this was launched, i.e. environment variables, priority,
    #  deployment, etc. Also think about what to pass through even if this is not itself
    #  a Job
    client.add_py_grid_job(
        job_id,
        friendly_name,
        MeadowGridVersionedDeployedRunnable(
            code_deployment,
            interpreter_deployment,
            MeadowGridFunction.from_pickled(pickled_function),
        ),
        arg_tuples,
        True,
        priority,
        resources_required_per_task,
    )

    # poll the coordinator until all of the tasks are done

    # we constructed the task_ids to be contiguous integers, so task_states[task_id]
    # will store that task_id's result
    task_states: List[Optional[ProcessState]] = [None] * len(arg_tuples)
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
        ts
        # convince mypy that all task_states are not None,
        # which is guaranteed by the while loop above.
        for ts in cast(List[ProcessState], task_states)
        if ts.state != ProcessState.ProcessStateEnum.SUCCEEDED
    ]
    if len(failed_tasks) > 0:
        # TODO create a new exception type
        raise ValueError(
            f"{len(failed_tasks)} failed: " + "; ".join(str(t) for t in failed_tasks)
        )

    result_task_states = []
    effects = []

    for task_state in task_states:
        if task_state is None:
            continue
        r, e = pickle.loads(task_state.pickled_result)
        result_task_states.append(r)
        effects.append(e)

    # TODO need to merge effects into the meadowflow.effects

    return result_task_states


async def grid_map_async(
    function: Callable[[_T], _U],
    args: Iterable[_T],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None,
    resources_required_per_task: Optional[Dict[str, float]] = None,
    priority: float = DEFAULT_PRIORITY,
    coordinator_host: str = DEFAULT_COORDINATOR_HOST,
    coordinator_port: int = DEFAULT_COORDINATOR_PORT,
) -> Sequence[Awaitable[_U]]:
    """The equivalent of map(function, args) but runs distributed on meadowgrid."""

    # Comments/todos on grid_map are not copied here, but most are also applicable here

    client = _get_coordinator_client_async(f"{coordinator_host}:{coordinator_port}")

    # prepare the job
    job_id, friendly_name, pickled_function = _get_id_name_function(function)

    # TODO given that we're starting an asyncio "background task" anyways, we should
    #  iterate args and start the job at the same time, so that if args takes a long
    #  time to iterate, we start working on the first set of tasks as soon as we're able
    #  to
    tuple_args: List[Tuple[int, Tuple, Dict[str, Any]]] = [
        (i, (arg,), {}) for i, arg in enumerate(args)
    ]

    if code_deployment is None:
        code_deployment = ServerAvailableFolder()

    # add the grid job to the meadowgrid coordinator
    await client.add_py_grid_job(
        job_id,
        friendly_name,
        MeadowGridVersionedDeployedRunnable(
            code_deployment,
            interpreter_deployment,
            MeadowGridFunction.from_pickled(pickled_function),
        ),
        tuple_args,
        True,
        priority,
        resources_required_per_task,
    )

    # poll the coordinator in the background until all of the tasks are done

    # TODO it's possible that people will want to await the results on a different event
    #  loop than the one that this function is running on
    event_loop = asyncio.get_running_loop()
    # TODO we should probably create a subclass of Future or an Awaitable that exposes
    #  metadata about the task (e.g. log file locations, pids, etc.)
    task_states: List[asyncio.Future[_U]] = [
        event_loop.create_future() for _ in tuple_args
    ]

    async def poll_for_results() -> None:
        task_ids_with_results: Set[int] = set()

        while len(task_ids_with_results) < len(task_states):
            await asyncio.sleep(0.2)

            # TODO if we're on the same event loop and talking to the same coordinator,
            #  we should consolidate into a single get_grid_task_states call for
            #  multiple jobs (also requires adding such an endpoint)
            curr_task_states = await client.get_grid_task_states(
                job_id, list(task_ids_with_results)
            )
            for task in curr_task_states:
                if task.process_state.state in _COMPLETED_PROCESS_STATES:
                    if (
                        task.process_state.state
                        != ProcessState.ProcessStateEnum.SUCCEEDED
                    ):
                        # TODO raise a better exception
                        task_states[task.task_id].set_exception(
                            ValueError("Task failed")
                        )
                    else:
                        result, effects = pickle.loads(
                            task.process_state.pickled_result
                        )
                        task_states[task.task_id].set_result(result)
                    task_ids_with_results.add(task.task_id)

    asyncio.create_task(poll_for_results())  # run in the background

    return task_states
