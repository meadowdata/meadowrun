import pickle
from typing import Iterable, Dict, Sequence, Tuple, Any, Optional, Literal

import grpc
import grpc.aio
import grpc.aio

from meadowgrid.config import DEFAULT_COORDINATOR_ADDRESS, JOB_ID_VALID_CHARACTERS
from meadowgrid.deployed_function import (
    Deployment,
    MeadowGridDeployedCommand,
    MeadowGridDeployedFunction,
    MeadowGridFunction,
    MeadowGridFunctionName,
)
from meadowgrid.meadowgrid_pb2 import (
    AddJobResponse,
    AddTasksToGridJobRequest,
    GridTaskUpdateAndGetNextRequest,
    GitRepoCommit,
    GridTask,
    GridTaskState,
    GridTaskStatesRequest,
    Job,
    JobStateUpdate,
    JobStateUpdates,
    JobStatesRequest,
    NextJobRequest,
    ProcessState,
    PyCommandJob,
    PyFunctionJob,
    PyGridJob,
    QualifiedFunctionName,
    ServerAvailableFolder,
    StringPair,
)
from meadowgrid.meadowgrid_pb2_grpc import MeadowGridCoordinatorStub

# make this enum available for users
ProcessStateEnum = ProcessState.ProcessStateEnum


def _make_valid_job_id(job_id: str) -> str:
    return "".join(c for c in job_id if c in JOB_ID_VALID_CHARACTERS)


def _string_pairs_from_dict(d: Dict[str, str]) -> Iterable[StringPair]:
    """
    Opposite of _string_pairs_to_dict in job_worker.py. Helper for dicts in protobuf.
    """
    if d is not None:
        for key, value in d.items():
            yield StringPair(key=key, value=value)


def _add_deployment_to_job(job: Job, deployment: Deployment) -> None:
    """
    Think of this as job.deployment = deployment, but it's complicated because it's
    a protobuf oneof
    """
    if isinstance(deployment, ServerAvailableFolder):
        job.server_available_folder.CopyFrom(deployment)
    elif isinstance(deployment, GitRepoCommit):
        job.git_repo_commit.CopyFrom(deployment)
    else:
        raise ValueError(f"Unknown deployment type {type(deployment)}")


def _pickle_protocol_for_deployed_interpreter() -> int:
    """
    This is a placeholder, the intention is to get the deployed interpreter's version
    somehow from the Deployment object or something like it and use that to determine
    what the highest pickle protocol version we can use safely is.
    """

    # TODO just hard-coding the interpreter version for now, need to actually grab it
    #  from the deployment somehow
    interpreter_version = (3, 8, 0)

    # based on documentation in
    # https://docs.python.org/3/library/pickle.html#data-stream-format
    if interpreter_version >= (3, 8, 0):
        protocol = 5
    elif interpreter_version >= (3, 4, 0):
        protocol = 4
    elif interpreter_version >= (3, 0, 0):
        protocol = 3
    else:
        # TODO support for python 2 would require dealing with the string/bytes issue
        raise NotImplementedError("We currently only support python 3")

    return min(protocol, pickle.HIGHEST_PROTOCOL)


def _create_py_command_job(
    job_id: str,
    job_friendly_name: str,
    deployed_command: MeadowGridDeployedCommand,
    priority: int,
) -> Job:
    # TODO see below about optimizations we could do for transferring pickled data
    if deployed_command.context_variables:
        pickled_context_variables = pickle.dumps(
            deployed_command.context_variables,
            protocol=_pickle_protocol_for_deployed_interpreter(),
        )
    else:
        pickled_context_variables = None

    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(job_friendly_name),
        priority=priority,
        environment_variables=_string_pairs_from_dict(
            deployed_command.environment_variables
        ),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_command=PyCommandJob(
            command_line=deployed_command.command_line,
            pickled_context_variables=pickled_context_variables,
        ),
    )
    _add_deployment_to_job(job, deployed_command.deployment)
    return job


def _create_py_function(
    meadowgrid_function: MeadowGridFunction, pickle_protocol: int
) -> PyFunctionJob:
    """
    Returns a PyFunctionJob, called by _create_py_func_job which creates a Job that has a
    PyFunctionJob in it.

    pickle_protocol should be the highest pickle protocol that the deployed function
    will be able to understand.
    """

    # first pickle the function arguments from job_run_spec

    # TODO add support for compressions, pickletools.optimize, possibly cloudpickle?

    # TODO also add the ability to write this to a shared location so that we don't need
    #  to pass it through the server.

    if meadowgrid_function.function_args or meadowgrid_function.function_kwargs:
        pickled_function_arguments = pickle.dumps(
            (meadowgrid_function.function_args, meadowgrid_function.function_kwargs),
            protocol=pickle_protocol,
        )
    else:
        pickled_function_arguments = None

    # then, construct the PyFunctionJob
    py_function = PyFunctionJob(pickled_function_arguments=pickled_function_arguments)

    function_spec = meadowgrid_function.function_spec
    if isinstance(function_spec, MeadowGridFunctionName):
        py_function.qualified_function_name.CopyFrom(
            QualifiedFunctionName(
                module_name=function_spec.module_name,
                function_name=function_spec.function_name,
            )
        )
    elif isinstance(function_spec, bytes):
        py_function.pickled_function = function_spec
    else:
        raise ValueError(f"Unknown type of function_spec {type(function_spec)}")

    return py_function


def _create_py_func_job(
    job_id: str,
    job_friendly_name: str,
    deployed_function: MeadowGridDeployedFunction,
    priority: int,
) -> Job:
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(job_friendly_name),
        priority=priority,
        environment_variables=_string_pairs_from_dict(
            deployed_function.environment_variables
        ),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_function=_create_py_function(
            deployed_function.meadowgrid_function,
            _pickle_protocol_for_deployed_interpreter(),
        ),
    )
    _add_deployment_to_job(job, deployed_function.deployment)
    return job


def _create_py_grid_job(
    job_id: str,
    job_friendly_name: str,
    deployed_function: MeadowGridDeployedFunction,
    tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
    all_tasks_added: bool,
    priority: int,
) -> Job:
    pickle_protocol = _pickle_protocol_for_deployed_interpreter()
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(job_friendly_name),
        priority=priority,
        environment_variables=_string_pairs_from_dict(
            deployed_function.environment_variables
        ),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_grid=PyGridJob(
            function=_create_py_function(
                deployed_function.meadowgrid_function, pickle_protocol
            ),
            tasks=_create_task_requests(tasks, pickle_protocol),
            all_tasks_added=all_tasks_added,
        ),
    )
    _add_deployment_to_job(job, deployed_function.deployment)
    return job


def _create_task_requests(
    tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]], pickle_protocol: int
) -> Sequence[GridTask]:
    """
    tasks should be a list of (task_id, args, kwargs)

    pickle_protocol should be the highest pickle protocol that the deployed function
    will be able to understand.
    """

    return [
        GridTask(
            task_id=task_id,
            pickled_function_arguments=pickle.dumps(
                (args, kwargs), protocol=pickle_protocol
            ),
        )
        for task_id, args, kwargs in tasks
    ]


AddJobState = Literal["ADDED", "IS_DUPLICATE"]


def _add_job_state_string(state: AddJobResponse) -> AddJobState:
    if state.state == AddJobResponse.AddJobState.ADDED:
        return "ADDED"
    elif state.state == AddJobResponse.AddJobState.IS_DUPLICATE:
        return "IS_DUPLICATE"
    else:
        raise ValueError(f"Unknown AddJobState {state.state}")


class MeadowGridCoordinatorClientAsync:
    """
    A client for MeadowGridCoordinator for "users" of the system. Effectively allows
    users to add jobs i.e. request that jobs get run, and then poll for their status.

    See also MeadowGridCoordinatorHandler docstring.
    """

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.aio.insecure_channel(address)
        self._stub = MeadowGridCoordinatorStub(self._channel)

    async def add_py_command_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_command: MeadowGridDeployedCommand,
        priority: int = 100,
    ) -> AddJobState:
        """
        Requests a run of the specified command in the context of a python environment
        on a meadowgrid worker. See also MeadowGridDeployedCommand docstring and Job in
        meadowgrid.proto.

        Return value will either be ADDED (success) or IS_DUPLICATE, indicating that
        the job_id has already been used.
        """
        return _add_job_state_string(
            await self._stub.add_job(
                _create_py_command_job(
                    job_id, job_friendly_name, deployed_command, priority
                )
            )
        )

    async def add_py_func_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: MeadowGridDeployedFunction,
        priority: int = 100,
    ) -> AddJobState:
        """
        Requests a run of the specified function on a meadowgrid worker. See also
        MeadowGridDeployedFunction docstring and Job in meadowgrid.proto.

        Return value will either be ADDED (success) or IS_DUPLICATE, indicating that
        the job_id has already been used.
        """
        return _add_job_state_string(
            await self._stub.add_job(
                _create_py_func_job(
                    job_id, job_friendly_name, deployed_function, priority
                )
            )
        )

    async def add_py_grid_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: MeadowGridDeployedFunction,
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
        priority: int = 100,
    ) -> AddJobState:
        """
        Creates a grid job. See also MeadowGridDeployedFunction, Job in
        meadowgrid.proto, and grid_map.

        If the request contains multiple tasks with the same id, only the first one
        will be taken and subsequent tasks will be ignored.
        """
        return _add_job_state_string(
            await self._stub.add_job(
                _create_py_grid_job(
                    job_id,
                    job_friendly_name,
                    deployed_function,
                    tasks,
                    all_tasks_added,
                    priority,
                )
            )
        )

    async def add_tasks_to_grid_job(
        self,
        job_id: str,
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
    ) -> None:
        """
        Adds tasks to an existing grid job

        Once all_tasks_added is set to True, no more tasks can be added to that grid
        job.

        If we try to add tasks with the same task id more than once, subsequent
        requests will be ignored silently. This applies within the same request also.
        """
        await self._stub.add_tasks_to_grid_job(
            AddTasksToGridJobRequest(
                job_id=job_id,
                # TODO we should get the highest pickle protocol from the deployment
                #  somehow...
                tasks=_create_task_requests(tasks, pickle.HIGHEST_PROTOCOL),
                all_tasks_added=all_tasks_added,
            )
        )

    async def get_simple_job_states(
        self, job_ids: Sequence[str]
    ) -> Sequence[ProcessState]:
        """
        Gets the states and results for the jobs corresponding to the specified
        job_ids. Will return one ProcessState for each job_id in the same order.

        See also ProcessStateEnum in meadowgrid.proto.

        TODO add the ability to send results back to a shared location so that we don't
         need to pass through the results through the server

        TODO consider adding the ability for the client to optionally register for a
         callback/push notification? Even if we do, though, polling will be important
         for clients that want to run jobs without starting a server for themselves.
        """
        return (
            await self._stub.get_simple_job_states(JobStatesRequest(job_ids=job_ids))
        ).process_states

    async def get_grid_task_states(
        self, job_id: str, task_ids_to_ignore: Sequence[int]
    ) -> Sequence[GridTaskState]:
        """
        Gets the states and results for the tasks in the specified grid job.
        task_ids_to_ignore tells the server to not send back results for those task_ids
        (presumably because we have the results already)
        """
        return (
            await self._stub.get_grid_task_states(
                GridTaskStatesRequest(
                    job_id=job_id, task_ids_to_ignore=task_ids_to_ignore
                )
            )
        ).task_states

    async def __aenter__(self):
        await self._channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)


class MeadowGridCoordinatorClientSync:
    """The non-async version of MeadowGridCoordinatorClientAsync"""

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.insecure_channel(address)
        self._stub = MeadowGridCoordinatorStub(self._channel)

    def add_py_command_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_command: MeadowGridDeployedCommand,
        priority: int = 100,
    ) -> AddJobState:
        return _add_job_state_string(
            self._stub.add_job(
                _create_py_command_job(
                    job_id, job_friendly_name, deployed_command, priority
                )
            )
        )

    def add_py_func_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: MeadowGridDeployedFunction,
        priority: int = 100,
    ) -> AddJobState:
        return _add_job_state_string(
            self._stub.add_job(
                _create_py_func_job(
                    job_id, job_friendly_name, deployed_function, priority
                )
            )
        )

    def add_py_grid_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: MeadowGridDeployedFunction,
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
        priority: int = 100,
    ) -> AddJobState:
        return _add_job_state_string(
            self._stub.add_job(
                _create_py_grid_job(
                    job_id,
                    job_friendly_name,
                    deployed_function,
                    tasks,
                    all_tasks_added,
                    priority,
                )
            )
        )

    def add_tasks_to_grid_job(
        self,
        job_id: str,
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
    ) -> None:
        self._stub.add_tasks_to_grid_job(
            AddTasksToGridJobRequest(
                job_id=job_id,
                tasks=_create_task_requests(tasks, pickle.HIGHEST_PROTOCOL),
                all_tasks_added=all_tasks_added,
            )
        )

    def get_simple_job_states(self, job_ids: Sequence[str]) -> Sequence[ProcessState]:
        return self._stub.get_simple_job_states(
            JobStatesRequest(job_ids=job_ids)
        ).process_states

    def get_grid_task_states(
        self, job_id: str, task_ids_to_ignore: Sequence[int]
    ) -> Sequence[GridTaskState]:
        return self._stub.get_grid_task_states(
            GridTaskStatesRequest(job_id=job_id, task_ids_to_ignore=task_ids_to_ignore)
        ).task_states

    def __enter__(self):
        self._channel.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._channel.__exit__(exc_type, exc_val, exc_tb)


class MeadowGridCoordinatorClientForWorkersAsync:
    """
    Talks to the same MeadowGridCoordinator server as MeadowGridCoordinatorClientAsync,
    but only has the functions needed by the workers. The separation is just for keeping
    the code organized.
    """

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.aio.insecure_channel(address)
        self._stub = MeadowGridCoordinatorStub(self._channel)

    async def update_job_states(self, job_states: Iterable[JobStateUpdate]) -> None:
        """
        Updates the coordinator that the specified jobs have entered the specified
        state.
        """
        await self._stub.update_job_states(JobStateUpdates(job_states=job_states))

    async def get_next_job(self) -> Job:
        """
        Gets a job that the current worker should work on. If there are no jobs to work
        on, bool(Job.job_id) will be false.
        """
        return await self._stub.get_next_job(NextJobRequest())

    async def update_grid_task_state_and_get_next(
        self, job_id: str, task_state: Optional[Tuple[int, ProcessState]]
    ) -> GridTask:
        """
        task_state can either be None or (task_id, process_state).

        If task_state is not None, we update the coordinator that the specified task in
        the specified grid job has entered the specified state. If task_state is None,
        we use task_id=-1 to represent that we don't have an update.

        At the same time, this requests the next task from the coordinator for the
        specified grid job. If there is no next task in the specified grid job,
        GridTask.task_id will be -1.

        The coordinator cannot explicitly tell the grid_worker to switch to a different
        job, it can only choose to give it a task or not give it another task from the
        current grid job.
        """

        if task_state is not None:
            task_state = GridTaskUpdateAndGetNextRequest(
                job_id=job_id, task_id=task_state[0], process_state=task_state[1]
            )
        else:
            task_state = GridTaskUpdateAndGetNextRequest(job_id=job_id, task_id=-1)

        return await self._stub.update_grid_task_state_and_get_next(task_state)

    async def __aenter__(self):
        await self._channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)


class MeadowGridCoordinatorClientForWorkersSync:
    """The non-async version of MeadowGridCoordinatorClientForWorkersAsync"""

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.insecure_channel(address)
        self._stub = MeadowGridCoordinatorStub(self._channel)

    def update_job_states(self, job_states: Iterable[JobStateUpdate]) -> None:
        self._stub.update_job_states(JobStateUpdates(job_states=job_states))

    def get_next_job(self) -> Job:
        return self._stub.get_next_job(NextJobRequest())

    def update_grid_task_state_and_get_next(
        self, job_id: str, task_state: Optional[Tuple[int, ProcessState]]
    ) -> GridTask:
        # job_id is always required
        if task_state is not None:
            task_state = GridTaskUpdateAndGetNextRequest(
                job_id=job_id, task_id=task_state[0], process_state=task_state[1]
            )
        else:
            task_state = GridTaskUpdateAndGetNextRequest(job_id=job_id, task_id=-1)

        return self._stub.update_grid_task_state_and_get_next(task_state)

    def __enter__(self):
        self._channel.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._channel.__exit__(exc_type, exc_val, exc_tb)
