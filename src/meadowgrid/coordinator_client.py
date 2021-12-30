from __future__ import annotations

import json
import pickle
from typing import Iterable, Dict, Sequence, Tuple, Any, Optional, Literal, List, Union

import grpc
import grpc.aio

from meadowgrid.config import (
    DEFAULT_COORDINATOR_ADDRESS,
    DEFAULT_LOGICAL_CPU_REQUIRED,
    DEFAULT_MEMORY_GB_REQUIRED,
    DEFAULT_PRIORITY,
    JOB_ID_VALID_CHARACTERS,
    LOGICAL_CPU,
    MEMORY_GB,
)
from meadowgrid.credentials import CredentialsSource, CredentialsService
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridCommand,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
    MeadowGridFunctionName,
    MeadowGridVersionedDeployedRunnable,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.meadowgrid_pb2 import (
    AddCredentialsRequest,
    AddJobResponse,
    AddTasksToGridJobRequest,
    AwsSecret,
    ContainerAtDigest,
    ContainerAtTag,
    Credentials,
    GitRepoBranch,
    GitRepoCommit,
    GridTask,
    GridTaskState,
    GridTaskStatesRequest,
    GridTaskUpdateAndGetNextRequest,
    Job,
    JobStateUpdate,
    JobStateUpdates,
    JobStatesRequest,
    NextJobRequest,
    NextJobResponse,
    ProcessState,
    PyCommandJob,
    PyFunctionJob,
    PyGridJob,
    QualifiedFunctionName,
    Resource,
    ServerAvailableContainer,
    ServerAvailableFile,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
    StringPair,
)
from meadowgrid.meadowgrid_pb2_grpc import MeadowGridCoordinatorStub

# make this enum available for users
ProcessStateEnum = ProcessState.ProcessStateEnum


def _construct_resources_required(
    resources: Optional[Dict[str, float]]
) -> Sequence[Resource]:
    """
    If resources is None, provides the defaults for resources required. If resources is
    not None, adds in the default resources if necessary. This means for default
    resources like LOGICAL_CPU and MEMORY_GB, the only way to "opt-out" of these
    resources is to explicitly set them to zero. Requiring zero of a resource is treated
    the same as not requiring that resource at all.
    """
    if resources is None:
        resources = {}

    result = _construct_resources(resources)
    if MEMORY_GB not in resources:
        result.append(Resource(name=MEMORY_GB, value=DEFAULT_MEMORY_GB_REQUIRED))
    if LOGICAL_CPU not in resources:
        result.append(Resource(name=LOGICAL_CPU, value=DEFAULT_LOGICAL_CPU_REQUIRED))
    return result


def _construct_resources(resources: Dict[str, float]) -> List[Resource]:
    """Small helper for constructing a sequence of Resource"""
    return [Resource(name=name, value=value) for name, value in resources.items()]


def _make_valid_job_id(job_id: str) -> str:
    return "".join(c for c in job_id if c in JOB_ID_VALID_CHARACTERS)


def _string_pairs_from_dict(d: Dict[str, str]) -> Iterable[StringPair]:
    """
    Opposite of _string_pairs_to_dict in job_worker.py. Helper for dicts in protobuf.
    """
    if d is not None:
        for key, value in d.items():
            yield StringPair(key=key, value=value)


def _add_deployments_to_job(
    job: Job,
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
) -> None:
    """
    Think of this as job.code_deployment = code_deployment; job.interpreter_deployment =
    interpreter_deployment, but it's complicated because these are protobuf oneofs
    """
    if isinstance(code_deployment, ServerAvailableFolder):
        job.server_available_folder.CopyFrom(code_deployment)
    elif isinstance(code_deployment, GitRepoCommit):
        job.git_repo_commit.CopyFrom(code_deployment)
    elif isinstance(code_deployment, GitRepoBranch):
        job.git_repo_branch.CopyFrom(code_deployment)
    else:
        raise ValueError(f"Unknown code deployment type {type(code_deployment)}")

    if isinstance(interpreter_deployment, ServerAvailableInterpreter):
        job.server_available_interpreter.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ContainerAtDigest):
        job.container_at_digest.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ServerAvailableContainer):
        job.server_available_container.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ContainerAtTag):
        job.container_at_tag.CopyFrom(interpreter_deployment)
    else:
        raise ValueError(
            f"Unknown interpreter deployment type {type(interpreter_deployment)}"
        )


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


def _create_py_function(
    meadowgrid_function: MeadowGridFunction, pickle_protocol: int
) -> PyFunctionJob:
    """
    Returns a PyFunctionJob, called by _create_py_runnable_job which creates a Job that
    has a PyFunctionJob in it.

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


def _create_py_runnable_job(
    job_id: str,
    job_friendly_name: str,
    deployed_runnable: Union[
        MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
    ],
    priority: float,
    resources_required: Optional[Dict[str, float]],
) -> Job:
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(job_friendly_name),
        priority=priority,
        environment_variables=_string_pairs_from_dict(
            deployed_runnable.environment_variables
        ),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        resources_required=_construct_resources_required(resources_required),
    )
    _add_deployments_to_job(
        job, deployed_runnable.code_deployment, deployed_runnable.interpreter_deployment
    )

    if isinstance(deployed_runnable.runnable, MeadowGridCommand):
        # TODO see _create_py_function about optimizations we could do for transferring
        #  pickled data
        if deployed_runnable.runnable.context_variables:
            pickled_context_variables = pickle.dumps(
                deployed_runnable.runnable.context_variables,
                protocol=_pickle_protocol_for_deployed_interpreter(),
            )
        else:
            pickled_context_variables = None

        job.py_command.CopyFrom(
            PyCommandJob(
                command_line=deployed_runnable.runnable.command_line,
                pickled_context_variables=pickled_context_variables,
            )
        )
    elif isinstance(deployed_runnable.runnable, MeadowGridFunction):
        job.py_function.CopyFrom(
            _create_py_function(
                deployed_runnable.runnable, _pickle_protocol_for_deployed_interpreter()
            )
        )
    else:
        raise ValueError(f"Unexpected runnable type {type(deployed_runnable.runnable)}")

    return job


def _create_py_grid_job(
    job_id: str,
    job_friendly_name: str,
    deployed_function: Union[
        MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
    ],
    tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
    all_tasks_added: bool,
    priority: float,
    resources_required_per_task: Optional[Dict[str, float]],
) -> Job:
    if not isinstance(deployed_function.runnable, MeadowGridFunction):
        raise ValueError("simple_job must have a MeadowGridFunction runnable")

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()
    job = Job(
        job_id=_make_valid_job_id(job_id),
        job_friendly_name=_make_valid_job_id(job_friendly_name),
        priority=priority,
        environment_variables=_string_pairs_from_dict(
            deployed_function.environment_variables
        ),
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        resources_required=_construct_resources_required(resources_required_per_task),
        py_grid=PyGridJob(
            function=_create_py_function(deployed_function.runnable, pickle_protocol),
            tasks=_create_task_requests(tasks, pickle_protocol),
            all_tasks_added=all_tasks_added,
        ),
    )
    _add_deployments_to_job(
        job, deployed_function.code_deployment, deployed_function.interpreter_deployment
    )
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


def _add_credentials_request(
    service: CredentialsService, service_url: str, source: CredentialsSource
) -> AddCredentialsRequest:
    result = AddCredentialsRequest(
        service=Credentials.Service.Value(service),
        service_url=service_url,
    )
    if isinstance(source, AwsSecret):
        result.aws_secret.CopyFrom(source)
    elif isinstance(source, ServerAvailableFile):
        result.server_available_file.CopyFrom(source)
    else:
        raise ValueError(f"Unknown type of credentials source {type(source)}")
    return result


def _grpc_retry_option(
    package: str, service: str
) -> Tuple[Literal["grpc.service_config"], str]:
    """Create a retry config.

    Args:
        package (str): package name (from proto file)
        service (str): service name (from proto file)
    """
    # https://stackoverflow.com/questions/64227270/use-retrypolicy-with-python-grpc-client
    json_config = json.dumps(
        {
            "methodConfig": [
                {
                    "name": [{"service": f"{package}.{service}"}],
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "1s",
                        "maxBackoff": "10s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["UNAVAILABLE"],
                    },
                }
            ]
        }
    )

    return ("grpc.service_config", json_config)


class MeadowGridCoordinatorClientAsync:
    """
    A client for MeadowGridCoordinator for "users" of the system. Effectively allows
    users to add jobs i.e. request that jobs get run, and then poll for their status.

    See also MeadowGridCoordinatorHandler docstring.
    """

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.aio.insecure_channel(
            address, options=[_grpc_retry_option("meadowgrid", "MeadowGridCoordinator")]
        )
        self._stub = MeadowGridCoordinatorStub(self._channel)

    async def add_py_runnable_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_runnable: Union[
            MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
        ],
        priority: float = DEFAULT_PRIORITY,
        resources_required: Optional[Dict[str, float]] = None,
    ) -> AddJobState:
        """
        Requests a run of the specified runnable in the context of a python environment
        on a meadowgrid worker. See also MeadowGridDeployedRunnable docstring and Job in
        meadowgrid.proto.

        Return value will either be ADDED (success) or IS_DUPLICATE, indicating that
        the job_id has already been used.
        """
        return _add_job_state_string(
            await self._stub.add_job(
                _create_py_runnable_job(
                    job_id,
                    job_friendly_name,
                    deployed_runnable,
                    priority,
                    resources_required,
                )
            )
        )

    async def add_py_grid_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: Union[
            MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
        ],
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
        priority: float = DEFAULT_PRIORITY,
        resources_required_per_task: Optional[Dict[str, float]] = None,
    ) -> AddJobState:
        """
        Creates a grid job. See also MeadowGridDeployedRunnable, Job in
        meadowgrid.proto, and grid_map.

        deployed_function.runnable must be a MeadowGridFunction. This is a bit hacky but
        seems okay for an internal API

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
                    resources_required_per_task,
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

    async def add_credentials(
        self, service: CredentialsService, service_url: str, source: CredentialsSource
    ) -> None:
        await self._stub.add_credentials(
            _add_credentials_request(service, service_url, source)
        )

    async def __aenter__(self) -> MeadowGridCoordinatorClientAsync:
        await self._channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)


class MeadowGridCoordinatorClientSync:
    """The non-async version of MeadowGridCoordinatorClientAsync"""

    def __init__(self, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._channel = grpc.insecure_channel(
            address, options=[_grpc_retry_option("meadowgrid", "MeadowGridCoordinator")]
        )
        self._stub = MeadowGridCoordinatorStub(self._channel)

    def add_py_runnable_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: Union[
            MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
        ],
        priority: float = DEFAULT_PRIORITY,
        resources_required: Optional[Dict[str, float]] = None,
    ) -> AddJobState:
        return _add_job_state_string(
            self._stub.add_job(
                _create_py_runnable_job(
                    job_id,
                    job_friendly_name,
                    deployed_function,
                    priority,
                    resources_required,
                )
            )
        )

    def add_py_grid_job(
        self,
        job_id: str,
        job_friendly_name: str,
        deployed_function: Union[
            MeadowGridDeployedRunnable, MeadowGridVersionedDeployedRunnable
        ],
        tasks: Sequence[Tuple[int, Sequence[Any], Dict[str, Any]]],
        all_tasks_added: bool,
        priority: float = DEFAULT_PRIORITY,
        resources_required_per_task: Optional[Dict[str, float]] = None,
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
                    resources_required_per_task,
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

    def add_credentials(
        self, service: CredentialsService, service_url: str, source: CredentialsSource
    ) -> None:
        self._stub.add_credentials(
            _add_credentials_request(service, service_url, source)
        )

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
        self._channel = grpc.aio.insecure_channel(
            address, options=[_grpc_retry_option("meadowgrid", "MeadowGridCoordinator")]
        )
        self._stub = MeadowGridCoordinatorStub(self._channel)

    async def update_job_states(self, job_states: Iterable[JobStateUpdate]) -> None:
        """
        Updates the coordinator that the specified jobs have entered the specified
        state.
        """
        await self._stub.update_job_states(JobStateUpdates(job_states=job_states))

    async def get_next_job(
        self, resources_available: Dict[str, float]
    ) -> NextJobResponse:
        """
        Gets a job that the current worker should work on. If there are no jobs to work
        on, bool(Job.job_id) will be false.
        """
        return await self._stub.get_next_job(
            NextJobRequest(
                resources_available=_construct_resources(resources_available)
            )
        )

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
        self._channel = grpc.insecure_channel(
            address, options=[_grpc_retry_option("meadowgrid", "MeadowGridCoordinator")]
        )
        self._stub = MeadowGridCoordinatorStub(self._channel)

    def update_job_states(self, job_states: Iterable[JobStateUpdate]) -> None:
        self._stub.update_job_states(JobStateUpdates(job_states=job_states))

    def get_next_job(self, resources_available: Dict[str, float]) -> NextJobResponse:
        return self._stub.get_next_job(
            NextJobRequest(
                resources_available=_construct_resources(resources_available)
            )
        )

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
