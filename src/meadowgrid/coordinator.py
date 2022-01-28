from __future__ import annotations

import itertools
import pickle
import traceback
from types import TracebackType
from typing import Dict, Iterable, Optional, Tuple, List, Type, Generator, Any

import grpc.aio

from meadowgrid.agent_creator import AgentCreatorType, AgentCreator
from meadowgrid.aws_integration import AwsAgentCreator
from meadowgrid.config import JOB_ID_VALID_CHARACTERS
from meadowgrid.credentials import (
    CredentialsDict,
    get_docker_credentials,
    get_matching_credentials,
)
from meadowgrid.deployed_function import (
    get_latest_code_version,
    get_latest_interpreter_version,
)
from meadowgrid.local_agent_creator import LocalAgentCreator
from meadowgrid.meadowgrid_pb2 import (
    AddCredentialsRequest,
    AddCredentialsResponse,
    AddJobResponse,
    AddTasksToGridJobRequest,
    AgentStateResponse,
    AgentStatesRequest,
    AgentStatesResponse,
    Credentials,
    GridTask,
    GridTaskStateResponse,
    GridTaskStatesRequest,
    GridTaskStatesResponse,
    GridTaskUpdateAndGetNextRequest,
    HealthCheckRequest,
    HealthCheckResponse,
    Job,
    JobStateUpdates,
    JobStatesRequest,
    JobToRun,
    NextJobsRequest,
    NextJobsResponse,
    ProcessState,
    ProcessStates,
    RegisterAgentRequest,
    RegisterAgentResponse,
    UpdateStateResponse,
)
from meadowgrid.meadowgrid_pb2_grpc import (
    MeadowGridCoordinatorServicer,
    add_MeadowGridCoordinatorServicer_to_server,
)
from meadowgrid.resource_allocation import (
    AgentState,
    GenericAgentState,
    GridJobState,
    GridTaskState,
    JobSpecificAgentState,
    JobState,
    Resources,
    SimpleJobState,
    assign_task_to_grid_worker,
    agent_available_resources_changed,
    job_num_workers_needed_changed,
    update_grid_job_state,
    update_simple_job_state,
    update_task_state,
)


def _add_tasks_to_grid_job(grid_job: GridJobState, tasks: Iterable[GridTask]) -> None:
    """
    Adds tasks to a grid job. Converts from GridTask protobuf messages to _GridTask (
    in-memory representation) and does a little validation.
    """
    for task_request in tasks:
        if task_request.task_id not in grid_job.all_tasks:
            # not ideal that we're checking this every time through the loop, but
            # shouldn't be a big deal
            if grid_job.all_tasks_added:
                raise ValueError(
                    f"Tried to add all_tasks to job {grid_job.job.job_id} after it had "
                    "already been marked as all_tasks_added"
                )

            # Negative task ids are reserved
            if task_request.task_id < 0:
                raise ValueError("task_ids cannot be negative")

            grid_task = GridTaskState(
                task_request.task_id,
                task_request.pickled_function_arguments,
            )
            grid_job.all_tasks[task_request.task_id] = grid_task
            grid_job.unassigned_tasks.append(grid_task)
        else:
            print(
                f"Ignoring duplicate task in job {grid_job.job.job_id} task "
                f"{task_request.task_id}"
            )


class MeadowGridCoordinatorHandler(MeadowGridCoordinatorServicer):
    """
    The meadowgrid coordinator is effectively a job queue. Clients (e.g. meadowflow,
    users, etc.) will add jobs to the queue with add_job and get results with
    get_simple_job_states and get_grid_task_states. Meanwhile meadowgrid.agents will
    call get_next_job so that they can work on jobs and send results to the coordinator
    with update_job_states.

    Also see MeadowGridCoordinatorClientAsync and
    MeadowGridCoordinatorClientForWorkersAsync

    In terms of implementation, the logic for "which agents should work on which jobs"
    is all in resource_allocation.py
    """

    # TODO we don't have any locks because we don't have any awaits, so we know that
    #  each function will always run without interruption. We might need a different
    #  model for improved performance at some point.

    def __init__(self, agent_creator_type: AgentCreatorType) -> None:
        self._awaited = False
        self._agent_creator_type = agent_creator_type

        # maps job_id -> _GridJob
        self._grid_jobs: Dict[str, GridJobState] = {}
        # maps job_id -> _SimpleJob
        self._simple_jobs: Dict[str, SimpleJobState] = {}

        # TODO at some point we should remove completed and queried grid jobs from these
        #  lists

        self._generic_agents: Dict[str, GenericAgentState] = {}

        # maps service -> (service_url, credentials)
        self._credentials_dict: CredentialsDict = {}

    async def __aenter__(self) -> MeadowGridCoordinatorHandler:
        if self._awaited:
            return self

        if self._agent_creator_type == "aws":
            self._agent_creator: Optional[AgentCreator] = await AwsAgentCreator(None)
        elif self._agent_creator_type == "local":
            self._agent_creator = await LocalAgentCreator()
        elif self._agent_creator_type is None:
            self._agent_creator = None
        else:
            raise ValueError(f"Invalid agent_creator_type {self._agent_creator_type}")

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    def __await__(self) -> Generator[Any, None, MeadowGridCoordinatorHandler]:
        return self.__aenter__().__await__()

    async def close(self) -> None:
        if self._agent_creator is not None:
            await self._agent_creator.close()

    async def _resolve_deployments(self, job: Job) -> None:
        """
        Modifies job in place!!!

        Resolves non-deterministic deployments like GitRepoBranch and ContainerAtTag.
        It's important that we do this here instead of later in the agent for grid jobs,
        otherwise we would run the risk of having different tasks running with different
        versions. For simple jobs, that's not as important, but I think it's still
        better to resolve these here. It's consistent with grid jobs, and if performance
        becomes a concern it allows us to batch and cache resolution requests centrally.
        """

        code_deployment = job.WhichOneof("code_deployment")
        if code_deployment == "git_repo_branch":
            # Because of the way protobuf works with oneof, setting git_repo_commit will
            # automatically unset git_repo_branch. The types are a bit sketchy here, but
            # should be okay
            job.git_repo_commit.CopyFrom(
                await get_latest_code_version(
                    job.git_repo_branch, self._credentials_dict
                )
            )

        interpreter_deployment = job.WhichOneof("interpreter_deployment")
        if interpreter_deployment == "container_at_tag":
            # see comment above about protobuf oneof behavior
            job.container_at_digest.CopyFrom(
                await get_latest_interpreter_version(
                    job.container_at_tag, self._credentials_dict
                )
            )

    async def add_job(  # type: ignore[override]
        self, request: Job, context: grpc.aio.ServicerContext
    ) -> AddJobResponse:

        # some basic validation

        if not request.job_id:
            raise ValueError("job_id must not be None/empty string")
        if any(c not in JOB_ID_VALID_CHARACTERS for c in request.job_id) or any(
            c not in JOB_ID_VALID_CHARACTERS for c in request.job_friendly_name
        ):
            raise ValueError(
                f"job_id {request.job_id} or friendly name {request.job_friendly_name} "
                "contains invalid characters. Only string.ascii_letters, numbers, ., -,"
                " and _ are permitted."
            )
        if request.job_id in self._grid_jobs or request.job_id in self._simple_jobs:
            return AddJobResponse(state=AddJobResponse.AddJobState.IS_DUPLICATE)

        if request.priority <= 0:
            raise ValueError("priority must be greater than 0")

        # resolve any non-deterministic CodeDeployment or InterpreterDeployments

        await self._resolve_deployments(request)

        # add to self.simple_jobs or self.grid_jobs

        job: JobState
        job_spec = request.WhichOneof("job_spec")
        if job_spec == "py_command" or job_spec == "py_function":
            simple_job = SimpleJobState(
                request, Resources.from_protobuf(request.resources_required)
            )
            self._simple_jobs[request.job_id] = simple_job
            job = simple_job
        elif job_spec == "py_grid":
            grid_job = GridJobState(
                request,
                Resources.from_protobuf(request.resources_required),
            )

            _add_tasks_to_grid_job(grid_job, request.py_grid.tasks)
            # Now that the tasks have been added to grid_job, we remove them from the
            # Job object. We're going to send this Job object to agents in the future,
            # and they need all of the information in Job EXCEPT for the tasks which
            # they'll request and get one by one.
            del grid_job.job.py_grid.tasks[:]

            # We have to initially set grid_job.all_tasks_added to False (regardless of
            # what the user specified), then call _add_tasks_to_grid_job which does
            # validation based on all_tasks_added, and then set all_tasks_added
            # afterwards
            grid_job.all_tasks_added = request.py_grid.all_tasks_added

            self._grid_jobs[request.job_id] = grid_job
            job = grid_job
        else:
            raise ValueError(f"Unknown job_spec {job_spec}")

        # TODO this shouldn't really block responding to the client
        await job_num_workers_needed_changed(
            job, list(self._generic_agents.values()), self._agent_creator
        )

        return AddJobResponse(state=AddJobResponse.AddJobState.ADDED)

    async def add_tasks_to_grid_job(  # type: ignore[override]
        self, request: AddTasksToGridJobRequest, context: grpc.aio.ServicerContext
    ) -> AddJobResponse:
        if request.job_id not in self._grid_jobs:
            raise ValueError(
                f"job_id {request.job_id} does not exist, so cannot add tasks to it"
            )
        job = self._grid_jobs[request.job_id]

        _add_tasks_to_grid_job(job, request.tasks)

        if request.all_tasks_added:
            job.all_tasks_added = True

        # TODO this shouldn't really block responding to the client
        await job_num_workers_needed_changed(
            job, list(self._generic_agents.values()), self._agent_creator
        )

        return AddJobResponse()

    def _all_jobs(self) -> List[JobState]:
        """Gets all jobs (simple jobs and grid jobs)"""
        return list(
            itertools.chain(self._grid_jobs.values(), self._simple_jobs.values())
        )

    async def update_job_states(  # type: ignore[override]
        self, request: JobStateUpdates, context: grpc.aio.ServicerContext
    ) -> UpdateStateResponse:
        agent = self._get_agent_state(request.agent_id, request.agent_job_id)
        any_available_resources_changed = False
        for job_state in request.job_states:
            available_resources_changed = False
            if job_state.job_id in self._simple_jobs:
                available_resources_changed = update_simple_job_state(
                    agent, self._simple_jobs[job_state.job_id], job_state.process_state
                )
            elif job_state.job_id in self._grid_jobs:
                grid_job = self._grid_jobs[job_state.job_id]
                if job_state.grid_worker_id not in grid_job.grid_workers:
                    print(
                        f"Tried to update status of job {job_state.job_id} but the "
                        f"grid_worker_id {job_state.grid_worker_id} is not known"
                    )
                available_resources_changed = update_grid_job_state(
                    agent,
                    grid_job.grid_workers[job_state.grid_worker_id],
                    grid_job,
                    job_state.process_state,
                )
            else:
                # There's not much we can do at this point--we could maybe keep these
                # and include them in get_simple_job_states?
                # TODO this indicates something really weird going on, we should log it
                #  somewhere more noticeable
                print(
                    f"Tried to update status of job {job_state.job_id} but it does not "
                    "exist, ignoring the update."
                )

            any_available_resources_changed = (
                any_available_resources_changed or available_resources_changed
            )

        # find new jobs for this agent if we have resources for it
        if any_available_resources_changed:
            if isinstance(agent, GenericAgentState):
                jobs = self._all_jobs()
            elif isinstance(agent, JobSpecificAgentState):
                jobs = [agent.job]
            else:
                raise ValueError(f"Unexpected type of AgentState {type(agent)}")
            agent_available_resources_changed(agent, jobs)

        return UpdateStateResponse()

    def _get_credentials_for_job(
        self, job: Job
    ) -> Tuple[Optional[Credentials], Optional[Credentials]]:
        """
        Returns code_deployment_credentials, interpreter_deployment_credentials

        Very important--assumes that _resolve_deployments has already been called on
        job, i.e. assumes that container_at_tag and git_repo_branch are not possible.
        """

        code_deployment_credentials, interpreter_deployment_credentials = None, None

        if job.WhichOneof("code_deployment") == "git_repo_commit":
            try:
                credentials = get_matching_credentials(
                    Credentials.Service.GIT,
                    job.git_repo_commit.repo_url,
                    self._credentials_dict,
                )
                if credentials is not None:
                    code_deployment_credentials = Credentials(
                        credentials=pickle.dumps(credentials)
                    )
            except Exception:
                # TODO ideally this would make it back to an error message for job if it
                #  eventually fails (and maybe even if it doesn't)
                print("Error trying to turn credentials source into actual credentials")
                traceback.print_exc()

        if job.WhichOneof("interpreter_deployment") == "container_at_digest":
            try:
                credentials = get_docker_credentials(
                    job.container_at_digest.repository, self._credentials_dict
                )
                if credentials is not None:
                    # TODO ideally we would use the pickle version that the remote job
                    #  can accept
                    interpreter_deployment_credentials = Credentials(
                        credentials=pickle.dumps(credentials)
                    )
            except Exception:
                print("Error trying to turn credentials source into actual credentials")
                traceback.print_exc()

        return code_deployment_credentials, interpreter_deployment_credentials

    def _get_agent_state(self, agent_id: str, job_id: Optional[str]) -> AgentState:
        """Gets the AgentState object representing the specified agent"""
        if job_id:
            return self._get_job_specific_agent_state(agent_id, job_id)
        else:
            return self._generic_agents[agent_id]

    def _get_job_specific_agent_state(
        self, agent_id: str, job_id: str
    ) -> JobSpecificAgentState:
        """JobSpecificAgentStates are stored on the relevant Job"""

        job: Optional[JobState]
        if job_id in self._simple_jobs:
            job = self._simple_jobs[job_id]
        elif job_id in self._grid_jobs:
            job = self._grid_jobs[job_id]
        else:
            raise ValueError(
                f"While looking for job-specific agent {agent_id}, job_id {job_id} was "
                f"not found"
            )

        if agent_id not in job.job_specific_agents:
            raise ValueError(
                f"Job-specific agent {agent_id} does not exist for job {job_id}"
            )

        return job.job_specific_agents[agent_id]

    async def register_agent(  # type: ignore[override]
        self, request: RegisterAgentRequest, context: grpc.aio.ServicerContext
    ) -> RegisterAgentResponse:
        if not request.agent_id:
            raise ValueError("agent_id must be non-None and non-empty")

        if request.job_id:
            # job specific agent
            self._get_job_specific_agent_state(
                request.agent_id, request.job_id
            ).has_registered = True

            # no need to call something like agent_available_resources_changed--workers
            # should have already been assigned to this agent when we decided to create
            # it

            # TODO should maybe reconcile request.resources against the existing
            # JobSpecificAgentState.total_resources
        else:
            # generic agent
            if request.agent_id in self._generic_agents:
                raise ValueError(f"agent_id {request.agent_id} already exists")

            resources = Resources.from_protobuf(request.resources)
            agent = GenericAgentState(request.agent_id, resources, resources)
            self._generic_agents[request.agent_id] = agent

            # give jobs to this agent if appropriate
            agent_available_resources_changed(agent, self._all_jobs())

        return RegisterAgentResponse()

    async def get_agent_states(  # type: ignore[override]
        self,
        request: AgentStatesRequest,
        context: grpc.ServicerContext,
    ) -> AgentStatesResponse:
        # TODO currently only returns generic agents
        return AgentStatesResponse(
            agents=[
                AgentStateResponse(
                    agent_id=agent.agent_id,
                    total_resources=agent.total_resources.to_protobuf(),
                    available_resources=agent.available_resources.to_protobuf(),
                )
                for agent in self._generic_agents.values()
            ]
        )

    async def get_next_jobs(  # type: ignore[override]
        self, request: NextJobsRequest, context: grpc.aio.ServicerContext
    ) -> NextJobsResponse:
        agent = self._get_agent_state(request.agent_id, request.job_id)

        results = []
        for job, grid_worker_id in agent.get_pending_workers():
            (
                code_deployment_credentials,
                interpreter_deployment_credentials,
            ) = self._get_credentials_for_job(job.job)
            results.append(
                JobToRun(
                    job=job.job,
                    grid_worker_id=grid_worker_id or "",
                    interpreter_deployment_credentials=interpreter_deployment_credentials,  # noqa: E501
                    code_deployment_credentials=code_deployment_credentials,
                )
            )
        return NextJobsResponse(jobs_to_run=results)

    async def update_grid_task_state_and_get_next(  # type: ignore[override]
        self,
        request: GridTaskUpdateAndGetNextRequest,
        context: grpc.aio.ServicerContext,
    ) -> GridTask:
        # See MeadowGridCoordinatorClientAsync docstring for this function and
        # GridTaskUpdateAndGetNextRequest in meadowgrid.proto

        if request.job_id not in self._grid_jobs:
            # TODO this indicates something really weird going on, we should log it
            #  somewhere more noticeable
            print(
                "update_grid_task_state_and_get_next was called for a grid job_id that "
                f"does not exist: {request.job_id} does not exist with task_id "
                f"{request.task_id} and state {request.process_state.state}"
            )
            return GridTask(task_id=-1)
        grid_job = self._grid_jobs[request.job_id]

        # update the task state if we have one
        if request.task_id != -1:
            if request.task_id not in grid_job.all_tasks:
                # TODO this indicates something really weird going on, we should log it
                #  somewhere more noticeable
                print(
                    f"Was trying to update task state for task {request.task_id} to "
                    f"state {request.process_state.state} but task does not exist"
                )
                # even though we might have more tasks, given that something really
                # weird just happened, we will tell the worker to stop working on this
                # job
                return GridTask(task_id=-1)

            update_task_state(
                grid_job.all_tasks[request.task_id], request.process_state
            )

        # if we have any work left to do on this job, assign it
        if request.grid_worker_id not in grid_job.grid_workers:
            print(
                f"Unexpected grid_worker_id {request.grid_worker_id} for job "
                f"{grid_job.job.job_id}"
            )
            return GridTask(task_id=-1)

        next_task = assign_task_to_grid_worker(
            grid_job.grid_workers[request.grid_worker_id], grid_job
        )

        if next_task is not None:
            return GridTask(
                task_id=next_task.task_id,
                pickled_function_arguments=next_task.pickled_function_arguments,
            )
        else:
            return GridTask(task_id=-1)

    async def get_simple_job_states(  # type: ignore[override]
        self, request: JobStatesRequest, context: grpc.aio.ServicerContext
    ) -> ProcessStates:
        process_states = []
        for job_id in request.job_ids:
            if job_id in self._simple_jobs:
                process_states.append(self._simple_jobs[job_id].state)
            else:
                process_states.append(
                    ProcessState(state=ProcessState.ProcessStateEnum.UNKNOWN)
                )

        return ProcessStates(process_states=process_states)

    async def get_grid_task_states(  # type: ignore[override]
        self, request: GridTaskStatesRequest, context: grpc.aio.ServicerContext
    ) -> GridTaskStatesResponse:
        if request.job_id not in self._grid_jobs:
            raise ValueError(f"grid job_id {request.job_id} does not exist")

        job = self._grid_jobs[request.job_id]

        # TODO performance would probably be better with a merge sort kind of thing,
        #  would require that task_ids are always sorted
        task_ids_to_ignore = set(request.task_ids_to_ignore)

        return GridTaskStatesResponse(
            task_states=[
                GridTaskStateResponse(task_id=task.task_id, process_state=task.state)
                for task in job.all_tasks.values()
                if task.task_id not in task_ids_to_ignore
            ]
        )

    async def add_credentials(  # type: ignore[override]
        self, request: AddCredentialsRequest, context: grpc.aio.ServicerContext
    ) -> AddCredentialsResponse:
        source = request.WhichOneof("source")
        if source is None:
            raise ValueError(
                f"AddCredentialsRequest request should have a source set: {request}"
            )
        self._credentials_dict.setdefault(request.service, []).append(
            (request.service_url, getattr(request, source))
        )
        return AddCredentialsResponse()

    async def Check(  # type: ignore[override]
        self, request: HealthCheckRequest, context: grpc.aio.ServicerContext
    ) -> HealthCheckResponse:
        if not request.service:
            return HealthCheckResponse(status=HealthCheckResponse.ServingStatus.SERVING)
        else:
            return HealthCheckResponse(status=HealthCheckResponse.ServingStatus.UNKNOWN)


async def start_meadowgrid_coordinator(
    host: str,
    port: int,
    meadowflow_address: Optional[str],
    agent_creator: AgentCreatorType,
) -> None:
    """
    Runs the meadowgrid coordinator server.

    If meadowflow_address is provided, this process will try to register itself with the
    meadowflow server at that address.
    """

    server = grpc.aio.server()
    async with MeadowGridCoordinatorHandler(agent_creator) as handler:
        add_MeadowGridCoordinatorServicer_to_server(handler, server)
        address = f"{host}:{port}"
        server.add_insecure_port(address)
        await server.start()

        if meadowflow_address is not None:
            # TODO this is a little weird that we're taking a dependency on the
            # meadowflow code
            import meadowflow.server.client

            async with meadowflow.server.client.MeadowFlowClientAsync(
                meadowflow_address
            ) as c:
                await c.register_job_runner("meadowgrid", address)

        try:
            await server.wait_for_termination()
        finally:
            # Shuts down the server with 5 seconds of grace period. During the grace
            # period, the server won't accept new connections and allow existing RPCs to
            # continue within the grace period.
            await server.stop(5)
