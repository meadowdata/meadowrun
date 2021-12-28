from __future__ import annotations

import collections
import dataclasses
import itertools
import pickle
import random
import traceback
from typing import Dict, List, Union, Iterable, Optional, Sequence, Tuple

import grpc.aio

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
from meadowgrid.meadowgrid_pb2 import (
    AddCredentialsRequest,
    AddCredentialsResponse,
    AddJobResponse,
    AddTasksToGridJobRequest,
    Credentials,
    GridTask,
    GridTaskState,
    GridTaskStates,
    GridTaskStatesRequest,
    GridTaskUpdateAndGetNextRequest,
    Job,
    JobStateUpdates,
    JobStatesRequest,
    NextJobRequest,
    NextJobResponse,
    ProcessState,
    ProcessStates,
    Resource,
    UpdateStateResponse,
)
from meadowgrid.meadowgrid_pb2_grpc import (
    MeadowGridCoordinatorServicer,
    add_MeadowGridCoordinatorServicer_to_server,
)


@dataclasses.dataclass
class _SimpleJob:
    """
    Keeps track of the state of a simple job. Simple jobs (unlike grid jobs) only
    require running a single command or function. Job.py_command or Job.py_function will
    be populated.

    Coordinator server-side equivalent to MeadowGridDeployedRunnable
    """

    job: Job
    state: ProcessState


@dataclasses.dataclass
class _GridJob:
    """
    Keeps track of the state of a grid job. Grid jobs have many "tasks" that will get
    run with the same function in the same process as other tasks from that same grid
    job. Job.py_grid will be populated.
    """

    job: Job

    # all tasks that have been added to this grid job so far, indexed by
    # _GridTask.task_id
    all_tasks: Dict[int, _GridTask]
    # Tasks that have not yet been assigned. This points to the same _GridTask objects
    # as all_tasks.
    unassigned_tasks: collections.deque[_GridTask]
    # Indicates whether all tasks have been added or not
    all_tasks_added: bool
    # The number of grid_workers currently working on this grid job. (A single
    # job_worker can spawn multiple grid_workers.)
    num_current_grid_workers: int


@dataclasses.dataclass
class _GridTask:
    """
    Keeps track of the state of a task within a grid job. Not to be confused with
    GridTask which is a protobuf message type.
    """

    task_id: int
    pickled_function_arguments: bytes
    state: ProcessState


def _add_tasks_to_grid_job(grid_job: _GridJob, tasks: Iterable[GridTask]) -> None:
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

            grid_task = _GridTask(
                task_request.task_id,
                task_request.pickled_function_arguments,
                ProcessState(state=ProcessState.ProcessStateEnum.RUN_REQUESTED),
            )
            grid_job.all_tasks[task_request.task_id] = grid_task
            grid_job.unassigned_tasks.append(grid_task)
        else:
            print(
                f"Ignoring duplicate task in job {grid_job.job.job_id} task "
                f"{task_request.task_id}"
            )


def _resources_required_are_available(
    required: Sequence[Resource], available: Dict[str, float]
) -> bool:
    for r in required:
        # special consideration--requiring 0 of a resource is considered the same as not
        # requiring it at all
        if r.value > 0 and (r.name not in available or available[r.name] < r.value):
            return False

    return True


class MeadowGridCoordinatorHandler(MeadowGridCoordinatorServicer):
    """
    The meadowgrid coordinator is effectively a job queue. Clients (e.g. meadowflow,
    users, etc.) will add jobs to the queue with add_job and get results with
    get_simple_job_states and get_grid_task_states. Meanwhile meadowgrid.job_workers
    will call get_next_job so that they can work on jobs and send results to the
    coordinator with update_job_states.

    Also see MeadowGridCoordinatorClientAsync and
    MeadowGridCoordinatorClientForWorkersAsync
    """

    # TODO we don't have any locks because we don't have any awaits, so we know that
    #  each function will always run without interruption. We might need a different
    #  model for improved performance at some point.

    def __init__(self):
        # maps job_id -> _GridJob
        self._grid_jobs: Dict[str, _GridJob] = {}
        # maps job_id -> _SimpleJob
        self._simple_jobs: Dict[str, _SimpleJob] = {}

        # TODO at some point we should remove completed and queried grid jobs from these
        #  lists

        # maps service -> (service_url, credentials)
        self._credentials_dict: CredentialsDict = {}

    async def _resolve_deployments(self, job: Job) -> Job:
        """
        Modifies job in place!!!

        Resolves non-deterministic deployments like GitRepoBranch and ContainerAtTag.
        It's important that we do this here instead of later in the job worker for grid
        jobs, otherwise we would run the risk of having different tasks running with
        different versions. For simple jobs, that's not as important, but I think it's
        still better to resolve these here. It's consistent with grid jobs, and if
        performance becomes a concern it allows us to batch and cache resolution
        requests centrally.
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

    async def add_job(
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

        job_spec = request.WhichOneof("job_spec")
        if job_spec == "py_command" or job_spec == "py_function":
            self._simple_jobs[request.job_id] = _SimpleJob(
                request, ProcessState(state=ProcessState.ProcessStateEnum.RUN_REQUESTED)
            )
        elif job_spec == "py_grid":
            grid_job = _GridJob(request, {}, collections.deque(), False, 0)

            _add_tasks_to_grid_job(grid_job, request.py_grid.tasks)
            # Now that the tasks have been added to grid_job, we remove them from the
            # Job object. We're going to send this Job object to job_workers in the
            # future, and they need all of the information in Job EXCEPT for the tasks
            # which they'll request and get one by one.
            del grid_job.job.py_grid.tasks[:]

            # We have to initially set grid_job.all_tasks_added to False (regardless of
            # what the user specified), then call _add_tasks_to_grid_job which does
            # validation based on all_tasks_added, and then set all_tasks_added
            # afterwards
            grid_job.all_tasks_added = request.py_grid.all_tasks_added

            self._grid_jobs[request.job_id] = grid_job
        else:
            raise ValueError(f"Unknown job_spec {job_spec}")

        return AddJobResponse(state=AddJobResponse.AddJobState.ADDED)

    async def add_tasks_to_grid_job(
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

        return AddJobResponse()

    async def update_job_states(
        self, request: JobStateUpdates, context: grpc.aio.ServicerContext
    ) -> UpdateStateResponse:
        for job_state in request.job_states:
            if job_state.job_id in self._simple_jobs:
                # TODO we should probably make it so that the state of the job can't
                #  "regress", e.g. go from SUCCEEDED to RUNNING.
                self._simple_jobs[job_state.job_id].state = job_state.process_state
            elif job_state.job_id in self._grid_jobs:
                # TODO some updates are redundant and can be ignored/turned off like
                #  RUNNING, but RUN_REQUEST_FAILED (and possibly others) should be
                #  handled correctly.
                if (
                    job_state.process_state.state
                    == ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED
                ):
                    error = pickle.loads(job_state.process_state.pickled_result)
                else:
                    error = ""
                print(
                    f"Got an update for a grid job {job_state.process_state.state}. "
                    + " ".join(
                        [
                            str(error),
                            str(job_state.process_state.pid),
                            job_state.process_state.container_id,
                            job_state.process_state.log_file_name,
                        ]
                    )
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

    async def get_next_job(
        self, request: NextJobRequest, context: grpc.aio.ServicerContext
    ) -> NextJobResponse:
        # the resources available on the job worker that's asking for a new job
        resources_available = {a.name: a.value for a in request.resources_available}

        # TODO we should let users know if there's a job that won't run because its
        #  required resources aren't available on any worker.
        # TODO we should also consider thinking about "conserving resources" when we
        #  assign jobs. E.g. if we have one worker with 1 CPU, and a second worker with
        #  16 CPUs, it seems strictly better to assign a job that only requires 1 CPU to
        #  the first worker. That way if a job comes along that requires 16 CPUs, we are
        #  able to send it to the second worker. Another related case is if we just have
        #  the 16 CPU machine, it's possible to get bad behavior if we have many
        #  low-priority jobs that require 1 CPU, and then a high-priority job requiring
        #  16 CPUs comes along. Because we're constantly trying to schedule jobs, unless
        #  all the low-priority jobs finish at the exact same time, we'll never "see"
        #  the 16 available CPUs to run the high-priority job

        # get grid_jobs where the number of tasks left to run is greater
        # than the number of workers currently working
        # TODO this could be way more sophisticated, e.g. estimating how long it takes
        #  to set up a new worker vs how long the existing workers would be able to
        #  finish the outstanding tasks. E.g. maybe this should be len(job.tasks_to_run)
        #  > job.num_current_workers_in_setup_phase
        available_grid_jobs = (
            job
            for job in self._grid_jobs.values()
            if len(job.unassigned_tasks) > job.num_current_grid_workers
            and _resources_required_are_available(
                job.job.resources_required, resources_available
            )
        )
        # get simple_jobs not being worked on
        available_simple_jobs = (
            job
            for job in self._simple_jobs.values()
            if job.state.state == ProcessState.ProcessStateEnum.RUN_REQUESTED
            and _resources_required_are_available(
                job.job.resources_required, resources_available
            )
        )
        available_jobs: List[Union[_GridJob, _SimpleJob]] = list(
            itertools.chain(available_grid_jobs, available_simple_jobs)
        )

        if len(available_jobs) > 0:
            # See the docstring on Job.priority in meadowgrid.proto for how jobs get
            # selected.

            job = random.choices(
                available_jobs, [job.job.priority for job in available_jobs]
            )[0]
            if isinstance(job, _GridJob):
                # TODO we need a way to decrement this even if the job_worker or
                #  grid_worker don't exit cleanly
                job.num_current_grid_workers += 1
            elif isinstance(job, _SimpleJob):
                # TODO we need a way to update this if the job_worker doesn't exit
                #  cleanly so that we're not in a running state forever
                job.state = ProcessState(state=ProcessState.ProcessStateEnum.ASSIGNED)
            else:
                raise ValueError(f"Unexpected type of job {type(job)}")

            (
                code_deployment_credentials,
                interpreter_deployment_credentials,
            ) = self._get_credentials_for_job(job.job)

            return NextJobResponse(
                job=job.job,
                code_deployment_credentials=code_deployment_credentials,
                interpreter_deployment_credentials=interpreter_deployment_credentials,
            )
        else:
            return NextJobResponse()

    async def update_grid_task_state_and_get_next(
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

            grid_job.all_tasks[request.task_id].state = request.process_state

        # if we have any work left to do on this job, assign it
        # TODO consider to not returning another task even if there are tasks remaining
        #  in the scenario where all the workers are busy with tasks for a relatively
        #  unimportant job, and a new important job comes in that can't get any tasks
        if len(grid_job.unassigned_tasks) > 0:
            chosen_task = grid_job.unassigned_tasks.popleft()
            # TODO deal with case where worker never returns--we don't want tasks to
            #  disappear forever
            return GridTask(
                task_id=chosen_task.task_id,
                pickled_function_arguments=chosen_task.pickled_function_arguments,
            )
        else:
            # TODO we should use worker ids instead to make sure we don't double
            #  decrement, rather than this big hack
            grid_job.num_current_grid_workers = max(
                0, grid_job.num_current_grid_workers - 1
            )
            return GridTask(task_id=-1)

    async def get_simple_job_states(
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

    async def get_grid_task_states(
        self, request: GridTaskStatesRequest, context: grpc.aio.ServicerContext
    ) -> GridTaskStates:
        if request.job_id not in self._grid_jobs:
            raise ValueError(f"grid job_id {request.job_id} does not exist")

        job = self._grid_jobs[request.job_id]

        # TODO performance would probably be better with a merge sort kind of thing,
        #  would require that task_ids are always sorted
        task_ids_to_ignore = set(request.task_ids_to_ignore)

        return GridTaskStates(
            task_states=[
                GridTaskState(task_id=task.task_id, process_state=task.state)
                for task in job.all_tasks.values()
                if task.task_id not in task_ids_to_ignore
            ]
        )

    async def add_credentials(
        self, request: AddCredentialsRequest, context: grpc.aio.ServicerContext
    ) -> AddCredentialsResponse:
        self._credentials_dict.setdefault(request.service, []).append(
            (request.service_url, getattr(request, request.WhichOneof("source")))
        )
        return AddCredentialsResponse()


async def start_meadowgrid_coordinator(
    host: str, port: int, meadowflow_address: Optional[str]
) -> None:
    """
    Runs the meadowgrid coordinator server.

    If meadowflow_address is provided, this process will try to register itself with the
    meadowflow server at that address.
    """

    server = grpc.aio.server()
    add_MeadowGridCoordinatorServicer_to_server(MeadowGridCoordinatorHandler(), server)
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    await server.start()

    if meadowflow_address is not None:
        # TODO this is a little weird that we're taking a dependency on the meadowflow
        #  code
        import meadowflow.server.client

        async with meadowflow.server.client.MeadowFlowClientAsync(
            meadowflow_address
        ) as c:
            await c.register_job_runner("meadowgrid", address)

    try:
        await server.wait_for_termination()
    finally:
        # Shuts down the server with 5 seconds of grace period. During the grace period,
        # the server won't accept new connections and allow existing RPCs to continue
        # within the grace period.
        await server.stop(5)
