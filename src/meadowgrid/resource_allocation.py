"""
The functions in the module "should" be part of coordinator.py, just splitting them out
here for readability/organization of the code. The coordinator calls into this module to
figure out which jobs/tasks to assign to which agents/GridWorkers.
"""


from __future__ import annotations

import abc
import collections
import dataclasses
import random
import uuid
from typing import Dict, List, Iterable, Optional, Tuple, Sequence

from meadowgrid.config import (
    MEMORY_GB,
    LOGICAL_CPU,
    DEFAULT_MEMORY_GB_REQUIRED,
    DEFAULT_LOGICAL_CPU_REQUIRED,
)
from meadowgrid.meadowgrid_pb2 import Resource, Job, ProcessState
from meadowgrid.shared import COMPLETED_PROCESS_STATES


@dataclasses.dataclass(frozen=True)
class Resources:
    """
    Can represent both resources available (i.e. on a agent) as well as resources
    required (i.e. by a job)
    """

    memory_gb: float
    logical_cpu: float
    custom: Dict[str, float]

    def subtract(self, required: Resources) -> Optional[Resources]:
        """
        Subtracts "resources required" for a job from self, which is interpreted as
        "resources available" on a agent.

        Returns None if the required resources are not available in self.
        """
        if self.memory_gb < required.memory_gb:
            return None
        if self.logical_cpu < required.logical_cpu:
            return None
        for key, value in required.custom.items():
            if key not in self.custom:
                return None
            if self.custom[key] < required.custom[key]:
                return None

        return Resources(
            self.memory_gb - required.memory_gb,
            self.logical_cpu - required.logical_cpu,
            {
                key: value - required.custom.get(key, 0)
                for key, value in self.custom.items()
            },
        )

    def add(self, returned: Resources) -> Resources:
        """
        Adds back "resources required" for a job to self, which is usually resources
        available on a agent.
        """
        return Resources(
            self.memory_gb + returned.memory_gb,
            self.logical_cpu + returned.logical_cpu,
            {
                key: self.custom.get(key, 0) + returned.custom.get(key, 0)
                for key in set().union(self.custom, returned.custom)
            },
        )

    @classmethod
    def from_protobuf(cls, resources: Iterable[Resource]) -> Resources:
        resources_dict = {r.name: r.value for r in resources}
        if MEMORY_GB in resources_dict:
            memory_gb = resources_dict[MEMORY_GB]
            del resources_dict[MEMORY_GB]
        else:
            memory_gb = 0.0

        if LOGICAL_CPU in resources_dict:
            logical_cpu = resources_dict[LOGICAL_CPU]
            del resources_dict[LOGICAL_CPU]
        else:
            logical_cpu = 0.0

        return cls(memory_gb, logical_cpu, resources_dict)

    def to_protobuf(self) -> List[Resource]:
        resources_dict = self.custom.copy()
        resources_dict[MEMORY_GB] = self.memory_gb
        resources_dict[LOGICAL_CPU] = self.logical_cpu
        return construct_resources_protobuf(resources_dict)


def construct_resources_required_protobuf(
    resources: Optional[Dict[str, float]]
) -> Sequence[Resource]:
    """
    If resources is None, provides the defaults for resources required. If resources is
    not None, adds in the default resources if necessary. This means for default
    resources like LOGICAL_CPU and MEMORY_GB, the only way to "opt-out" of these
    resources is to explicitly set them to zero. Requiring zero of a resource is treated
    the same as not requiring that resource at all.

    "Opposite" of Resources.from_protobuf
    """
    if resources is None:
        resources = {}

    result = construct_resources_protobuf(resources)
    if MEMORY_GB not in resources:
        result.append(Resource(name=MEMORY_GB, value=DEFAULT_MEMORY_GB_REQUIRED))
    if LOGICAL_CPU not in resources:
        result.append(Resource(name=LOGICAL_CPU, value=DEFAULT_LOGICAL_CPU_REQUIRED))
    return result


def construct_resources_protobuf(resources: Dict[str, float]) -> List[Resource]:
    """Small helper for constructing a sequence of Resource"""
    return [Resource(name=name, value=value) for name, value in resources.items()]


@dataclasses.dataclass
class AgentState:
    """The coordinator's representation of an agent (i.e. a process running agent.py)"""

    # constants

    agent_id: str
    total_resources: Resources

    # state that will be updated

    # The coordinator assigns jobs to agents, but doesn't have a way of pushing new jobs
    # to agents, it has to wait for the agent to poll the coordinator. So "pending"
    # workers indicate jobs that need to be "picked up" by this agent. This dictionary
    # maps job_id to JobState, and JobState.get_pending_workers(AgentState) gives more
    # complete information about what workers need to be created.
    pending_workers: Dict[str, JobState]
    # Resources available on the agent. Calculated as if pending jobs have already been
    # "picked up".
    available_resources: Resources


@dataclasses.dataclass
class _JobStateDataClass:
    """
    Should be part of JobState, just separated out because mypy doesn't work with
    abstract dataclasses: https://github.com/python/mypy/issues/5374
    """

    job: Job
    resources_required: Resources


class JobState(_JobStateDataClass, abc.ABC):
    """Coordinator's representation a job"""

    @abc.abstractmethod
    def create_pending_worker(self, agent: AgentState) -> None:
        """
        See AgentState.pending_jobs for an explanation of what a pending job is. This
        function creates a pending worker on the specified agent. This is important to
        keep track of for each job because we don't want to end up with e.g. more
        pending workers than we need.

        For grid jobs, we can/want to create multiple pending workers, and we can have
        multiple workers on the same agent.
        """
        pass

    @abc.abstractmethod
    def get_pending_workers_for_agent(
        self, agent: AgentState
    ) -> Iterable[Optional[str]]:
        """
        See AgentState.pending_jobs and add_pending_worker. This gets the pending
        workers for the specified agent. This function will also mark any pending
        workers it returns as no longer pending, so that we don't try to start the same
        worker more than once.

        This returns an iterable of worker ids. Grid jobs have grid workers which need
        grid_worker_ids. Simple jobs don't need worker ids so will just use None as the
        worker id. Simple jobs will always return either 0 or 1 `None`s.
        """
        pass

    @abc.abstractmethod
    def num_workers_needed(self) -> int:
        """
        Returns how many additional workers we need for this job to be running at max
        parallelism. For simple jobs, this will either be 0 or 1. For grid jobs, this
        will be between 0 and the total number of tasks. This counts pending workers as
        "real" workers.
        """
        pass

    @abc.abstractmethod
    def fail_job(self, state: ProcessState) -> None:
        """This indicates that the entire job should fail with the given ProcessState"""
        pass


@dataclasses.dataclass
class SimpleJobState(JobState):
    """
    The coordinator's representation of a simple job. Simple jobs (unlike grid jobs)
    only require running a single command or function. Job.py_command or Job.py_function
    will be populated.
    """

    state: ProcessState = dataclasses.field(
        default_factory=lambda: ProcessState(
            state=ProcessState.ProcessStateEnum.RUN_REQUESTED
        )
    )
    worker: Optional[SimpleJobWorkerState] = None

    def num_workers_needed(self) -> int:
        if self.worker is None and self.state.state not in COMPLETED_PROCESS_STATES:
            return 1
        else:
            return 0

    def create_pending_worker(self, agent: AgentState) -> None:
        if self.worker is not None:
            raise ValueError("Cannot create more than one worker on a SimpleJob")

        self.worker = SimpleJobWorkerState(agent, True)

    def get_pending_workers_for_agent(
        self, agent: AgentState
    ) -> Iterable[Optional[str]]:
        if (
            self.worker is not None
            and agent.agent_id == self.worker.agent.agent_id
            and self.worker.is_pending
            and self.state.state not in COMPLETED_PROCESS_STATES
        ):
            self.worker.is_pending = False
            yield None

    def fail_job(self, state: ProcessState) -> None:
        self.state = state


@dataclasses.dataclass
class SimpleJobWorkerState:
    """
    The coordinator's representation of a scheduled/running execution of a SimpleJob on
    an agent.

    SimpleJobWorkers aren't really "real", especially in the case of PyCommand where
    there's no wrapper around the actual job--i.e. there's no separate SimpleJobWorker
    process, so this is really just keeping track of the execution of a job on a
    particular agent. This class really just exists as a parallel to GridWorkerState,
    especially for keeping track of is_pending.
    """

    agent: AgentState
    is_pending: bool


@dataclasses.dataclass
class GridJobState(JobState):
    """
    The coordinator's representation of a grid job. Grid jobs have many "tasks" that
    will get run with the same function in the same process as other tasks from that
    same grid job. Job.py_grid will be populated.
    """

    # all tasks that have been added to this grid job so far, indexed by
    # GridTaskState.task_id
    all_tasks: Dict[int, GridTaskState] = dataclasses.field(default_factory=dict)
    # Tasks that have not yet been assigned. This points to the same GridTaskState
    # objects as all_tasks.
    unassigned_tasks: collections.deque[GridTaskState] = dataclasses.field(
        default_factory=collections.deque
    )
    # Indicates whether all tasks have been added or not
    all_tasks_added: bool = False
    # all of the GridWorkers that are pending or have been created for this job, indexed
    # by the grid_worker_id
    grid_workers: Dict[str, GridWorkerState] = dataclasses.field(default_factory=dict)

    def num_workers_needed(self) -> int:
        # TODO it's possible we shouldn't just blindly create workers as we do here,
        #  e.g. if it takes a long time for a new agent to get going but the tasks are
        #  very short, creating as many workers as there are tasks might not be smart
        return len(self.unassigned_tasks) - sum(
            1 for w in self.grid_workers.values() if w.grid_task is None
        )

    def create_pending_worker(self, agent: AgentState) -> None:
        new_grid_worker = GridWorkerState(str(uuid.uuid4()), agent, None, True)
        self.grid_workers[new_grid_worker.grid_worker_id] = new_grid_worker

    def get_pending_workers_for_agent(
        self, agent: AgentState
    ) -> Iterable[Optional[str]]:
        # TODO there could be a lookup for this
        for grid_worker in self.grid_workers.values():
            if grid_worker.agent.agent_id == agent.agent_id and grid_worker.is_pending:
                grid_worker.is_pending = False
                yield grid_worker.grid_worker_id

    def fail_job(self, state: ProcessState) -> None:
        while len(self.unassigned_tasks) > 0:
            self.unassigned_tasks.popleft().state = state
            # TODO is it okay that tasks that are already running on a GridWorker keep
            #  going? (Shouldn't happen with current usage)


@dataclasses.dataclass
class GridWorkerState:
    """
    Coordinator's representation of a GridWorker. A GridWorker is a worker launched by
    an agent that will run tasks from a specific job (i.e. a process running
    grid_worker.py).

    A GridWorker goes through the following states:
    - First, is_pending = True, current_grid_task = None. The GridWorker doesn't
      actually exist yet, and will be be created by the agent when it calls
      get_next_jobs()
    - Once that happens, is_pending = False, current_grid_task = None. The GridWorker is
      now being launched, but it hasn't actually picked up a task yet. The GridWorker
      will eventually call update_grid_task_state_and_get_next and get a task
    - Once that happens, is_pending = False, current_grid_task will be populated.
    - Once that task completes, the GridWorker might pick up another task via
      update_grid_task_state_and_get_next, and current_grid_task will change.
    - If there are no more tasks to pick up, the GridWorker will shut down
      (current_grid_task will continue to be set to the last task that was worked on)
    """

    # constants

    grid_worker_id: str

    # state that will be updated

    agent: AgentState
    # The grid task that this worker is either currently working on or the last task
    # that it worked on.
    grid_task: Optional[GridTaskState]
    is_pending: bool


@dataclasses.dataclass
class GridTaskState:
    """Coordinator's representation of a task within a grid job"""

    task_id: int
    pickled_function_arguments: bytes
    state: ProcessState = dataclasses.field(
        default_factory=lambda: ProcessState(
            state=ProcessState.ProcessStateEnum.RUN_REQUESTED
        )
    )


def job_num_workers_needed_changed(job: JobState, agents: List[AgentState]) -> None:
    """
    This function should be called by the coordinator whenever the number of workers
    needed changes for a job, e.g. the job is created or tasks are added to a grid job.
    agents should be the list of all available agents.
    """
    num_workers_needed = job.num_workers_needed()

    # first assign tasks to agents based on their availability
    workers_created = _create_workers_for_job(job, agents, num_workers_needed)

    if num_workers_needed - workers_created <= 0:
        return  # we created all the workers we need

    # If we weren't able to get any workers at all, hopefully some agents will become
    # available later. Let's check that at least theoretically we have enough resources
    # on our existing agents that it is possible we will be able to create workers in
    # the future. If that's not the case we should just fail the job now.
    if workers_created == 0 and all(
        worker.total_resources.subtract(job.resources_required) is None
        for worker in agents
    ):
        job.fail_job(
            ProcessState(state=ProcessState.ProcessStateEnum.RESOURCES_NOT_AVAILABLE)
        )


def _remaining_resources_sort_key(
    available_resources: Resources, resources_required: Resources
) -> Tuple[int, Optional[Tuple[float, float]]]:
    """
    This takes the available resources for an agent and returns (indicator,
    remaining_resources_sort_key).

    remaining_resources_sort_key indicates how "good" it is to run the job on that agent
    based on how many resources would be left if we created a worker for that job on
    that agent. The general idea is that having fewer resources left is better. E.g. if
    you have agent1 with 8GB+4cpu and agent2 with 2GB+1cpu, and you have a job that
    requires 2GB+1cpu, it's probably better to run it on agent2--that way if another job
    comes along that requires 8GB+4cpu, agent1 is still available to run that job. I.e.
    we prefer to create workers on agents that have a smaller
    remaining_resources_sort_key.

    indicator is either 0 or 1--0 means that it is possible to run the job on this
    agent, whereas 1 means that there are not enough resources to run the job on this
    agent. (We use the indicator this way because smaller is better for the
    remaining_resources_sort_key, so 0 is good and 1 is bad.)

    TODO we should probably be more thoughtful about the exact implementation of
     remaining_resources_sort_key.

    TODO it's possible we should "reserve" availability for future jobs that might come
     along. E.g. if we have many low-priority jobs that require 1 CPU, and then a
     high-priority job requiring 16 CPUs comes along we might wish that we had not used
     all of our resources to run the low-priority jobs. On the other hand, if you
     reserve resources and no future jobs come along, then you're just making the
     current job run slower for no reason.
    """
    remaining_resources = available_resources.subtract(resources_required)
    if remaining_resources is not None:
        # 0 is an indicator saying we can run this job
        return 0, (
            sum(remaining_resources.custom.values()),
            remaining_resources.memory_gb + 2 * remaining_resources.logical_cpu,
        )
    else:
        # 1 is an indicator saying we cannot run this job
        return 1, None


def _create_workers_for_job(
    job: JobState, agents: List[AgentState], num_workers_needed: int
) -> int:
    """
    Creates (pending) workers for job based on the available resources on all available
    agents and how many workers that job needs.

    Returns how many workers were created
    """

    sort_keys = [
        _remaining_resources_sort_key(agent.available_resources, job.resources_required)
        for agent in agents
    ]

    num_workers_created = 0

    if sort_keys:
        while num_workers_created < num_workers_needed:
            # choose an agent
            chosen_index = min(range(len(sort_keys)), key=lambda i: sort_keys[i])
            # if the indicator is 1, that means none of the agents can run our job
            if sort_keys[chosen_index][0] == 1:
                break

            # we successfully chose an agent!
            chosen_agent = agents[chosen_index]
            # decrease the agent's available_resources
            new_available_resources = chosen_agent.available_resources.subtract(
                job.resources_required
            )
            # this is just for mypy: new_available_resources is guaranteed to not be
            # None because we know that sort_keys[chosen_index][0] == 0
            assert new_available_resources is not None
            chosen_agent.available_resources = new_available_resources
            # decrease the sort key for the chosen agent
            sort_keys[chosen_index] = _remaining_resources_sort_key(
                chosen_agent.available_resources, job.resources_required
            )
            # create the pending worker and add it to the agent
            job.create_pending_worker(chosen_agent)
            chosen_agent.pending_workers[job.job.job_id] = job

            num_workers_created += 1

    return num_workers_created


def agent_available_resources_changed(agent: AgentState, jobs: List[JobState]) -> None:
    """
    This function should be called whenever an agent's available resources changes,
    either due to the agent being brand new or an agent's worker exiting. This will
    create new pending workers for the agent. jobs should be the list of all jobs.
    """
    while True:
        # jobs that still need workers and "fit" on the agent
        available_jobs = [
            job
            for job in jobs
            if job.num_workers_needed() > 0
            and agent.available_resources.subtract(job.resources_required) is not None
        ]

        if len(available_jobs) > 0:
            # Choose a job. See the docstring on Job.priority in meadowgrid.proto for
            # how job.priority is used.
            job = random.choices(
                available_jobs, [job.job.priority for job in available_jobs]
            )[0]

            # decrease the agent's available_resources
            new_available_resources = agent.available_resources.subtract(
                job.resources_required
            )
            # just for mypy, we know that this isn't None because of how we construct
            # available_jobs above
            assert new_available_resources is not None
            agent.available_resources = new_available_resources
            # create the pending worker and add it to the agent
            job.create_pending_worker(agent)
            agent.pending_workers[job.job.job_id] = job
        else:
            break


def get_pending_workers_for_agent(
    agent: AgentState,
) -> List[Tuple[JobState, Optional[str]]]:
    """
    Gets the pending jobs for the agent and makes them no longer pending so that we
    don't get the same workers twice.

    This returns a list of (job, worker_id). Grid jobs have grid workers which need
    grid_worker_ids. Simple jobs don't need worker ids so will just use None as the
    worker id (remember that a single agent might run multiple grid workers for a single
    grid job).
    """
    results = [
        (job, grid_worker_id)
        for job in agent.pending_workers.values()
        for grid_worker_id in job.get_pending_workers_for_agent(agent)
    ]

    agent.pending_workers.clear()
    return results


def assign_task_to_grid_worker(
    grid_worker: GridWorkerState, job: GridJobState
) -> Optional[GridTaskState]:

    # TODO consider not returning another task even if there are tasks remaining in the
    #  scenario where all the agents are busy with tasks for a relatively unimportant
    #  job, and a new important job comes in that can't get any workers

    if len(job.unassigned_tasks) > 0:
        chosen_task = job.unassigned_tasks.popleft()
        # TODO add a timeout for if the grid_worker never comes back with a state
        grid_worker.grid_task = chosen_task
        return chosen_task
    else:
        return None


def update_task_state(grid_task: GridTaskState, state: ProcessState) -> None:
    grid_task.state = state


def update_grid_job_state(
    agent: AgentState,
    grid_worker: GridWorkerState,
    job: GridJobState,
    state: ProcessState,
) -> bool:
    """
    Grid jobs get both task state updates and job state updates. The job state updates
    come from the agent which indicate something happened with the grid worker, and the
    agent doesn't know what task the grid worker was working on (if any). Returns True
    if we've changed resources_available for agent, False otherwise.
    """
    if state.state == ProcessState.ProcessStateEnum.RUNNING:
        # We mostly ignore that the GridWorker has launched successfully and is running.
        # We could introduce another GridWorkerState between LAUNCHING and
        # TASK_ACQUIRED, but it doesn't really seem necessary--RUNNING and TASK_ACQUIRED
        # will happen so close to each other and sometimes RUNNING will even happen
        # after TASK_ACQUIRED
        return False
    elif state.state in COMPLETED_PROCESS_STATES:
        if state.state != ProcessState.ProcessStateEnum.SUCCEEDED:
            if grid_worker.grid_task is not None:
                # If the GridWorker failed/exited unexpectedly, and we know what task it
                # was working on, then we'll fail that task with whatever state we got.
                if (
                    grid_worker.grid_task.state is None
                    or grid_worker.grid_task.state.state not in COMPLETED_PROCESS_STATES
                ):
                    grid_worker.grid_task.state = state
                else:
                    # TODO not sure if there's something better we can do here?
                    print(
                        f"Funny situation: task {grid_worker.grid_task.task_id} for job"
                        f" {job.job.job_id} was already complete with state "
                        f"{grid_worker.grid_task.state} but then grid_worker "
                        f"{grid_worker.grid_worker_id} gave us a job state update of "
                        f"{state}"
                    )
            else:
                # If the GridWorker was not working on a task, we will assign it a task
                # and then have that immediately fail. This might seem a bit harsh, but
                # grid jobs don't have a concept of failing other than having their
                # tasks fail. So for example, if launching a GridWorker fails with
                # RUN_REQUEST_FAILED because e.g. the docker container specified is not
                # available, it will feel intuitive to the user to see that as a task
                # failure.
                if len(job.unassigned_tasks) > 0:
                    grid_task = job.unassigned_tasks.popleft()
                    grid_worker.grid_task = grid_task
                    grid_task.state = state
                else:
                    print(
                        f"Ignoring failure for job {job.job.job_id} from grid worker "
                        f"{grid_worker.grid_worker_id} because there are no unassigned "
                        f"tasks: {state}"
                    )

        # if a GridWorker exited, the agent needs to reclaim its resources
        agent.available_resources = agent.available_resources.add(
            job.resources_required
        )
        return True
    else:
        raise ValueError(f"Unexpected state {state.state}")


def update_simple_job_state(
    agent: AgentState, job: SimpleJobState, state: ProcessState
) -> bool:
    """Returns True if we've changed resources_available for agent, False otherwise."""

    # TODO we should probably make it so that the state of the job can't "regress", e.g.
    #  go from SUCCEEDED to RUNNING.
    job.state = state
    if job.state.state in COMPLETED_PROCESS_STATES:
        agent.available_resources = agent.available_resources.add(
            job.resources_required
        )
        return True
    else:
        return False
