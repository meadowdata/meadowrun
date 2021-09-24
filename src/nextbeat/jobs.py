from __future__ import annotations

import abc
import dataclasses
import random
import uuid
from dataclasses import dataclass
from typing import (
    Final,
    Iterable,
    Mapping,
    Sequence,
    List,
    Any,
    Dict,
    Optional,
    Union,
    Literal,
    Callable,
)

from nextbeat.event_log import EventLog, Event, Timestamp
import nextbeat.topic
from nextrun.deployed_function import NextRunDeployedFunction


JobState = Literal[
    # Nothing is currently happening with the job
    "WAITING",
    # A run of the job has been requested on a job runner
    "RUN_REQUESTED",
    # The job is currently running. JobPayload.pid will be populated
    "RUNNING",
    # The job has completed normally. JobPayload.result_value and pid will be populated
    "SUCCEEDED",
    # The job was cancelled by the user. JobPayload.pid will be populated
    "CANCELLED",
    # The job failed. JobPayload.failure_type and pid will be populated. If failure_type
    # is PYTHON_EXCEPTION or RUN_REQUEST_FAILED, raised_exception will be populated, if
    # failure_type is NON_ZERO_RETURN_CODE, return_code will be populated
    "FAILED",
]


@dataclass(frozen=True)
class RaisedException:
    """Represents a python exception raised by a remote process"""

    exception_type: str
    exception_message: str
    exception_traceback: str


@dataclasses.dataclass(frozen=True)
class Effects:
    """Represents effects created by the running of a job"""

    add_jobs: List[Job]


@dataclass(frozen=True)
class JobPayload:
    """The Event.payload for Job-related events. See JobStateType docstring."""

    request_id: Optional[str]
    state: JobState
    failure_type: Optional[
        Literal["PYTHON_EXCEPTION", "NON_ZERO_RETURN_CODE", "RUN_REQUEST_FAILED"]
    ] = None
    pid: Optional[int] = None
    result_value: Any = None
    effects: Optional[Effects] = None
    raised_exception: Union[RaisedException, BaseException, None] = None
    return_code: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class LocalFunction:
    """
    A function pointer in the current codebase with arguments for calling the
    function
    """

    function_pointer: Callable[..., Any]
    function_args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    function_kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


JobRunnerFunctionTypes = (LocalFunction, NextRunDeployedFunction)
# A JobRunnerFunction is a function/executable/script that one or more JobRunners will
# know how to run along with the arguments for that function/executable/script
JobRunnerFunction = Union[JobRunnerFunctionTypes]


class JobRunner(abc.ABC):
    """An interface for job runner clients"""

    @abc.abstractmethod
    async def run(
        self, job_name: str, run_request_id: str, job_runner_function: JobRunnerFunction
    ) -> None:
        pass

    @abc.abstractmethod
    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """
        last_events is the last event we've recorded for the jobs that we are interested
        in. poll_jobs will add new events to the EventLog for these jobs if there's been
        any change in their state.
        """
        pass


class VersionedJobRunnerFunction(abc.ABC):
    """
    Similar to a JobRunnerFunction, but instead of a single version of the code (e.g. a
    specific commit in a git repo), specifies a versioned codebase (e.g. a git repo),
    along with a function/executable/script in that repo (and also including the
    arguments to call that function/executable/script).

    TODO this is not yet fully fleshed out and the interface will probably need to
     change
    """

    @abc.abstractmethod
    def get_job_runner_function(self) -> JobRunnerFunction:
        pass


class JobRunnerPredicate(abc.ABC):
    """JobRunnerPredicates specify which job runners a job can run on"""

    @abc.abstractmethod
    def apply(self, job_runner: JobRunner) -> bool:
        pass


JobFunction = Union[JobRunnerFunction, VersionedJobRunnerFunction]


@dataclass(frozen=True)
class Job(nextbeat.topic.Topic):
    """
    A job runs python code (specified job_run_spec) on a job_runner. The scheduler will
    also perform actions automatically based on trigger_actions.
    """

    # job_function specifies "where is the codebase and interpreter" (called a
    # "deployment" in nextrun), "how do we invoke the function/executable/script for
    # this job" (e.g. NextRunFunction), and "what are the arguments for that
    # function/executable/script". This can be a JobRunnerFunction, which is something
    # that at least one job runner will know how to run, or a
    # "VersionedJobRunnerFunction" (currently the only implementation is
    # NextRunFunctionGitRepo) which is something that can produce different versions of
    # a JobRunnerFunction
    job_function: JobFunction

    # specifies what actions to take when
    trigger_actions: Sequence[nextbeat.topic.TriggerAction]

    # specifies which job runners this job can run on
    job_runner_predicate: Optional[JobRunnerPredicate] = None


@dataclass(frozen=True)
class Run(nextbeat.topic.Action):
    """Runs the job"""

    async def execute(
        self,
        job: Job,
        available_job_runners: List[JobRunner],
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> None:
        ev = event_log.last_event(job.name, timestamp)
        # TODO not clear that this is the right behavior, vs queuing up another run once
        #  the current run is done.
        if not ev or ev.payload.state not in ["RUN_REQUESTED", "RUNNING"]:
            run_request_id = str(uuid.uuid4())

            # convert a job_function into a job_runner_function
            if isinstance(job.job_function, VersionedJobRunnerFunction):
                job_runner_function = job.job_function.get_job_runner_function()
            elif isinstance(job.job_function, JobRunnerFunctionTypes):
                job_runner_function = job.job_function
            else:
                raise ValueError(
                    "job_run_spec is neither VersionedJobRunnerFunction nor a "
                    f"JobRunnerFunction, instead is a {type(job.job_function)}"
                )

            await choose_job_runner(job, available_job_runners).run(
                job.name, run_request_id, job_runner_function
            )


def choose_job_runner(job: Job, job_runners: List[JobRunner]) -> JobRunner:
    """
    Chooses a job_runner that is compatible with job.

    TODO this logic should be much more sophisticated, look at available resources, etc.
    """
    if job.job_runner_predicate is None:
        compatible_job_runners = job_runners
    else:
        compatible_job_runners = [
            jr for jr in job_runners if job.job_runner_predicate.apply(jr)
        ]

    if len(compatible_job_runners) == 0:
        # TODO this should probably get sent to the event log somehow. Also, what if we
        #  don't currently have any job runners that satisfy the predicates but one
        #  shows up in the near future?
        raise ValueError(
            f"No job runners were found that satisfy the predicates for {job.name}"
        )
    else:
        return random.choice(compatible_job_runners)


class Actions:
    """All the available actions"""

    run: Final[Run] = Run()
    # TODO other actions: abort, bypass, init, pause


@dataclass(frozen=True)
class AnyJobStateEventFilter(nextbeat.topic.EventFilter):
    """Triggers when any of job_names is in any of on_states"""

    job_names: Sequence[str]
    on_states: Sequence[JobState]

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield from self.job_names

    def apply(self, event: Event) -> bool:
        return event.payload.state in self.on_states


@dataclass(frozen=True)
class AllJobStatePredicate(nextbeat.topic.StatePredicate):
    """Condition is met when all of job_names are in one of on_states"""

    job_names: Sequence[str]
    on_states: Sequence[JobState]

    def topic_names_to_query(self) -> Iterable[str]:
        yield from self.job_names

    def apply(self, events: Mapping[str, Sequence[Event]]) -> bool:
        # make sure the most recent event is in the specified state
        return all(
            events[name][0].payload.state in self.on_states for name in self.job_names
        )
