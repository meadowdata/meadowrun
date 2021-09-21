import abc
import random
import uuid
from dataclasses import dataclass
from typing import (
    Final,
    Iterable,
    Mapping,
    Tuple,
    Sequence,
    List,
    Any,
    Type,
    Dict,
    Optional,
    Union,
)

from nextbeat.event_log import EventLog, Event, Timestamp
from nextbeat.jobs_common import (
    JobRunner,
    JobState,
    JobRunnerFunction,
    JobRunnerFunctionTypes,
    VersionedJobRunnerFunction,
)
from nextbeat.local_job_runner import LocalJobRunner
from nextbeat.nextrun_job_runner import NextRunJobRunner
from nextbeat.topic import Topic, Action, Trigger


class JobRunnerPredicate(abc.ABC):
    """JobRunnerPredicates specify which job runners a job can run on"""

    @abc.abstractmethod
    def apply(self, job_runner: JobRunner) -> bool:
        pass


class AndPredicate(JobRunnerPredicate):
    def __init__(self, a: JobRunnerPredicate, b: JobRunnerPredicate):
        self._a = a
        self._b = b

    def apply(self, job_runner: JobRunner) -> bool:
        return self._a.apply(job_runner) and self._b.apply(job_runner)


class OrPredicate(JobRunnerPredicate):
    def __init__(self, a: JobRunnerPredicate, b: JobRunnerPredicate):
        self._a = a
        self._b = b

    def apply(self, job_runner: JobRunner) -> bool:
        return self._a.apply(job_runner) or self._b.apply(job_runner)


class ValueInPropertyPredicate(JobRunnerPredicate):
    """E.g. ValueInPropertyPredicate("capabilities", "chrome")"""

    def __init__(self, property_name: str, value: Any):
        self._property_name = property_name
        self._value = value

    def apply(self, job_runner: JobRunner) -> bool:
        return self._value in getattr(job_runner, self._property_name)


_JOB_RUNNER_TYPES: Dict[str, Type] = {
    "local": LocalJobRunner,
    "nextrun": NextRunJobRunner,
}


class JobRunnerTypePredicate(JobRunnerPredicate):
    def __init__(self, job_runner_type: str):
        self._job_runner_type = _JOB_RUNNER_TYPES[job_runner_type]

    def apply(self, job_runner: JobRunner) -> bool:
        # noinspection PyTypeHints
        return isinstance(job_runner, self._job_runner_type)


JobFunction = Union[JobRunnerFunction, VersionedJobRunnerFunction]


@dataclass(frozen=True)
class Job(Topic):
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

    # explains what actions to take when
    trigger_actions: Tuple[Tuple[Trigger, Action], ...]

    # specifies which job runners this job can run on
    job_runner_predicate: Optional[JobRunnerPredicate] = None


@dataclass(frozen=True)
class Run(Action):
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
class JobStateChangeTrigger(Trigger):
    """A trigger for when a job changes JobState"""

    job_name: str
    on_states: Tuple[JobState, ...]

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield self.job_name

    def is_active(self, events: Mapping[str, Sequence[Event]]) -> bool:
        # TODO should we check if events have the right type of payload?
        evs = events[self.job_name]
        return any(ev.payload.state in self.on_states for ev in evs)
