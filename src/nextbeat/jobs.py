import uuid
from dataclasses import dataclass
from typing import Final, Iterable, Mapping, Tuple, Sequence

from nextbeat.event_log import EventLog, Event, Timestamp
from nextbeat.jobs_common import JobRunner, JobRunSpec, JobState
from nextbeat.topic import Topic, Action, Trigger


@dataclass(frozen=True)
class Job(Topic):
    """
    A job runs python code (specified job_run_spec) on a job_runner. The scheduler will
    also perform actions automatically based on trigger_actions.
    """

    # the function to execute this job
    job_run_spec: JobRunSpec

    # the job runner to use
    job_runner: JobRunner

    # explains what actions to take when
    trigger_actions: Tuple[Tuple[Trigger, Action], ...]


@dataclass(frozen=True)
class Run(Action):
    """Runs the job"""

    async def execute(
        self,
        job: Job,
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> None:
        ev = event_log.last_event(job.name, timestamp)
        # TODO not clear that this is the right behavior, vs queuing up another run once
        #  the current run is done.
        if not ev or ev.payload.state not in ["RUN_REQUESTED", "RUNNING"]:
            run_request_id = str(uuid.uuid4())
            await job.job_runner.run(job.name, run_request_id, job.job_run_spec)


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
