from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Final,
    Generic,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
)
import uuid
from nextbeat.event_log import EventLog, Event, Timestamp
import itertools

from .job_runner import LocalJobRunner


F = TypeVar("F", bound=Callable[..., Any])

JobState = Literal["waiting", "launched", "running", "succeeded", "failed", "cancelled"]


@dataclass(frozen=True)
class JobPayload:
    """This is the Event.payload for jobs"""

    state: JobState
    extra_info: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Job(Generic[F]):
    """A job describes how events with Job.name get generated"""

    # the name of the job, and the name of the Events that get generated for this job
    name: str
    # the function to execute this job
    run_func: F
    # explains what actions to take when
    trigger_actions: Tuple[Tuple[Trigger, Action], ...]

    args: Optional[List[Any]] = None
    kwargs: Optional[Dict[str, Any]] = None

    # TODO besides launching there should also be a way to poll progress, and figure out
    #  whether the job succeeded or not etc


class Action(ABC):
    """
    An Action can do something to a Job, and should involve the job changing state after
    execution.
    """

    @abstractmethod
    def execute(
        self,
        job: Job,
        job_runner: LocalJobRunner,
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> None:
        """execute should call log.append_job_event"""
        pass


@dataclass(frozen=True)
class Run(Action):
    """Runs the job"""

    def execute(
        self,
        job: Job,
        job_runner: LocalJobRunner,
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> None:
        ev = event_log.last_event(job.name, timestamp)
        if not ev or ev.payload.state not in ["launched", "running"]:
            launch_id = uuid.uuid4()
            event_log.append_event(
                job.name, JobPayload("launched", dict(launch_id=launch_id))
            )
            job_runner.run(launch_id, job.run_func, job.args, job.kwargs)


class Actions:
    """All the available actions"""

    run: Final[Run] = Run()
    # TODO other actions: abort, bypass, init, pause


class Trigger(ABC):
    """A trigger indicates when/whether an action should happen"""

    @abstractmethod
    def topic_names_to_subscribe(self) -> Iterable[str]:
        """Returns the names of the events that this trigger is interested in"""
        pass

    @abstractmethod
    def is_active(self, events: Mapping[str, Iterable[Event]]) -> bool:
        """
        events is the list of events for each event name this trigger is interested in.
        The list of events is all of the events in the current processing batch. If
        there are no events in the current batch, then the most recent event will also
        be included.
        """
        pass


@dataclass(frozen=True)
class JobStateChangeTrigger(Trigger):
    """A trigger for when a job changes JobState"""

    job_name: str
    on_states: Tuple[JobState, ...]

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield self.job_name

    def is_active(self, events: Mapping[str, Iterable[Event]]) -> bool:
        evs = events[self.job_name]
        return any(ev.payload.state in self.on_states for ev in evs)


# like Rx:
# join means trigger for each combination
# merge means trigger for each run
# zip means trigger for each pair exactly once


@dataclass(frozen=True)
class JoinTrigger(Trigger):
    left: Trigger
    right: Trigger

    def topic_names_to_subscribe(self) -> Iterable[str]:
        return itertools.chain(
            self.left.topic_names_to_subscribe(), self.right.topic_names_to_subscribe()
        )

    def is_active(self, events: Mapping[str, Iterable[Event]]) -> bool:
        return self.left.is_active(events) and self.right.is_active(events)


@dataclass(frozen=True)
class MergeTrigger(Trigger):
    left: Trigger
    right: Trigger

    def topic_names_to_subscribe(self) -> Iterable[str]:
        return itertools.chain(
            self.left.topic_names_to_subscribe(), self.right.topic_names_to_subscribe()
        )

    def is_active(self, events: Mapping[str, Iterable[Event]]) -> bool:
        return self.left.is_active(events) or self.right.is_active(events)
