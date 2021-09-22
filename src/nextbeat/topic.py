"""Topics, Actions, and Triggers"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import itertools
from typing import Iterable, Mapping, Sequence, List

from nextbeat.event_log import EventLog, Event, Timestamp
import nextbeat.jobs


@dataclass(frozen=True)
class Topic(ABC):
    """
    A topic conceptually groups together Events, Actions, and Triggers. Actions act on
    an instance of a Topic, and they will usually generate Events that have the
    topic_name of the Topic instance, and a payload appropriate for the Topic type.
    Triggers will trigger off of events of a particular Topic type.

    E.g. the Job topic will have:
    - Actions: Run, Cancel, etc.
    - Triggers: JobStateChangeTrigger
    - Payloads: JobPayload(JobState, ...)
    """

    # the name of the topic which corresponds to Event.topic_name for Events related
    # to this topic
    name: str


class Action(ABC):
    """
    An Action can do something to a topic, and should involve the topic changing state
    after execution, i.e. it should create an event for the topic.
    """

    @abstractmethod
    async def execute(
        self,
        topic: Topic,
        available_job_runners: List[nextbeat.jobs.JobRunner],
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> None:
        """execute should call log.append_job_event"""
        # TODO the signature of execute doesn't make that much sense for actions other
        #  than run, we should reconsider these APIs when we add additional actions
        pass


class Trigger(ABC):
    """A trigger indicates when/whether an action should happen based on events"""

    @abstractmethod
    def topic_names_to_subscribe(self) -> Iterable[str]:
        """Returns the names of the events that this trigger is interested in"""
        pass

    @abstractmethod
    def is_active(self, events: Mapping[str, Sequence[Event]]) -> bool:
        """
        events is the list of events for each event name this trigger is interested in.
        The list of events is all of the events in the current processing batch. If
        there are no events in the current batch, then the most recent event will also
        be included.
        """
        pass


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

    def is_active(self, events: Mapping[str, Sequence[Event]]) -> bool:
        return self.left.is_active(events) and self.right.is_active(events)


@dataclass(frozen=True)
class MergeTrigger(Trigger):
    left: Trigger
    right: Trigger

    def topic_names_to_subscribe(self) -> Iterable[str]:
        return itertools.chain(
            self.left.topic_names_to_subscribe(), self.right.topic_names_to_subscribe()
        )

    def is_active(self, events: Mapping[str, Sequence[Event]]) -> bool:
        return self.left.is_active(events) or self.right.is_active(events)
