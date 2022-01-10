"""Topics, Actions, and Triggers"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Generic,
    Iterable,
    Optional,
    Sequence,
    List,
    Callable,
    TypeVar,
    TYPE_CHECKING,
)

from meadowflow.event_log import EventLog, Event, Timestamp
from meadowflow.topic_names import TopicName


@dataclass
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
    # to this topic. See comment on Event.topic_name
    name: TopicName


TTopic = TypeVar("TTopic", bound=Topic)

if TYPE_CHECKING:
    # Action.run_overrides and available_job_runners' types
    # cause circular import issues that are hard to resolve. In any case, as per the
    # todo below, we want to change the signature of those fields eventually anyways
    from meadowflow.jobs import JobRunOverrides, JobRunner


class Action(ABC, Generic[TTopic]):
    """
    An Action can do something to a topic, and should involve the topic changing state
    after execution, i.e. it should create an event for the topic.
    """

    @abstractmethod
    async def execute(
        self,
        topic: TTopic,
        run_overrides: Optional[JobRunOverrides],
        available_job_runners: List[JobRunner],
        event_log: EventLog,
        timestamp: Timestamp,
    ) -> str:
        """execute should call log.append_job_event"""
        # TODO the signature of execute doesn't make that much sense for actions other
        #  than run, we should reconsider these APIs when we add additional actions
        pass


class EventFilter(ABC):
    """See TriggerAction docstring"""

    @abstractmethod
    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        """
        Limits the scope of events that need to have apply called on them.
        """
        pass

    @abstractmethod
    def apply(self, event: Event) -> bool:
        """
        Returning true means that we want to trigger based on the event. This code
        should assume that event has one of the topic_names specified in
        topic_names_to_subscribe.
        """
        pass


@dataclass(frozen=True)
class TopicEventFilter(EventFilter):
    """Triggers on any event on the specified topic"""

    topic_name: TopicName

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        yield self.topic_name

    def apply(self, event: Event) -> bool:
        return True


class StatePredicate(ABC):
    """See TriggerAction docstring"""

    @abstractmethod
    def topic_names_to_query(self) -> Iterable[TopicName]:
        """
        Gets the list of topic names that this TriggerCondition needs to look at
        in order to determine whether the condition is met or not.
        """
        pass

    @abstractmethod
    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        """
        Unfortunately there's no enforced relationship between topic_names_to_query and
        this function
        """
        pass

    def map(
        self, function: Callable[[StatePredicate], StatePredicate]
    ) -> StatePredicate:
        """
        This must be overridden in any StatePredicates that combine other
        StatePredicates! Should call function on all of the child StatePredicates.
        """
        return self


class TruePredicate(StatePredicate):
    """This condition is always true"""

    def topic_names_to_query(self) -> Iterable[TopicName]:
        yield from ()

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        return True


@dataclass(frozen=True)
class AllPredicate(StatePredicate):
    children: Sequence[StatePredicate]

    def topic_names_to_query(self) -> Iterable[TopicName]:
        seen = set()
        for child_trigger in self.children:
            for name in child_trigger.topic_names_to_query():
                if name not in seen:
                    yield name
                    seen.add(name)

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        for child_trigger in self.children:
            if not child_trigger.apply(
                event_log, low_timestamp, high_timestamp, current_job_name
            ):
                return False

        return True

    def map(
        self, function: Callable[[StatePredicate], StatePredicate]
    ) -> StatePredicate:
        return AllPredicate([function(child) for child in self.children])


@dataclass(frozen=True)
class AnyPredicate(StatePredicate):
    children: Sequence[StatePredicate]

    def topic_names_to_query(self) -> Iterable[TopicName]:
        seen = set()
        for child_trigger in self.children:
            for name in child_trigger.topic_names_to_query():
                if name not in seen:
                    yield name
                    seen.add(name)

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        for child_trigger in self.children:
            if child_trigger.apply(
                event_log, low_timestamp, high_timestamp, current_job_name
            ):
                return True

        return False

    def map(
        self, function: Callable[[StatePredicate], StatePredicate]
    ) -> StatePredicate:
        return AnyPredicate([function(child) for child in self.children])


@dataclass(frozen=True)
class NotPredicate(StatePredicate):
    child: StatePredicate

    def topic_names_to_query(self) -> Iterable[TopicName]:
        return self.child.topic_names_to_query()

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        return not self.child.apply(
            event_log, low_timestamp, high_timestamp, current_job_name
        )

    def map(
        self, function: Callable[[StatePredicate], StatePredicate]
    ) -> StatePredicate:
        return NotPredicate(function(self.child))


@dataclass(frozen=True)
class TriggerAction:
    """
    Whenever an event happens that satisfies one or more of the EventFilters specified
    in wake_on, we will check whether state_predicate is met. If state_predicate is met,
    then the specified action will be triggered.

    EventFilter and StatePredicate seem similar but specify different things.
    EventFilter/wake_on specifies "when should I wake up to check if I should trigger",
    i.e., it picks out individual events that we might want to trigger on.
    StatePredicate/state_predicate specifies "what state should we be in in order to
    trigger" and cares about the state of each topic.

    Examples (see tests in test_triggers):
        > TriggerAction(AnyJobStateEventFilter(["A", "B"], ["SUCCEEDED"]),
            AllJobStatePredicate(["A", "B"], ["SUCCEEDED"]))
        Trigger any time A or B succeeds AND both are in the "SUCCEEDED" state

        > TriggerAction(AnyJobStateEventFilter(["A", "B"], ["SUCCEEDED"]))
        Trigger any time A or B succeeds (even if e.g. the other one has not run, or has
        even failed.)

        > TriggerAction(AnyJobStateEventFilter(["A"], ["SUCCEEDED"]),
            AllJobStatePredicate(["B"], ["SUCCEEDED"]))
        Trigger any time A succeeds AND B is in a "SUCCEEDED" state. Do NOT trigger when
        B "SUCCEEDS", even if A is already in a "SUCCEEDED" state.
    """

    action: Action

    # wake_on implicitly ORs the EventFilters together. There's no point in having an
    # AND on EventFilters
    wake_on: Sequence[EventFilter]

    # The default for state_predicate is TruePredicate, which means that we will always
    # trigger whenever any of the events specified in wake_on happens
    state_predicate: StatePredicate = TruePredicate()
