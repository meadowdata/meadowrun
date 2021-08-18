from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Generic,
)

Timestamp = int
Subscriber = Callable[[Timestamp, Timestamp], None]


T = TypeVar("T")


@dataclass(frozen=True)
class Event(Generic[T]):
    """
    Currently, name is the job name, payload is the job state, and timestamp indicates
    when the event happened. This will evolve as we add more functionality
    """

    timestamp: Timestamp
    name: str
    payload: T


class EventLog:
    """
    EventLog keeps track of events and executes subscribers based on those events.

    TODO currently not threadsafe
    TODO EventLog should be persisting data (potentially create a LocalEventLog and a
     PersistedEventLog)
    """

    def __init__(self) -> None:
        # the timestamp that is assigned to the next appended event
        self._next_timestamp: Timestamp = 0
        # the timestamp up to which all subscriptions to changes have been executed
        self._subscriptions_executed_timestamp: Timestamp = 0
        # the log of events, in increasing timestamp order
        self._event_log: List[Event] = []

        self._name_to_events: Dict[str, List[Event]] = {}
        self._subscribers: Dict[str, Set[Subscriber]] = {}

    @property
    def curr_timestamp(self) -> Timestamp:
        """
        The smallest timestamp that has not yet been used. I.e. for all events,
        currently event < curr_timestamp
        """
        return self._next_timestamp

    def append_job_event(self, name: str, payload: T) -> None:
        """Append a new state change to the event log, at a new and latest time"""
        event = Event(self._next_timestamp, name, payload)
        self._next_timestamp = self._next_timestamp + 1
        self._event_log.append(event)
        self._name_to_events.setdefault(name, []).append(event)

    def events(
        self, low_timestamp: Timestamp, high_timestamp: Timestamp
    ) -> Iterable[Event]:
        """
        Return all events with timestamp: low_timestamp <= timestamp < high_timestamp
        in decreasing timestamp order.
        """
        all_events = self._event_log
        for event in reversed(all_events):
            if low_timestamp <= event.timestamp < high_timestamp:
                yield event
            elif event.timestamp < low_timestamp:
                break

    def events_and_state(
        self, name: str, low_timestamp: Timestamp, high_timestamp: Timestamp
    ) -> Iterable[Event]:
        """
        Return all events with timestamp: low_timestamp <= timestamp < high_timestamp,
        in decreasing timestamp order. If there is an event, at least one is always
        returned to represent the current state, even if it is before the low_timestamp.
        """
        all_events = self._name_to_events.get(name, [])
        yielded_one = False
        for event in reversed(all_events):
            if low_timestamp <= event.timestamp < high_timestamp:
                yield event
                yielded_one = True
            elif event.timestamp < low_timestamp:
                if yielded_one:
                    break
                else:
                    yield event
                    yielded_one = True

    def last_event(self, name: str, timestamp: Timestamp) -> Optional[Event]:
        """
        Return the most recent event with event.timestamp <= timestamp. None if no such
        event exists.
        """
        all_events = self._name_to_events.get(name, [])
        for event in reversed(all_events):
            if event.timestamp <= timestamp:
                return event
        return None

    def subscribe(self, event_names: Iterable[str], subscriber: Subscriber) -> None:
        """
        Subscribe subscriber to be called whenever there is an event matching one of the
        event_names.

        The EventLog processes a batch of events at a time, which means that subscriber
        may be getting called once even though multiple events have happened. subscriber
        will get called with subscriber(low_timestamp, high_timestamp). The current
        batch of events being processed are the events where low_timestamp <=
        event.timestamp < high_timestamp. It is guaranteed that subscriber has never
        been called for any of the events in the current batch. It is NOT the case that
        a particular subscriber will always see a low_timestamp equal to the previous
        high_timestamp: subscriber won't be called at all for a batch if there are no
        matching events.
        """
        for event_name in event_names:
            self._subscribers.setdefault(event_name, set()).add(subscriber)

    def all_subscriptions_executed(self) -> bool:
        return self._subscriptions_executed_timestamp < self._next_timestamp

    def execute_subscriptions(self) -> Timestamp:
        """
        Execute all subscribers for any outstanding events. See subscribe for details on
        semantics.
        """
        subscribers: Set[Subscriber] = set()
        low_timestamp = self._subscriptions_executed_timestamp
        high_timestamp = self._next_timestamp
        for event in self.events(low_timestamp, high_timestamp):
            subscribers.update(self._subscribers.get(event.name, set()))
        for subscriber in subscribers:
            subscriber(low_timestamp, high_timestamp)
        self._subscriptions_executed_timestamp = high_timestamp
        return self._subscriptions_executed_timestamp
