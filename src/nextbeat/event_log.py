import asyncio
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
    Awaitable,
    Any,
)

Timestamp = int
Subscriber = Callable[[Timestamp, Timestamp], Awaitable[None]]


T = TypeVar("T")
AppendEventType = Callable[[str, Any], None]


@dataclass(frozen=True)
class Event(Generic[T]):
    """
    timestamp indicates when the event happened, topic_name is an identifier that allows
    subscribers to filter to a subset of events (e.g. the job name), and payload is the
    arbitrary data (e.g. job state like "RUNNING" or "SUCCEEDED")
    """

    timestamp: Timestamp
    topic_name: str
    payload: T


class EventLog:
    """
    EventLog keeps track of events and calls subscribers based on those events. Think of
    it like a pub-sub system

    TODO there are a lot of weird assumptions about what's called "on the event loop" vs
     from outside of it/on a different thread and what's threadsafe

    TODO EventLog should be persisting data (potentially create a LocalEventLog and a
     PersistedEventLog)
    """

    def __init__(self, event_loop: asyncio.AbstractEventLoop) -> None:
        # TODO consider preventing anything from running off of this event loop?
        self._event_loop = event_loop
        # the timestamp that is assigned to the next appended event
        self._next_timestamp: Timestamp = 0
        # the timestamp up to which all subscribers have been called. I.e. subscribers
        # have already been called for all events with timestamp <
        # _subscribers_called_timestamp
        self._subscribers_called_timestamp: Timestamp = 0
        # the log of events, in increasing timestamp order
        self._event_log: List[Event] = []

        self._topic_name_to_events: Dict[str, List[Event]] = {}
        self._subscribers: Dict[str, Set[Subscriber]] = {}

    @property
    def curr_timestamp(self) -> Timestamp:
        """
        The smallest timestamp that has not yet been used. I.e. for all events,
        currently event < curr_timestamp
        """
        return self._next_timestamp

    def append_event(self, topic_name: str, payload: T) -> None:
        """Append a new state change to the event log, at a new and latest time"""

        event = Event(self._next_timestamp, topic_name, payload)
        self._next_timestamp = self._next_timestamp + 1
        self._event_log.append(event)
        self._topic_name_to_events.setdefault(topic_name, []).append(event)

        # Schedule a call to subscribers--the next time there's "time" on the asyncio
        # event_loop, call_subscribers will get called. Importantly, there's no
        # guarantee that call_subscribers will get called once per event--the event_loop
        # might not "have time" until multiple events have been posted first, and then
        # multiple call_subscribers will run (the subsequent ones will presumably do
        # nothing.)
        self._event_loop.call_soon_threadsafe(
            lambda: self._event_loop.create_task(self.call_subscribers())
        )

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
        self, topic_name: str, low_timestamp: Timestamp, high_timestamp: Timestamp
    ) -> Iterable[Event]:
        """
        Return all events with timestamp: low_timestamp <= timestamp < high_timestamp,
        in decreasing timestamp order. If there is an event, at least one is always
        returned to represent the current state, even if it is before the low_timestamp.
        """
        all_events = self._topic_name_to_events.get(topic_name, [])
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

    def last_event(self, topic_name: str, timestamp: Timestamp) -> Optional[Event]:
        """
        Return the most recent event with event.timestamp <= timestamp. None if no such
        event exists.
        """
        all_events = self._topic_name_to_events.get(topic_name, [])
        for event in reversed(all_events):
            if event.timestamp <= timestamp:
                return event
        return None

    def subscribe(self, topic_names: Iterable[str], subscriber: Subscriber) -> None:
        """
        Subscribe subscriber to be called whenever there is an event matching one of the
        topic_names.

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
        for topic_name in topic_names:
            self._subscribers.setdefault(topic_name, set()).add(subscriber)

    def all_subscribers_called(self) -> bool:
        return self._subscribers_called_timestamp >= self._next_timestamp

    async def call_subscribers(self) -> Timestamp:
        """
        Call all subscribers for any outstanding events. See subscribe for details on
        semantics.
        """
        try:
            if asyncio.get_running_loop() != self._event_loop:
                raise ValueError(
                    "EventLog.call_subscribers was called from a different _event_loop "
                    "than expected"
                )

            # get the list of subscribers that need to be called
            subscribers: Set[Subscriber] = set()
            low_timestamp = self._subscribers_called_timestamp
            high_timestamp = self._next_timestamp
            for event in self.events(low_timestamp, high_timestamp):
                subscribers.update(self._subscribers.get(event.topic_name, set()))

            # call the subscribers
            await asyncio.gather(
                *(
                    subscriber(low_timestamp, high_timestamp)
                    for subscriber in subscribers
                )
            )

            self._subscribers_called_timestamp = high_timestamp
            return self._subscribers_called_timestamp
        except Exception as e:
            # TODO this function isn't awaited, so exceptions need to make it back into
            #  the scheduler somehow
            print(e)
