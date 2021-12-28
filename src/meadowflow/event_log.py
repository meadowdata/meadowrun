from __future__ import annotations

import asyncio
import traceback
from dataclasses import dataclass
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Generic,
    Awaitable,
)

from meadowflow.topic_names import TopicName

Timestamp = int
Subscriber = Callable[[Timestamp, Timestamp], Awaitable[None]]


T = TypeVar("T")


@dataclass(frozen=True)
class Event(Generic[T]):
    """
    timestamp indicates when the event happened, topic_name is an identifier that allows
    subscribers to filter to a subset of events (e.g. the job name), and payload is the
    arbitrary data (e.g. job state like "RUNNING" or "SUCCEEDED").

    topic_name is a FrozenDict rather than a string so that we can identify topics by
    key-value pairs. E.g. it's common to want a job identified by (name="data_loader",
    date=datetime.date(2021, 9, 1))
    """

    timestamp: Timestamp
    topic_name: TopicName
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

    def __init__(self) -> None:
        # the timestamp that is assigned to the next appended event
        self._next_timestamp: Timestamp = 0

        # the timestamp up to which all subscribers have been called. I.e. subscribers
        # have already been called for all events with timestamp <
        # _subscribers_called_timestamp
        self._subscribers_called_timestamp: Timestamp = 0

        # the log of events, in increasing timestamp order
        self._event_log: List[Event] = []

        self._topic_name_to_events: Dict[TopicName, List[Event]] = {}

        # subscribers to a specific topic
        self._topic_subscribers: Dict[TopicName, Set[Subscriber]] = {}

        # subscribers to all events
        self._universal_subscribers: Set[Subscriber] = set()

        # Init for these needs to be on an event loop - see _init_async and __await__.
        # notify our call subscribers loop that events have been posted to the event log
        self._notify_call_subscribers: asyncio.Event

        # keep the task for the subscribers loop so we can cancel it in close().
        self._subscibers_loop: asyncio.Task

        # make sure awaiting more than once doesn't mess with us.
        self._awaited = False

    async def _init_async(self) -> EventLog:
        if self._awaited:
            return self
        self._notify_call_subscribers = asyncio.Event()
        self._subscibers_loop = asyncio.create_task(self._call_subscribers_loop())
        self._awaited = True
        return self

    def __await__(self) -> Generator[Any, None, EventLog]:
        return self._init_async().__await__()

    @property
    def curr_timestamp(self) -> Timestamp:
        """
        The smallest timestamp that has not yet been used. I.e. for all events,
        currently event < curr_timestamp
        """
        return self._next_timestamp

    def append_event(self, topic_name: TopicName, payload: Any) -> None:
        """Append a new state change to the event log, at a new and latest time"""

        print(f"append_event {self._next_timestamp} {topic_name} {payload}")

        event = Event(self._next_timestamp, topic_name, payload)
        self._next_timestamp = self._next_timestamp + 1
        self._event_log.append(event)
        self._topic_name_to_events.setdefault(topic_name, []).append(event)

        # Tell the call_subscribers_loop to wake up. Importantly, there's no guarantee
        # that the call_subscribers_loop will happen once per event--the loop might get
        # "backed up" until multiple events have been posted first, and then multiple
        # events will get processed in the same iteration of the loop.
        asyncio.get_running_loop().call_soon(self._notify_call_subscribers.set)

    def events(
        self,
        topic_name: Optional[TopicName],
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
    ) -> Iterable[Event]:
        """
        Return all events with timestamp: low_timestamp <= timestamp < high_timestamp in
        decreasing timestamp order. If topic_name is not None, then restrict to events
        with the specified topic_name
        """
        if topic_name is None:
            all_events = self._event_log
        else:
            all_events = self._topic_name_to_events.get(topic_name, [])

        for event in reversed(all_events):
            if low_timestamp <= event.timestamp < high_timestamp:
                yield event
            elif event.timestamp < low_timestamp:
                break

    def events_and_state(
        self,
        topic_name: TopicName,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
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

    def last_event(
        self, topic_name: TopicName, timestamp: Timestamp
    ) -> Optional[Event]:
        """
        Return the most recent event with event.timestamp <= timestamp. None if no such
        event exists.
        """
        all_events = self._topic_name_to_events.get(topic_name, [])
        for event in reversed(all_events):
            if event.timestamp <= timestamp:
                return event
        return None

    def subscribe(
        self, topic_names: Optional[Iterable[TopicName]], subscriber: Subscriber
    ) -> None:
        """
        If topic_names is None, subscribe subscriber to be called whenever there is an
        event. If topic_names is not None, subscribe to events matching one of the
        event. If topic_names is not None, subscribe to events matching one of the
        topic_names.

        The EventLog processes a batch of events at a time, which means that subscriber
        may be getting called once even though multiple events have happened. subscriber
        will get called with subscriber(low_timestamp, high_timestamp). The current
        batch of events being processed are the events where low_timestamp <=
        event.timestamp < high_timestamp. It is guaranteed that subscriber has never
        been called for any of the events in the current batch. It is NOT the case that
        a particular subscriber will always see a low_timestamp equal to the previous
        high_timestamp: subscriber won't be called at all for a batch if there are no
        matching events (a "universal subscriber" i.e. one without topic_names will
        always see low_timestamp equal to the previous high_timestamp).
        """
        if topic_names is None:
            self._universal_subscribers.add(subscriber)
        else:
            for topic_name in topic_names:
                self._topic_subscribers.setdefault(topic_name, set()).add(subscriber)

    def all_subscribers_called(self) -> bool:
        return self._subscribers_called_timestamp >= self._next_timestamp

    async def _call_subscribers_loop(self) -> None:
        """
        Call all subscribers for any outstanding events. See subscribe for details on
        semantics.
        """
        while True:
            await self._notify_call_subscribers.wait()
            self._notify_call_subscribers.clear()

            try:
                # get the list of subscribers that need to be called
                subscribers: Set[Subscriber] = self._universal_subscribers.copy()
                low_timestamp = self._subscribers_called_timestamp
                high_timestamp = self._next_timestamp
                for event in self.events(None, low_timestamp, high_timestamp):
                    subscribers.update(
                        self._topic_subscribers.get(event.topic_name, set())
                    )

                # call the subscribers
                # TODO I think ideal behavior here would be to
                # retry failures 3 times or something before giving up on a
                # subscriber. Current behavior is very bad--failure in one
                # subscriber means we will never progress. I think the outer
                # try/except is probably not necessary.
                await asyncio.gather(
                    *(
                        subscriber(low_timestamp, high_timestamp)
                        for subscriber in subscribers
                    )
                )

                self._subscribers_called_timestamp = high_timestamp
            except Exception:
                # TODO this function isn't awaited, so exceptions need to make it
                # back into the scheduler somehow
                traceback.print_exc()

    async def close(self) -> None:
        self._subscibers_loop.cancel()
        try:
            await self._subscibers_loop
        except asyncio.CancelledError:
            pass  # that's fine, we just cancelled

    def __aenter__(self) -> EventLog:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()
