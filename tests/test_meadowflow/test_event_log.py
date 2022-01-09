import asyncio
from typing import List

import pytest
from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.topic_names import pname


@pytest.mark.asyncio
async def test_append_event(event_log: EventLog) -> None:
    event_log.append_event(pname("A"), "waiting")
    actual = list(event_log.events(None, 0, 1))
    expected = [Event(0, pname("A"), "waiting")]
    assert expected == actual

    event_log.append_event(pname("B"), "waiting")
    actual = list(event_log.events(None, 0, 2))
    assert len(actual) == 2

    actual = list(event_log.events(None, 1, 2))
    expected = [Event(1, pname("B"), "waiting")]
    assert expected == actual


@pytest.mark.asyncio
async def test_events_and_state(event_log: EventLog) -> None:
    events = [
        Event(0, pname("A"), "waiting"),
        Event(1, pname("B"), "waiting"),
        Event(2, pname("B"), "running"),
        Event(3, pname("B"), "succeeded"),
    ]
    for event in events:
        event_log.append_event(event.topic_name, event.payload)

    actual = list(event_log.events_and_state(pname("A"), 0, 1))
    assert events[0:1] == actual

    actual = list(event_log.events_and_state(pname("B"), 0, 2))
    assert events[1:2] == actual

    actual = list(event_log.events_and_state(pname("A"), 1, 2))
    assert events[0:1] == actual


@pytest.mark.asyncio
async def test_subscriber_notification(event_log: EventLog) -> None:
    called = asyncio.Event()

    async def call(low: Timestamp, high: Timestamp, events: List[Event]) -> None:
        nonlocal called
        assert called.is_set() is False  # checks only called once
        called.set()
        assert low == 1
        assert high == 2
        assert len(events) == 1
        assert events[0].timestamp == 1
        assert events[0].topic_name == pname("A")
        assert events[0].payload == "waiting"

    event_log.subscribe([pname("A")], call)

    event_log.append_event(pname("B"), "waiting")
    await asyncio.sleep(delay=0.1)  # wait for subscribers to get called
    assert called.is_set() is False

    event_log.append_event(pname("A"), "waiting")
    await asyncio.wait_for(
        called.wait(), timeout=1
    )  # wait for subscribers to get called
    assert called.is_set() is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_topic,second_topic",
    [
        (None, None),
        (None, [pname("A")]),
        ([pname("A")], None),
        ([pname("A")], [pname("A")]),
    ],
)
async def test_subscribe_idempotence(
    event_log: EventLog, first_topic, second_topic
) -> None:
    """Check that multiple subscriptions does not result in multiple notifications."""
    called = asyncio.Event()

    async def call(low: Timestamp, high: Timestamp, events: List[Event]) -> None:
        nonlocal called
        assert not called.is_set()  # checks only called once
        called.set()
        assert len(events) == 1
        assert events[0].topic_name == pname("A")

    # same subscriber - double subscribe
    event_log.subscribe(first_topic, call)
    event_log.subscribe(second_topic, call)
    event_log.append_event(pname("A"), "waiting")
    await asyncio.wait_for(
        called.wait(), timeout=1
    )  # wait for subscribers to get called
    assert called.is_set() is True
