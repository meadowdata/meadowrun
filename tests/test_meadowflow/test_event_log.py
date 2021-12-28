import asyncio

import pytest
from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.topic_names import pname


@pytest.mark.asyncio
async def test_append_event() -> None:
    async with EventLog() as log:
        log.append_event(pname("A"), "waiting")
        actual = list(log.events(None, 0, 1))
        expected = [Event(0, pname("A"), "waiting")]
        assert expected == actual

        log.append_event(pname("B"), "waiting")
        actual = list(log.events(None, 0, 2))
        assert len(actual) == 2

        actual = list(log.events(None, 1, 2))
        expected = [Event(1, pname("B"), "waiting")]
        assert expected == actual


@pytest.mark.asyncio
async def test_events_and_state() -> None:
    async with EventLog() as log:
        events = [
            Event(0, pname("A"), "waiting"),
            Event(1, pname("B"), "waiting"),
            Event(2, pname("B"), "running"),
            Event(3, pname("B"), "succeeded"),
        ]
        for event in events:
            log.append_event(event.topic_name, event.payload)

        actual = list(log.events_and_state(pname("A"), 0, 1))
        assert events[0:1] == actual

        actual = list(log.events_and_state(pname("B"), 0, 2))
        assert events[1:2] == actual

        actual = list(log.events_and_state(pname("A"), 1, 2))
        assert events[0:1] == actual


@pytest.mark.asyncio
async def test_subscribers() -> None:
    async with EventLog() as log:

        called = asyncio.Event()

        async def call(low: Timestamp, high: Timestamp) -> None:
            nonlocal called
            called.set()
            assert low == 1
            assert high == 2

        log.subscribe([pname("A")], call)

        log.append_event(pname("B"), "waiting")
        await asyncio.sleep(delay=0.1)  # wait for subscribers to get called
        assert called.is_set() is False

        log.append_event(pname("A"), "waiting")
        await asyncio.wait_for(
            called.wait(), timeout=1
        )  # wait for subscribers to get called
        assert called.is_set() is True
