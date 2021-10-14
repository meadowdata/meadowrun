import asyncio
import threading
import time

from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.topic_names import pname


def test_append_event() -> None:
    log = EventLog(asyncio.new_event_loop())
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


def test_events_and_state() -> None:
    log = EventLog(asyncio.new_event_loop())
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


def test_subscribers() -> None:
    loop = asyncio.new_event_loop()

    try:
        log = EventLog(loop)
        threading.Thread(
            target=lambda: loop.run_until_complete(log.call_subscribers_loop()),
            daemon=True,
        ).start()

        called = False

        async def call(low: Timestamp, high: Timestamp) -> None:
            nonlocal called
            called = True
            assert low == 1
            assert high == 2

        log.subscribe([pname("A")], call)

        log.append_event(pname("B"), "waiting")
        time.sleep(0.05)  # wait for subscribers to get called
        assert called is False

        log.append_event(pname("A"), "waiting")
        time.sleep(0.05)  # wait for subscribers to get called
        assert called is True
    finally:
        loop.stop()
