import threading

from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.topic_names import pname


def test_append_event(event_loop) -> None:
    log = EventLog(event_loop)
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


def test_events_and_state(event_loop) -> None:
    log = EventLog(event_loop)
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


def test_subscribers(event_loop) -> None:
    try:
        log = EventLog(event_loop)
        task = event_loop.create_task(log.call_subscribers_loop())
        thread = threading.Thread(
            target=lambda: event_loop.run_until_complete(task),
            daemon=True,
        )
        thread.start()

        called = threading.Event()

        async def call(low: Timestamp, high: Timestamp) -> None:
            nonlocal called
            called.set()
            assert low == 1
            assert high == 2

        log.subscribe([pname("A")], call)

        log.append_event(pname("B"), "waiting")
        called.wait(timeout=0.1)  # wait for subscribers to get called
        assert called.is_set() is False

        log.append_event(pname("A"), "waiting")
        called.wait(timeout=1)  # wait for subscribers to get called
        assert called.is_set() is True
    finally:
        event_loop.call_soon_threadsafe(task.cancel)
        thread.join()
