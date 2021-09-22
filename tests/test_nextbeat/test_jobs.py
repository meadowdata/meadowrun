from nextbeat.event_log import Event
from nextbeat.jobs import JobStateChangeTrigger, JobPayload
from nextbeat.topic import JoinTrigger, MergeTrigger


def test_job_state_change_trigger() -> None:
    trigger = JobStateChangeTrigger("A", ("SUCCEEDED", "FAILED"))
    assert not trigger.is_active({"A": [Event(0, "A", JobPayload(None, "WAITING"))]})
    assert trigger.is_active({"A": [Event(0, "A", JobPayload(None, "SUCCEEDED"))]})
    assert trigger.is_active({"A": [Event(0, "A", JobPayload(None, "FAILED"))]})


def test_join_trigger() -> None:
    left = JobStateChangeTrigger("A", ("FAILED",))
    right = JobStateChangeTrigger("B", ("FAILED",))
    trigger = JoinTrigger(left, right)
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "WAITING"))],
            "B": [Event(0, "B", JobPayload(None, "WAITING"))],
        }
    )
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "FAILED"))],
            "B": [Event(0, "B", JobPayload(None, "WAITING"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "FAILED"))],
            "B": [Event(0, "B", JobPayload(None, "FAILED"))],
        }
    )


def test_merge_trigger() -> None:
    left = JobStateChangeTrigger("A", ("FAILED",))
    right = JobStateChangeTrigger("B", ("FAILED",))
    trigger = MergeTrigger(left, right)
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "WAITING"))],
            "B": [Event(0, "B", JobPayload(None, "WAITING"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "FAILED"))],
            "B": [Event(0, "B", JobPayload(None, "WAITING"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload(None, "FAILED"))],
            "B": [Event(0, "B", JobPayload(None, "FAILED"))],
        }
    )
