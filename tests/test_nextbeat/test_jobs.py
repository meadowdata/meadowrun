from nextbeat.event_log import Event
from nextbeat.jobs import JobStateChangeTrigger, JoinTrigger, MergeTrigger, JobPayload


def test_job_state_change_trigger() -> None:
    trigger = JobStateChangeTrigger("A", ("succeeded", "failed"))
    assert not trigger.is_active({"A": [Event(0, "A", JobPayload("waiting"))]})
    assert trigger.is_active({"A": [Event(0, "A", JobPayload("succeeded"))]})
    assert trigger.is_active({"A": [Event(0, "A", JobPayload("failed"))]})


def test_join_trigger() -> None:
    left = JobStateChangeTrigger("A", ("failed",))
    right = JobStateChangeTrigger("B", ("failed",))
    trigger = JoinTrigger(left, right)
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("waiting"))],
            "B": [Event(0, "B", JobPayload("waiting"))],
        }
    )
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("failed"))],
            "B": [Event(0, "B", JobPayload("waiting"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("failed"))],
            "B": [Event(0, "B", JobPayload("failed"))],
        }
    )


def test_merge_trigger() -> None:
    left = JobStateChangeTrigger("A", ("failed",))
    right = JobStateChangeTrigger("B", ("failed",))
    trigger = MergeTrigger(left, right)
    assert not trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("waiting"))],
            "B": [Event(0, "B", JobPayload("waiting"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("failed"))],
            "B": [Event(0, "B", JobPayload("waiting"))],
        }
    )
    assert trigger.is_active(
        {
            "A": [Event(0, "A", JobPayload("failed"))],
            "B": [Event(0, "B", JobPayload("failed"))],
        }
    )
