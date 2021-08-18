from typing import Any
from nextbeat.jobs import Actions, Job, JobStateChangeTrigger, JoinTrigger
from nextbeat.scheduler import Scheduler
import time


def run_func(*_args: Any, **_kwargs: Any) -> bool:
    return True


def test_scheduling_sequential_jobs() -> None:
    scheduler = Scheduler()

    scheduler.add_job(Job("A", run_func, ()))
    trigger_action = (JobStateChangeTrigger("A", ("succeeded", "failed")), Actions.run)
    scheduler.add_job(Job("B", run_func, (trigger_action,)))
    scheduler.update_subscriptions()

    while not scheduler.is_done():
        scheduler.step()
    assert 1 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))

    scheduler.manual_run("A")

    # launched
    assert 2 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))

    while not scheduler.is_done():
        time.sleep(0.1)
        scheduler.step()

    # waiting, launched, running, succeeded
    assert 4 == len(scheduler.events_of("A"))
    assert 4 == len(scheduler.events_of("B"))


def test_scheduling_join() -> None:
    scheduler = Scheduler()

    scheduler.add_job(Job("A", run_func, (), args=["A"]))
    scheduler.add_job(Job("B", run_func, (), args=["B"]))
    trigger_a = JobStateChangeTrigger("A", ("succeeded",))
    trigger_b = JobStateChangeTrigger("B", ("succeeded",))
    trigger_action = (JoinTrigger(trigger_a, trigger_b), Actions.run)
    scheduler.add_job(Job("C", run_func, (trigger_action,), args=["C"]))
    scheduler.update_subscriptions()

    while not scheduler.is_done():
        scheduler.step()

    assert 1 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))
    assert 1 == len(scheduler.events_of("C"))

    scheduler.manual_run("A")

    while not scheduler.is_done():
        scheduler.step()

    assert 4 == len(scheduler.events_of("A"))

    scheduler.manual_run("B")

    while not scheduler.is_done():
        scheduler.step()

    assert 4 == len(scheduler.events_of("A"))
    assert 4 == len(scheduler.events_of("B"))
    assert 4 == len(scheduler.events_of("C"))
