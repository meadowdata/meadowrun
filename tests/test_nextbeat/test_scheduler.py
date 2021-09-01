from typing import Any

from nextbeat.jobs import Actions, Job, JobStateChangeTrigger
from nextbeat.local_job_runner import LocalJobRunner
from nextbeat.nextrun_job_runner import NextRunJobRunner
from nextbeat.topic import JoinTrigger
from nextbeat.scheduler import Scheduler
import time

from nextrun.job_run_spec import JobRunSpecFunction
from nextrun.server_main import main_in_child_process


def run_func(*args: Any, **_kwargs: Any) -> str:
    return ", ".join(args)


def test_scheduling_sequential_jobs_local() -> None:
    _test_scheduling_sequential_jobs(Scheduler(LocalJobRunner, 0.05))


def test_scheduling_sequential_jobs_nextrun() -> None:
    with main_in_child_process():
        _test_scheduling_sequential_jobs(Scheduler(NextRunJobRunner, 0.05))


def _test_scheduling_sequential_jobs(scheduler: Scheduler) -> None:
    scheduler.add_job(
        Job(
            "A",
            JobRunSpecFunction(run_func, args=["hello", "there"]),
            scheduler._job_runner,
            (),
        )
    )
    trigger_action = (
        JobStateChangeTrigger("A", ("SUCCEEDED", "FAILED")),
        Actions.run,
    )
    scheduler.add_job(
        Job("B", JobRunSpecFunction(run_func), scheduler._job_runner, (trigger_action,))
    )
    scheduler.create_job_subscriptions()

    scheduler.main_loop()

    while not scheduler.all_are_waiting():
        time.sleep(0.01)
    assert 1 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))

    scheduler.manual_run("A")
    time.sleep(0.05)  # see docstring of manual_run for why we need to wait

    while not scheduler.all_are_waiting():
        time.sleep(0.01)

    assert 4 == len(scheduler.events_of("A"))
    assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
        e.payload.state for e in scheduler.events_of("A")
    ]
    assert "hello, there" == scheduler.events_of("A")[0].payload.result_value
    assert 4 == len(scheduler.events_of("B"))


def test_scheduling_join() -> None:
    scheduler = Scheduler(LocalJobRunner, 0.05)

    scheduler.add_job(
        Job("A", JobRunSpecFunction(run_func, args=["A"]), scheduler._job_runner, ())
    )
    scheduler.add_job(
        Job("B", JobRunSpecFunction(run_func, args=["B"]), scheduler._job_runner, ())
    )
    trigger_a = JobStateChangeTrigger("A", ("SUCCEEDED",))
    trigger_b = JobStateChangeTrigger("B", ("SUCCEEDED",))
    trigger_action = (JoinTrigger(trigger_a, trigger_b), Actions.run)
    scheduler.add_job(
        Job(
            "C",
            JobRunSpecFunction(run_func, args=["C"]),
            scheduler._job_runner,
            (trigger_action,),
        )
    )
    scheduler.create_job_subscriptions()

    scheduler.main_loop()

    while not scheduler.all_are_waiting():
        time.sleep(0.01)

    assert 1 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))
    assert 1 == len(scheduler.events_of("C"))

    scheduler.manual_run("A")
    time.sleep(0.05)  # see docstring of manual_run for why we need to wait

    while not scheduler.all_are_waiting():
        time.sleep(0.01)

    assert 4 == len(scheduler.events_of("A"))
    assert 1 == len(scheduler.events_of("B"))
    assert 1 == len(scheduler.events_of("C"))

    scheduler.manual_run("B")
    time.sleep(0.05)  # see docstring of manual_run for why we need to wait

    while not scheduler.all_are_waiting():
        time.sleep(0.01)

    assert 4 == len(scheduler.events_of("A"))
    assert 4 == len(scheduler.events_of("B"))
    assert 4 == len(scheduler.events_of("C"))
