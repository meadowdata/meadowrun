import datetime
from typing import Any

import pytz

from nextbeat.jobs import Actions, Job, JobStateChangeTrigger
from nextbeat.local_job_runner import LocalJobRunner
from nextbeat.nextrun_job_runner import NextRunJobRunner
from nextbeat.topic import JoinTrigger
from nextbeat.scheduler import Scheduler
import time

from nextrun.job_run_spec import JobRunSpecFunction
from nextrun.server_main import main_in_child_process

from test_nextbeat.test_time_events import _TIME_INCREMENT, _TIME_DELAY


def run_func(*args: Any, **_kwargs: Any) -> str:
    return ", ".join(args)


def test_scheduling_sequential_jobs_local() -> None:
    with Scheduler(LocalJobRunner, 0.05) as s:
        _test_scheduling_sequential_jobs(s)


def test_scheduling_sequential_jobs_nextrun() -> None:
    with main_in_child_process():
        with Scheduler(NextRunJobRunner, 0.05) as s:
            _test_scheduling_sequential_jobs(s)


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
    with Scheduler(LocalJobRunner, 0.05) as scheduler:
        scheduler.add_job(
            Job(
                "A", JobRunSpecFunction(run_func, args=["A"]), scheduler._job_runner, ()
            )
        )
        scheduler.add_job(
            Job(
                "B", JobRunSpecFunction(run_func, args=["B"]), scheduler._job_runner, ()
            )
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


# TODO test adding jobs while scheduler is running


def test_time_topics_1():
    with Scheduler(LocalJobRunner, 0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                JobRunSpecFunction(run_func, args=["A"]),
                s._job_runner,
                (
                    (
                        s.time.point_in_time_trigger(now - 3 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                ),
            )
        )

        s.create_job_subscriptions()

        s.main_loop()

        time.sleep(_TIME_DELAY)

        while not s.all_are_waiting():
            time.sleep(0.01)
        assert 4 == len(s.events_of("A"))


def test_time_topics_2():
    with Scheduler(LocalJobRunner, 0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                JobRunSpecFunction(run_func, args=["A"]),
                s._job_runner,
                (
                    (
                        s.time.point_in_time_trigger(now - 3 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                    (
                        s.time.point_in_time_trigger(now - 2 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                ),
            )
        )

        s.create_job_subscriptions()

        s.main_loop()

        time.sleep(_TIME_DELAY)

        while not s.all_are_waiting():
            time.sleep(0.01)
        # TODO it's not clear that Job A getting run once is the right semantics. What's
        #  happening is that as long as EventLog.call_subscribers for the second point
        #  in time gets called while Job A is still running from the first point in
        #  time, we'll ignore run request. See Run.execute
        assert 4 == len(s.events_of("A"))


def test_time_topics_3():
    with Scheduler(LocalJobRunner, 0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                JobRunSpecFunction(run_func, args=["A"]),
                s._job_runner,
                (
                    (
                        s.time.point_in_time_trigger(now - 3 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                    (
                        s.time.point_in_time_trigger(now - 2 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                    (
                        s.time.point_in_time_trigger(now + 2 * _TIME_INCREMENT),
                        Actions.run,
                    ),
                ),
            )
        )

        s.create_job_subscriptions()

        s.main_loop()

        time.sleep(_TIME_DELAY)

        while not s.all_are_waiting():
            time.sleep(0.01)
        assert 4 == len(s.events_of("A"))

        # wait for the next point_in_time_trigger, which should cause another run to
        # happen
        time.sleep(2 * _TIME_INCREMENT.total_seconds())

        while not s.all_are_waiting():
            time.sleep(0.01)
        assert 7 == len(s.events_of("A"))
