import datetime
import pathlib
import sys
from typing import Any, Sequence, Callable

import pytz

from nextbeat.jobs import (
    Actions,
    Job,
    JobStateChangeTrigger,
    JobRunnerPredicate,
    JobRunnerTypePredicate,
    JobFunction,
)
from nextbeat.nextrun_job_runner import NextRunJobRunner, NextRunFunctionGitRepo
import nextbeat.server.config
from nextbeat.topic import JoinTrigger
from nextbeat.scheduler import Scheduler
import nextbeat.server.server_main
import time

from nextbeat.jobs_common import LocalFunction
import nextrun.server_main
from nextrun.deployed_function import NextRunFunction

from test_nextbeat.test_time_events import _TIME_INCREMENT, _TIME_DELAY


def run_func(*args: Any, **_kwargs: Any) -> str:
    return ", ".join(args)


def test_scheduling_sequential_jobs_local() -> None:
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        _test_scheduling_sequential_jobs(
            s,
            lambda args: LocalFunction(run_func, args),
            JobRunnerTypePredicate("local"),
        )


def test_scheduling_sequential_jobs_nextbeat_server() -> None:
    # to run under the debugger as separate processes, launch
    # `nextbeat/server/server_main.py`, then `nextrun/server_main.py --nextbeat_address
    # localhost:15321`, then run this test. The child processes we try to spawn will
    # fail to bind to their ports, but that won't prevent the main test logic from
    # completing successfully.

    with nextbeat.server.server_main.main_in_child_process(
        nextbeat.server.config.DEFAULT_HOST, nextbeat.server.config.DEFAULT_PORT, 0.05
    ), nextrun.server_main.main_in_child_process(
        nextbeat_address=nextbeat.server.config.DEFAULT_ADDRESS
    ), nextbeat.server.client.NextBeatClientSync(
        nextbeat.server.config.DEFAULT_ADDRESS
    ) as client:
        # wait for the nextrun job runner to register itself with the scheduler
        time.sleep(2)

        client.add_jobs(
            [
                Job(
                    "A",
                    LocalFunction(run_func, ["hello", "there"]),
                    (),
                    JobRunnerTypePredicate("nextrun"),
                ),
                Job(
                    "B",
                    LocalFunction(run_func),
                    (
                        (
                            JobStateChangeTrigger("A", ("SUCCEEDED", "FAILED")),
                            Actions.run,
                        ),
                    ),
                    JobRunnerTypePredicate("nextrun"),
                ),
            ]
        )

        # poll for 1s for one event of B to show up
        start = time.time()
        events = None
        while time.time() - start < 1:
            events = client.get_events(["B"])
            if len(events) > 0:
                break

        if events is None:
            raise ValueError(
                "This should never happen--test process suffered a very long pause (1s)"
            )

        if len(events) == 0:
            raise AssertionError(
                "Waited 1 second for WAITING event on A but did not happen"
            )

        # This is a little sketchy because we aren't strictly guaranteed that A will get
        # created before B, but this is good enough for now
        assert 1 == len(client.get_events(["A"]))
        assert 1 == len(client.get_events(["B"]))

        # now run manually and then poll for 2s for events on A and B to show up
        client.manual_run("A")

        start = time.time()
        events = None
        while time.time() - start < 2:
            events = client.get_events(["B"])
            if len(events) > 1:
                break

        if events is None:
            raise ValueError(
                "This should never happen--test process suffered a very long pause (2s)"
            )

        if len(events) <= 1:
            raise AssertionError(
                "Waited 2 seconds for running-related events on B but did not happen"
            )

        a_events = client.get_events(["A"])
        b_events = client.get_events(["A"])
        assert 4 == len(a_events)
        assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
            e.payload.state for e in a_events
        ]
        assert "hello, there" == a_events[0].payload.result_value
        assert 4 == len(b_events)


def test_scheduling_sequential_jobs_nextrun() -> None:
    with nextrun.server_main.main_in_child_process():
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(NextRunJobRunner)
            _test_scheduling_sequential_jobs(
                s,
                lambda args: LocalFunction(run_func, args),  # TODO change to
                # ServerAvailable
                JobRunnerTypePredicate("nextrun"),
            )


def test_scheduling_sequential_jobs_nextrun_git() -> None:
    """
    Running this requires cloning https://github.com/nextdataplatform/test_repo next
    to the nextdataplatform repo.
    """

    test_repo = str(
        (pathlib.Path(__file__).parent.parent.parent.parent / "test_repo").resolve()
    )

    with nextrun.server_main.main_in_child_process():
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(NextRunJobRunner)
            _test_scheduling_sequential_jobs(
                s,
                lambda args: NextRunFunctionGitRepo(
                    test_repo,
                    "main",
                    sys.executable,
                    NextRunFunction("example_package.example", "join_strings", args),
                ),
                JobRunnerTypePredicate("nextrun"),
            )


def _test_scheduling_sequential_jobs(
    scheduler: Scheduler,
    job_function_constructor: Callable[[Sequence[Any]], JobFunction],
    job_runner_predicate: JobRunnerPredicate,
) -> None:
    """job_function_constructor takes some arguments and should return a JobFunction"""
    scheduler.add_job(
        Job(
            "A",
            job_function_constructor(["hello", "there"]),
            (),
            job_runner_predicate,
        )
    )
    trigger_action = (
        JobStateChangeTrigger("A", ("SUCCEEDED", "FAILED")),
        Actions.run,
    )
    scheduler.add_job(
        Job("B", job_function_constructor([]), (trigger_action,), job_runner_predicate)
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
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_job(Job("A", LocalFunction(run_func, ["A"]), ()))
        scheduler.add_job(Job("B", LocalFunction(run_func, ["B"]), ()))
        trigger_a = JobStateChangeTrigger("A", ("SUCCEEDED",))
        trigger_b = JobStateChangeTrigger("B", ("SUCCEEDED",))
        trigger_action = (JoinTrigger(trigger_a, trigger_b), Actions.run)
        scheduler.add_job(
            Job(
                "C",
                LocalFunction(run_func, ["C"]),
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
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                LocalFunction(run_func, ["A"]),
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
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                LocalFunction(run_func, ["A"]),
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
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                "A",
                LocalFunction(run_func, ["A"]),
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
