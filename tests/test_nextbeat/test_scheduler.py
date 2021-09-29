import datetime
import pathlib
import sys
from typing import Any, Sequence, Callable, List

import pytz

from nextbeat.topic_names import pname
from nextbeat.jobs import (
    Actions,
    Job,
    AnyJobStateEventFilter,
    JobRunnerPredicate,
    JobFunction,
    LocalFunction,
    AllJobStatePredicate,
)
from nextbeat.job_runner_predicates import JobRunnerTypePredicate
from nextbeat.nextrun_job_runner import NextRunJobRunner, NextRunFunctionGitRepo
import nextbeat.server.config
from nextbeat.scheduler import Scheduler
import nextbeat.server.server_main
import time

import nextrun.server_main
from nextbeat.time_event_publisher import PointInTime
from nextbeat.topic import TriggerAction
from nextrun.deployed_function import NextRunFunction

from test_nextbeat.test_time_events import _TIME_INCREMENT, _TIME_DELAY


def _run_func(*args: Any, **_kwargs: Any) -> str:
    return ", ".join(args)


def test_simple_jobs_local() -> None:
    """Tests a simple use case using the local jobrunner"""
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        _test_simple_jobs(
            s,
            lambda args: LocalFunction(_run_func, args),
            JobRunnerTypePredicate("local"),
        )


def test_simple_jobs_nextrun_server_available() -> None:
    """
    Tests a simple use case using the nextrun jobrunner with a ServerAvailableFolder
    deployment
    """
    with nextrun.server_main.main_in_child_process():
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(NextRunJobRunner)
            _test_simple_jobs(
                s,
                # TODO NextRunJobRunner automatically converts LocalFunctions into
                #  ServerAvailableFolder, not sure if that really makes sense.
                lambda args: LocalFunction(_run_func, args),
                JobRunnerTypePredicate("nextrun"),
            )


def test_simple_jobs_nextrun_git() -> None:
    """
    Tests a simple use case using the nextrun jobrunner with a git repo deployment

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
            _test_simple_jobs(
                s,
                lambda args: NextRunFunctionGitRepo(
                    test_repo,
                    "main",
                    sys.executable,
                    NextRunFunction("example_package.example", "join_strings", args),
                ),
                JobRunnerTypePredicate("nextrun"),
            )


def _test_simple_jobs(
    scheduler: Scheduler,
    job_function_constructor: Callable[[Sequence[Any]], JobFunction],
    job_runner_predicate: JobRunnerPredicate,
) -> None:
    """job_function_constructor takes some arguments and should return a JobFunction"""
    scheduler.add_job(
        Job(
            pname("A"),
            job_function_constructor(["hello", "there"]),
            (),
            job_runner_predicate,
        )
    )
    trigger_action = TriggerAction(
        Actions.run,
        [AnyJobStateEventFilter([pname("A")], ["SUCCEEDED", "FAILED"])],
    )
    scheduler.add_job(
        Job(
            pname("B"),
            job_function_constructor([]),
            [trigger_action],
            job_runner_predicate,
        )
    )
    scheduler.create_job_subscriptions()

    scheduler.main_loop()

    while not scheduler.all_are_waiting():
        time.sleep(0.01)
    assert 1 == len(scheduler.events_of(pname("A")))
    assert 1 == len(scheduler.events_of(pname("B")))

    scheduler.manual_run(pname("A"))
    time.sleep(0.05)  # see docstring of manual_run for why we need to wait

    while not scheduler.all_are_waiting():
        time.sleep(0.01)

    assert 4 == len(scheduler.events_of(pname("A")))
    assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
        e.payload.state for e in scheduler.events_of(pname("A"))
    ]
    assert "hello, there" == scheduler.events_of(pname("A"))[0].payload.result_value
    assert 4 == len(scheduler.events_of(pname("B")))


def test_simple_jobs_nextbeat_server() -> None:
    """
    Tests a simple use case using an actual nextbeat server (the tests above use a local
    in-process scheduler).

    to run under the debugger as separate processes, launch
    `nextbeat/server/server_main.py`, then `nextrun/server_main.py --nextbeat_address
    localhost:15321`, then run this test. The child processes we try to spawn will fail
    to bind to their ports, but that won't prevent the main test logic from completing
    successfully.
    """

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
                    pname("A"),
                    LocalFunction(_run_func, ["hello", "there"]),
                    [],
                    JobRunnerTypePredicate("nextrun"),
                ),
                Job(
                    pname("B"),
                    LocalFunction(_run_func),
                    [
                        TriggerAction(
                            Actions.run,
                            [
                                AnyJobStateEventFilter(
                                    [pname("A")], ["SUCCEEDED", "FAILED"]
                                )
                            ],
                        )
                    ],
                    JobRunnerTypePredicate("nextrun"),
                ),
                Job(
                    pname("T"),
                    LocalFunction(_run_func),
                    [
                        TriggerAction(
                            Actions.run,
                            [
                                PointInTime(
                                    nextbeat.time_event_publisher._utc_now()
                                    + datetime.timedelta(seconds=1)
                                )
                            ],
                        )
                    ],
                    JobRunnerTypePredicate("nextrun"),
                ),
            ]
        )

        # poll for 1s for one event of B to show up
        start = time.time()
        events = None
        while time.time() - start < 1:
            events = client.get_events([pname("B")])
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
        assert 1 == len(client.get_events([pname("A")]))
        assert 1 == len(client.get_events([pname("B")]))

        # now run manually and then poll for 2s for events on A and B to show up
        client.manual_run(pname("A"))

        start = time.time()
        events = None
        while time.time() - start < 2:
            events = client.get_events([pname("B")])
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

        a_events = client.get_events([pname("A")])
        b_events = client.get_events([pname("A")])
        assert 4 == len(a_events)
        assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
            e.payload.state for e in a_events
        ]
        assert "hello, there" == a_events[0].payload.result_value
        assert 4 == len(b_events)

        # wait another second, which means that T should have automatically been
        # triggered

        time.sleep(1)

        t_events = client.get_events([pname("T")])
        assert 4 == len(t_events)
        assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
            e.payload.state for e in t_events
        ]


def test_triggers() -> None:
    """Test different interesting trigger combinations"""

    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_job(Job(pname("A"), LocalFunction(_run_func, ["A"]), ()))
        scheduler.add_job(Job(pname("B"), LocalFunction(_run_func, ["B"]), ()))

        # see TriggerAction docstring for explanations of these triggers
        scheduler.add_job(
            Job(
                pname("C1"),
                LocalFunction(_run_func, ["C1"]),
                [
                    TriggerAction(
                        Actions.run,
                        [
                            AnyJobStateEventFilter(
                                [pname("A"), pname("B")], ["SUCCEEDED"]
                            )
                        ],
                        AllJobStatePredicate([pname("A"), pname("B")], ["SUCCEEDED"]),
                    )
                ],
            )
        )
        scheduler.add_job(
            Job(
                pname("C2"),
                LocalFunction(_run_func, ["C2"]),
                [
                    TriggerAction(
                        Actions.run,
                        [
                            AnyJobStateEventFilter(
                                [pname("A"), pname("B")], ["SUCCEEDED"]
                            )
                        ],
                    )
                ],
            )
        )
        scheduler.add_job(
            Job(
                pname("C3"),
                LocalFunction(_run_func, ["C3"]),
                [
                    TriggerAction(
                        Actions.run,
                        [AnyJobStateEventFilter([pname("A")], ["SUCCEEDED"])],
                        AllJobStatePredicate([pname("B")], ["SUCCEEDED"]),
                    )
                ],
            )
        )
        scheduler.create_job_subscriptions()

        scheduler.main_loop()

        while not scheduler.all_are_waiting():
            time.sleep(0.01)

        # Note that we use the event count to tell whether the job has run or not--each
        # run will generate 3 events: RUN_REQUESTED, RUNNING, SUCCEEDED.
        def assert_num_events(expected: List[int]) -> None:
            assert [
                len(scheduler.events_of(pname(n))) for n in ("A", "B", "C1", "C2", "C3")
            ] == expected

        assert_num_events([1, 1, 1, 1, 1])

        # first we run A:
        # - C1 doesn't run because its condition is not met--B is not SUCCEEDED
        # - C2 runs because it triggers any time A or B succeeds, regardless of the
        #   overall state of the jobs
        # - C3 doesn't run because its condition is not met--B is not SUCCEEDED

        scheduler.manual_run(pname("A"))

        time.sleep(0.05)  # see docstring of manual_run for why we need to wait
        while not scheduler.all_are_waiting():
            time.sleep(0.01)

        assert_num_events([4, 1, 1, 4, 1])

        # next, we run B:
        # - C1 runs because because B succeeding wakes it up, and its conditions are met
        #   (A and B are both SUCCEEDED)
        # - C2 runs again
        # - C3 does not run, because it only wakes up when A succeeds. B succeeding is a
        #   condition, but doesn't wake it up

        scheduler.manual_run(pname("B"))

        time.sleep(0.05)  # see docstring of manual_run for why we need to wait
        while not scheduler.all_are_waiting():
            time.sleep(0.01)

        assert_num_events([4, 4, 4, 7, 1])

        # finally, we run A again
        # - C1 runs again
        # - C2 runs again
        # - C3 finally runs because A succeeding wakes it up, and B is in a SUCCEEDED
        #   state

        scheduler.manual_run(pname("A"))

        time.sleep(0.05)  # see docstring of manual_run for why we need to wait
        while not scheduler.all_are_waiting():
            time.sleep(0.01)

        assert_num_events([7, 4, 7, 10, 4])


def test_time_topics_1():
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                pname("A"),
                LocalFunction(_run_func, ["A"]),
                [
                    TriggerAction(
                        Actions.run,
                        [PointInTime(now - 3 * _TIME_INCREMENT)],
                    )
                ],
            )
        )

        s.create_job_subscriptions()

        s.main_loop()

        time.sleep(_TIME_DELAY)

        while not s.all_are_waiting():
            time.sleep(0.01)
        assert 4 == len(s.events_of(pname("A")))


def test_time_topics_2():
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        s.add_job(
            Job(
                pname("A"),
                LocalFunction(_run_func, ["A"]),
                [
                    TriggerAction(
                        Actions.run,
                        [
                            PointInTime(now - 3 * _TIME_INCREMENT),
                            PointInTime(now - 2 * _TIME_INCREMENT),
                            PointInTime(now + 2 * _TIME_INCREMENT),
                        ],
                    )
                ],
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
        assert 4 == len(s.events_of(pname("A")))

        # wait for the next point_in_time_trigger, which should cause another run to
        # happen
        time.sleep(2 * _TIME_INCREMENT.total_seconds())

        while not s.all_are_waiting():
            time.sleep(0.01)
        assert 7 == len(s.events_of(pname("A")))
