import datetime
import time
from typing import Any, Sequence, Callable, List, Optional

import pytz

import meadowflow.server.config
import meadowflow.server.server_main
import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowflow.event_log import Event
from meadowflow.events_arg import LatestEventsArg
from meadowflow.job_runner_predicates import JobRunnerTypePredicate
from meadowflow.jobs import (
    Actions,
    AllJobStatePredicate,
    AnyJobStateEventFilter,
    Job,
    JobFunction,
    JobPayload,
    JobRunOverrides,
    JobRunnerPredicate,
    LocalFunction,
    add_scope_jobs_decorator,
)
from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.scopes import ScopeValues, ScopeInstantiated
from meadowflow.server.client import MeadowFlowClientSync
from meadowflow.time_event_publisher import PointInTime, PointInTimePredicate
from meadowflow.topic import TriggerAction, NotPredicate
from meadowflow.topic_names import pname, FrozenDict, TopicName, CURRENT_JOB
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.deployed_function import (
    MeadowGridFunction,
    MeadowGridVersionedDeployedRunnable,
)
from meadowgrid.meadowgrid_pb2 import ServerAvailableInterpreter, GitRepoBranch
from test_meadowflow.test_time_events import _TIME_INCREMENT
from test_meadowgrid.test_meadowgrid_basics import TEST_REPO, TEST_WORKING_FOLDER


def _run_func(*args: Any, **_kwargs: Any) -> str:
    """A simple example "user function" for our tests"""
    return ", ".join([str(a) for a in args])


def _run_latest_events(events: FrozenDict[TopicName, Optional[Event]]) -> Any:
    """A "user function" that is interested in what events caused it to run"""

    # get the latest event
    latest_topic_name, latest_event = max(
        ((topic, event) for (topic, event) in events.items() if event is not None),
        key=lambda kv: kv[1].timestamp,
    )

    # if the latest event was a JobPayload, return the topic name and result value,
    # otherwise just return the topic name
    if isinstance(latest_event.payload, JobPayload):
        print(latest_topic_name, latest_event.payload.result_value)
        return latest_topic_name, latest_event.payload.result_value
    else:
        print(latest_topic_name)
        return latest_topic_name


def test_simple_jobs_local() -> None:
    """Tests a simple use case using the local jobrunner"""
    with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
        _test_simple_jobs(
            s,
            lambda args: LocalFunction(_run_func, args),
            JobRunnerTypePredicate("local"),
        )


def test_simple_jobs_meadowgrid_server_available() -> None:
    """
    Tests a simple use case using the meadowgrid jobrunner with a ServerAvailableFolder
    deployment
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(MeadowGridJobRunner)
            _test_simple_jobs(
                s,
                # TODO MeadowGridJobRunner automatically converts LocalFunctions into
                #  ServerAvailableFolder, not sure if that really makes sense.
                lambda args: LocalFunction(_run_func, args),
                JobRunnerTypePredicate("meadowgrid"),
            )


def test_simple_jobs_meadowgrid_git() -> None:
    """
    Tests a simple use case using the meadowgrid jobrunner with a git repo deployment

    Running this requires cloning https://github.com/meadowdata/test_repo next to the
    meadowdata repo.
    """

    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(MeadowGridJobRunner)
            _test_simple_jobs(
                s,
                lambda args: MeadowGridVersionedDeployedRunnable(
                    GitRepoBranch(repo_url=TEST_REPO, branch="main"),
                    ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
                    MeadowGridFunction.from_name(
                        "example_package.example", "join_strings", args
                    ),
                ),
                JobRunnerTypePredicate("meadowgrid"),
            )


def _wait_for_scheduler(scheduler: Scheduler) -> None:
    # the initial 50ms wait is in case we did any actions like manual_run that execute
    # on the event loop, we want to wait for those to execute before we query
    # all_are_waiting
    time.sleep(0.05)
    while not scheduler.all_are_waiting():
        time.sleep(0.01)


def _test_simple_jobs(
    scheduler: Scheduler,
    job_function_constructor: Callable[[Sequence[Any]], JobFunction],
    job_runner_predicate: JobRunnerPredicate,
) -> None:
    """job_function_constructor takes some arguments and should return a JobFunction"""
    scheduler.add_jobs(
        [
            Job(
                pname("A"),
                job_function_constructor(["hello", "there"]),
                (),
                job_runner_predicate,
            ),
            Job(
                pname("B"),
                job_function_constructor([]),
                [
                    TriggerAction(
                        Actions.run,
                        [AnyJobStateEventFilter([pname("A")], ["SUCCEEDED", "FAILED"])],
                    )
                ],
                job_runner_predicate,
            ),
        ]
    )

    scheduler.main_loop()

    _wait_for_scheduler(scheduler)
    assert 1 == len(scheduler.events_of(pname("A")))
    assert 1 == len(scheduler.events_of(pname("B")))

    scheduler.manual_run(pname("A"))
    _wait_for_scheduler(scheduler)
    assert 4 == len(scheduler.events_of(pname("A")))
    assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
        e.payload.state for e in scheduler.events_of(pname("A"))
    ]
    assert "hello, there" == scheduler.events_of(pname("A"))[0].payload.result_value
    assert 4 == len(scheduler.events_of(pname("B")))


def _wait_for_events(
    client: MeadowFlowClientSync,
    seconds_to_wait: float,
    topic_name: TopicName,
    num_events_to_wait_for: int,
) -> None:
    """
    Waits up to seconds_to_wait for at least num_events_to_wait_for to show up on
    topic_name
    """
    start = time.time()
    events = None
    while time.time() - start < seconds_to_wait:
        events = client.get_events([topic_name])
        if len(events) >= num_events_to_wait_for:
            break

    if events is None:
        raise ValueError(
            "This should never happen--test process suffered a very long pause "
            f"{seconds_to_wait}"
        )

    if len(events) < num_events_to_wait_for:
        raise AssertionError(
            f"Waited {seconds_to_wait} second for {num_events_to_wait_for} event(s) on "
            f"{topic_name} but only got {len(events)} events"
        )


def test_simple_jobs_meadowflow_server() -> None:
    """
    Tests a simple use case using an actual meadowflow server (the tests above use a
    local in-process scheduler).

    to run under the debugger as separate processes, launch
    `meadowflow/server/server_main.py`, then `meadowgrid/server_main.py
    --meadowflow_address localhost:15321`, then run this test. The child processes we
    try to spawn will fail to bind to their ports, but that won't prevent the main test
    logic from completing successfully.
    """

    with meadowflow.server.server_main.main_in_child_process(
        job_runner_poll_delay_seconds=0.05,
    ), meadowgrid.coordinator_main.main_in_child_process(
        meadowflow_address=meadowflow.server.config.DEFAULT_ADDRESS
    ), meadowgrid.job_worker_main.main_in_child_process(
        TEST_WORKING_FOLDER
    ):
        with meadowflow.server.client.MeadowFlowClientSync() as client:
            # wait for the meadowgrid job runner to register itself with the scheduler
            time.sleep(3)

            client.add_jobs(
                [
                    Job(
                        pname("A"),
                        LocalFunction(_run_func, ["hello", "there"]),
                        [],
                        JobRunnerTypePredicate("meadowgrid"),
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
                        JobRunnerTypePredicate("meadowgrid"),
                    ),
                    Job(
                        pname("T"),
                        LocalFunction(_run_func),
                        [
                            TriggerAction(
                                Actions.run,
                                [
                                    PointInTime(
                                        meadowflow.time_event_publisher._utc_now()
                                        + datetime.timedelta(seconds=1)
                                    )
                                ],
                            )
                        ],
                        JobRunnerTypePredicate("meadowgrid"),
                    ),
                ]
            )

            # wait for initial WAITING events to get created
            _wait_for_events(client, 1, pname("B"), 1)
            # This is a little sketchy because we aren't strictly guaranteed that A will
            # get created before B, but this is good enough for now
            assert 1 == len(client.get_events([pname("A")]))
            assert 1 == len(client.get_events([pname("B")]))

            # now run manually and then poll for 10s for events on A and B to show up
            client.manual_run(pname("A"))
            _wait_for_events(client, 10, pname("B"), 4)
            a_events = client.get_events([pname("A")])
            b_events = client.get_events([pname("B")])
            assert 4 == len(a_events)
            assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
                e.payload.state for e in a_events
            ]
            assert "hello, there" == a_events[0].payload.result_value
            assert 4 == len(b_events)

            # Now run B manually with an args override and make sure the arguments make
            # it through. Also use wait_for_completion=True, which means we don't need
            # to use _wait_for_events
            client.manual_run(
                pname("B"),
                JobRunOverrides(["manual", "override"]),
                wait_for_completion=True,
            )
            assert (
                "manual, override"
                == client.get_events([pname("B")])[0].payload.result_value
            )

            # wait up to 2s, which means that T should have automatically been triggered
            # and completed running
            _wait_for_events(client, 2, pname("T"), 4)
            t_events = client.get_events([pname("T")])
            assert 4 == len(t_events)
            assert ["SUCCEEDED", "RUNNING", "RUN_REQUESTED", "WAITING"] == [
                e.payload.state for e in t_events
            ]


def test_triggers() -> None:
    """Test different interesting trigger combinations"""

    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_run_func, ["A"]), ()),
                Job(pname("B"), LocalFunction(_run_func, ["B"]), ()),
                # see TriggerAction docstring for explanations of these triggers
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
                            AllJobStatePredicate(
                                [pname("A"), pname("B")], ["SUCCEEDED"]
                            ),
                        )
                    ],
                ),
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
                ),
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
                ),
            ]
        )

        scheduler.main_loop()

        # Note that we use the event count to tell whether the job has run or not--each
        # run will generate 3 events: RUN_REQUESTED, RUNNING, SUCCEEDED.
        def assert_num_events(expected: List[int]) -> None:
            assert [
                len(scheduler.events_of(pname(n))) for n in ("A", "B", "C1", "C2", "C3")
            ] == expected

        _wait_for_scheduler(scheduler)
        assert_num_events([1, 1, 1, 1, 1])

        # first we run A:
        # - C1 doesn't run because its condition is not met--B is not SUCCEEDED
        # - C2 runs because it triggers any time A or B succeeds, regardless of the
        #   overall state of the jobs
        # - C3 doesn't run because its condition is not met--B is not SUCCEEDED

        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)
        assert_num_events([4, 1, 1, 4, 1])

        # next, we run B:
        # - C1 runs because because B succeeding wakes it up, and its conditions are met
        #   (A and B are both SUCCEEDED)
        # - C2 runs again
        # - C3 does not run, because it only wakes up when A succeeds. B succeeding is a
        #   condition, but doesn't wake it up

        scheduler.manual_run(pname("B"))
        _wait_for_scheduler(scheduler)
        assert_num_events([4, 4, 4, 7, 1])

        # finally, we run A again
        # - C1 runs again
        # - C2 runs again
        # - C3 finally runs because A succeeding wakes it up, and B is in a SUCCEEDED
        #   state

        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)
        assert_num_events([7, 4, 7, 10, 4])


_succeed_on_third_try_runs = 0


def _succeed_on_third_try():
    """Fake job. Fails the first two times it's run, succeeds afterwards."""
    global _succeed_on_third_try_runs
    orig_runs = _succeed_on_third_try_runs

    _succeed_on_third_try_runs += 1

    if orig_runs <= 1:
        raise ValueError(f"Failing as it's run number {orig_runs}")


def test_current_job_state():
    """Tests using CURRENT_JOB with AllJobStatePredicate"""
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_run_func), []),
                Job(
                    pname("B"),
                    LocalFunction(_succeed_on_third_try),
                    [
                        TriggerAction(
                            Actions.run,
                            [AnyJobStateEventFilter([pname("A")], "SUCCEEDED")],
                            NotPredicate(
                                AllJobStatePredicate([CURRENT_JOB], ["SUCCEEDED"])
                            ),
                        )
                    ],
                ),
            ]
        )
        scheduler.main_loop()

        # Sequence of events should be:
        # 1. A runs, causes B to run, but B fails
        # 2. A runs, causes B to run, but B fails
        # 3. A runs, causes B to run, succeeds
        # 4. A runs, B does not run because it has already succeeded
        for _ in range(4):
            scheduler.manual_run(pname("A"))
            _wait_for_scheduler(scheduler)

        assert len(scheduler.events_of(pname("B"))) == 10


def test_time_topics_1():
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        past_dt = now - 3 * _TIME_INCREMENT

        scheduler.add_jobs(
            [
                Job(
                    pname("A"),
                    LocalFunction(_run_func, ["A"]),
                    [
                        TriggerAction(
                            Actions.run,
                            [PointInTime(past_dt)],
                            # this will always be true, just testing that the
                            # functionality works
                            PointInTimePredicate(past_dt, "after"),
                        )
                    ],
                ),
                Job(
                    pname("B"),
                    LocalFunction(_run_func, ["B"]),
                    [
                        TriggerAction(
                            Actions.run,
                            [PointInTime(past_dt)],
                            # this is an impossible predicate to satisfy, so this job
                            # should never get triggered automatically
                            PointInTimePredicate(past_dt, "before"),
                        )
                    ],
                ),
            ]
        )

        scheduler.main_loop()
        _wait_for_scheduler(scheduler)
        assert 4 == len(scheduler.events_of(pname("A")))
        assert 1 == len(scheduler.events_of(pname("B")))


def test_time_topics_2():
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        scheduler.add_jobs(
            [
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
            ]
        )

        scheduler.main_loop()
        _wait_for_scheduler(scheduler)
        # TODO it's not clear that Job A getting run once is the right semantics. What's
        #  happening is that as long as EventLog.call_subscribers for the second point
        #  in time gets called while Job A is still running from the first point in
        #  time, we'll ignore run request. See Run.execute
        assert 4 == len(scheduler.events_of(pname("A")))

        # wait for the next point_in_time_trigger, which should cause another run to
        # happen
        time.sleep(2 * _TIME_INCREMENT.total_seconds())

        _wait_for_scheduler(scheduler)
        assert 7 == len(scheduler.events_of(pname("A")))


def test_latest_events_arg():
    """Test behavior of LatestEventsArg"""
    now = pytz.utc.localize(datetime.datetime.utcnow())

    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_run_func, ["hello", "there"]), ()),
                Job(
                    pname("B"),
                    LocalFunction(_run_latest_events, [LatestEventsArg.construct()]),
                    [
                        TriggerAction(
                            Actions.run,
                            [
                                AnyJobStateEventFilter(
                                    [pname("A")], ["SUCCEEDED", "FAILED"]
                                ),
                                PointInTime(now),
                            ],
                        )
                    ],
                ),
            ]
        )

        scheduler.main_loop()

        # give the PointInTime a little bit of time to kick in, then wait for B to get
        # triggered off of that
        _wait_for_scheduler(scheduler)
        assert 1 == len(scheduler.events_of(pname("A")))
        b_events = scheduler.events_of(pname("B"))
        assert 4 == len(b_events)
        assert (
            pname("time/point", dt=now, tzinfo=now.tzinfo)
            == b_events[0].payload.result_value
        )

        # kick off A, wait for B to get triggered off of that
        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)
        b_events = scheduler.events_of(pname("B"))
        assert 7 == len(scheduler.events_of(pname("B")))
        assert (pname("A"), "hello, there") == b_events[0].payload.result_value


_TEST_DATE_1 = datetime.date(2021, 9, 21)
_TEST_DATE_2 = datetime.date(2021, 9, 22)


def _run_instantiate_date_scope() -> ScopeValues:
    return ScopeValues(date=_TEST_DATE_1)


@add_scope_jobs_decorator
def _run_add_scope_jobs(scope: ScopeValues) -> Sequence[Job]:
    return [
        Job(pname("date_job"), LocalFunction(_run_func, ["hello", scope["date"]]), [])
    ]


def test_scopes():
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(
                    pname("instantiate_date_scopes"),
                    LocalFunction(_run_instantiate_date_scope),
                    [],
                ),
                Job(
                    pname("add_date_scope_jobs"),
                    LocalFunction(_run_add_scope_jobs, [LatestEventsArg.construct()]),
                    [TriggerAction(Actions.run, [ScopeInstantiated.construct("date")])],
                ),
            ]
        )

        scheduler.main_loop()

        # kick off instantiate_date_scopes, this should cause add_date_scope_jobs to
        # run, which will create date_job for _TEST_DATE
        scheduler.manual_run(pname("instantiate_date_scopes"))
        _wait_for_scheduler(scheduler)

        # so now we should be able to run date_job for _TEST_DATE_1:
        scheduler.manual_run(pname("date_job", date=_TEST_DATE_1))
        _wait_for_scheduler(scheduler)
        events = scheduler.events_of(pname("date_job", date=_TEST_DATE_1))
        assert 4 == len(events)
        assert f"hello, {_TEST_DATE_1}" == events[0].payload.result_value

        # create another scope manually and run date_job in that scope manually
        scheduler.instantiate_scope(ScopeValues(date=_TEST_DATE_2))
        _wait_for_scheduler(scheduler)
        scheduler.manual_run(pname("date_job", date=_TEST_DATE_2))
        _wait_for_scheduler(scheduler)
        events = scheduler.events_of(pname("date_job", date=_TEST_DATE_2))
        assert 4 == len(events)
        assert f"hello, {_TEST_DATE_2}" == events[0].payload.result_value
