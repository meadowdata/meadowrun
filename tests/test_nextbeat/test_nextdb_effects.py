import datetime

import pandas as pd

import nextdb
from nextbeat.jobs import (
    Job,
    LocalFunction,
    Actions,
    AnyJobStateEventFilter,
)
from nextbeat.effects import NextdbDynamicDependency, UntilNextdbWritten
from nextbeat.scheduler import Scheduler
from nextbeat.scopes import BASE_SCOPE, ALL_SCOPES, ScopeValues
from nextbeat.topic import TriggerAction
from nextbeat.topic_names import pname
from nextdb.connection import NextdbEffects, prod_userspace_name
from test_nextbeat.test_scheduler import _wait_for_scheduler, _run_func
import tests.test_nextdb


def _get_connection():
    return nextdb.Connection(
        nextdb.TableVersionsClientLocal(tests.test_nextdb._TEST_DATA_DIR)
    )


def _write_to_table():
    """A fake job. Writes to Table A"""
    conn = _get_connection()
    conn.write("A", pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}))
    # TODO this should happen automatically in nextdb
    conn.table_versions_client._save_table_versions()

    # also read from Table A--this should not cause A to run in an infinite loop
    conn.read("A").to_pd()


def _read_from_table():
    """Another fake job. Reads from Table A"""
    conn = _get_connection()
    conn.read("A").to_pd()


def test_nextdb_dependency():
    """Tests NextdbDynamicDependency"""
    date = datetime.date(2021, 9, 1)
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(
                    pname("A"),
                    LocalFunction(_write_to_table),
                    [TriggerAction(Actions.run, [NextdbDynamicDependency(ALL_SCOPES)])],
                    # implicitly in BASE_SCOPE
                ),
                Job(
                    pname("A", date=date),
                    LocalFunction(_write_to_table),
                    [TriggerAction(Actions.run, [NextdbDynamicDependency(ALL_SCOPES)])],
                    scope=ScopeValues(date=date),
                ),
                Job(
                    pname("B_ALL"),
                    LocalFunction(_read_from_table),
                    [TriggerAction(Actions.run, [NextdbDynamicDependency(ALL_SCOPES)])],
                ),
                Job(
                    pname("B_BASE"),
                    LocalFunction(_read_from_table),
                    [TriggerAction(Actions.run, [NextdbDynamicDependency(BASE_SCOPE)])],
                ),
                Job(
                    pname("B_DATE"),
                    LocalFunction(_read_from_table),
                    [
                        TriggerAction(
                            Actions.run,
                            [NextdbDynamicDependency(ScopeValues(date=date))],
                        )
                    ],
                ),
                Job(
                    pname("B_WRONG_DATE"),
                    LocalFunction(_read_from_table),
                    [
                        TriggerAction(
                            Actions.run,
                            [
                                NextdbDynamicDependency(
                                    ScopeValues(date=date + datetime.timedelta(days=1))
                                )
                            ],
                        )
                    ],
                ),
            ]
        )
        scheduler.main_loop()

        # we'll check on whether the various B jobs ran by checking the number of events
        def assert_b_events(
            b_all: int, b_base: int, b_date: int, b_wrong_date: int
        ) -> None:
            assert b_all == len(scheduler.events_of(pname("B_ALL")))
            assert b_base == len(scheduler.events_of(pname("B_BASE")))
            assert b_date == len(scheduler.events_of(pname("B_DATE")))
            assert b_wrong_date == len(scheduler.events_of(pname("B_WRONG_DATE")))

        # run A, make sure the effects are as we expect, and make sure B didn't get
        # triggered
        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)

        a_events = scheduler.events_of(pname("A"))
        assert 4 == len(a_events)
        all_effects = a_events[0].payload.effects.nextdb_effects
        assert len(all_effects) == 1
        effects: NextdbEffects = list(all_effects.values())[0]
        assert list(effects.tables_read.keys()) == [("prod", "A")]
        assert list(effects.tables_written.keys()) == [("prod", "A")]

        assert_b_events(1, 1, 1, 1)

        # now run all of the Bs so that we pick up their dependencies
        scheduler.manual_run(pname("B_ALL"))
        scheduler.manual_run(pname("B_BASE"))
        scheduler.manual_run(pname("B_DATE"))
        scheduler.manual_run(pname("B_WRONG_DATE"))
        _wait_for_scheduler(scheduler)
        assert_b_events(4, 4, 4, 4)

        # now run A again, which should cause B_ALL and B_BASE to re-trigger
        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)
        assert_b_events(7, 7, 4, 4)

        # now run A in the date scope which should cause B_ALL and B_DATE to trigger
        scheduler.manual_run(pname("A", date=date))
        _wait_for_scheduler(scheduler)
        assert_b_events(10, 7, 7, 4)


_write_on_third_try_runs = 0


def _write_on_third_try():
    """
    Fake job. Does nothing the first two times it's run, afterwards writes to Table T
    """
    global _write_on_third_try_runs
    if _write_on_third_try_runs >= 2:
        conn = _get_connection()
        conn.write("T", pd.DataFrame({"col1": [1, 2, 3]}))

    _write_on_third_try_runs += 1


def test_nextdb_table_written():
    """Tests UntilNextdbWritten"""
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_run_func), []),
                Job(
                    pname("B"),
                    LocalFunction(_write_on_third_try),
                    [
                        TriggerAction(
                            Actions.run,
                            [AnyJobStateEventFilter([pname("A")], "SUCCEEDED")],
                            UntilNextdbWritten.all((prod_userspace_name, "T")),
                        )
                    ],
                ),
            ]
        )
        scheduler.main_loop()

        # Sequence of events should be:
        # 1. A runs, causes B to run, but B doesn't write any data
        # 2. A runs, causes B to run, but B doesn't write any data
        # 3. A runs, causes B to run, writes data
        # 4. A runs, B does not run because data has been written
        for _ in range(4):
            scheduler.manual_run(pname("A"))
            _wait_for_scheduler(scheduler)

        assert len(scheduler.events_of(pname("B"))) == 10
