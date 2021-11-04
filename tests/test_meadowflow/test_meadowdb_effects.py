import datetime
import sys

import pandas as pd

import meadowdb
from meadowflow.jobs import (
    Job,
    LocalFunction,
    Actions,
    AnyJobStateEventFilter,
    JobRunOverrides,
)
from meadowflow.effects import MeadowdbDynamicDependency, UntilMeadowdbWritten
from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.scopes import BASE_SCOPE, ALL_SCOPES, ScopeValues
from meadowflow.topic import TriggerAction
from meadowflow.topic_names import pname
from meadowdb.connection import MeadowdbEffects, prod_userspace_name
import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid.deployed_function import (
    MeadowGridDeployedCommand,
    MeadowGridDeployedFunction,
    MeadowGridFunction,
)
from meadowgrid.meadowgrid_pb2 import ServerAvailableFolder
from test_meadowflow.test_scheduler import _wait_for_scheduler, _run_func
import tests.test_meadowdb
from test_meadowgrid import MEADOWDATA_CODE, EXAMPLE_CODE


def _get_connection():
    return meadowdb.Connection(
        meadowdb.TableVersionsClientLocal(tests.test_meadowdb._TEST_DATA_DIR)
    )


def _write_to_table():
    """A fake job. Writes to Table A"""
    conn = _get_connection()
    conn.write("A", pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}))
    # TODO this should happen automatically in meadowdb
    conn.table_versions_client._save_table_versions()

    # also read from Table A--this should not cause A to run in an infinite loop
    conn.read("A").to_pd()


def _read_from_table():
    """Another fake job. Reads from Table A"""
    conn = _get_connection()
    conn.read("A").to_pd()


def test_meadowdb_effects():
    """
    Tests that meadowdb effects come through meadowgrid commands and functions
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(),
    ):
        with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
            scheduler.register_job_runner(MeadowGridJobRunner)

            scheduler.add_jobs(
                [
                    Job(
                        pname("Command"),
                        MeadowGridDeployedCommand(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                                interpreter_path=sys.executable,
                            ),
                            command_line=["python", "write_to_table.py"],
                        ),
                        (),
                    ),
                    Job(
                        pname("Func"),
                        MeadowGridDeployedFunction(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                                interpreter_path=sys.executable,
                            ),
                            MeadowGridFunction.from_name("write_to_table", "main"),
                        ),
                        (),
                    ),
                ]
            )

            scheduler.main_loop()

            scheduler.manual_run(pname("Command"))
            scheduler.manual_run(pname("Func"))
            _wait_for_scheduler(scheduler)

            command_events = scheduler.events_of(pname("Command"))
            func_events = scheduler.events_of(pname("Func"))
            for events in (command_events, func_events):
                assert 4 == len(events)
                assert "SUCCEEDED" == events[0].payload.state
                all_effects = events[0].payload.effects.meadowdb_effects
                assert len(all_effects) == 1
                effects: MeadowdbEffects = list(all_effects.values())[0]
                assert list(effects.tables_read.keys()) == [("prod", "A")]
                assert list(effects.tables_written.keys()) == [("prod", "A")]

            # do everything again with meadowdb_userspace set
            scheduler.manual_run(
                pname("Command"), JobRunOverrides(meadowdb_userspace="U1")
            )
            scheduler.manual_run(
                pname("Func"), JobRunOverrides(meadowdb_userspace="U1")
            )
            _wait_for_scheduler(scheduler)

            command_events = scheduler.events_of(pname("Command"))
            func_events = scheduler.events_of(pname("Func"))
            for events in (command_events, func_events):
                assert 7 == len(events)
                assert "SUCCEEDED" == events[0].payload.state
                all_effects = events[0].payload.effects.meadowdb_effects
                assert len(all_effects) == 1
                effects: MeadowdbEffects = list(all_effects.values())[0]
                assert list(effects.tables_read.keys()) == [("U1", "A")]
                assert list(effects.tables_written.keys()) == [("U1", "A")]


def test_meadowdb_dependency():
    """Tests MeadowdbDynamicDependency"""
    date = datetime.date(2021, 9, 1)
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(
                    pname("A"),
                    LocalFunction(_write_to_table),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                    # implicitly in BASE_SCOPE
                ),
                Job(
                    pname("A", date=date),
                    LocalFunction(_write_to_table),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                    scope=ScopeValues(date=date),
                ),
                Job(
                    pname("B_ALL"),
                    LocalFunction(_read_from_table),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                ),
                Job(
                    pname("B_BASE"),
                    LocalFunction(_read_from_table),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(BASE_SCOPE)]
                        )
                    ],
                ),
                Job(
                    pname("B_DATE"),
                    LocalFunction(_read_from_table),
                    [
                        TriggerAction(
                            Actions.run,
                            [MeadowdbDynamicDependency(ScopeValues(date=date))],
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
                                MeadowdbDynamicDependency(
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
        all_effects = a_events[0].payload.effects.meadowdb_effects
        assert len(all_effects) == 1
        effects: MeadowdbEffects = list(all_effects.values())[0]
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


def test_meadowdb_table_written():
    """Tests UntilMeadowdbWritten"""
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
                            UntilMeadowdbWritten.all((prod_userspace_name, "T")),
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