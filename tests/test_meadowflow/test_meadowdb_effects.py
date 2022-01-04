import datetime

import meadowdb
import meadowgrid.coordinator_main
import meadowgrid.agent_main
import pandas as pd
import pytest
from meadowdb import MAIN_USERSPACE_NAME
from meadowdb.connection import MeadowdbEffects
from meadowflow.effects import MeadowdbDynamicDependency, UntilMeadowdbWritten
from meadowflow.jobs import (
    Actions,
    AnyJobStateEventFilter,
    Job,
    JobRunOverrides,
    LocalFunction,
)
from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.scopes import ALL_SCOPES, BASE_SCOPE, ScopeValues
from meadowflow.topic import TriggerAction
from meadowflow.topic_names import pname
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientAsync
from meadowgrid.deployed_function import (
    MeadowGridCommand,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
)
from meadowgrid.meadowgrid_pb2 import ServerAvailableFolder, ServerAvailableInterpreter
from test_meadowgrid.test_meadowgrid_basics import (
    EXAMPLE_CODE,
    MEADOWDATA_CODE,
    TEST_WORKING_FOLDER,
    wait_for_agents_async,
)

from test_meadowflow.test_scheduler import _run_func, _wait_for_scheduler


def _write_to_table(mdb_data_dir):
    """Create a fake job that writes to Table A"""
    conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(mdb_data_dir))
    conn.write("A", pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}))
    # TODO this should happen automatically in meadowdb
    conn.table_versions_client._save_table_versions()

    # also read from Table A--this should not cause A to run in an infinite loop
    conn.read("A").to_pd()


def _read_from_table(mdb_data_dir):
    """Create a fake job that reads from Table A"""
    conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(mdb_data_dir))
    conn.read("A").to_pd()

    return _read_from_table


@pytest.mark.asyncio
async def test_meadowdb_effects():
    """
    Tests that meadowdb effects come through meadowgrid commands and functions
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        async with Scheduler(
            job_runner_poll_delay_seconds=0.05
        ) as scheduler, MeadowGridCoordinatorClientAsync() as coordinator_client:
            scheduler.register_job_runner(MeadowGridJobRunner)
            await wait_for_agents_async(coordinator_client, 1)

            scheduler.add_jobs(
                [
                    Job(
                        pname("Command"),
                        MeadowGridDeployedRunnable(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                            ),
                            ServerAvailableInterpreter(
                                interpreter_path=MEADOWGRID_INTERPRETER,
                            ),
                            MeadowGridCommand(
                                command_line=["python", "write_to_table.py"]
                            ),
                        ),
                        (),
                    ),
                    Job(
                        pname("Func"),
                        MeadowGridDeployedRunnable(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                            ),
                            ServerAvailableInterpreter(
                                interpreter_path=MEADOWGRID_INTERPRETER,
                            ),
                            MeadowGridFunction.from_name("write_to_table", "main"),
                        ),
                        (),
                    ),
                ]
            )

            scheduler.manual_run(pname("Command"))
            scheduler.manual_run(pname("Func"))
            await _wait_for_scheduler(scheduler)

            command_events = scheduler.events_of(pname("Command"))
            func_events = scheduler.events_of(pname("Func"))
            for events in (command_events, func_events):
                assert 4 == len(events)
                assert "SUCCEEDED" == events[0].payload.state
                all_effects = events[0].payload.effects.meadowdb_effects
                assert len(all_effects) == 1
                effects: MeadowdbEffects = list(all_effects.values())[0]
                assert list(effects.tables_read.keys()) == [(MAIN_USERSPACE_NAME, "A")]
                assert list(effects.tables_written.keys()) == [
                    (MAIN_USERSPACE_NAME, "A")
                ]

            # do everything again with meadowdb_userspace set
            scheduler.manual_run(
                pname("Command"), JobRunOverrides(meadowdb_userspace="U1")
            )
            scheduler.manual_run(
                pname("Func"), JobRunOverrides(meadowdb_userspace="U1")
            )
            await _wait_for_scheduler(scheduler)

            command_events = scheduler.events_of(pname("Command"))
            func_events = scheduler.events_of(pname("Func"))
            for events in (command_events, func_events):
                assert 7 == len(events)
                assert "SUCCEEDED" == events[0].payload.state
                all_effects = events[0].payload.effects.meadowdb_effects
                assert len(all_effects) == 1
                effects = list(all_effects.values())[0]
                assert list(effects.tables_read.keys()) == [("U1", "A")]
                assert list(effects.tables_written.keys()) == [("U1", "A")]


@pytest.mark.asyncio
async def test_meadowdb_dependency(mdb_data_dir):
    """Tests MeadowdbDynamicDependency"""
    date = datetime.date(2021, 9, 1)
    async with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(
                    pname("A"),
                    LocalFunction(_write_to_table, [mdb_data_dir]),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                    # implicitly in BASE_SCOPE
                ),
                Job(
                    pname("A", date=date),
                    LocalFunction(_write_to_table, [mdb_data_dir]),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                    scope=ScopeValues(date=date),
                ),
                Job(
                    pname("B_ALL"),
                    LocalFunction(_read_from_table, [mdb_data_dir]),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(ALL_SCOPES)]
                        )
                    ],
                ),
                Job(
                    pname("B_BASE"),
                    LocalFunction(_read_from_table, [mdb_data_dir]),
                    [
                        TriggerAction(
                            Actions.run, [MeadowdbDynamicDependency(BASE_SCOPE)]
                        )
                    ],
                ),
                Job(
                    pname("B_DATE"),
                    LocalFunction(_read_from_table, [mdb_data_dir]),
                    [
                        TriggerAction(
                            Actions.run,
                            [MeadowdbDynamicDependency(ScopeValues(date=date))],
                        )
                    ],
                ),
                Job(
                    pname("B_WRONG_DATE"),
                    LocalFunction(_read_from_table, [mdb_data_dir]),
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
        await _wait_for_scheduler(scheduler)

        a_events = scheduler.events_of(pname("A"))
        assert 4 == len(a_events)
        all_effects = a_events[0].payload.effects.meadowdb_effects
        assert len(all_effects) == 1
        effects: MeadowdbEffects = list(all_effects.values())[0]
        assert list(effects.tables_read.keys()) == [(MAIN_USERSPACE_NAME, "A")]
        assert list(effects.tables_written.keys()) == [(MAIN_USERSPACE_NAME, "A")]

        assert_b_events(1, 1, 1, 1)

        # now run all of the Bs so that we pick up their dependencies
        scheduler.manual_run(pname("B_ALL"))
        scheduler.manual_run(pname("B_BASE"))
        scheduler.manual_run(pname("B_DATE"))
        scheduler.manual_run(pname("B_WRONG_DATE"))
        await _wait_for_scheduler(scheduler)
        assert_b_events(4, 4, 4, 4)

        # now run A again, which should cause B_ALL and B_BASE to re-trigger
        scheduler.manual_run(pname("A"))
        await _wait_for_scheduler(scheduler)
        assert_b_events(7, 7, 4, 4)

        # now run A in the date scope which should cause B_ALL and B_DATE to trigger
        scheduler.manual_run(pname("A", date=date))
        await _wait_for_scheduler(scheduler)
        assert_b_events(10, 7, 7, 4)


_write_on_third_try_runs = 0


def _write_on_third_try(mdb_data_dir):
    """
    Fake job. Does nothing the first two times it's run, afterwards writes to Table T
    """

    global _write_on_third_try_runs
    if _write_on_third_try_runs >= 2:
        conn = meadowdb.Connection(meadowdb.TableVersionsClientLocal(mdb_data_dir))
        conn.write("T", pd.DataFrame({"col1": [1, 2, 3]}))

    _write_on_third_try_runs += 1


@pytest.mark.asyncio
async def test_meadowdb_table_written(mdb_data_dir):
    """Tests UntilMeadowdbWritten"""
    async with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_run_func), []),
                Job(
                    pname("B"),
                    LocalFunction(_write_on_third_try, (mdb_data_dir,)),
                    [
                        TriggerAction(
                            Actions.run,
                            [AnyJobStateEventFilter([pname("A")], ["SUCCEEDED"])],
                            UntilMeadowdbWritten.all((MAIN_USERSPACE_NAME, "T")),
                        )
                    ],
                ),
            ]
        )

        # Sequence of events should be:
        # 1. A runs, causes B to run, but B doesn't write any data
        # 2. A runs, causes B to run, but B doesn't write any data
        # 3. A runs, causes B to run, writes data
        # 4. A runs, B does not run because data has been written
        for _ in range(4):
            scheduler.manual_run(pname("A"))
            await _wait_for_scheduler(scheduler)

        assert len(scheduler.events_of(pname("B"))) == 10
