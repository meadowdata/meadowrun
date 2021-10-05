import pandas as pd

import nextdb
from nextbeat.jobs import Job, LocalFunction, Actions
from nextbeat.effects import NextdbDynamicDependency
from nextbeat.scheduler import Scheduler
from nextbeat.topic import TriggerAction
from nextbeat.topic_names import pname
from nextdb.connection import NextdbEffects
from test_nextbeat.test_scheduler import _wait_for_scheduler
import tests.test_nextdb


def _write_to_table():
    """A fake job. Writes to Table A"""
    conn = nextdb.Connection(
        nextdb.TableVersionsClientLocal(tests.test_nextdb._TEST_DATA_DIR)
    )
    conn.write("A", pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}))
    # TODO this should happen automatically in nextdb
    conn.table_versions_client._save_table_versions()


def _read_from_table():
    """Another fake job. Reads from Table A"""
    conn = nextdb.Connection(
        nextdb.TableVersionsClientLocal(tests.test_nextdb._TEST_DATA_DIR)
    )
    conn.read("A").to_pd()


def test_nextdb_dependency():
    """Tests NextdbDynamicDependency"""
    with Scheduler(job_runner_poll_delay_seconds=0.05) as scheduler:
        scheduler.add_jobs(
            [
                Job(pname("A"), LocalFunction(_write_to_table), []),
                Job(
                    pname("B"),
                    LocalFunction(_read_from_table),
                    [TriggerAction(Actions.run, [NextdbDynamicDependency()])],
                ),
            ]
        )
        scheduler.main_loop()

        # run A, make sure the effects are as we expect, and make sure B didn't get
        # triggered
        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)

        a_events = scheduler.events_of(pname("A"))
        assert 4 == len(a_events)
        all_effects = a_events[0].payload.effects.nextdb_effects
        assert len(all_effects) == 1
        effects: NextdbEffects = list(all_effects.values())[0]
        assert len(effects.tables_read) == 0
        assert list(effects.tables_written.keys()) == [("prod", "A")]

        assert 1 == len(scheduler.events_of(pname("B")))

        # now run B
        scheduler.manual_run(pname("B"))
        _wait_for_scheduler(scheduler)
        assert 4 == len(scheduler.events_of(pname("B")))

        # now run A again, which should cause B to re-trigger
        scheduler.manual_run(pname("A"))
        _wait_for_scheduler(scheduler)
        assert 7 == len(scheduler.events_of(pname("B")))
