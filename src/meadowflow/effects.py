from __future__ import annotations

import atexit
import dataclasses
import pickle
from typing import Iterable, Dict, Optional, Sequence, Tuple, Set, Literal

# TODO consider making this work without the meadowdb dependency?
import meadowdb.connection
from meadowflow.event_log import Event, EventLog, Timestamp
import meadowflow.scopes
import meadowflow.topic
import meadowflow.context
from meadowflow.topic_names import TopicName, CURRENT_JOB


@dataclasses.dataclass(frozen=True)
class Effects:
    """
    Represents effects produced by the running of a job. See also
    Scheduler._process_effects
    """

    # maps Connection.key() to MeadowdbEffects
    meadowdb_effects: Dict[
        meadowdb.connection.ConnectionKey, meadowdb.connection.MeadowdbEffects
    ] = dataclasses.field(default_factory=lambda: {})


# The effects produced by the job (if one exists) running in the current process
_effects = Effects()


def reset_effects() -> None:
    """
    This is needed when you want to run than one job in the same process, which can be
    advantageous because of sharing "good" global state (e.g. caches that properly
    invalidate) but can be dangerous because of sharing "bad" global state such as these
    effects
    """

    global _effects
    _effects = Effects()

    # this is a bit hacky, but any global state we depend on in get_effects needs to be
    # reset also
    for conn in meadowdb.connection.all_connections:
        conn.reset_effects()


def get_effects() -> Effects:
    """Updates _effects and returns it"""

    # get meadowdb effects
    for connection in meadowdb.connection.all_connections:
        if connection.effects.tables_read or connection.effects.tables_written:
            if connection.key() not in _effects.meadowdb_effects:
                _effects.meadowdb_effects[connection.key()] = connection.effects
            else:
                merge_into = _effects.meadowdb_effects[connection.key()]
                for table_key, versions_read in connection.effects.tables_read.items():
                    if table_key not in merge_into.tables_read:
                        merge_into.tables_read[table_key] = versions_read
                    else:
                        merge_into.tables_read[table_key].update(versions_read)
                for (
                    table_key,
                    versions_written,
                ) in connection.effects.tables_written.items():
                    if table_key not in merge_into.tables_written:
                        merge_into.tables_written[table_key] = versions_written
                    else:
                        merge_into.tables_written[table_key].update(versions_written)

    return _effects


@dataclasses.dataclass(frozen=True)
class MeadowdbDynamicDependency(meadowflow.topic.EventFilter):
    """
    If a job is set up with a MeadowdbDynamicDependency, the Scheduler will keep track
    of all the meadowdb tables that that job reads when it runs. Then, this EventFilter
    will be triggered whenever one of those tables is written to.
    """

    # Optionally, we can restrict this meadowdb dynamic dependency to only care about
    # writes from jobs that are in the specified scope. The default is ALL_SCOPES, which
    # is a special meta-scope which says that this dependency should not be restricted
    # to a single scope. Note that this can be distinct from the parent job's scope
    # (this is necessarily so in the case of ALL_SCOPES as jobs cannot have ALL_SCOPES
    # as their scope; BASE_SCOPE is different and does not have special semantics for
    # dependencies.)
    dependency_scope: meadowflow.scopes.ScopeValues = meadowflow.scopes.ALL_SCOPES

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        raise ValueError(
            "MeadowdbDynamicDependencies get handled by the Scheduler, they should "
            "never be called directly"
        )

    def apply(self, event: Event) -> bool:
        raise ValueError(
            "MeadowdbDynamicDependencies get handled by the Scheduler, they should "
            "never be called directly"
        )


@dataclasses.dataclass(frozen=True)
class UntilMeadowdbWritten(meadowflow.topic.StatePredicate):
    """
    Complicated to explain but hopefully intuitive in an example:
    UntilMeadowdbWritten.all(T1, T2, ..., job_name=J) returns True if J has NOT yet
    written to all of T1, T2, ... where T1, T2 are meadowdb tables and J is a job
    (defaults to CURRENT_JOB). UntilMeadowdbWritten.any is analogous.
    UntilMeadowdbWritten.any() is special in that if you provide no table names, it will
    return True while J has written nothing, and then will return False if J has written
    anything.

    TODO this isn't great--probably needs to be way more expressive, ideally this would
    be more like a lambda or at least a full language (eval?), but it needs to be part
    of the job definition and therefore serializable (I think). You might care about
    the connection key, limit how far you go back when looking, more complicated logic
    on which tables were written
    """

    job_name: TopicName
    table_names: Sequence[Tuple[str, str]]
    operation: Literal["all", "any"]

    def topic_names_to_query(self) -> Iterable[TopicName]:
        yield self.job_name

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        if self.job_name == CURRENT_JOB:
            job_name = current_job_name
        else:
            job_name = self.job_name

        # probably need an option to not go all the way back in time...
        all_effects: Iterable[Effects] = (
            event.payload.effects
            for event in event_log.events(job_name, 0, high_timestamp)
            if event.payload.effects is not None
        )
        tables_written: Set[Tuple[str, str]] = set(
            table_written
            for effects in all_effects
            for conn, meadowdb_effects in effects.meadowdb_effects.items()
            for table_written in meadowdb_effects.tables_written.keys()
        )

        if self.operation == "all":
            return not all(table in tables_written for table in self.table_names)
        elif self.operation == "any":
            if not self.table_names:
                return not bool(tables_written)
            else:
                return not any(table in tables_written for table in self.table_names)
        else:
            raise ValueError(f"Unexpected operation {self.operation}")

    @classmethod
    def any(
        cls, *table_names: Tuple[str, str], job_name: TopicName = CURRENT_JOB
    ) -> UntilMeadowdbWritten:
        return cls(job_name, table_names, "any")

    @classmethod
    def all(
        cls, *table_names: Tuple[str, str], job_name: TopicName = CURRENT_JOB
    ) -> UntilMeadowdbWritten:
        return cls(job_name, table_names, "all")


def _save_effects(result_file: str, result_pickle_protocol: Optional[int]) -> None:
    """
    When meadowgrid runs a command, it's not able to wrap the process to guarantee that
    the effects get written out. If that's the case, we will do our best to make sure
    that we write effects to the file that meadowgrid requests via environment variables
    """
    with open(result_file, "wb") as f:
        pickle.dump((None, get_effects()), f, protocol=result_pickle_protocol)


# See _save_effects docstring
_save_effects_registered = False
if not _save_effects_registered:
    _save_effects_registered = True
    result_request = meadowflow.context.result_request()
    if result_request is not None:
        atexit.register(_save_effects, *result_request)
