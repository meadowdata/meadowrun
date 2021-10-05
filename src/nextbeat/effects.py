from __future__ import annotations

import dataclasses
from typing import Iterable, Dict

# TODO consider making this work without the nextdb dependency?
import nextdb.connection
from nextbeat.event_log import Event
import nextbeat.scopes
import nextbeat.topic
from nextbeat.topic_names import TopicName


@dataclasses.dataclass(frozen=True)
class Effects:
    """
    Represents effects produced by the running of a job. See also
    Scheduler._process_effects
    """

    # maps Connection.key() to NextdbEffects
    nextdb_effects: Dict[
        nextdb.connection.ConnectionKey, nextdb.connection.NextdbEffects
    ] = dataclasses.field(default_factory=lambda: {})


# The effects produced by the job (if one exists) running in the current process
_effects = Effects()


def reset_effects():
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
    for conn in nextdb.connection.all_connections:
        conn.reset_effects()


def get_effects():
    """Updates _effects and returns it"""

    # get nextdb effects
    for connection in nextdb.connection.all_connections:
        if connection.effects.tables_read or connection.effects.tables_written:
            if connection.key() not in _effects.nextdb_effects:
                _effects.nextdb_effects[connection.key()] = connection.effects
            else:
                merge_into = _effects.nextdb_effects[connection.key()]
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
class NextdbDynamicDependency(nextbeat.topic.EventFilter):
    """
    If a job is set up with a NextdbDynamicDependency, the Scheduler will keep track
    of all the nextdb tables that that job reads when it runs. Then, this EventFilter
    will be triggered whenever one of those tables is written to.
    """

    # Optionally, we can restrict this nextdb dynamic dependency to only care about
    # writes from jobs that are in the specified scope. The default is ALL_SCOPES, which
    # is a special meta-scope which says that this dependency should not be restricted
    # to a single scope. Note that this can be distinct from the parent job's scope
    # (this is necessarily so in the case of ALL_SCOPES as jobs cannot have ALL_SCOPES
    # as their scope; BASE_SCOPE is different and does not have special semantics for
    # dependencies.)
    dependency_scope: nextbeat.scopes.ScopeValues = nextbeat.scopes.ALL_SCOPES

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        raise ValueError(
            "NextdbDynamicDependencies get handled by the Scheduler, they should never "
            "be called directly"
        )

    def apply(self, event: Event) -> bool:
        raise ValueError(
            "NextdbDynamicDependencies get handled by the Scheduler, they should never "
            "be called directly"
        )
