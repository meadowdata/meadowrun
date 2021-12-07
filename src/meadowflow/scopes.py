"""
Scopes are a way to run Jobs that create other Jobs based on a set of parameters. E.g.
you might have an add_daily_jobs Job (see add_scope_jobs_decorator for how to define
this) which creates a set of jobs that are parameterized by a date and a userspace. In
that case, this add_daily_jobs Job will be subscribed (using the ScopeInstantiated
EventFilter) to scope instantiation events where the scope contains a date value and a
userspace value. Scopes can be instantiated by jobs that return scopes or manually (see
MeadowFlowClientAsync.instantiate_scopes).
"""

from __future__ import annotations

import dataclasses
from typing import Iterable, Any

from meadowflow.event_log import Event
from meadowflow.topic import EventFilter
from meadowflow.topic_names import FrozenDict, TopicName, pname


class ScopeValues(FrozenDict):
    """Represents an instantiated scope."""

    def topic_name(self) -> TopicName:
        """
        The topic name for a given scope is the keys of the scope. So e.g. if a scope is
        (date=2021-09-21, userspace=prod), the topic name will be ("date", "userspace").
        This way, other jobs can subscribe to this topic, and get called whenever a
        scope is instantiated that provides a certain set of values. Usually, the
        subscriber will then create more jobs in that scope.
        """
        return pname("scope", **{k: None for k in self.keys()})


# TODO not totally sure these are the right concepts
# Any instantiation of ScopeValues without arguments refers to the "base scope", this is
# just a convenient shorthand for it
BASE_SCOPE = ScopeValues()
# ALL_SCOPES is a special scope that is not a real scope that jobs can exist in or be
# instantiated. It is just used for MeadowdbDynamicDependency to indicate that it should
# not be restricted to a scope. Implementation is a bit hacky!
ALL_SCOPES = ScopeValues(__meadowflow_internal__="ALL_SCOPES")


@dataclasses.dataclass
class ScopeInstantiated(EventFilter):
    """
    An event filter that looks for when scopes get created with a particular set of
    keys. Event payload will be a ScopeValues.
    """

    scope_vars: frozenset

    @classmethod
    def construct(cls, *args: Any) -> ScopeInstantiated:
        return ScopeInstantiated(frozenset(args))

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        # TODO comment?
        yield pname("scope", **{v: None for v in self.scope_vars})

    def apply(self, event: Event) -> bool:
        return True
