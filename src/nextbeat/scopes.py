"""
Scopes are a way to run Jobs that create other Jobs based on a set of parameters. E.g.
you might have an add_daily_jobs Job (see add_scope_jobs_decorator for how to define
this) which creates a set of jobs that are parameterized by a date and a userspace. In
that case, this add_daily_jobs Job will be subscribed (using the ScopeInstantiated
EventFilter) to scope instantiation events where the scope contains a date value and a
userspace value. Scopes can be instantiated by jobs that return scopes or manually (see
NextBeatClientAsync.instantiate_scopes).
"""

from __future__ import annotations

import dataclasses
import functools
from typing import Iterable, Callable, Sequence, Optional, Any

from nextbeat.event_log import Event
import nextbeat.jobs
from nextbeat.topic import EventFilter
from nextbeat.topic_names import FrozenDict, TopicName, pname


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


@dataclasses.dataclass
class ScopeInstantiated(EventFilter):
    """
    An event filter that looks for when scopes get created with a particular set of
    keys. Event payload will be a ScopeValues.
    """

    scope_vars: frozenset

    @classmethod
    def construct(cls, *args: Any):
        return ScopeInstantiated(frozenset(args))

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        # TODO comment?
        yield pname("scope", **{v: None for v in self.scope_vars})

    def apply(self, event: Event) -> bool:
        return True


def add_scope_jobs_decorator(
    func: Callable[[ScopeValues, ...], Sequence[nextbeat.jobs.Job]]
) -> Callable[
    [FrozenDict[TopicName, Optional[Event]], ...], Sequence[nextbeat.jobs.Job]
]:
    """
    A little bit of boilerplate to make it easier to write functions that create jobs in
    a specific scope, which is a common use case.

    Example:
        @add_scope_jobs_decorator
        def add_scope_jobs(scope: ScopeValues, arg1: Any, ...) -> Sequence[Job]:
            return [
                # use e.g. scope["date"] or scope["userspace"] in the job definition
                Job(pname("my_job1"), ...),
                Job(pname("my_job2"), ...)
            ]

    This function can then be scheduled like:
        Job(
            pname("add_date_scope_jobs"),
            Function(_run_add_scope_jobs, [LatestEventsArg.construct()]),
            [
                TriggerAction(
                    Actions.run, [ScopeInstantiated(frozenset("date", "userspace"))]
                )
            ]
        )

    This function takes care of two bits of boilerplate:
    1. At the start of the function, converts a FrozenDict[TopicName, Optional[Event]],
       (which is what LatestEventsArg gives us) into ScopeValues, which is what we can
       actually use
    2. At the end of the function, append all of the scope key/value pairs to the names
       of all of the jobs created in func. If you don't do this, then every instance of
       the scope will create identical jobs which is not what you want. Also, this means
       that you cannot have job names that include keys that are the same as the scope.
    """

    # this functools.wraps is more important than it seems--functions that are not
    # decorated using this pattern cannot be pickled
    @functools.wraps(func)
    def wrapper(
        events: FrozenDict[TopicName, Optional[Event]], *args, **kwargs
    ) -> Sequence[nextbeat.jobs.Job]:
        # find the instantiate scope event:
        scopes = [
            e.payload for e in events.values() if isinstance(e.payload, ScopeValues)
        ]
        if len(scopes) != 1:
            raise ValueError(
                "the adds_scope_jobs decorator must be used on a function that depends "
                "exactly one ScopeInstantiated topic. This function was called with "
                f"{len(scopes)} scopes"
            )
        scope = scopes[0]

        # now call the wrapped function
        jobs_to_add = func(scope, *args, **kwargs)

        # now adjust the names of the returned jobs
        for job in jobs_to_add:
            name = job.name.as_mutable()
            for key, value in scope.items():
                if key in name:
                    raise ValueError(
                        f"Cannot create job {job.name} in scope because both job name "
                        f"and scope have a {key} key"
                    )
                name[key] = value
            job.name = TopicName(name)

        return jobs_to_add

    return wrapper
