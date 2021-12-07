from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import Optional, Tuple, Any, Dict, Union, overload

from meadowflow.event_log import EventLog, Event, Timestamp
import meadowflow.jobs
from meadowflow.topic_names import TopicName, FrozenDict
from meadowgrid.deployed_function import (
    MeadowGridCommand,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
)


@dataclasses.dataclass(frozen=True)
class LatestEventsArg:
    """
    This is a placeholder that can be added to the arguments to a function for a job.
    When the job gets run, the scheduler will replace this placeholder with a
    FrozenDict[TopicName, Optional[Event]] containing the latest event for each
    topic_name in topic_names. If topic_names is empty, then the scheduler will
    automatically get events for all topics that the job depends on via trigger_actions.
    """

    topic_names: Sequence[TopicName]

    @classmethod
    def construct(cls, *topic_names: TopicName) -> LatestEventsArg:
        return LatestEventsArg(topic_names)


def replace_latest_events(
    job_runner_function: meadowflow.jobs.JobRunnerFunction,
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> meadowflow.jobs.JobRunnerFunction:
    """
    Replaces any instances of LatestEventsArg() in the job_runner_function. See
    LatestEventsArg for description of semantics.
    """
    if isinstance(job_runner_function, meadowflow.jobs.LocalFunction):
        return _replace_latest_events_function(
            job_runner_function, job, event_log, latest_timestamp
        )[1]
    elif isinstance(job_runner_function, MeadowGridDeployedRunnable):
        if isinstance(job_runner_function.runnable, MeadowGridFunction):
            need_replacement, new_function = _replace_latest_events_function(
                job_runner_function.runnable, job, event_log, latest_timestamp
            )
            if need_replacement:
                return dataclasses.replace(job_runner_function, runnable=new_function)
            else:
                return job_runner_function
        elif isinstance(job_runner_function.runnable, MeadowGridCommand):
            if job_runner_function.runnable.context_variables:
                need_replacement, new_vars = _replace_latest_events_dict(
                    job_runner_function.runnable.context_variables,
                    job,
                    event_log,
                    latest_timestamp,
                )
                if need_replacement:
                    dataclasses.replace(
                        job_runner_function,
                        runnable=dataclasses.replace(
                            job_runner_function.runnable, context_variables=new_vars
                        ),
                    )
            return job_runner_function
        else:
            raise ValueError(
                f"Unknown runnable type {type(job_runner_function.runnable)}"
            )
    else:
        raise ValueError(f"Unknown type {type(job_runner_function)}")


@overload
def _replace_latest_events_function(
    function: meadowflow.jobs.LocalFunction,
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> Tuple[bool, meadowflow.jobs.LocalFunction]:
    ...


@overload
def _replace_latest_events_function(
    function: MeadowGridFunction,
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> Tuple[bool, MeadowGridFunction]:
    ...


def _replace_latest_events_function(
    function: Union[meadowflow.jobs.LocalFunction, MeadowGridFunction],
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> Tuple[bool, Union[meadowflow.jobs.LocalFunction, MeadowGridFunction]]:
    """
    Helper for replace_latest_events. This function should work on any dataclass that
    has a function_args and function_kwargs property
    """
    to_replace: Dict[str, Sequence | Dict[str, Any]] = {}

    if function.function_args:
        need_replacement, new_args = _replace_latest_events_list(
            function.function_args, job, event_log, latest_timestamp
        )
        if need_replacement:
            to_replace["function_args"] = new_args

    if function.function_kwargs:
        need_replacement, new_kwargs = _replace_latest_events_dict(
            function.function_kwargs, job, event_log, latest_timestamp
        )
        if need_replacement:
            to_replace["function_kwargs"] = new_kwargs

    if len(to_replace) > 0:
        return True, dataclasses.replace(function, **to_replace)
    else:
        return False, function


def _replace_latest_events_list(
    xs: Sequence[Any],
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> Tuple[bool, Sequence[Any]]:
    """Helper for replace_latest_events"""
    new_xs = []
    any_need_replacement = False
    for x in xs:
        if isinstance(x, LatestEventsArg):
            new_xs.append(
                _replace_latest_events_arg(x, job, event_log, latest_timestamp)
            )
            any_need_replacement = True
        else:
            new_xs.append(x)
    if any_need_replacement:
        # we always return a list regardless of what kind of sequence xs originally was,
        # which should be okay?
        return True, new_xs
    else:
        return False, xs


def _replace_latest_events_dict(
    kwargs: Dict[str, Any],
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> Tuple[bool, Dict[str, Any]]:
    """Helper for replace_latest_events"""
    new_kwargs = {}
    any_need_replacement = False
    for key, value in kwargs.items():
        if isinstance(value, LatestEventsArg):
            new_kwargs[key] = _replace_latest_events_arg(
                value, job, event_log, latest_timestamp
            )
            any_need_replacement = True
        else:
            new_kwargs[key] = value
    if any_need_replacement:
        return True, new_kwargs
    else:
        return False, kwargs


def _replace_latest_events_arg(
    arg: LatestEventsArg,
    job: meadowflow.jobs.Job,
    event_log: EventLog,
    latest_timestamp: Timestamp,
) -> FrozenDict[TopicName, Optional[Event]]:
    """Helper for replace_latest_events"""
    topic_names = arg.topic_names
    if len(topic_names) == 0:
        topic_names = job.all_subscribed_topics or tuple()

    return FrozenDict(
        (topic_name, event_log.last_event(topic_name, latest_timestamp))
        for topic_name in topic_names
    )
