from __future__ import annotations

import asyncio
from asyncio.tasks import Task
import dataclasses
import itertools
import traceback
from types import TracebackType
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Tuple,
    Iterable,
    Optional,
    Callable,
    Awaitable,
    Type,
    Union,
    Literal,
    Set,
    overload,
)

from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.scopes import ScopeValues, ALL_SCOPES
from meadowflow.topic_names import TopicName
from meadowflow.jobs import Actions, Job, JobPayload, JobRunner, JobRunOverrides
from meadowflow.local_job_runner import LocalJobRunner
from meadowflow.time_event_publisher import (
    TimeEventPublisher,
    create_time_event_state_predicates,
    create_time_event_filters,
)
from meadowflow.topic import Action, Topic, StatePredicate, EventFilter
from meadowflow.effects import MeadowdbDynamicDependency
from meadowdb.connection import ConnectionKey


@overload
def _get_jobs_or_scopes_from_result(result: Job) -> Tuple[Literal["jobs"], List[Job]]:
    ...


@overload
def _get_jobs_or_scopes_from_result(
    result: ScopeValues,
) -> Tuple[Literal["scopes"], List[ScopeValues]]:
    ...


@overload
def _get_jobs_or_scopes_from_result(
    result: Union[
        List[Job],
        Tuple[Job, ...],
    ],
) -> Tuple[Literal["jobs"], Union[List[Job], Tuple[Job, ...], None]]:
    ...


@overload
def _get_jobs_or_scopes_from_result(
    result: Union[
        List[ScopeValues],
        Tuple[ScopeValues, ...],
    ],
) -> Tuple[Literal["scopes"], Union[List[ScopeValues], Tuple[ScopeValues, ...], None]]:
    ...


def _get_jobs_or_scopes_from_result(
    result: Union[
        Job,
        ScopeValues,
        List[Job],
        List[ScopeValues],
        Tuple[Job, ...],
        Tuple[ScopeValues, ...],
    ],
) -> Tuple[
    Literal["jobs", "scopes", "none"],
    Union[List[Job], Tuple[Job, ...], List[ScopeValues], Tuple[ScopeValues, ...], None],
]:
    """
    If a job returns Job definitions or ScopeValues, we need to add those jobs/scopes to
    the scheduler. This function inspects a job's result to figure out if there are
    jobs/scopes that we need to add. Return types are hopefully clear from the type
    signature.
    """
    if isinstance(result, Job):
        return "jobs", [result]
    elif isinstance(result, ScopeValues):
        return "scopes", [result]
    elif isinstance(result, (list, tuple)) and len(result) > 0:
        # We don't want to just iterate through every Iterable we get back, iterating
        # through those Iterables could have side effects. Seems fine to only accept
        # lists and tuples.
        element_type = None
        for r in result:
            if isinstance(r, Job):
                if element_type is None:
                    element_type = "job"
                elif element_type == "job":
                    pass
                else:
                    # this means we have incompatible types
                    return "none", None
            elif isinstance(r, ScopeValues):
                if element_type is None:
                    element_type = "scope"
                elif element_type == "scope":
                    pass
                else:
                    # this means we have incompatible types
                    return "none", None
            else:
                # this means we have a non-job or non-scope in our iterable
                return "none", None
        if element_type == "job":
            return "jobs", result
        elif element_type == "scope":
            return "scopes", result
        else:
            # this should never happen, but probably better to be defensive here
            return "none", None
    else:
        return "none", None


@dataclasses.dataclass
class MeadowdbDependencyAction:
    """
    Scheduler creates one of these for each MeadowdbDynamicDependency that we see, as
    they require keeping some state in order to trigger them correctly
    """

    # These should be frozen, they tell us how to trigger the action. E.g. if we have a
    # Job that has a trigger_action that has a MeadowdbDependencyAction as one of its
    # wake_ons, we'll save the Job, TriggerAction.state_predicate, and
    # TriggerAction.action here so we know how/whether to trigger the action.
    job: Job
    action: Action
    state_predicate: StatePredicate

    # This holds state, "what tables did this job read the last time it ran". If any of
    # these tables get written to, that means we should trigger the job/action that this
    # MeadowdbDependencyAction represents.
    latest_tables_read: Optional[Set[Tuple[ConnectionKey, Tuple[str, str]]]] = None


class Scheduler:
    """
    A scheduler gets set up with jobs, and then executes actions on jobs as per the
    triggers defined on those jobs.
    """

    _JOB_RUNNER_POLL_DELAY_SECONDS: float = 1

    def __init__(
        self,
        job_runner_poll_delay_seconds: float = _JOB_RUNNER_POLL_DELAY_SECONDS,
    ) -> None:
        """
        job_runner_poll_delay_seconds is primarily to make unit tests run faster.
        """
        # all jobs that have been added to this scheduler
        self._jobs: Dict[TopicName, Job] = {}
        # the list of jobs that we've added but haven't created subscriptions for yet,
        # see create_job_subscriptions docstring. Only used temporarily when adding jobs
        self._create_job_subscriptions_queue: List[Job] = []
        # See comment on MeadowdbDependencyAction, this allows us to implement
        # MeadowdbDynamicDependency. We need to be able to look up keep track of the
        # MeadowdbDynamicDependencies per dependency scope (NOT the job scope!) AND by
        # the job name (see _process_meadowdb_effects docstring for more information).
        # Very important that the MeadowdbDependencyActions referenced by both
        # dictionaries are the same objects.
        self._meadowdb_dependencies_scope: Dict[
            ScopeValues, List[MeadowdbDependencyAction]
        ] = {}
        self._meadowdb_dependencies_name: Dict[TopicName, MeadowdbDependencyAction] = {}

        # how frequently to poll the job runners
        self._job_runner_poll_delay_seconds: float = job_runner_poll_delay_seconds

        self._awaited = False

    async def _async_init(self) -> Scheduler:
        if self._awaited:
            return self
        # the event log stores events and lets us subscribe to events
        self._event_log: EventLog = await EventLog()
        # the TimeEventPublisher enables users to create time-based triggers, and
        # creates the right events at the right time.
        self.time: TimeEventPublisher = await TimeEventPublisher(
            self._event_log.append_event
        )
        # create the effects subscriber
        self._event_log.subscribe(None, self._process_effects)

        # The local job runner is a special job runner that runs on the same machine as
        # meadowflow via multiprocessing.
        self._local_job_runner: LocalJobRunner = LocalJobRunner(self._event_log)
        # all job runners that have been added to this scheduler
        self._job_runners: List[JobRunner] = [self._local_job_runner]

        self._poll_job_runners_loop = asyncio.create_task(
            self._call_poll_job_runners_loop()
        )

        self._awaited = True
        return self

    def __await__(self) -> Generator[Any, None, Scheduler]:
        return self._async_init().__await__()

    def register_job_runner(
        self, job_runner_constructor: Callable[[EventLog], JobRunner]
    ) -> None:
        """
        Registers the job runner with the scheduler.

        TODO add graceful shutdown, deal with job runners going offline, etc.
        """
        self._job_runners.append(job_runner_constructor(self._event_log))

    def add_job(self, job: Job) -> None:
        """
        Note that create_job_subscriptions needs to be called separately (see
        docstring).
        """
        if job.name in self._jobs:
            raise ValueError(f"Job with name {job.name} already exists.")
        self._jobs[job.name] = job
        self._create_job_subscriptions_queue.append(job)
        self._event_log.append_event(job.name, JobPayload(None, "WAITING"))

    def create_job_subscriptions(self) -> None:
        """
        Should be called after all jobs are added.

        Adding jobs and creating subscriptions is done in two phases to avoid order
        dependence (otherwise can't add a job that triggers based on another without
        adding the other first), and allows even circular dependencies. I.e. add_job
        should be called (repeatedly), then create_job_subscriptions should be called.
        """

        # TODO: this should also check the new jobs' preconditions against the existing
        #  state. Perhaps they should already trigger.
        # TODO: should make sure we don't try to proceed without calling
        #  create_job_subscriptions first
        for job in self._create_job_subscriptions_queue:
            job.all_subscribed_topics = []
            for trigger_action in job.trigger_actions:
                # this registers time events in the StatePredicate with our
                # TimeEventPublisher so that it knows we need to trigger at those times
                condition = create_time_event_state_predicates(
                    self.time, trigger_action.state_predicate
                )

                for event_filter in trigger_action.wake_on:
                    # this registers time events in the EventFilter with our
                    # TimeEventPublisher so that it knows we need to trigger at those
                    # times.
                    event_filter = create_time_event_filters(self.time, event_filter)

                    if isinstance(event_filter, MeadowdbDynamicDependency):
                        # This implements MeadowdbDynamicDependency, which can't be
                        # implemented as a normal subscriber, and instead is taken care
                        # of in _process_effects
                        # TODO hopefully no one creates multiple of these on the same
                        #  trigger action, we should probably throw an error in that
                        #  case
                        dependency_action = MeadowdbDependencyAction(
                            job, trigger_action.action, trigger_action.state_predicate
                        )
                        self._meadowdb_dependencies_scope.setdefault(
                            event_filter.dependency_scope, []
                        ).append(dependency_action)
                        self._meadowdb_dependencies_name[job.name] = dependency_action
                    else:

                        async def subscriber(
                            low_timestamp: Timestamp,
                            high_timestamp: Timestamp,
                            # to avoid capturing loop variables
                            job: Job = job,
                            event_filter: EventFilter = event_filter,
                            condition: StatePredicate = condition,
                            action: Action = trigger_action.action,
                        ) -> None:
                            # first check that there's at least one event that passes
                            # the EventFilter
                            if any(
                                event_filter.apply(event)
                                for topic_name in event_filter.topic_names_to_subscribe()  # noqa: E501
                                for event in self._event_log.events(
                                    topic_name, low_timestamp, high_timestamp
                                )
                            ):
                                # then check that the condition is met and if so execute
                                # the action
                                if condition.apply(
                                    self._event_log,
                                    low_timestamp,
                                    high_timestamp,
                                    job.name,
                                ):
                                    await action.execute(
                                        job,
                                        None,
                                        self._job_runners,
                                        self._event_log,
                                        high_timestamp,
                                    )

                        # TODO we should consider throwing an exception if the topic
                        #  does not already exist (otherwise there's actually no point
                        #  in breaking out this create_job_subscriptions into a separate
                        #  function)
                        self._event_log.subscribe(
                            event_filter.topic_names_to_subscribe(), subscriber
                        )

                        # TODO would be nice to somehow get the dynamically subscribed
                        #  "topics" into all_subscribed_topics as well somehow...

                        job.all_subscribed_topics.extend(
                            event_filter.topic_names_to_subscribe()
                        )
                job.all_subscribed_topics.extend(condition.topic_names_to_query())
        self._create_job_subscriptions_queue.clear()

    def add_jobs(self, jobs: Iterable[Job]) -> None:
        for job in jobs:
            self.add_job(job)
        self.create_job_subscriptions()

    def instantiate_scope(self, scope: ScopeValues) -> None:
        self._event_log.append_event(scope.topic_name(), scope)

    async def _process_effects(
        self, low_timestamp: Timestamp, high_timestamp: Timestamp
    ) -> None:
        """
        Should get called for all events. Idea is to react to effects in Job-related
        events
        """

        futures: List[Awaitable] = []

        # we want to iterate through events oldest first
        for event in reversed(
            list(self._event_log.events(None, low_timestamp, high_timestamp))
        ):
            if isinstance(event.payload, JobPayload):
                if event.payload.state == "SUCCEEDED":
                    # Adding jobs and instantiating scopes isn't a normal effect in
                    # terms of getting added to meadowflow.effects (that would be weird
                    # because they would only get "actioned" after the job completes).
                    # Instead, they get returned by the function but it makes sense to
                    # process them here as well. So here we check if results is Job,
                    # ScopeValues, or a list/tuple of those, and add them to the
                    # scheduler

                    result_type, result = _get_jobs_or_scopes_from_result(
                        event.payload.result_value
                    )
                    if result_type == "jobs":
                        self.add_jobs(result)
                    elif result_type == "scopes":
                        for scope in result:
                            self.instantiate_scope(scope)
                    elif result_type == "none":
                        pass
                    else:
                        raise ValueError(
                            "Internal error, got an unexpected result_type "
                            f"{result_type}"
                        )

                    # Now process meadowdb_effects
                    # TODO Some effects (i.e. writes) should probably still be processed
                    #  even if the job was not successful.
                    futures.extend(
                        self._process_meadowdb_effects(
                            event, low_timestamp, high_timestamp
                        )
                    )

        await asyncio.gather(*futures)

    def _process_meadowdb_effects(
        self,
        event: Event[JobPayload],
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
    ) -> Iterable[Awaitable]:
        """
        Processes the MeadowdbEffects on event (if any). Updates
        MeadowdbDependencyActions. For reads, we find the current event's
        MeadowdbDependencyAction (if it exists) via _meadowdb_dependencies_name and
        update its latest_tables_read. For writes, we find any MeadowdbDependencyActions
        that depend on the current event's job's scope (or ALL_SCOPES) via
        _meadowdb_dependencies_scopes and triggers those actions if they read the table
        we wrote to. Remember that _meadowdb_dependencies_scopes and
        _meadowdb_dependencies_names refer to the same MeadowdbDependencyAction objects.
        Returns the futures created from executing the actions (if any).
        """
        # TODO this implementation is probably overly simplistic. Consider scenarios:
        # J, K, L are jobs, T, U are tables
        # 1. In this batch of events, K writes to T, then J reads from T, then J runs
        #    again and reads from U and not T. Should J be triggered again? (Also, is
        #    weird that J can "stop" reading from U, this probably deserves some
        #    thought.)
        # 2. J reads from T, K writes to T, then L writes to T. Should J be triggered
        #    once or twice?
        # 3. K writes to T, J reads from T. Should J be triggered again? Answer to this
        #    one is to check if the last run of J read the version that K wrote
        # 4. J reads from T and U. K writes to T, L reads from T and writes to U.
        #    After K runs, should we run J? Or should we wait until L runs?
        # Probably will be more efficient to process all of the events in the
        # _process_effects batch together depending on the exact semantics we go with.
        # Even outside of a batch of events, keep in mind that the order we see the
        # events for jobs completing actually has no relationship to when those jobs
        # read particular tables. I.e. all 4 combinations of "J read T before K wrote to
        # T"/"K wrote to T before J read T", and "J finishes before K"/"K finishes
        # before J" are possible.

        if event.payload.effects is not None:
            writes = set(
                (conn, table)
                for conn, effects in event.payload.effects.meadowdb_effects.items()
                for table in effects.tables_written.keys()
            )

            # If a job has a dynamic meadowdb dependency and it just ran, we need to
            # update its latest_tables_read to be whatever it just read. Importantly, we
            # ignore reads from tables that we also wrote to, otherwise we would end up
            # in an infinite loop.
            # TODO figure out how to detect and stop(?) longer cycles (A -> B -> A)
            if event.topic_name in self._meadowdb_dependencies_name:
                reads = set(
                    (conn, table)
                    for conn, effects in event.payload.effects.meadowdb_effects.items()
                    for table in effects.tables_read.keys()
                    if (conn, table) not in writes
                )
                if len(reads) == 0:
                    # TODO this should probably do more than just warn
                    print(
                        f"Job {event.topic_name} with dynamic meadowdb dependencies "
                        "did not read any meadowdb tables, dynamic dependencies will "
                        "not be triggered again until the job is rerun"
                    )
                self._meadowdb_dependencies_name[
                    event.topic_name
                ].latest_tables_read = reads

            # now trigger jobs based on writes, check both this event's job's scopes and
            # ALL_SCOPES
            if writes:
                dependencies_to_check = []
                # TODO what if the job has been removed or changed while it's been
                #  running?
                job_scope = self._jobs[event.topic_name].scope
                if job_scope in self._meadowdb_dependencies_scope:
                    dependencies_to_check.append(
                        self._meadowdb_dependencies_scope[job_scope]
                    )
                # job_scope should never be ALL_SCOPES, so no need to check for
                # redundancy here
                if ALL_SCOPES in self._meadowdb_dependencies_scope:
                    dependencies_to_check.append(
                        self._meadowdb_dependencies_scope[ALL_SCOPES]
                    )

                for meadowdb_dependency in itertools.chain(*dependencies_to_check):
                    if (
                        meadowdb_dependency.latest_tables_read is not None
                        and meadowdb_dependency.latest_tables_read.intersection(writes)
                    ) and meadowdb_dependency.state_predicate.apply(
                        self._event_log,
                        low_timestamp,
                        high_timestamp,
                        meadowdb_dependency.job.name,
                    ):
                        yield meadowdb_dependency.action.execute(
                            meadowdb_dependency.job,
                            None,
                            self._job_runners,
                            self._event_log,
                            high_timestamp,
                        )

    def manual_run(
        self, job_name: TopicName, overrides: Optional[JobRunOverrides] = None
    ) -> Task[str]:
        """
        Execute the Run Action on the specified job, returns a Task that returns the
        request_id (see Run.execute for semantics of request_id)

        Important--when this function returns, it's possible that no events have been
        created yet, not even RUN_REQUESTED.
        """
        # TODO see if we can eliminate a little copy/paste here
        if job_name not in self._jobs:
            raise ValueError(f"Unknown job: {job_name}")
        job = self._jobs[job_name]
        return asyncio.create_task(self._run_action(job, Actions.run, overrides))

    async def _run_action(
        self, topic: Topic, action: Action, overrides: Optional[JobRunOverrides]
    ) -> str:
        """Returns the request_id (see Run.execute for semantics of request_id)"""
        try:
            return await action.execute(
                topic,
                overrides,
                self._job_runners,
                self._event_log,
                self._event_log.next_timestamp,
            )
        except Exception as e:
            # TODO this function isn't awaited, so exceptions need to make it back into
            #  the scheduler somehow
            traceback.print_exc()
            # Since exceptions are already caught in execute and an event is logged in
            # that case, we shouldn't be getting here
            return f"Unexpected error: {str(e)}"

    def _get_running_and_requested_jobs(self) -> Iterable[Event[JobPayload]]:
        """
        Returns the latest event for any job that's in RUN_REQUESTED or RUNNING state
        """
        timestamp = self._event_log.next_timestamp
        for name in self._jobs.keys():
            ev = self._event_log.last_event(name, timestamp)
            if ev and ev.payload.state in ("RUN_REQUESTED", "RUNNING"):
                yield ev

    async def _call_poll_job_runners_loop(self) -> None:
        """Periodically polls the job runners we know about"""
        while True:
            try:
                # TODO should we keep track of which jobs are running on which job
                #  runner and only poll for those jobs?
                last_events = list(self._get_running_and_requested_jobs())
                await asyncio.gather(
                    *[jr.poll_jobs(last_events) for jr in self._job_runners]
                )
            except Exception:
                # TODO do something smarter here...
                traceback.print_exc()
            await asyncio.sleep(self._job_runner_poll_delay_seconds)

    async def close(self) -> None:
        self._poll_job_runners_loop.cancel()
        # any exceptions are ignored here
        ev_ex, time_ex, poll_ex = await asyncio.gather(
            self._event_log.close(),
            self.time.close(),
            self._poll_job_runners_loop,
            return_exceptions=True,
        )
        # we can still lose exceptions here.
        # asyncio does not have the equivalent of AggregateException.
        if poll_ex is not None and not isinstance(poll_ex, asyncio.CancelledError):
            raise poll_ex
        if ev_ex is not None:
            raise ev_ex
        if time_ex is not None:
            raise time_ex

    def __aenter__(self) -> Scheduler:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    def all_are_waiting(self) -> bool:
        """
        Returns true if everything is in a "waiting" state. I.e. no jobs are running,
        all subscribers have been processed.
        """
        return self._event_log.all_subscribers_called() and not any(
            True for _ in self._get_running_and_requested_jobs()
        )

    def events_of(self, topic_name: TopicName) -> List[Event]:
        """For unit tests/debugging"""
        return list(
            self._event_log.events_and_state(
                topic_name, 0, self._event_log.next_timestamp
            )
        )
