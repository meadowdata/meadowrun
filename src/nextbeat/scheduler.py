import asyncio
import threading
import traceback
from asyncio import Task
from typing import Dict, List, Tuple, Iterable, Optional, Callable, Awaitable

from nextbeat.event_log import Event, EventLog, Timestamp
from nextbeat.topic_names import TopicName
from nextbeat.jobs import Actions, Job, JobPayload, JobRunner
from nextbeat.local_job_runner import LocalJobRunner
from nextbeat.time_event_publisher import TimeEventPublisher, TimeEventFilterPlaceholder
from nextbeat.topic import Action, Topic, StatePredicate, EventFilter


class Scheduler:
    """
    A scheduler gets set up with jobs, and then executes actions on jobs as per the
    triggers defined on those jobs.

    TODO there are a lot of weird assumptions about what's called "on the event loop" vs
     from outside of it/on a different thread and what's threadsafe
    """

    _JOB_RUNNER_POLL_DELAY_SECONDS: float = 1

    def __init__(
        self,
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
        job_runner_poll_delay_seconds: float = _JOB_RUNNER_POLL_DELAY_SECONDS,
    ) -> None:
        """
        job_runner_poll_delay_seconds is primarily to make unit tests run faster.
        """

        # the asyncio event_loop that we will use
        if event_loop is not None:
            self._event_loop: asyncio.AbstractEventLoop = event_loop
        else:
            self._event_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        # we hang onto these tasks to allow for graceful shutdown
        self._main_loop_task: Optional[Task[None]] = None
        self._time_event_publisher_task: Optional[Task[None]] = None

        # the event log stores events and lets us subscribe to events
        self._event_log: EventLog = EventLog(self._event_loop)
        # the TimeEventPublisher enables users to create time-based triggers, and
        # creates the right events at the right time.
        self.time: TimeEventPublisher = TimeEventPublisher(
            self._event_loop, self._event_log.append_event
        )

        # create the effects subscriber
        self._event_log.subscribe(None, self._process_effects)

        # all jobs that have been added to this scheduler
        self._jobs: Dict[TopicName, Job] = {}
        # the list of jobs that we've added but haven't created subscriptions for yet,
        # see create_job_subscriptions docstring. Only used temporarily when adding jobs
        self._create_job_subscriptions_queue: List[Job] = []

        # The local job runner is a special job runner that runs on the same machine as
        # nextbeat via multiprocessing.
        self._local_job_runner: LocalJobRunner = LocalJobRunner(self._event_log)
        # all job runners that have been added to this scheduler
        self._job_runners: List[JobRunner] = [self._local_job_runner]
        # how frequently to poll the job runners
        self._job_runner_poll_delay_seconds: float = job_runner_poll_delay_seconds

    def register_job_runner(
        self, job_runner_constructor: Callable[[EventLog], JobRunner]
    ) -> None:
        """
        Registers the job runner with the scheduler. As with manual_run, all this does
        is schedule a callback on the event_loop, so it's possible no state has changed
        when this function returns.
        """
        self._event_loop.call_soon_threadsafe(
            self.register_job_runner_on_event_loop, job_runner_constructor
        )

    def register_job_runner_on_event_loop(
        self, job_runner_constructor: Callable[[EventLog], JobRunner]
    ) -> None:
        """
        Registers the job runner with the scheduler. Must be run on self._event_loop.

        TODO add graceful shutdown, deal with job runners going offline, etc.
        """
        if asyncio.get_running_loop() != self._event_loop:
            raise ValueError(
                "Scheduler.register_job_runner_async was called from a different "
                "_event_loop than expected"
            )

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
            for trigger_action in job.trigger_actions:
                for event_filter in trigger_action.wake_on:
                    # this registers time events with our TimeEventPublisher so that it
                    # knows we need to trigger at those times.
                    if isinstance(event_filter, TimeEventFilterPlaceholder):
                        event_filter = event_filter.create(self.time)

                    async def subscriber(
                        low_timestamp: Timestamp,
                        high_timestamp: Timestamp,
                        # to avoid capturing loop variables
                        job: Job = job,
                        event_filter: EventFilter = event_filter,
                        condition: StatePredicate = trigger_action.state_predicate,
                        action: Action = trigger_action.action,
                    ) -> None:
                        # first check that there's at least one event that passes the
                        # EventFilter
                        if any(
                            event_filter.apply(event)
                            for topic_name in event_filter.topic_names_to_subscribe()
                            for event in self._event_log.events(
                                topic_name, low_timestamp, high_timestamp
                            )
                        ):
                            # then check that the condition is met
                            events: Dict[TopicName, Tuple[Event, ...]] = {}
                            for name in condition.topic_names_to_query():
                                events[name] = tuple(
                                    self._event_log.events_and_state(
                                        name, low_timestamp, high_timestamp
                                    )
                                )
                            if condition.apply(events):
                                # if so, execute the action
                                await action.execute(
                                    job,
                                    self._job_runners,
                                    self._event_log,
                                    high_timestamp,
                                )

                    # TODO we should consider throwing an exception if the topic does
                    #  not already exist (otherwise there's actually no point in
                    #  breaking out this create_job_subscriptions into a separate
                    #  function)
                    self._event_log.subscribe(
                        event_filter.topic_names_to_subscribe(), subscriber
                    )
        self._create_job_subscriptions_queue.clear()

    def add_jobs(self, jobs: Iterable[Job]) -> None:
        for job in jobs:
            self.add_job(job)
        self.create_job_subscriptions()

    async def _process_effects(
        self, low_timestamp: Timestamp, high_timestamp: Timestamp
    ) -> None:
        """
        Should get called for all events. Idea is to react to effects in Job-related
        events
        """
        for event in self._event_log.events(None, low_timestamp, high_timestamp):
            if (
                isinstance(event.payload, JobPayload)
                and event.payload.effects is not None
            ):
                if event.payload.effects.add_jobs:
                    self.add_jobs(event.payload.effects.add_jobs)
                    self.create_job_subscriptions()

    def manual_run(self, job_name: TopicName) -> None:
        """
        Execute the Run Action on the specified job.

        Important--when this function returns, it's possible that no events have been
        created yet, not even RUN_REQUESTED.
        """
        if job_name not in self._jobs:
            raise ValueError(f"Unknown job: {job_name}")
        job = self._jobs[job_name]
        self._event_loop.call_soon_threadsafe(
            lambda: self._event_loop.create_task(self._run_action(job, Actions.run))
        )

    async def manual_run_on_event_loop(self, job_name: TopicName) -> None:
        """Execute the Run Action on the specified job."""
        # TODO see if we can eliminate a little copy/paste here
        if job_name not in self._jobs:
            raise ValueError(f"Unknown job: {job_name}")
        job = self._jobs[job_name]
        await self._run_action(job, Actions.run)

    async def _run_action(self, topic: Topic, action: Action) -> None:
        try:
            await action.execute(
                topic,
                self._job_runners,
                self._event_log,
                self._event_log.curr_timestamp,
            )
        except Exception:
            # TODO this function isn't awaited, so exceptions need to make it back into
            #  the scheduler somehow
            traceback.print_exc()

    def _get_running_and_requested_jobs(self) -> Iterable[Event[JobPayload]]:
        """
        Returns the latest event for any job that's in RUN_REQUESTED or RUNNING state
        """
        timestamp = self._event_log.curr_timestamp
        for name in self._jobs.keys():
            ev = self._event_log.last_event(name, timestamp)
            if ev and ev.payload.state in ("RUN_REQUESTED", "RUNNING"):
                yield ev

    async def _poll_job_runners_loop(self) -> None:
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

    def get_main_loop_tasks(self) -> Iterable[Awaitable]:
        yield self._event_loop.create_task(self._poll_job_runners_loop())
        yield self._event_loop.create_task(self.time.main_loop())
        yield self._event_loop.create_task(self._event_log.call_subscribers_loop())

    def main_loop(self) -> threading.Thread:
        """
        This starts a daemon (background) thread that runs forever. This code just polls
        the job runners, but other code will add callbacks to run on this event loop
        (e.g. EventLog.call_subscribers).

        TODO not clear that this is really necessary anymore
        """

        t = threading.Thread(
            target=lambda: self._event_loop.run_until_complete(
                asyncio.wait(list(self.get_main_loop_tasks()))
            ),
            daemon=True,
        )
        t.start()
        return t

    def shutdown(self) -> None:
        if self._main_loop_task is not None:
            self._main_loop_task.cancel()
            self._time_event_publisher_task.cancel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

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
                topic_name, 0, self._event_log.curr_timestamp
            )
        )
