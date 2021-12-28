from __future__ import annotations

import abc
import asyncio
import asyncio.events
import datetime
import functools
import heapq
import time
import traceback
from dataclasses import dataclass, field
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Type,
)

import pytz

from meadowflow.event_log import Event, EventLog, Timestamp
from meadowflow.topic import AllPredicate, EventFilter, StatePredicate, TopicEventFilter
from meadowflow.topic_names import TopicName, pname


def _datetime_has_timezone(dt: datetime.datetime) -> bool:
    """Returns whether dt is timezone-aware or timezone-naive"""
    # https://stackoverflow.com/questions/5802108/how-to-check-if-a-datetime-object-is-localized-with-pytz
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def _timedelta_to_str(t: datetime.timedelta) -> str:
    """A human-readable representation of a timedelta"""
    x, fractional_seconds = divmod(t.total_seconds(), 1)
    x, seconds = divmod(x, 60)
    hours, minutes = divmod(x, 60)

    if fractional_seconds == 0.0:
        fractional_seconds_str = ""
    else:
        fractional_seconds_str = str(fractional_seconds)[1:]

    return f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}{fractional_seconds_str}"


class TimeEventFilterPlaceholder(EventFilter):
    """
    When a user wants to use a time event filter, they just use a
    TimeEventFilterPlaceholder in their job definition. Then,
    Scheduler.create_job_subscriptions will call TimeEventFilterPlaceholder.create,
    which will create a "real" EventFilter that the job can actually depend on. These
    "extra steps" are necessary because often the job definition will be created in a
    different process than the one that has the Scheduler and TimeEventPublisher.
    There's also a small benefit that jobs that get created but never added won't result
    in unnecessary time events being created. topic_names_to_subscribe and apply should
    never be called if all of the machinery described here is working correctly.
    """

    def topic_names_to_subscribe(self) -> Iterable[TopicName]:
        raise ValueError(
            "Cannot use a TimeEventFilterPlaceholder without calling create."
        )

    def apply(self, event: Event) -> bool:
        raise ValueError(
            "Cannot use a TimeEventFilterPlaceholder without calling create."
        )

    @abc.abstractmethod
    def create(self, time_event_publisher: TimeEventPublisher) -> EventFilter:
        """
        This should register this time event with the TimeEventPublisher and return a
        "real" EventFilter
        """
        pass


def create_time_event_filters(
    time_event_publisher: TimeEventPublisher, event_filter: EventFilter
) -> EventFilter:
    """Calls create on the event_filter if needed, returns the modified EventFilter"""
    if isinstance(event_filter, TimeEventFilterPlaceholder):
        return event_filter.create(time_event_publisher)
    else:
        return event_filter


@dataclass(frozen=True)
class PointInTime(TimeEventFilterPlaceholder):
    """
    Creates a time event that will be triggered at the specified point in time.

    dt must have tzinfo set to a timezone created by pytz.

    The original datetime (including timezone information) will be the event payload.
    """

    dt: datetime.datetime

    def create(self, time_event_publisher: TimeEventPublisher) -> EventFilter:
        return time_event_publisher.create_point_in_time(self)

    def __eq__(self, other: object) -> bool:
        # important to include the tzinfo explicitly here as it gets lost when comparing
        # datetime.datetime
        return (
            isinstance(other, PointInTime)
            and self.dt == other.dt
            and self.dt.tzinfo == other.dt.tzinfo
        )

    def __hash__(self) -> int:
        # see __eq__
        return hash((self.dt, self.dt.tzinfo))


@dataclass(frozen=True)
class Periodic(TimeEventFilterPlaceholder):
    """
    Creates a time event that will be triggered every period. period must be longer than
    1 second to avoid performance problems
    TODO even a 1s period can cause performance problems

    Note: A period of 24 hours is not recommended--consider time_of_day_trigger instead
    (even though it is usually not exactly equivalent because of daylight savings).

    Periodic triggers will get called as if they first got called at the Unix epoch
    (1970 Jan 1 midnight) in the UTC timezone. This should coincide with what we
    intuitively think of as scheduling periodic triggers "from the top of the hour" for
    periods that divide evenly into hours. For weird periods (e.g. 61s), the behavior
    will be less intuitive. As a result, if you create a periodic trigger for e.g. 48
    hours, you may need to wait up to 48 hours for it to get called.
    """

    period: datetime.timedelta

    def create(self, time_event_publisher: TimeEventPublisher) -> EventFilter:
        return time_event_publisher.create_periodic(self)


@dataclass(frozen=True)
class TimeOfDay(TimeEventFilterPlaceholder):
    """
    Creates a time event that will be triggered at local_time_of_day in time_zone every
    day.

    For local_time_of_day, we use datetime.timedelta rather than datetime.time because
    we want to be able to represent negative times (i.e. the previous day) and times
    >=24 hours (the next day or beyond).

    local_time_of_day must have a 1 second resolution (i.e. any timedeltas with
    fractional seconds will raise an exception). This is to avoid performance problems
    TODO is that really necessary?

    local_time_of_day is actually slightly misnamed--it reflects the timedelta from
    midnight of each day, rather than time of day. This distinction matters because of
    daylight savings time. E.g. on 7 Nov 2021 in America/New_York, if local_time_of_day
    is hours=10, the actual local time at which that occurrence will happen is 9am. This
    seems like better behavior than scheduling that occurrence at 10am, because that
    would mean that local_time_of_day = 2hr and local_time_of_day = 1hr, those
    occurrences would happen at the same time! This weirdness will happen with any
    date/local_time_of_day combinations that cross a daylight savings time boundary.
    I.e. it seems more important to maintain relative timing between two
    local_time_of_day, so that for X hrs and Y hrs local_time_of_days, those events
    always happen X-Y hrs apart.

    time_zone must be created by pytz.timezone

    The original local_time_of_day and time_zone will be included in the event payload
    (TimeOfDayPayload).
    """

    local_time_of_day: datetime.timedelta
    time_zone: pytz.BaseTzInfo

    def create(self, time_event_publisher: TimeEventPublisher) -> EventFilter:
        return time_event_publisher.create_time_of_day(self)


@dataclass(frozen=True)
class TimeOfDayPayload:
    # The original local_time_of_day parameter used to create the trigger
    local_time_of_day: datetime.timedelta
    # The original time zone used to create the trigger
    time_zone: pytz.BaseTzInfo
    # The date we're triggering for. Note that this does not have to be the current day
    # (e.g. if local_time_of_day is <0 or >24hours)
    date: datetime.date
    # The point_in_time we're triggering for which should be roughly now, localized to
    # time_zone
    point_in_time: datetime.datetime


class TimeEventPredicatePlaceholder(StatePredicate):
    """Analogous to TimeEventFilterPlaceholder, see docstring for that class"""

    def topic_names_to_query(self) -> Iterable[TopicName]:
        raise ValueError(
            "Cannot use a TimeEventPredicatePlaceholder without calling create."
        )

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        raise ValueError(
            "Cannot use a TimeEventPredicatePlaceholder without calling create."
        )

    @abc.abstractmethod
    def create(self, time_event_publisher: TimeEventPublisher) -> StatePredicate:
        """Analogous to TimeEventFilterPlaceholder, see docstring for that class"""
        pass


def create_time_event_state_predicates(
    time_event_publisher: TimeEventPublisher, state_predicate: StatePredicate
) -> StatePredicate:
    """Analogous to create_time_event_filters, see docstring for that function"""
    if isinstance(state_predicate, TimeEventPredicatePlaceholder):
        return state_predicate.create(time_event_publisher)
    else:
        return state_predicate.map(
            functools.partial(create_time_event_state_predicates, time_event_publisher)
        )


@dataclass(frozen=True)
class PointInTimePredicate(TimeEventPredicatePlaceholder):
    """
    A predicate requiring that we are before or after a particular time. Note that
    this is not based on "real time", but on a PointInTime event. This will not
    usually be an important distinction, but will matter  in cases where the scheduler
    process is overloaded, the scheduler process has crashed and is being brought back
    up, or in a simulation.
    """

    dt: datetime.datetime
    relation: Literal["before", "after"]

    @classmethod
    def between(cls, dt1: datetime.datetime, dt2: datetime.datetime) -> StatePredicate:
        return AllPredicate([cls(dt1, "after"), cls(dt2, "before")])

    def create(self, time_event_publisher: TimeEventPublisher) -> StatePredicate:
        event = time_event_publisher.create_point_in_time(PointInTime(self.dt))
        return _PointInTimePredicateCreated(event.topic_name, self.relation)


@dataclass(frozen=True)
class _PointInTimePredicateCreated(StatePredicate):
    topic_name: TopicName
    relation: Literal["before", "after"]

    def topic_names_to_query(self) -> Iterable[TopicName]:
        yield self.topic_name

    def apply(
        self,
        event_log: EventLog,
        low_timestamp: Timestamp,
        high_timestamp: Timestamp,
        current_job_name: TopicName,
    ) -> bool:
        # PointInTime topics should have no events until the point in time happens, at
        # which point there should just be one event.
        no_events = event_log.last_event(self.topic_name, high_timestamp) is None
        if self.relation == "before":
            return no_events
        elif self.relation == "after":
            return not no_events
        else:
            raise ValueError(f"Unexpected relation {self.relation}")


_SCHEDULE_RECURRING_LIMIT = datetime.timedelta(days=5)
_SCHEDULE_RECURRING_FREQ = datetime.timedelta(days=1)


class TimeEventPublisher:
    """
    Creates events in the specified EventLog at the right time based on the Triggers
    that get created

    For any two time events t0 < t1, we can only guarantee that t0 will hit the event
    log before t1 if t0 is added before t1. Practically speaking, even if t1 is added
    before t0, if t0 is added sufficiently in advance, ordering will be as expected.

    TODO what if time goes backwards (due to NTP or manually setting clocks?)
    """

    def __init__(
        self,
        append_event: Callable[[TopicName, Any], None],
        schedule_recurring_limit: datetime.timedelta = _SCHEDULE_RECURRING_LIMIT,
        schedule_recurring_freq: datetime.timedelta = _SCHEDULE_RECURRING_FREQ,
    ):
        """
        schedule_recurring_limit and schedule_recurring_freq are just configurable for
        testing.
        """

        self._append_event: Callable[[TopicName, Any], None] = append_event

        # Keep a cache so that we don't create duplicate topics/events/triggers.
        self._all_point_in_time: Dict[PointInTime, TopicEventFilter] = {}
        self._all_periodic: Dict[Periodic, TopicEventFilter] = {}
        self._all_time_of_day: Dict[TimeOfDay, TopicEventFilter] = {}

        # see _schedule_recurring
        self._schedule_recurring_up_to: Optional[datetime.datetime] = None
        if schedule_recurring_freq >= schedule_recurring_limit:
            raise ValueError(
                "schedule_recurring_freq must be more frequent than "
                "schedule_recurring_limit"
            )
        self._schedule_recurring_limit: datetime.timedelta = schedule_recurring_limit
        self._schedule_recurring_freq: datetime.timedelta = schedule_recurring_freq

        # Async init
        self._call_at: _CallAt
        self._awaited = False

        # TODO on startup, we should look at the event log, figure out what we
        # "missed" and backfill those events

        # TODO we should probably generate events when we call run (and not before)
        # so that we don't end up with a bunch of events being generated if we take
        # a long time between creating triggers and calling run

    async def _async_init(self) -> TimeEventPublisher:
        if self._awaited:
            return self
        self._call_at = await _CallAt()
        # self._main_loop = asyncio.create_task(self._call_main_loop())
        self._schedule_recurring()
        self._awaited = True
        return self

    def __await__(self) -> Generator[Any, None, TimeEventPublisher]:
        return self._async_init().__await__()

    def create_point_in_time(self, point_in_time: PointInTime) -> TopicEventFilter:
        """See PointInTime docstring."""

        # TODO consider calling `dt = dt.tzinfo.normalize(dt)` in case the user does not
        #  know how to use pytz timezones correctly...

        # TODO should this behave more like the recurring triggers? I.e. if you schedule
        #  something in the past (either from when you call this function or it becomes
        #  in the past between when you call this function and when you call run, should
        #  it execute?

        # We don't collapse the same point in time for different timezones (e.g.
        # 2021-08-01 12pm America/New_York and 2021-08-01 9am America/Los_Angeles) so
        # that we can include the original timezone in the event payload. This means we
        # have to be careful not to use dt0 == dt1 because that does not care about
        # timezone!

        if not _datetime_has_timezone(point_in_time.dt):
            raise ValueError("datetime must have a pytz timezone")

        time_zone = point_in_time.dt.tzinfo
        if not isinstance(time_zone, pytz.BaseTzInfo):
            raise ValueError("datetime must have a pytz timezone")

        if point_in_time in self._all_point_in_time:
            event_filter = self._all_point_in_time[point_in_time]
        else:
            # important to include the tzinfo explicitly here as it gets lost when
            # comparing datetime.datetime
            name = pname(
                "time/point", dt=point_in_time.dt, tzinfo=point_in_time.dt.tzinfo
            )
            event_filter = TopicEventFilter(name)
            self._call_at.call_at(
                point_in_time.dt, lambda: self._append_event(name, point_in_time.dt)
            )
            self._all_point_in_time[point_in_time] = event_filter

        return event_filter

    def create_periodic(self, periodic: Periodic) -> TopicEventFilter:
        """See Periodic docstring"""

        if periodic.period < datetime.timedelta(seconds=1):
            raise ValueError("Period must be longer than 1s")

        if periodic in self._all_periodic:
            event_filter = self._all_periodic[periodic]
        else:
            event_filter = TopicEventFilter(
                pname("time/periodic", period=periodic.period)
            )
            self._all_periodic[periodic] = event_filter
            if self._schedule_recurring_up_to is not None:
                now = _utc_now()
                self._schedule_periodic(
                    event_filter.topic_name,
                    periodic,
                    now,
                    self._schedule_recurring_up_to,
                )
        return event_filter

    def _schedule_periodic(
        self,
        topic_name: TopicName,
        periodic: Periodic,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
    ) -> None:
        """Same semantics as _schedule_time_of_day_trigger"""

        # see comment under periodic_trigger about how periodic triggers get scheduled
        from_ts = from_dt.timestamp()
        to_ts = to_dt.timestamp()
        period_seconds = periodic.period.total_seconds()
        modulo = from_ts % period_seconds
        if modulo == 0:
            occurrence_ts = from_ts
        else:
            occurrence_ts = from_ts - modulo + period_seconds

        # the above math should guarantee that occurrence >= from_dt
        while occurrence_ts < to_ts:
            occurrence_dt = pytz.utc.localize(
                datetime.datetime.utcfromtimestamp(occurrence_ts)
            )

            def append(occurrence_dt: datetime.datetime = occurrence_dt) -> None:
                self._append_event(topic_name, occurrence_dt)

            self._call_at.call_at(
                occurrence_dt,
                append,
            )

            occurrence_ts += period_seconds

    def create_time_of_day(self, time_of_day: TimeOfDay) -> TopicEventFilter:
        if (
            time_of_day.local_time_of_day.microseconds != 0
            or getattr(time_of_day.local_time_of_day, "nanoseconds", 0) != 0
        ):
            raise ValueError(
                "time_of_day_triggers does not support sub-second times of day"
            )

        if not isinstance(
            time_of_day.time_zone, pytz.tzinfo.StaticTzInfo
        ) and not isinstance(time_of_day.time_zone, pytz.tzinfo.DstTzInfo):
            # We're going to rely on the fact that a pytz timezone's str representation
            # is unique, so make sure that we have a pytz timezone
            raise ValueError("time_zone must be constructed via pytz.timezone")

        # Different time of day/timezone combinations that are "the same time" will not
        # be collapsed because we want to include the original time of day/timezone in
        # the payload. Also, daylight savings time rules can cause them to go out of
        # sync in the future. E.g. America/Los_Angeles/12pm and America/New_York/3pm are
        # the same right now, but they could diverge in the future if California gets
        # rid of daylight savings. pytz unfortunately does not have an easy way of
        # resolving aliases, so e.g. US/Eastern and America/New_York will be treated as
        # two different timezones when they are actually exactly the same.

        if time_of_day in self._all_time_of_day:
            event_filter = self._all_time_of_day[time_of_day]
        else:
            event_filter = TopicEventFilter(
                pname(
                    "time/of_day",
                    local_time_of_day=time_of_day.local_time_of_day,
                    time_zone=time_of_day.time_zone,
                )
            )
            self._all_time_of_day[time_of_day] = event_filter
            if self._schedule_recurring_up_to is not None:
                now = _utc_now()
                self._schedule_time_of_day(
                    event_filter.topic_name,
                    time_of_day,
                    now,
                    self._schedule_recurring_up_to,
                )

        return event_filter

    def _schedule_time_of_day(
        self,
        topic_name: TopicName,
        time_of_day: TimeOfDay,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
    ) -> None:
        """
        Schedules event generation for time_of_day_trigger where from_dt <= event <
        to_dt. MUST be called with _schedule_recurring_lock
        """

        # we want to get the first date in the specified time_zone that could be
        # relevant--keep in mind that local_time_of_day can be negative and >24 hours
        date = (
            from_dt.astimezone(time_of_day.time_zone) - time_of_day.local_time_of_day
        ).date()

        # see details in time_of_day_trigger about crossing DST boundaries
        def apply_to_date(d: datetime.date) -> datetime.datetime:
            return time_of_day.time_zone.normalize(
                time_of_day.time_zone.localize(
                    datetime.datetime.combine(d, datetime.time())
                )
                + time_of_day.local_time_of_day
            )

        occurrence = apply_to_date(date)

        # schedule callbacks to generate events at the right time
        while occurrence < to_dt:
            if occurrence >= from_dt:

                def append(
                    # capture the value of date and occurrence
                    date: datetime.date = date,
                    occurrence: datetime.datetime = occurrence,
                ) -> None:
                    self._append_event(
                        topic_name,
                        TimeOfDayPayload(
                            time_of_day.local_time_of_day,
                            time_of_day.time_zone,
                            date,
                            occurrence,
                        ),
                    )

                self._call_at.call_at(
                    occurrence,
                    append,
                )

            # we assume that this math on date/occurrence always results in an
            # occurrence that is in the future compared to the previous occurrence. This
            # should be the case with all "real" time zones. Also, very important, we
            # are adding a timedelta to a date (not a datetime). This moves us forward
            # one day, NOT 24 hours, which is what we want when daylight savings gets
            # involved.
            date = date + datetime.timedelta(days=1)
            occurrence = apply_to_date(date)

    def _schedule_recurring(self) -> None:
        """
        Here's how recurring time events get scheduled:
        1. TimeEventPublisher is created
        2. Recurring time triggers are created (no event generation is scheduled)
        3. run is called, which calls _schedule_recurring. _schedule_recurring_up_to is
           set to 5 days from now, and any recurring events are scheduled from now up
           until 5 days from now
           TODO is this the right approach? Should we schedule some in the past?
        4. Additional recurring time triggers are created. As they are being created,
           they schedule events for themselves up to _schedule_recurring_up_to
        5. In step 3, _schedule_recurring also scheduled a rerun of _schedule_recurring
           for 1 day in the future. That run will schedule any previously unscheduled
           events within the next 5 days, advance _schedule_recurring_up_to, and
           schedule another run of _schedule_recurring
        """

        # If this is the first time we're being called, start from now
        now = _utc_now()
        prev_schedule_recurring_up_to = self._schedule_recurring_up_to
        if prev_schedule_recurring_up_to is None:
            prev_schedule_recurring_up_to = now

        # The most we ever schedule is e.g. 5 days in the future
        next_schedule_recurring_up_to = now + self._schedule_recurring_limit

        for periodic, event_filter in self._all_periodic.items():
            self._schedule_periodic(
                event_filter.topic_name,
                periodic,
                prev_schedule_recurring_up_to,
                next_schedule_recurring_up_to,
            )

        for time_of_day, event_filter in self._all_time_of_day.items():
            self._schedule_time_of_day(
                event_filter.topic_name,
                time_of_day,
                prev_schedule_recurring_up_to,
                next_schedule_recurring_up_to,
            )

        self._schedule_recurring_up_to = next_schedule_recurring_up_to

        # schedule to get called back in e.g. a day so that we stay up to date
        self._call_at.call_at(
            now + self._schedule_recurring_freq, self._schedule_recurring
        )

    async def close(self) -> None:
        await self._call_at.close()

    def __aenter__(self) -> TimeEventPublisher:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()


# Why not just use callback: Callable[[], None] in _CallAtCallback?
# Because mypy (and also python) has issue with class fields of type Callable.
# It's confused whether they are methods or fields.
# See e.g. https://github.com/python/mypy/issues/708
class _CallAtCallable(Protocol):
    def __call__(self) -> None:
        ...


@dataclass(frozen=True, order=True)
class _CallAtCallback:
    """Call callback when time.time() == timestamp"""

    timestamp: float
    callback: _CallAtCallable = field(compare=False)


# ONLY FOR TESTING. Lets tests pretend that it's a different time
_TEST_TIME_OFFSET: float = 0


def _utc_now() -> datetime.datetime:
    """
    Returns a datetime that represents the current moment in time properly
    localized to the UTC timezone.

    This should ALWAYS be used instead of datetime.datetime.utcnow or
    datetime.datetime.now in this module so that _TEST_TIME_OFFSET works correctly.
    """
    return pytz.utc.localize(datetime.datetime.utcnow()) + datetime.timedelta(
        seconds=_TEST_TIME_OFFSET
    )


def _time_time() -> float:
    """
    Equivalent to time.time() but allows for pretending that it's a different time.

    This should ALWAYS be used instead of time.time in this module so that
    _TEST_TIME_OFFSET works correctly.
    """
    return time.time() + _TEST_TIME_OFFSET


class _CallAt:
    """
    Presents an interface for scheduling callbacks at a particular datetime.datetime.
    Loosely based on sched.scheduler.

    TODO consider replacing this with event_loop.call_later. Reasons not to do that are:
    - in python 3.7 and prior call_later could not handle delays greater than one day
    - the absolute time of the event_loop appears to be arbitrary (i.e. not
      time.time()). This means that we'll need to schedule like
      event_loop.call_later(dt.timestamp() - time.time()). If our process freezes for X
      seconds between the time.time() call and the .call_later(...) call, then it will
      be as if we scheduled for dt + X. It's unlikely for pauses to be long enough for
      events to get scheduled out of order, but it is definitely possible.
    """

    def __init__(self) -> None:
        # all of the outstanding callbacks
        self._callback_queue: List[_CallAtCallback] = []

        # Init for these needs to be on an event loop - see _init_async and __await__.
        # If _main_loop is waiting to callback something in the future, we need a way to
        # notify it when we create a callback that needs to happen first
        self._queue_modified: asyncio.Event

        # keep the task for the main loop so we can cancel it in close().
        self._main_loop: asyncio.Task

        # make sure awaiting more than once doesn't mess with us.
        self._awaited = False

    async def _init_async(self) -> _CallAt:
        if self._awaited:
            return self
        self._queue_modified = asyncio.Event()
        self._main_loop = asyncio.create_task(self._call_main_loop())
        self._awaited = True
        return self

    def __await__(self) -> Generator[Any, None, _CallAt]:
        return self._init_async().__await__()

    def call_at(self, dt: datetime.datetime, callback: Callable[[], None]) -> None:
        """Schedule callback to be called at dt"""

        if not _datetime_has_timezone(dt):
            raise ValueError("datetime must have a timezone")

        # dt.timestamp is always in UTC and comparable to time.time(), regardless of the
        # timezone of dt
        time_and_callback = _CallAtCallback(dt.timestamp(), callback)

        if len(self._callback_queue) == 0:
            prev_next_callback = None
        else:
            prev_next_callback = self._callback_queue[0]

        # add the callback
        heapq.heappush(self._callback_queue, time_and_callback)

        # If we've changed when the next callback is, we need to notify the main
        # loop. new_next_callback == prev_next_callback doesn't require a
        # notification--the run function will just pick up whichever one happens to
        # be in front and execute that one
        if prev_next_callback is None or time_and_callback < prev_next_callback:
            asyncio.get_running_loop().call_soon(self._queue_modified.set)

    async def _call_main_loop(self) -> None:
        """
        Runs forever.

        Callbacks run on "the" event loop, so they cannot be blocking.

        This coroutine is started as part of initialization when this class is
        awaited, it should not be called manually.
        """
        while True:
            # with the lock, figure out what we need to do next
            self._queue_modified.clear()

            if len(self._callback_queue) == 0:
                # if we have no callbacks scheduled, wait for one to get
                # scheduled
                await self._queue_modified.wait()
            else:
                next_callback = self._callback_queue[0]
                now = _time_time()
                if next_callback.timestamp > now:
                    # if the next callback is in the future, sleep until it's
                    # time for that callback (and also watch out for new
                    # callbacks that get scheduled)
                    print(f"About to sleep for {next_callback.timestamp - now}")
                    try:
                        await asyncio.wait_for(
                            self._queue_modified.wait(),
                            timeout=next_callback.timestamp - now,
                        )
                    except asyncio.exceptions.TimeoutError:
                        pass
                else:
                    # if the next callback is in the past/present, execute it
                    try:
                        next_callback.callback()
                    except Exception:
                        # TODO do something smarter here...
                        traceback.print_exc()
                    heapq.heappop(self._callback_queue)

    async def close(self) -> None:
        # cancel and wait until the loop exits.
        # If we don't wait we get "Task was destroyed but it is pending" warnings.
        self._main_loop.cancel()
        try:
            await self._main_loop
        except asyncio.exceptions.CancelledError:
            pass

    def __aenter__(self) -> _CallAt:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()
