import asyncio
import asyncio.events
import datetime
import threading
import traceback
from dataclasses import dataclass, field
from typing import (
    Iterable,
    Dict,
    Callable,
    List,
    Optional,
    Union,
    Any,
)
import time
import heapq

import pytz
import pytz.tzinfo

from nextbeat.event_log import Event
from nextbeat.topic import EventFilter


# We would ideally use pytz.BaseTzInfo but that doesn't have the .normalize method on it
PytzTzInfo = Union[pytz.tzinfo.StaticTzInfo, pytz.tzinfo.DstTzInfo]


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


@dataclass(frozen=True)
class PointInTimeTrigger(EventFilter):
    """See TimeEventPublisher.point_in_time_trigger"""

    topic_name: str

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield self.topic_name

    def apply(self, event: Event) -> bool:
        return True


@dataclass(frozen=True)
class TimeOfDayTrigger(EventFilter):
    """See TimeEventPublisher.time_of_day_trigger"""

    topic_name: str
    local_time_of_day: datetime.timedelta
    time_zone: PytzTzInfo

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield self.topic_name

    def apply(self, event: Event) -> bool:
        return True


@dataclass(frozen=True)
class TimeOfDayPayload:
    # The original local_time_of_day parameter used to create the trigger
    local_time_of_day: datetime.timedelta
    # The original time zone used to create the trigger
    time_zone: PytzTzInfo
    # The date we're triggering for. Note that this does not have to be the current day
    # (e.g. if local_time_of_day is <0 or >24hours)
    date: datetime.date
    # The point_in_time we're triggering for which should be roughly now, localized to
    # time_zone
    point_in_time: datetime.datetime


@dataclass(frozen=True)
class PeriodicTrigger(EventFilter):
    """See TimeEventPublisher.periodic_trigger"""

    topic_name: str
    period: datetime.timedelta

    def topic_names_to_subscribe(self) -> Iterable[str]:
        yield self.topic_name

    def apply(self, event: Event) -> bool:
        return True


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
        event_loop: asyncio.AbstractEventLoop,
        append_event: Callable[[str, Any], None],
        schedule_recurring_limit: datetime.timedelta = _SCHEDULE_RECURRING_LIMIT,
        schedule_recurring_freq: datetime.timedelta = _SCHEDULE_RECURRING_FREQ,
    ):
        """
        schedule_recurring_limit and schedule_recurring_freq are just configurable for
        testing.
        """

        self._append_event: Callable[[str, Any], None] = append_event

        self._call_at: _CallAt = _CallAt(event_loop)

        # keep a cache so that we don't create duplicate topics/events/triggers
        self._all_periodic_triggers: Dict[str, PeriodicTrigger] = {}
        self._all_time_of_day_triggers: Dict[str, TimeOfDayTrigger] = {}
        self._all_point_in_time_events: Dict[str, PointInTimeTrigger] = {}

        # see _schedule_recurring
        self._schedule_recurring_up_to: Optional[datetime.datetime] = None
        if schedule_recurring_freq >= schedule_recurring_limit:
            raise ValueError(
                "schedule_recurring_freq must be more frequent than "
                "schedule_recurring_limit"
            )
        self._schedule_recurring_limit: datetime.timedelta = schedule_recurring_limit
        self._schedule_recurring_freq: datetime.timedelta = schedule_recurring_freq

        # The functions on this class can not be called concurrently in general, but we
        # have to deal with the fact that users could be adding new triggers while we
        # are trying to schedule new callbacks for recurring triggers
        self._schedule_recurring_lock: threading.RLock = threading.RLock()

    def point_in_time_trigger(self, dt: datetime.datetime) -> PointInTimeTrigger:
        """
        Creates a trigger that will be triggered at the specified point in time.

        dt must have tzinfo set to a timezone created by pytz.

        The original datetime (including timezone information) will be the event
        payload.
        """

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

        if not _datetime_has_timezone(dt):
            raise ValueError("datetime must have a pytz timezone")

        time_zone = dt.tzinfo
        if not isinstance(time_zone, pytz.BaseTzInfo):
            raise ValueError("datetime must have a pytz timezone")

        name = time_zone.zone + "/" + dt.strftime("%Y-%m-%d_%H:%M:%S.%f%z")
        if name in self._all_point_in_time_events:
            trigger = self._all_point_in_time_events[name]
        else:
            trigger = PointInTimeTrigger("time/" + name)
            # TODO it isn't really right to schedule this trigger here--it should really
            #  get created when a job containing this trigger gets added to the
            #  Scheduler.
            self._call_at.call_at(dt, lambda: self._append_event("time/" + name, dt))
            self._all_point_in_time_events[name] = trigger

        return trigger

    def periodic_trigger(self, period: datetime.timedelta) -> PeriodicTrigger:
        """
        Creates a trigger that will be triggered every period. period must be longer
        than 1 second to avoid performance problems
        TODO even a 1s period can cause performance problems

        Note: A period of 24 hours is not recommended--consider time_of_day_trigger
        instead (even though it is usually not exactly equivalent because of daylight
        savings).

        Periodic triggers will get called as if they first got called at the Unix epoch
        (1970 Jan 1 midnight) in the UTC timezone. This should coincide with what we
        intuitively think of as scheduling periodic triggers "from the top of the hour"
        for periods that divide evenly into hours. For weird periods (e.g. 61s), the
        behavior will be less intuitive. As a result, if you create a periodic trigger
        for e.g. 48 hours, you may need to wait up to 48 hours for it to get called.
        """
        if period < datetime.timedelta(seconds=1):
            raise ValueError("Period must be longer than 1s")

        name = _timedelta_to_str(period)

        with self._schedule_recurring_lock:
            if name in self._all_periodic_triggers:
                trigger = self._all_periodic_triggers[name]
            else:
                trigger = PeriodicTrigger("time/" + name, period)
                self._all_periodic_triggers[name] = trigger
                if self._schedule_recurring_up_to is not None:
                    now = _utc_now()
                    # TODO it isn't really right to schedule this trigger here--it
                    #  should really get created when a job containing this trigger gets
                    #  added to the Scheduler.
                    self._schedule_periodic_trigger(
                        trigger, now, self._schedule_recurring_up_to
                    )
        return trigger

    def _schedule_periodic_trigger(
        self,
        periodic_trigger: PeriodicTrigger,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
    ) -> None:
        """Same semantics as _schedule_time_of_day_trigger"""

        # see comment under periodic_trigger about how periodic triggers get scheduled
        from_ts = from_dt.timestamp()
        to_ts = to_dt.timestamp()
        period_seconds = periodic_trigger.period.total_seconds()
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
            self._call_at.call_at(
                occurrence_dt,
                lambda occurrence_dt=occurrence_dt: self._append_event(
                    periodic_trigger.topic_name, occurrence_dt
                ),
            )

            occurrence_ts += period_seconds

    def time_of_day_trigger(
        self, local_time_of_day: datetime.timedelta, time_zone: PytzTzInfo
    ) -> TimeOfDayTrigger:
        """
        Creates a trigger that will be triggered at local_time_of_day in time_zone every
        day.

        For local_time_of_day, we use datetime.timedelta rather than datetime.time
        because we want to be able to represent negative times (i.e. the previous day)
        and times >=24 hours (the next day or beyond).

        local_time_of_day must have a 1 second resolution (i.e. any timedeltas with
        fractional seconds will raise an exception). This is to avoid performance
        problems
        TODO is that really necessary?

        local_time_of_day is actually slightly misnamed--it reflects the timedelta from
        midnight of each day, rather than time of day. This distinction matters because
        of daylight savings time. E.g. on 7 Nov 2021 in America/New_York, if
        local_time_of_day is hours=10, the actual local time at which that occurrence
        will happen is 9am. This seems like better behavior than scheduling that
        occurrence at 10am, because that would mean that local_time_of_day = 2hr and
        local_time_of_day = 1hr, those occurrences would happen at the same time! This
        weirdness will happen with any date/local_time_of_day combinations that cross a
        daylight savings time boundary. I.e. it seems more important to maintain
        relative timing between two local_time_of_day, so that for X hrs and Y hrs
        local_time_of_days, those events always happen X-Y hrs apart.

        time_zone must be created by pytz.timezone

        The original local_time_of_day and time_zone will be included in the event
        payload (TimeOfDayPayload).
        """
        if (
            local_time_of_day.microseconds != 0
            or getattr(local_time_of_day, "nanoseconds", 0) != 0
        ):
            raise ValueError(
                "time_of_day_triggers does not support sub-second times of day"
            )

        if not isinstance(time_zone, pytz.tzinfo.StaticTzInfo) and not isinstance(
            time_zone, pytz.tzinfo.DstTzInfo
        ):
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
        name = f"{time_zone.zone}/{_timedelta_to_str(local_time_of_day)}"

        with self._schedule_recurring_lock:
            if name in self._all_time_of_day_triggers:
                trigger = self._all_time_of_day_triggers[name]
            else:
                trigger = TimeOfDayTrigger("time/" + name, local_time_of_day, time_zone)
                self._all_time_of_day_triggers[name] = trigger
                if self._schedule_recurring_up_to is not None:
                    now = _utc_now()
                    # TODO it isn't really right to schedule this trigger here--it
                    #  should really get created when a job containing this trigger gets
                    #  added to the Scheduler.
                    self._schedule_time_of_day_trigger(
                        trigger, now, self._schedule_recurring_up_to
                    )

        return trigger

    def _schedule_time_of_day_trigger(
        self,
        time_of_day_trigger: TimeOfDayTrigger,
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
            from_dt.astimezone(time_of_day_trigger.time_zone)
            - time_of_day_trigger.local_time_of_day
        ).date()

        # see details in time_of_day_trigger about crossing DST boundaries
        def apply_to_date(d: datetime.date) -> datetime.datetime:
            return time_of_day_trigger.time_zone.normalize(
                time_of_day_trigger.time_zone.localize(
                    datetime.datetime.combine(d, datetime.time())
                )
                + time_of_day_trigger.local_time_of_day
            )

        occurrence = apply_to_date(date)

        # schedule callbacks to generate events at the right time
        while occurrence < to_dt:
            if occurrence >= from_dt:
                self._call_at.call_at(
                    occurrence,
                    # capture the value of date and occurrence
                    lambda date=date, occurrence=occurrence: self._append_event(
                        time_of_day_trigger.topic_name,
                        TimeOfDayPayload(
                            time_of_day_trigger.local_time_of_day,
                            time_of_day_trigger.time_zone,
                            date,
                            occurrence,
                        ),
                    ),
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

        with self._schedule_recurring_lock:
            # If this is the first time we're being called, start from now
            now = _utc_now()
            prev_schedule_recurring_up_to = self._schedule_recurring_up_to
            if prev_schedule_recurring_up_to is None:
                prev_schedule_recurring_up_to = now

            # The most we ever schedule is e.g. 5 days in the future
            next_schedule_recurring_up_to = now + self._schedule_recurring_limit

            for time_of_day_trigger in self._all_time_of_day_triggers.values():
                self._schedule_time_of_day_trigger(
                    time_of_day_trigger,
                    prev_schedule_recurring_up_to,
                    next_schedule_recurring_up_to,
                )

            for periodic_trigger in self._all_periodic_triggers.values():
                self._schedule_periodic_trigger(
                    periodic_trigger,
                    prev_schedule_recurring_up_to,
                    next_schedule_recurring_up_to,
                )

            self._schedule_recurring_up_to = next_schedule_recurring_up_to

        # schedule to get called back in e.g. a day so that we stay up to date
        self._call_at.call_at(
            now + self._schedule_recurring_freq, self._schedule_recurring
        )

    async def main_loop(self) -> None:
        # TODO on startup, we should look at the event log, figure out what we
        #  "missed" and backfill those events

        # TODO we should probably generate events when we call run (and not before)
        #  so that we don't end up with a bunch of events being generated if we take
        #  a long time between creating triggers and calling run

        self._schedule_recurring()
        await self._call_at.main_loop()


@dataclass(frozen=True, order=True)
class _CallAtCallback:
    """Call callback when time.time() == timestamp"""

    timestamp: float
    callback: Callable[[], None] = field(compare=False)


# ONLY FOR TESTING. Lets tests pretend that it's a different time
_TEST_TIME_OFFSET: float = 0


def _utc_now():
    """
    Returns a datetime that represents the current moment in time properly
    localized to the UTC timezone.

    This should ALWAYS be used instead of datetime.datetime.utcnow or
    datetime.datetime.now in this module so that _TEST_TIME_OFFSET works correctly.
    """
    return pytz.utc.localize(datetime.datetime.utcnow()) + datetime.timedelta(
        seconds=_TEST_TIME_OFFSET
    )


def _time_time():
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

    def __init__(self, event_loop: asyncio.AbstractEventLoop):
        self._event_loop: asyncio.AbstractEventLoop = event_loop
        self._lock: threading.RLock = threading.RLock()

        # all of the outstanding callbacks
        self._callback_queue: List[_CallAtCallback] = []

        self._is_running: bool = False
        self._is_shutting_down: bool = False

        # If _main_loop is waiting to callback something in the future, we need a way to
        # notify it when we create a callback that needs to happen first
        self._queue_modified: Optional[asyncio.Event] = None

        # we have to construct _queue_modified on the event loop
        def construct_queue_modified():
            self._queue_modified = asyncio.Event()

        self._event_loop.call_soon_threadsafe(construct_queue_modified)

    def call_at(self, dt: datetime.datetime, callback: Callable[[], None]) -> None:
        """Schedule callback to be called at dt"""

        if not _datetime_has_timezone(dt):
            raise ValueError("datetime must have a timezone")

        # dt.timestamp is always in UTC and comparable to time.time(), regardless of the
        # timezone of dt
        time_and_callback = _CallAtCallback(dt.timestamp(), callback)

        with self._lock:
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
                # we have to call Event.set on its event loop for it to have the desired
                # effect. We currently don't assume that we are on self._event_loop in
                # this function so we use call_soon_threadsafe
                self._event_loop.call_soon_threadsafe(
                    lambda: self._queue_modified.set()
                )

    async def main_loop(self) -> None:
        """
        Runs forever.

        If main_loop is called more than once, subsequent calls will have no effect.

        TODO the concurrency model needs to be clarified for this class as well as
         Scheduler and EventLog. Currently, the model is roughly that there are two
         threads, the main thread where a user "pokes at the system" (e.g. adds
         callbacks), and a background thread running "the" event loop which updates the
         system in the background. This is not at all rigorous though and probably
         should be changed

        Callbacks run on "the" event loop, so they cannot be blocking.
        """

        if asyncio.get_running_loop() != self._event_loop:
            raise ValueError(
                "_CallAt.main_loop was called from a different _event_loop than "
                "expected"
            )

        # Avoid running more than once. No lock is necessary as long as this always gets
        # run on self._event_loop...
        if self._is_running:
            return
        self._is_running = True

        while not self._is_shutting_down:
            # with the lock, figure out what we need to do next
            with self._lock:
                self._queue_modified.clear()

                if len(self._callback_queue) == 0:
                    # if we have no callbacks, scheduled just wait for one to get
                    # scheduled
                    next_step = "wait_for_heap_modified"
                else:
                    next_callback = self._callback_queue[0]
                    now = _time_time()
                    if next_callback.timestamp > now:
                        # if the next callback is in the future, sleep until it's time
                        # for that callback (and also watch out for new callbacks that
                        # get scheduled)
                        next_step = "wait_for_next_callback"
                    else:
                        # if the next callback is in the past/present, execute it
                        next_step = "call_next_callback"
                        heapq.heappop(self._callback_queue)

            # without the lock, do the next step
            if next_step == "wait_for_heap_modified":
                await self._queue_modified.wait()
            elif next_step == "wait_for_next_callback":
                print(f"About to sleep for {next_callback.timestamp - now}")
                await asyncio.wait(
                    [
                        self._queue_modified.wait(),
                        asyncio.sleep(next_callback.timestamp - now),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
            else:
                try:
                    next_callback.callback()
                except Exception:
                    # TODO do something smarter here...
                    traceback.print_exc()
