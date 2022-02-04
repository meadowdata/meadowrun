import asyncio
import datetime

import time
from pprint import pprint
from typing import List, Optional, Tuple

import meadowflow.event_log
import meadowflow.jobs
import meadowflow.time_event_publisher
import pytest
import pytz
from meadowflow.time_event_publisher import (
    Periodic,
    PointInTime,
    TimeEventPublisher,
    TimeOfDay,
    TimeOfDayPayload,
    _timedelta_to_str,
)

# these need to be tuned to make the tests run fast, but avoid false negatives
_TIME_DELAY = 0.1
_TIME_INCREMENT = datetime.timedelta(seconds=1)


@pytest.mark.asyncio
async def test_call_at():
    # this uses the higher level interface (TimeEventPublisher) but mostly tests the low
    # level functionality of _CallAt and whether it's robust to different
    # sequences of events

    # test basic callback functionality

    async with meadowflow.event_log.EventLog() as event_log, TimeEventPublisher(
        event_log.append_event
    ) as p:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        p.create_point_in_time(PointInTime(now))  # called
        p.create_point_in_time(PointInTime(now - _TIME_INCREMENT))  # called
        p.create_point_in_time(PointInTime(now + 3 * _TIME_INCREMENT))  # not called

        await asyncio.sleep(_TIME_DELAY)

        assert len(event_log._event_log) == 2

        now = pytz.utc.localize(datetime.datetime.utcnow())
        p.create_point_in_time(PointInTime(now))  # called

        await asyncio.sleep(_TIME_DELAY)
        assert len(event_log._event_log) == 3

        p.create_point_in_time(PointInTime(now + 3 * _TIME_INCREMENT))  # not called
        p.create_point_in_time(PointInTime(now - _TIME_INCREMENT))  # called

        await asyncio.sleep(_TIME_DELAY)

        assert len(event_log._event_log) == 4


@pytest.mark.asyncio
async def test_call_at_callbacks_before_running():
    # test adding callbacks before running
    # TODO this test seems moot now...the publisher is running
    # from the start, but it doesn't get a chance to schedule callbacks
    # because nothing is awaited until the sleep.

    async with meadowflow.event_log.EventLog() as event_log, TimeEventPublisher(
        event_log.append_event
    ) as p:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        p.create_point_in_time(PointInTime(now))  # called
        p.create_point_in_time(PointInTime(now - _TIME_INCREMENT))  # called
        p.create_point_in_time(PointInTime(now + _TIME_INCREMENT))  # not called

        assert len(event_log._event_log) == 0

        await asyncio.sleep(_TIME_DELAY)

        assert len(event_log._event_log) == 2


def _dt_to_str(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d-%H-%M-%S-%f-%z-%Z")


def _date_to_str(dt: datetime.date) -> str:
    return dt.strftime("%Y-%m-%d")


@pytest.mark.asyncio
async def test_time_event_publisher_point_in_time():
    """Test TimeEventPublisher.point_in_time_trigger"""

    async with meadowflow.event_log.EventLog() as event_log, TimeEventPublisher(
        event_log.append_event
    ) as p:
        now = pytz.utc.localize(datetime.datetime.utcnow())

        tz_ldn = pytz.timezone("Europe/London")
        tz_ny = pytz.timezone("America/New_York")
        tz_la = pytz.timezone("America/Los_Angeles")

        dts = [
            now.astimezone(tz_ny) - _TIME_INCREMENT,
            now.astimezone(tz_la) + 1.5 * _TIME_INCREMENT,
            now.astimezone(tz_ldn) + 1.5 * _TIME_INCREMENT,
            now.astimezone(tz_ldn) + 3 * _TIME_INCREMENT,
        ]

        for dt in dts:
            p.create_point_in_time(PointInTime(dt))

        # It's important to compare the results in string format because we care about
        # what timezone a datetime is in, and datetime equality does not care about the
        # timezone
        dt_strings = [_dt_to_str(dt) for dt in dts]

        t0 = time.time()

        await asyncio.sleep(_TIME_DELAY)

        assert 1 == len(event_log._event_log)
        assert dt_strings[0] == _dt_to_str(event_log._event_log[0].payload)

        await asyncio.sleep(1.5 * _TIME_INCREMENT.total_seconds() + t0 - time.time())

        assert 3 == len(event_log._event_log)
        # make sure that 2 times with the same point in time but different timezones
        # create separate events
        assert 3 == len(event_log._topic_name_to_events)
        assert set(dt_strings[:3]) == set(
            _dt_to_str(e.payload) for e in event_log._event_log
        )

        await asyncio.sleep(3 * _TIME_INCREMENT.total_seconds() + t0 - time.time())

        assert 4 == len(event_log._event_log)
        assert set(dt_strings) == set(
            _dt_to_str(e.payload) for e in event_log._event_log
        )

        pprint(dt_strings)


@pytest.mark.asyncio
async def test_time_event_publisher_periodic():
    """
    Test TimeEventPublisher.periodic_trigger. This can take up to 12 seconds in the
    worst case: 6 seconds to get to the top of a 6 second cycle, and then 6 seconds
    worth of events.
    """
    async with meadowflow.event_log.EventLog() as event_log, TimeEventPublisher(
        event_log.append_event,
        # we're testing 6 seconds worth of time, so we set the schedule_recurring_limit
        # even shorter than that to test that "rolling over" to the next period works
        # correctly
        datetime.timedelta(seconds=4),
        datetime.timedelta(seconds=2),
    ) as p:

        # get us to just after the "top of a 6 second cycle", as that means both the 2s
        # and 3s periodic triggers will be "at the top of their cycles"
        await asyncio.sleep(6 - time.time() % 6 + _TIME_DELAY)
        t0 = time.time()

        p.create_periodic(Periodic(datetime.timedelta(seconds=1)))
        p.create_periodic(Periodic(datetime.timedelta(seconds=2)))
        p.create_periodic(Periodic(datetime.timedelta(seconds=3)))

        assert 0 == len(event_log._event_log)
        # these are effectively sleep(1), but this reduces the likelihood that we go out
        # of sync
        await asyncio.sleep(max(t0 + 1 - time.time(), 0))
        assert 1 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 2 - time.time(), 0))
        assert 1 + 2 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 3 - time.time(), 0))
        assert 1 + 2 + 2 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 4 - time.time(), 0))
        assert 1 + 2 + 2 + 2 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 5 - time.time(), 0))
        assert 1 + 2 + 2 + 2 + 1 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 6 - time.time(), 0))
        assert 1 + 2 + 2 + 2 + 1 + 3 == len(event_log._event_log)
        await asyncio.sleep(max(t0 + 7 - time.time(), 0))


@pytest.mark.asyncio
async def test_time_event_publisher_time_of_day():
    """Test TimeEventPublisher.time_of_day_trigger"""
    await _test_time_event_publisher_time_of_day()


@pytest.mark.asyncio
async def test_time_event_publisher_time_of_day_daylight_savings():
    """
    Test TimeEventPublisher.time_of_day_trigger in a case where we're crossing a
    daylight savings boundary.
    """

    # New Zealand daylight savings time ended on 2021-04-04 at 3am, clocks turned
    # backward 1 hour at that point
    test_dt = pytz.timezone("Pacific/Auckland").localize(
        datetime.datetime(2021, 4, 4, 14, 0, 0)
    )
    meadowflow.time_event_publisher._TEST_TIME_OFFSET = (
        test_dt.timestamp() - time.time()
    )
    try:
        await _test_time_event_publisher_time_of_day()
    finally:
        meadowflow.time_event_publisher._TEST_TIME_OFFSET = 0


async def _test_time_event_publisher_time_of_day():

    async with meadowflow.event_log.EventLog() as event_log, TimeEventPublisher(
        event_log.append_event
    ) as p:

        tz_hi = pytz.timezone("Pacific/Honolulu")
        tz_nz = pytz.timezone("Pacific/Auckland")

        now = meadowflow.time_event_publisher._utc_now()
        now_rounded = datetime.datetime(
            year=now.year,
            month=now.month,
            day=now.day,
            hour=now.hour,
            minute=now.minute,
            second=now.second,
            tzinfo=now.tzinfo,
        ) + datetime.timedelta(seconds=1)

        # this should make sure we're very close to now_rounded and possibly a little
        # bit after it
        await asyncio.sleep(
            max(
                now_rounded.timestamp() - meadowflow.time_event_publisher._time_time(),
                0,
            )
        )

        day_delta = datetime.timedelta(days=1)

        now_hi = now_rounded.astimezone(tz_hi)
        today_hi = now_hi.date()
        today_dt_hi = tz_hi.localize(
            datetime.datetime.combine(today_hi, datetime.time())
        )
        yesterday_dt_hi = tz_hi.localize(
            datetime.datetime.combine(today_hi - day_delta, datetime.time())
        )
        tomorrow_dt_hi = tz_hi.localize(
            datetime.datetime.combine(today_hi + day_delta, datetime.time())
        )

        now_nz = now_rounded.astimezone(tz_nz)
        today_nz = now_nz.date()
        today_dt_nz = tz_nz.localize(
            datetime.datetime.combine(today_nz, datetime.time())
        )
        yesterday_dt_nz = tz_nz.localize(
            datetime.datetime.combine(today_nz - day_delta, datetime.time())
        )
        tomorrow_dt_nz = tz_nz.localize(
            datetime.datetime.combine(today_nz + day_delta, datetime.time())
        )

        expected_payloads: List[Tuple[str, Optional[str], str, str]] = []

        def payload_to_strs(
            payload: TimeOfDayPayload,
        ) -> Tuple[str, Optional[str], str, str]:
            return (
                _timedelta_to_str(payload.local_time_of_day),
                payload.time_zone.zone,
                _date_to_str(payload.date),
                _dt_to_str(payload.point_in_time),
            )

        def add_trigger_and_payload(
            # the current time in the local timezone
            now_local: datetime.datetime,
            # midnight of the date you want to trigger for in the local timezone
            date_dt_local: datetime.datetime,
            # any jitter you want to add
            time_increment: datetime.timedelta,
            # the local timezone
            time_zone: pytz.BaseTzInfo,
        ):
            time_of_day = now_local - date_dt_local + time_increment
            p.create_time_of_day(TimeOfDay(time_of_day, time_zone))
            expected_payloads.append(
                (
                    _timedelta_to_str(time_of_day),
                    time_zone.zone,
                    _date_to_str(date_dt_local.date()),
                    _dt_to_str(time_zone.normalize(date_dt_local + time_of_day)),
                )
            )

        # not called
        p.create_time_of_day(
            TimeOfDay(now_hi - today_dt_hi - 3 * _TIME_INCREMENT, tz_hi)
        )
        p.create_time_of_day(
            TimeOfDay(now_nz - today_dt_nz - 3 * _TIME_INCREMENT, tz_nz)
        )

        add_trigger_and_payload(now_hi, today_dt_hi, _TIME_INCREMENT, tz_hi)
        # duplicate should be ignored
        p.create_time_of_day(TimeOfDay(now_hi - today_dt_hi + _TIME_INCREMENT, tz_hi))
        add_trigger_and_payload(now_hi, yesterday_dt_hi, _TIME_INCREMENT, tz_hi)
        add_trigger_and_payload(now_nz, tomorrow_dt_nz, _TIME_INCREMENT, tz_nz)

        add_trigger_and_payload(now_hi, tomorrow_dt_hi, 2 * _TIME_INCREMENT, tz_hi)
        add_trigger_and_payload(now_nz, today_dt_nz, 2 * _TIME_INCREMENT, tz_nz)
        add_trigger_and_payload(now_nz, yesterday_dt_nz, 2 * _TIME_INCREMENT, tz_nz)

        assert 0 == len(event_log._event_log)

        await asyncio.sleep(_TIME_INCREMENT.total_seconds() + _TIME_DELAY)

        assert 3 == len(event_log._event_log)
        assert set(expected_payloads[:3]) == set(
            payload_to_strs(e.payload) for e in event_log._event_log
        )

        await asyncio.sleep(_TIME_INCREMENT.total_seconds())
        assert 6 == len(event_log._event_log)
        assert set(expected_payloads) == set(
            payload_to_strs(e.payload) for e in event_log._event_log
        )

        pprint(expected_payloads)
