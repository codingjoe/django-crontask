import datetime
import zoneinfo

import pytest
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from crontask import _monitor_config, cron, interval, scheduler, tasks
from django.utils import timezone

from tests.testapp.tasks import my_task

DEFAULT_TZINFO = zoneinfo.ZoneInfo(key="Europe/Berlin")


def test_heartbeat(caplog):
    with caplog.at_level("INFO"):
        tasks.heartbeat.func()
    assert "ﮩ٨ـﮩﮩ٨ـ♡ﮩ٨ـﮩﮩ٨ـ" in caplog.text


def test_my_task(caplog):
    """Regression test for logger usage in task decorated functions."""
    with caplog.at_level("INFO"):
        my_task.func()
    assert "Hello World!" in caplog.text


def test_cron__stars():
    assert not scheduler.remove_all_jobs()
    assert cron("* * * * *")(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 1, 0, 1, tzinfo=DEFAULT_TZINFO)


def test_cron__day_of_week():
    assert not scheduler.remove_all_jobs()
    assert cron("* * * * Mon")(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)  # Friday
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 4, 0, 0, tzinfo=DEFAULT_TZINFO)


@pytest.mark.parametrize(
    "schedule",
    [
        "0 0 * * Tue-Wed",
        "0 0 * * Tue,Wed",
    ],
)
def test_cron_day_range(schedule):
    assert not scheduler.remove_all_jobs()
    assert cron(schedule)(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)  # Friday
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 5, 0, 0, tzinfo=DEFAULT_TZINFO)
    init = datetime.datetime(2021, 1, 5, 0, 0, 0, tzinfo=DEFAULT_TZINFO)  # Tuesday
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 6, 0, 0, tzinfo=DEFAULT_TZINFO)


def test_cron__every_15_minutes():
    assert not scheduler.remove_all_jobs()
    assert cron("*/15 * * * *")(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 1, 0, 15, tzinfo=DEFAULT_TZINFO)


def test_cron__trigger_attribute():
    assert not scheduler.remove_all_jobs()
    cron("*/10 * * * *")(tasks.heartbeat)
    scheduler.get_jobs()[0].modify(next_run_time=None)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 1, 0, 10, tzinfo=DEFAULT_TZINFO)


@pytest.mark.parametrize(
    "schedule",
    [
        "* * * * 1",
        "* * * * 2-3",
        "* * * * 1,7",
    ],
)
def test_cron__error(schedule):
    assert not scheduler.remove_all_jobs()
    with pytest.raises(ValueError) as e:
        cron(schedule)(tasks.heartbeat)
    assert (
        "Please use a literal day of week (Mon, Tue, Wed, Thu, Fri, Sat, Sun) or *"
        in str(e.value)
    )


def test_cron__custom_trigger():
    assert not scheduler.remove_all_jobs()
    every_30_secs = IntervalTrigger(
        seconds=30, timezone=timezone.get_default_timezone()
    )
    assert cron(every_30_secs)(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 1, 0, 0, 30, tzinfo=DEFAULT_TZINFO)


def test_interval__seconds():
    assert not scheduler.remove_all_jobs()
    with pytest.deprecated_call():
        assert interval(seconds=30)(tasks.heartbeat)
    init = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=DEFAULT_TZINFO)
    assert scheduler.get_jobs()[0].trigger.get_next_fire_time(
        init, init
    ) == datetime.datetime(2021, 1, 1, 0, 0, 30, tzinfo=DEFAULT_TZINFO)


def test_monitor_config__crontab_string():
    assert _monitor_config("0 2 * * *") == {
        "schedule": {"type": "crontab", "value": "0 2 * * *"},
        "timezone": "Europe/Berlin",
    }


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (60, {"type": "interval", "value": 1, "unit": "minute"}),
        (300, {"type": "interval", "value": 5, "unit": "minute"}),
        (3600, {"type": "interval", "value": 1, "unit": "hour"}),
        (86400, {"type": "interval", "value": 1, "unit": "day"}),
        (86400 * 3, {"type": "interval", "value": 3, "unit": "day"}),
    ],
)
def test_monitor_config__interval_trigger(seconds, expected):
    trigger = IntervalTrigger(seconds=seconds, timezone=timezone.get_default_timezone())
    assert _monitor_config(trigger)["schedule"] == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"years": 1}, {"type": "interval", "value": 1, "unit": "year"}),
        ({"months": 2}, {"type": "interval", "value": 2, "unit": "month"}),
        ({"weeks": 1}, {"type": "interval", "value": 1, "unit": "week"}),
        ({"days": 7}, {"type": "interval", "value": 7, "unit": "day"}),
    ],
)
def test_monitor_config__calendar_interval_trigger(kwargs, expected):
    trigger = CalendarIntervalTrigger(
        **kwargs, timezone=timezone.get_default_timezone()
    )
    assert _monitor_config(trigger)["schedule"] == expected


@pytest.mark.parametrize(
    "trigger",
    [
        IntervalTrigger(seconds=30, timezone=timezone.get_default_timezone()),
        CalendarIntervalTrigger(
            months=1, days=1, timezone=timezone.get_default_timezone()
        ),
        CronTrigger.from_crontab("* * * * *", timezone=timezone.get_default_timezone()),
    ],
)
def test_monitor_config__unsupported(trigger):
    assert _monitor_config(trigger) is None
