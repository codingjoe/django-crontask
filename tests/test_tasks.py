import datetime
import zoneinfo

import pytest
from apscheduler.triggers.interval import IntervalTrigger
from crontask import cron, interval, scheduler, tasks
from django.utils import timezone

DEFAULT_TZINFO = zoneinfo.ZoneInfo(key="Europe/Berlin")


def test_heartbeat(caplog):
    with caplog.at_level("INFO"):
        tasks.heartbeat.func()
    assert "ﮩ٨ـﮩﮩ٨ـ♡ﮩ٨ـﮩﮩ٨ـ" in caplog.text


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
