import datetime
import sys
import types
from unittest.mock import Mock

import pytest
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from crontask.contrib.sentry import monitor_cron_task, trigger_to_monitor_config
from django.tasks import task
from django.utils import timezone


@task
def sentry_monitor_test_task():
    return None


def test_monitor_config__cron_trigger():
    assert trigger_to_monitor_config(
        CronTrigger.from_crontab("0 2 * * *", timezone=timezone.get_default_timezone())
    ) == {
        "schedule": {"type": "crontab", "value": "0 2 * * *"},
        "timezone": "Europe/Berlin",
    }


def test_monitor_config__cron_trigger__with_day_of_week():
    # Literals are allowed, first day of the week is Sunday:
    # https://discord.com/channels/621778831602221064/1496192154702315560
    trigger = CronTrigger.from_crontab(
        "0 2 * * Mon-Fri", timezone=timezone.get_default_timezone()
    )
    assert trigger_to_monitor_config(trigger) == {
        "schedule": {"type": "crontab", "value": "0 2 * * mon-fri"},
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
        (604800, {"type": "interval", "value": 1, "unit": "week"}),
        (2592000, {"type": "interval", "value": 1, "unit": "month"}),
        (31536000, {"type": "interval", "value": 1, "unit": "year"}),
        (13 * 2592000, {"type": "interval", "value": 13, "unit": "month"}),
        (12 * 604800, {"type": "interval", "value": 12, "unit": "week"}),
    ],
)
def test_monitor_config__interval_trigger(seconds, expected):
    trigger = IntervalTrigger(seconds=seconds, timezone=timezone.get_default_timezone())
    assert trigger_to_monitor_config(trigger) is not None
    assert trigger_to_monitor_config(trigger)["schedule"] == expected


@pytest.mark.parametrize(
    "seconds",
    [
        42,  # less than 60 seconds
        69,  # not a multiple of any supported unit
    ],
)
def test_monitor_config__unsupported_interval_trigger(seconds):
    trigger = IntervalTrigger(seconds=seconds, timezone=timezone.get_default_timezone())
    assert trigger_to_monitor_config(trigger) is None


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
    assert trigger_to_monitor_config(trigger)["schedule"] == expected


@pytest.mark.parametrize(
    "trigger",
    [
        IntervalTrigger(seconds=30, timezone=timezone.get_default_timezone()),
        CalendarIntervalTrigger(
            months=1, days=1, timezone=timezone.get_default_timezone()
        ),
        DateTrigger(run_date=datetime.datetime(2021, 1, 1)),
    ],
)
def test_monitor_config__unsupported(trigger):
    assert trigger_to_monitor_config(trigger) is None


def test_monitor_cron_task__disabled(monkeypatch):
    monitor = Mock(side_effect=lambda *_args, **_kwargs: lambda func: func)
    monkeypatch.setitem(
        sys.modules, "sentry_sdk", types.SimpleNamespace(monitor=monitor)
    )

    trigger = CronTrigger.from_crontab(
        "0 2 * * *", timezone=timezone.get_default_timezone()
    )
    assert (
        monitor_cron_task(
            sentry_monitor_test_task,
            trigger,
            sentry_monitor_config=False,
        )
        is sentry_monitor_test_task
    )
    monitor.assert_not_called()


def test_monitor_cron_task__custom_monitor_config(monkeypatch):
    monitor = Mock(side_effect=lambda *_args, **_kwargs: lambda func: func)
    monkeypatch.setitem(
        sys.modules, "sentry_sdk", types.SimpleNamespace(monitor=monitor)
    )

    monitor_config = {
        "schedule": {"type": "interval", "value": 1, "unit": "hour"},
        "timezone": "Europe/Berlin",
    }
    trigger = DateTrigger(run_date=datetime.datetime(2021, 1, 1))
    wrapped_task = monitor_cron_task(
        sentry_monitor_test_task,
        trigger,
        sentry_monitor_config=monitor_config,
    )
    assert wrapped_task is not sentry_monitor_test_task
    monitor.assert_called_once_with(
        sentry_monitor_test_task.name,
        monitor_config=monitor_config,
    )
