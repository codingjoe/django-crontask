"""Cron style scheduler for Django's task framework."""

import typing
import warnings
from unittest.mock import Mock

from apscheduler.schedulers.base import STATE_STOPPED
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from django.tasks import Task
from django.utils import timezone

from . import _version

__version__ = _version.version
VERSION = _version.version_tuple

__all__ = ["cron", "interval", "scheduler"]


def _monitor_config(schedule: str | BaseTrigger) -> dict | None:
    tz = str(timezone.get_default_timezone())
    match schedule:
        case str():
            return {"schedule": {"type": "crontab", "value": schedule}, "timezone": tz}
        case IntervalTrigger():
            delta = schedule.interval
            if delta.seconds == 0 and delta.days > 0:
                return {
                    "schedule": {
                        "type": "interval",
                        "value": delta.days,
                        "unit": "day",
                    },
                    "timezone": tz,
                }
            if delta.days == 0:
                for unit, size in (("hour", 3600), ("minute", 60)):
                    if delta.seconds >= size and delta.seconds % size == 0:
                        return {
                            "schedule": {
                                "type": "interval",
                                "value": delta.seconds // size,
                                "unit": unit,
                            },
                            "timezone": tz,
                        }
            return None
        case CalendarIntervalTrigger():
            fields = {
                "year": schedule.years,
                "month": schedule.months,
                "week": schedule.weeks,
                "day": schedule.days,
            }
            set_fields = {k: v for k, v in fields.items() if v}
            if len(set_fields) == 1:
                ((unit, value),) = set_fields.items()
                return {
                    "schedule": {"type": "interval", "value": value, "unit": unit},
                    "timezone": tz,
                }
            return None
        case _:
            return None


class LazyBlockingScheduler(BlockingScheduler):
    """Avoid annoying info logs for pending jobs."""

    def add_job(self, *args, **kwargs):
        logger = self._logger
        if self.state == STATE_STOPPED:
            # We don't want to schedule jobs before the scheduler is started.
            self._logger = Mock()
        super().add_job(*args, **kwargs)
        self._logger = logger


scheduler = LazyBlockingScheduler()


def cron(schedule: str | BaseTrigger) -> typing.Callable[[Task], Task]:
    """
    Run task on a scheduler with a cron schedule.

    Usage:
        @cron("0 0 * * *")
        @task
        def cron_test():
            print("Cron test")

    Sentry cron monitor are automatically
    upserted on the every check-in using the task name as the monitor slug.
    """

    def decorator(task: Task) -> Task:
        trigger = schedule
        if isinstance(schedule, str):
            *_, day_schedule = schedule.split(" ")
            # CronTrigger uses Python's timezone dependent first weekday,
            # so in Berlin monday is 0 and sunday is 6. We use literals to avoid
            # confusion. Literals are also more readable and crontab conform.
            if any(i.isdigit() for i in day_schedule):
                raise ValueError(
                    "Please use a literal day of week (Mon, Tue, Wed, Thu, Fri, Sat, Sun) or *"
                )
            trigger = CronTrigger.from_crontab(
                schedule,
                timezone=timezone.get_default_timezone(),
            )

        try:
            from sentry_sdk.crons import monitor
        except ImportError:
            fn = task.func
        else:
            fn = monitor(task.name, monitor_config=_monitor_config(schedule))(task.func)

        task = type(task)(
            priority=task.priority,
            func=fn,
            queue_name=task.queue_name,
            backend=task.backend,
            takes_context=task.takes_context,
            run_after=task.run_after,
        )

        scheduler.add_job(
            func=task.enqueue,
            trigger=trigger,
            name=task.name,
        )
        # We don't add the Sentry monitor on the actor itself, because we only want to
        # monitor the cron job, not the actor itself, or it's direct invocations.
        return task

    return decorator


def interval(*, seconds):
    """
    Run task on a periodic interval.

    The interval decorator is deprecated and will be removed in a future release.
    Please use the cron decorator with an 'IntervalTrigger' instead.
    """
    warnings.warn(
        "The interval decorator is deprecated and will be removed in a future release. "
        "Please use the cron decorator with an 'IntervalTrigger' instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    return cron(
        IntervalTrigger(
            seconds=seconds,
            timezone=timezone.get_default_timezone(),
        )
    )
