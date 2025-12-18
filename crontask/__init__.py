"""Cron style scheduler for Django's task framework."""

from unittest.mock import Mock

from apscheduler.schedulers.base import STATE_STOPPED
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from django.utils import timezone

from . import _version

try:
    from sentry_sdk.crons import monitor
except ImportError:
    monitor = None

__version__ = _version.version
VERSION = _version.version_tuple

__all__ = ["cron", "interval", "scheduler"]


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


def cron(
    schedule=None,
    *,
    year=None,
    month=None,
    day=None,
    week=None,
    day_of_week=None,
    hour=None,
    minute=None,
    second=None,
):
    """
    Run task on a scheduler with a cron schedule.

    Usage:
        @cron("0 0 * * *")
        @task
        def cron_test():
            print("Cron test")

        or pass values individually

        @cron(hour=0, minute=0)
        @task
        def midnight_cron_test():
            print("Midnight test")

    Please don't forget to set up a sentry monitor for the actor, otherwise you won't
    get any notifications if the cron job fails.

    The monitor slug is your actor name, the schedule should be set to the same
    cron schedule as the cron decorator. The schedule type should be set to cron.
    The monitors timezone should be set to Europe/Berlin.
    """

    def decorator(task):
        *_, day_schedule = schedule.split(" ")

        # CronTrigger uses Python's timezone dependent first weekday,
        # so in Berlin monday is 0 and sunday is 6. We use literals to avoid
        # confusion. Literals are also more readable and crontab conform.
        if any(i.isdigit() for i in day_schedule):
            raise ValueError(
                "Please use a literal day of week (Mon, Tue, Wed, Thu, Fri, Sat, Sun) or *"
            )

        if schedule:
            trigger = CronTrigger.from_crontab(
                schedule,
                timezone=timezone.get_default_timezone(),
            )
        else:
            trigger = CronTrigger(
                year=year,
                month=month,
                day=day,
                week=week,
                day_of_week=day_of_week,
                hour=hour,
                minute=minute,
                second=second,
                timezone=timezone.get_default_timezone(),
            )

        if monitor is not None:
            task = type(task)(
                priority=task.priority,
                func=monitor(task.name)(task.func),
                queue_name=task.queue_name,
                backend=task.backend,
                takes_context=task.takes_context,
                run_after=task.run_after,
            )

        scheduler.add_job(
            task.enqueue,
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

    Usage:
        @interval(seconds=30)
        @task
        def interval_test():
            print("Interval test")

    Please note that the interval is relative to the time the scheduler is started. For
    example, if you start the scheduler at 12:00:00, the first run will be at 12:00:30.
    However, if you restart the scheduler at 12:00:15, the first run will be at
    12:00:45.

    For an interval that is consistent with the clock, use the `cron` decorator instead.
    """

    def decorator(task):
        if monitor is not None:
            task = type(task)(
                priority=task.priority,
                func=monitor(task.name)(task.func),
                queue_name=task.queue_name,
                backend=task.backend,
                takes_context=task.takes_context,
                run_after=task.run_after,
            )

        scheduler.add_job(
            task.enqueue,
            IntervalTrigger(
                seconds=seconds,
                timezone=timezone.get_default_timezone(),
            ),
            name=task.name,
        )
        return task

    return decorator
