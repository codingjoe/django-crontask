"""Cron style scheduler for Django's task framework."""

import typing
import warnings
from unittest.mock import Mock

from apscheduler.schedulers.base import STATE_STOPPED
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from django.tasks import Task
from django.utils import timezone

from . import _version

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


def cron(schedule: str | BaseTrigger) -> typing.Callable[[Task], Task]:
    """
    Run task on a scheduler with a cron schedule.

    Usage:
        @cron("0 0 * * *")
        @task
        def cron_test():
            print("Cron test")


    Please don't forget to set up a sentry monitor for the actor, otherwise you won't
    get any notifications if the cron job fails.

    The monitor slug is your actor name, the schedule should be set to the same
    cron schedule as the cron decorator. The schedule type should be set to cron.
    The monitors timezone should be set to Europe/Berlin.
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
            fn = monitor(task.name)(task.func)

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
