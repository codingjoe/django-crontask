import typing

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from django.tasks import Task
from django.utils import timezone


def trigger_to_monitor_config(trigger: BaseTrigger) -> dict[str, typing.Any] | None:
    """Convert an APScheduler trigger to a Sentry monitor configuration if possible."""
    tz = str(timezone.get_default_timezone())
    match trigger:
        case CronTrigger():
            fields = {f.name: str(f) for f in trigger.fields}
            return {
                "schedule": {
                    "type": "crontab",
                    "value": "{minute} {hour} {day} {month} {day_of_week}".format(
                        **fields
                    ),
                },
                "timezone": tz,
            }
        case IntervalTrigger():
            seconds = trigger.interval.total_seconds()
            for unit, size in (
                ("year", 31536000),
                ("month", 2592000),
                ("week", 604800),
                ("day", 86400),
                ("hour", 3600),
                ("minute", 60),
            ):
                if seconds % size == 0:
                    return {
                        "schedule": {
                            "type": "interval",
                            "value": seconds // size,
                            "unit": unit,
                        },
                        "timezone": tz,
                    }
            return None  # Less than a minute
        case CalendarIntervalTrigger():
            fields = {
                "year": trigger.years,
                "month": trigger.months,
                "week": trigger.weeks,
                "day": trigger.days,
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


def monitor_cron_task(task: Task, trigger: BaseTrigger) -> Task:
    """
    Wrap the task function in a Sentry monitor for a suitable trigger.

    You don't need to create the monitor in advance since
    the Sentry monitor configuration is upserted on each task execution.
    If the trigger is not supported, the task is returned unchanged.
    """
    try:
        from sentry_sdk import monitor
    except ImportError:
        return task
    else:
        if monitor_config := trigger_to_monitor_config(trigger):
            return type(task)(
                priority=task.priority,
                func=monitor(task.name, monitor_config=monitor_config)(task.func),
                queue_name=task.queue_name,
                backend=task.backend,
                takes_context=task.takes_context,
                run_after=task.run_after,
            )

    return task
