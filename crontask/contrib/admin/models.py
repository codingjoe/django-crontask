from django.db import models
from django.utils.translation import gettext_lazy as _


class CronTask(models.Model):  # noqa: DJ008
    """
    Inspect registered cron tasks and enqueue them on demand.

    The list shows each task's schedule and next enqueue time.
    Use the Enqueue now button to enqueue a task immediately.
    """

    class Meta:
        default_permissions = ("view",)
        managed = False
        permissions = (("enqueue_crontask", _("Can enqueue cron task")),)
        verbose_name = "cron task"
        verbose_name_plural = _("task schedules")
