import logging

from crontask import cron
from django.tasks import task

logger = logging.getLogger(__name__)


@cron("*/5 * * * *")
@task
def my_task():
    """
    Log a friendly greeting every five minutes.

    This example task writes ``Hello World!`` to the ``tests.testapp.tasks``
    logger. Its schedule is listed in the :model:`crontask_admin.CronTask`
    admin page. See :view:`admin:index` for the admin dashboard.
    """
    logger.info("Hello World!")
