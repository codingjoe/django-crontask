import logging

from crontask import cron
from django.tasks import task

logger = logging.getLogger(__name__)


@cron("*/5 * * * *")
@task
def my_task():
    logger.info("Hello World!")
