import logging

from django.tasks import task

from . import cron

logger = logging.getLogger(__name__)


@cron("* * * * *")
@task
def heartbeat():
    """Log a heartbeat every minute."""
    logger.info("ﮩ٨ـﮩﮩ٨ـ♡ﮩ٨ـﮩﮩ٨ـ")
