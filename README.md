# Django CronTask

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/codingjoe/django-crontask/raw/main/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/codingjoe/django-crontask/raw/main/images/logo-light.svg">
    <img alt="Django CronTask: Cron style scheduler for Django's task framework" src="https://github.com/codingjoe/django-crontask/raw/main/images/logo-light.svg">
  </picture>
</p>

**Cron style scheduler for asynchronous tasks in Django.**

- setup recurring tasks via crontab syntax
- lightweight helpers build on top of [APScheduler]
- [Sentry] cron monitor support

[![PyPi Version](https://img.shields.io/pypi/v/django-crontask.svg)](https://pypi.python.org/pypi/django-crontask/)
[![Test Coverage](https://codecov.io/gh/codingjoe/django-crontask/branch/main/graph/badge.svg)](https://codecov.io/gh/codingjoe/django-crontask)
[![GitHub License](https://img.shields.io/github/license/codingjoe/django-crontask)](https://raw.githubusercontent.com/codingjoe/django-crontask/master/LICENSE)

## Setup

You need to have [Django's Task framework][django-tasks] setup properly.

```ShellSession
python3 -m pip install django-crontask
# or
python3 -m pip install django-crontask[sentry]  # with sentry cron monitor support
```

Add `crontask` to your `INSTALLED_APPS` in `settings.py`:

```python
# settings.py
INSTALLED_APPS = [
    "crontask",
    # ...
]
```

Finally, you launch the scheduler in a separate process:

```ShellSession
python3 manage.py crontask
```

### Setup Redis as a lock backend (optional)

If you use Redis as a broker, you can use Redis as a lock backend as well.
The lock backend is used to prevent multiple instances of the scheduler
from running at the same time. This is important if you have multiple
instances of your application running.

```python
# settings.py
CRONTASK = {
    "REDIS_URL": "redis://localhost:6379/0",
}
```

## Usage

```python
# tasks.py
import logging

from django.tasks import task
from crontask import cron

logger = logging.getLogger(__name__)


@cron("*/5 * * * *")  # every 5 minutes
@task
def my_task():
    logger.info("Hello World")
```

### Advanced Trigger Usage

You can also use other trigger types from [APScheduler].
Just import the trigger and use it in the `cron` decorator.

```python
import logging

from django.tasks import task
from django.utils import timezone
from apscheduler.triggers.interval import IntervalTrigger
from crontask import cron

logger = logging.getLogger(__name__)

every_ten_minutes = IntervalTrigger(
    minutes=10, timezone=timezone.get_default_timezone()
)


@cron(every_ten_minutes)
@task
def my_interval_task():
    logger.info("Hello from interval task")
```

### Sentry Cron Monitors

If you use [Sentry] you can add cron monitors to your tasks.
The monitor's slug will be the actor's name. Like `my_task` in the example above.

### The crontask command

```ShellSession
$ python3 manage.py crontask --help
usage: manage.py crontask [-h] [--no-task-loading] [--no-heartbeat] [--version] [-v {0,1,2,3}]
                         [--settings SETTINGS] [--pythonpath PYTHONPATH] [--traceback] [--no-color]
                         [--force-color] [--skip-checks]

Run task scheduler for all tasks with the `cron` decorator.

options:
  -h, --help            show this help message and exit
  --no-task-loading     Don't load tasks from installed apps.
  --no-heartbeat        Don't start the heartbeat actor.
```

[apscheduler]: https://apscheduler.readthedocs.io/en/stable/
[django-tasks]: https://docs.djangoproject.com/en/6.0/topics/tasks/
[sentry]: https://docs.sentry.io/product/crons/
