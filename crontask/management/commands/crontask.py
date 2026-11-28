import importlib
import signal
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from django.apps import apps
from django.core.management import BaseCommand
from django.utils.module_loading import module_has_submodule

from ... import conf, utils

try:
    from sentry_sdk import capture_exception
except ImportError:
    capture_exception = lambda e: None  # noqa: E731

from ... import scheduler


def kill_softly(signum, frame):
    """Raise a KeyboardInterrupt to stop the scheduler and release the lock."""
    signame = signal.Signals(signum).name
    msg = f"Received {signame} ({signum}), shutting down…"
    raise KeyboardInterrupt(msg)


class Command(BaseCommand):
    """Run task scheduler for all tasks with the `cron` decorator."""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-task-loading",
            action="store_true",
            help="Don't load tasks from installed apps.",
        )
        parser.add_argument(
            "--no-heartbeat",
            action="store_true",
            help="Don't start the heartbeat actor.",
        )

    def handle(self, *args, **options):
        if not options["no_task_loading"]:
            self.load_tasks(options)
        if not options["no_heartbeat"]:
            importlib.import_module("crontask.tasks")
            self.stdout.write("Scheduling heartbeat.")
        try:
            if not isinstance(utils.lock, utils.FakeLock):
                self.stdout.write("Acquiring lock…")
            # Lock scheduler to prevent multiple instances from running.
            with utils.lock as lock:
                if sys.stdin.isatty() and sys.stdout.isatty():
                    self.launch_inspector(lock, scheduler)
                else:
                    self.launch_scheduler(lock, scheduler)
        except utils.LockNotOwnedError as e:
            capture_exception(e)
            self.stderr.write(
                "The lock is no longer owned by the scheduler. Shutting down."
            )
        except utils.LockError as e:
            capture_exception(e)
            self.stderr.write("Another scheduler is already running.")

    def add_lock_refresh_job(self, lock, scheduler):
        scheduler.add_job(
            utils.extend_lock,
            IntervalTrigger(seconds=conf.get_settings().LOCK_REFRESH_INTERVAL),
            args=(lock, scheduler),
            name="crontask.utils.lock.extend",
        )

    def launch_inspector(self, lock, scheduler):
        try:
            from ...inspector.app import InspectorApp
        except ModuleNotFoundError as e:
            if e.name != "textual":
                raise
            self.stderr.write(
                self.style.WARNING(
                    "Textual is not installed. "
                    "Install it with `uv add --group dev django-crontask[tui]`."
                )
            )
            return self.launch_scheduler(lock, scheduler)
        background = BackgroundScheduler()
        for job in scheduler.get_jobs():
            background.add_job(
                job.func,
                job.trigger,
                args=job.args,
                kwargs=job.kwargs,
                id=job.id,
                name=job.name,
            )
        self.add_lock_refresh_job(lock, background)
        background.start()
        try:
            InspectorApp(scheduler=background).run()
        finally:
            if background.running:
                background.shutdown()

    def launch_scheduler(self, lock, scheduler):
        match sys.platform:
            case "win32":
                signal.signal(signal.SIGBREAK, kill_softly)
            case _:
                signal.signal(signal.SIGHUP, kill_softly)
        signal.signal(signal.SIGTERM, kill_softly)
        signal.signal(signal.SIGINT, kill_softly)
        self.stdout.write(self.style.SUCCESS("Starting scheduler…"))
        self.add_lock_refresh_job(lock, scheduler)
        try:
            scheduler.start()
        except KeyboardInterrupt as e:
            self.stdout.write(self.style.WARNING(str(e)))
            self.stdout.write(self.style.NOTICE("Shutting down scheduler…"))
            scheduler.shutdown()

    def load_tasks(self, options):
        """
        Load all tasks modules within installed apps.

        If they are not imported, they will not have registered
        their tasks with the scheduler.
        """
        for app in apps.get_app_configs():
            if app.name == "crontask":
                # Our heartbeat is loaded earlier
                continue
            if app.ready:
                try:
                    importlib.import_module(f"{app.name}.tasks")
                    self.stdout.write(
                        f"Loaded tasks from {self.style.NOTICE(app.name)}."
                    )
                except ImportError:
                    if module_has_submodule(app.module, "tasks"):
                        raise
