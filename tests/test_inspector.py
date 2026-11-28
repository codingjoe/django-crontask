import asyncio
import io
import logging
import sys
from datetime import timedelta

import pytest
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from crontask.inspector.app import CronTaskOverview, InspectorApp
from django.tasks import task
from django.utils import timezone
from textual.notifications import Notify
from textual.widgets import DataTable, RichLog, Static

from tests.testapp.tasks import my_task

BACKEND_ERROR = "backend down"
CALLS = []


@task
def recording_task():
    """Append to the call log."""
    CALLS.append(1)


class NotificationCollector:
    def __init__(self):
        self.messages = []

    def __call__(self, message):
        if isinstance(message, Notify):
            self.messages.append(message.notification.message)


class InspectorSession:
    def __init__(self, scheduler, message_hook=None):
        self.runner = asyncio.Runner()
        self.app = InspectorApp(scheduler=scheduler)
        self._message_hook = message_hook

    def __enter__(self):
        self._context = self.app.run_test(
            size=(120, 40),
            notifications=True,
            message_hook=self._message_hook,
        )
        self.pilot = self.runner.run(self._context.__aenter__())
        self.pause()
        return self

    def __exit__(self, *exc_info):
        try:
            self.runner.run(self._context.__aexit__(*exc_info))
        finally:
            self.runner.close()

    def pause(self, delay=None):
        self.runner.run(self.pilot.pause(delay))

    def press(self, key):
        self.runner.run(self.pilot.press(key))
        self.pause()

    def wait(self):
        self.runner.run(self.app.workers.wait_for_complete())
        self.pause()


class FailingTask:
    module_path = "tests.test_inspector.failing_task"

    def enqueue(self):
        raise RuntimeError(BACKEND_ERROR)


@pytest.fixture
def scheduler():
    return BackgroundScheduler()


def add_task_job(scheduler, task_instance, job_id, *, next_run_time=None):
    job = scheduler.add_job(
        task_instance.enqueue,
        IntervalTrigger(minutes=5),
        id=job_id,
        name=task_instance.name,
    )
    job.next_run_time = next_run_time
    return job


def test_overview_next_enqueue(scheduler):
    job = add_task_job(scheduler, recording_task, "recording")
    (overview,) = CronTaskOverview.from_jobs(scheduler.get_jobs())
    assert overview.next_enqueue_label == "-"
    assert overview.next_enqueue_iso == "-"
    job.next_run_time = timezone.now() + timedelta(minutes=5)
    assert "minutes from now" in overview.next_enqueue_label
    assert (
        overview.next_enqueue_iso == timezone.localtime(job.next_run_time).isoformat()
    )


def test_from_jobs(scheduler):
    add_task_job(scheduler, my_task, "my-task")
    add_task_job(scheduler, recording_task, "recording")
    scheduler.add_job(lambda: None, IntervalTrigger(minutes=5), id="function")
    scheduler.add_job(io.StringIO().getvalue, IntervalTrigger(minutes=5), id="method")
    overviews = CronTaskOverview.from_jobs(scheduler.get_jobs())
    assert [overview.job.id for overview in overviews] == ["recording", "my-task"]


def test_inspector_lists_tasks(scheduler):
    recording_job = add_task_job(
        scheduler,
        recording_task,
        "recording",
        next_run_time=timezone.now() + timedelta(minutes=5),
    )
    add_task_job(
        scheduler,
        my_task,
        "my-task",
        next_run_time=timezone.now() + timedelta(minutes=5),
    )
    with InspectorSession(scheduler) as session:
        app = session.app
        table = app.query_one("#tasks", DataTable)
        detail = app.query_one("#detail", Static)
        assert table.row_count == 2
        assert [row.key.value for row in table.ordered_rows] == [
            "recording",
            "my-task",
        ]
        assert table.get_cell("recording", "task") == recording_task.module_path
        assert table.get_cell("recording", "schedule") == str(recording_job.trigger)
        assert app.selected.task is recording_task
        assert f"Task: {recording_task.module_path}" in detail.content
        assert "Append to the call log." in detail.content
        assert (
            timezone.localtime(recording_job.next_run_time).isoformat()
            in detail.content
        )
        content = detail.content
        app.update_detail()
        assert detail.content == content
        session.press("down")
        assert app.selected.task is my_task
        assert f"Task: {my_task.module_path}" in detail.content
        assert "Append to the call log." not in detail.content
        previous_cell = table.get_cell("recording", "next")
        recording_job.next_run_time = timezone.now() + timedelta(days=2)
        session.pause(1.1)
        assert table.get_cell("recording", "next") != previous_cell
        assert (
            table.get_cell("recording", "next")
            == app.overviews["recording"].next_enqueue_label
        )
        app.refresh_next_enqueue()
        assert (
            table.get_cell("recording", "next")
            == app.overviews["recording"].next_enqueue_label
        )


def test_log_pane(scheduler, monkeypatch):
    crontask_logger = logging.getLogger("crontask")
    apscheduler_logger = logging.getLogger("apscheduler")
    monkeypatch.setattr(crontask_logger, "level", logging.WARNING)
    monkeypatch.setattr(apscheduler_logger, "level", logging.ERROR)
    stream_handler = logging.StreamHandler(sys.__stderr__)
    other_handler = logging.StreamHandler(io.StringIO())
    null_handler = logging.NullHandler()
    handlers = (stream_handler, other_handler, null_handler)
    for handler in handlers:
        crontask_logger.addHandler(handler)
    try:
        with InspectorSession(scheduler) as session:
            app = session.app
            assert crontask_logger.level == logging.INFO
            assert apscheduler_logger.level == logging.INFO
            assert stream_handler not in crontask_logger.handlers
            assert other_handler in crontask_logger.handlers
            assert null_handler in crontask_logger.handlers
            log = app.query_one("#logs", RichLog)
            assert log.border_title == "Logs"
            logging.getLogger("crontask.test").info("visible log line")
            logging.getLogger("crontask.test").info("crontask.utils.lock.extend")
            session.pause()
            text = "\n".join(line.text for line in log.lines)
            assert "Starting scheduler…" in text
            assert "visible log line" in text
            assert "crontask.utils.lock.extend" not in text
        assert crontask_logger.level == logging.WARNING
        assert apscheduler_logger.level == logging.ERROR
        assert stream_handler in crontask_logger.handlers
        assert other_handler in crontask_logger.handlers
        assert null_handler in crontask_logger.handlers
    finally:
        for handler in handlers:
            crontask_logger.removeHandler(handler)


def test_enqueue(scheduler):
    CALLS.clear()
    add_task_job(
        scheduler,
        recording_task,
        "recording",
        next_run_time=timezone.now() + timedelta(minutes=5),
    )
    collector = NotificationCollector()
    with InspectorSession(scheduler, message_hook=collector) as session:
        app = session.app
        session.press("e")
        session.wait()
        assert CALLS == [1]
        assert app.enqueuing == set()
        assert collector.messages == [f"Enqueued {recording_task.module_path}."]
        app.enqueuing.add(recording_task.module_path)
        session.press("e")
        session.wait()
        assert CALLS == [1]
        assert collector.messages == [f"Enqueued {recording_task.module_path}."]


def test_no_tasks(scheduler):
    collector = NotificationCollector()
    with InspectorSession(scheduler, message_hook=collector) as session:
        app = session.app
        assert app.selected is None
        detail = app.query_one("#detail", Static)
        assert detail.content == "No cron tasks registered."
        session.press("e")
        session.wait()
        assert app.enqueuing == set()
        assert collector.messages == []


def test_enqueue_task_failure(scheduler):
    job = add_task_job(scheduler, recording_task, "recording")
    app = InspectorApp(scheduler)
    app.enqueuing.add(FailingTask.module_path)
    with pytest.raises(RuntimeError, match="backend down"):
        app.enqueue_task(CronTaskOverview(task=FailingTask(), job=job))
    assert app.enqueuing == set()
