import inspect
import logging
import sys
from collections.abc import Iterable
from dataclasses import dataclass

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from django.contrib.humanize.templatetags.humanize import naturaltime
from django.tasks import Task
from django.utils import timezone
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widgets import DataTable, Footer, Header, RichLog, Static


class LogTextMessage(Message):
    def __init__(self, text: str) -> None:
        self.text = text
        super().__init__()


class RichLogHandler(logging.Handler):
    def __init__(self, app: InspectorApp) -> None:
        super().__init__(logging.INFO)
        self.app = app
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    def emit(self, record: logging.LogRecord) -> None:
        if "crontask.utils.lock.extend" in record.getMessage():
            return
        self.app.post_message(LogTextMessage(self.format(record)))


@dataclass(frozen=True)
class CronTaskOverview:
    task: Task
    job: Job

    @property
    def next_enqueue_label(self) -> str:
        if self.job.next_run_time is None:
            return "-"
        return naturaltime(timezone.localtime(self.job.next_run_time))

    @property
    def next_enqueue_iso(self) -> str:
        if self.job.next_run_time is None:
            return "-"
        return timezone.localtime(self.job.next_run_time).isoformat()

    @classmethod
    def from_jobs(cls, jobs: Iterable[Job]) -> list[CronTaskOverview]:
        return sorted(
            (
                cls(task=task, job=job)
                for job in jobs
                if isinstance(task := getattr(job.func, "__self__", None), Task)
            ),
            key=lambda overview: overview.task.module_path,
        )


class InspectorApp(App):
    TITLE = "crontask"
    SUB_TITLE = "cron task inspector"
    CSS_PATH = "inspector.tcss"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("e", "enqueue", "Enqueue now"),
    ]

    def __init__(self, scheduler: BaseScheduler) -> None:
        super().__init__()
        self.scheduler = scheduler
        self.overviews: dict[str, CronTaskOverview] = {}
        self.enqueuing: set[str] = set()
        self._rich_log_handler = RichLogHandler(self)
        self._handlers: list[tuple[logging.Logger, logging.StreamHandler]] = []
        self._logger_levels: dict[str, int] = {}
        self._detail_text = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="split"):
            with Vertical(id="tasks-pane"):
                yield DataTable(id="tasks", cursor_type="row")
                yield Static(id="detail", markup=False)
            with Vertical(id="logs-pane"):
                yield RichLog(id="logs", wrap=True, max_lines=1000, min_width=1)
        yield Footer()

    def on_mount(self) -> None:
        self.theme = "monokai"
        streams = (sys.stdout, sys.stderr, sys.__stdout__, sys.__stderr__)
        for logger in (
            logging.getLogger(),
            *logging.root.manager.loggerDict.values(),
        ):
            if not isinstance(logger, logging.Logger):
                continue
            for handler in list(logger.handlers):
                if (
                    isinstance(handler, logging.StreamHandler)
                    and handler.stream in streams
                ):
                    logger.removeHandler(handler)
                    self._handlers.append((logger, handler))
        for name in ("apscheduler", "crontask"):
            log = logging.getLogger(name)
            self._logger_levels[name] = log.level
            log.setLevel(logging.INFO)
        logging.getLogger().addHandler(self._rich_log_handler)
        table = self.query_one(DataTable)
        table.add_columns(
            ("Task", "task"), ("Schedule", "schedule"), ("Next enqueue", "next")
        )
        logs = self.query_one("#logs", RichLog)
        logs.border_title = "Logs"
        logs.write("Starting scheduler…")
        self.overviews = {
            overview.job.id: overview
            for overview in CronTaskOverview.from_jobs(self.scheduler.get_jobs())
        }
        for overview in self.overviews.values():
            table.add_row(
                overview.task.module_path,
                str(overview.job.trigger),
                overview.next_enqueue_label,
                key=overview.job.id,
            )
        self.update_detail()
        self.set_interval(1, self.refresh_next_enqueue)

    def on_unmount(self) -> None:
        logging.getLogger().removeHandler(self._rich_log_handler)
        for logger, handler in self._handlers:
            logger.addHandler(handler)
        for name, level in self._logger_levels.items():
            logging.getLogger(name).setLevel(level)

    @on(LogTextMessage)
    def on_log_text(self, event: LogTextMessage) -> None:
        log = self.query_one("#logs", RichLog)
        log.write(
            event.text,
            width=max(1, log.scrollable_content_region.width - 1),
        )

    @property
    def selected(self) -> CronTaskOverview | None:
        table = self.query_one(DataTable)
        if not self.overviews or not table.row_count:
            return None
        return self.overviews.get(table.ordered_rows[table.cursor_row].key.value)

    @on(DataTable.RowHighlighted)
    def on_row_highlighted(self) -> None:
        self.update_detail()

    def refresh_next_enqueue(self) -> None:
        table = self.query_one(DataTable)
        for job_id, overview in self.overviews.items():
            if table.get_cell(job_id, "next") != overview.next_enqueue_label:
                table.update_cell(
                    job_id,
                    "next",
                    overview.next_enqueue_label,
                    update_width=True,
                )
        self.update_detail()

    def update_detail(self) -> None:
        if overview := self.selected:
            text = (
                f"Task: {overview.task.module_path}\n"
                f"Schedule: {overview.job.trigger}\n"
                f"Next enqueue: {overview.next_enqueue_iso}\n\n"
                f"{inspect.getdoc(overview.task.func) or ''}"
            )
        else:
            text = "No cron tasks registered."
        if self._detail_text != text:
            self._detail_text = text
            self.query_one("#detail", Static).update(text)

    def action_enqueue(self) -> None:
        if (overview := self.selected) is not None and (
            task_id := overview.task.module_path
        ) not in self.enqueuing:
            self.enqueuing.add(task_id)
            self.run_worker(lambda: self.enqueue_task(overview), thread=True)

    def enqueue_task(self, overview: CronTaskOverview) -> None:
        task_id = overview.task.module_path
        try:
            overview.task.enqueue()
        finally:
            self.enqueuing.discard(task_id)
        self.notify(f"Enqueued {task_id}.")
