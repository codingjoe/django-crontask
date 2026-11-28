import builtins
import io
import signal
import sys
from unittest.mock import Mock, patch

import pytest
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from crontask import utils
from crontask.management.commands import crontask
from django.core.management import call_command
from django.test.utils import isolate_apps

from tests.testapp.tasks import my_task


def test_kill_softly():
    with pytest.raises(KeyboardInterrupt) as e:
        crontask.kill_softly(15, None)
    assert "Received SIGTERM (15), shutting down…" in str(e.value)


class Testcrontask:
    @pytest.fixture
    def patch_launch(self, monkeypatch):
        monkeypatch.setattr(
            "crontask.management.commands.crontask.Command.launch_scheduler",
            lambda *args, **kwargs: None,
        )

    def test_default(self, patch_launch):
        with io.StringIO() as stdout:
            call_command("crontask", stdout=stdout)
            assert "Loaded tasks from tests.testapp." in stdout.getvalue()
            assert "Scheduling heartbeat." in stdout.getvalue()

    def test_no_task_loading(self, patch_launch):
        with io.StringIO() as stdout:
            call_command("crontask", "--no-task-loading", stdout=stdout)
            assert "Loaded tasks from tests.testapp." not in stdout.getvalue()
            assert "Scheduling heartbeat." in stdout.getvalue()

    def test_no_heartbeat(self, patch_launch):
        with io.StringIO() as stdout:
            call_command("crontask", "--no-heartbeat", stdout=stdout)
            assert "Loaded tasks from tests.testapp." in stdout.getvalue()
            assert "Scheduling heartbeat." not in stdout.getvalue()

    @pytest.mark.parametrize("tasks_path", ["tasks.py", "tasks/__init__.py"])
    @pytest.mark.parametrize(
        ("app_name", "tasks_source", "error", "message"),
        [
            (
                "app_with_dependency_error",
                "import crontask_unavailable_dependency",
                ModuleNotFoundError,
                "crontask_unavailable_dependency",
            ),
            (
                "app_with_import_error",
                "from . import undefined_task",
                ImportError,
                "undefined_task",
            ),
        ],
    )
    def test_load_tasks__import_error(
        self,
        patch_launch,
        tmp_path,
        monkeypatch,
        tasks_path,
        app_name,
        tasks_source,
        error,
        message,
    ):
        app_name += "_package" if "/" in tasks_path else "_module"
        app_path = tmp_path / app_name
        app_path.mkdir()
        (app_path / "__init__.py").touch()
        (app_path / tasks_path).parent.mkdir(exist_ok=True)
        (app_path / tasks_path).write_text(tasks_source)
        monkeypatch.syspath_prepend(tmp_path)

        with isolate_apps(app_name) as apps:
            monkeypatch.setattr(crontask, "apps", apps)
            with pytest.raises(error, match=message):
                call_command("crontask", "--no-heartbeat")

    def test_load_tasks__optional_tasks(self, patch_launch, monkeypatch):
        with isolate_apps("django.contrib.sessions") as apps:
            monkeypatch.setattr(crontask, "apps", apps)
            with io.StringIO() as stdout:
                call_command("crontask", "--no-heartbeat", stdout=stdout)
                assert "Loaded tasks" not in stdout.getvalue()

    def test_locked(self):
        """A lock was already acquired by another process."""
        pytest.importorskip("redis", reason="redis is not installed")
        with (
            utils.redis_client.lock("crontask-lock", blocking_timeout=0),
            io.StringIO() as stderr,
        ):
            call_command("crontask", stderr=stderr)
            assert "Another scheduler is already running." in stderr.getvalue()

    def test_locked_no_refresh(self, monkeypatch):
        """A lock was acquired, but it was not refreshed."""
        pytest.importorskip("redis", reason="redis is not installed")
        scheduler = Mock()
        monkeypatch.setattr(crontask, "scheduler", scheduler)
        utils.redis_client.lock(
            "crontask-lock", blocking_timeout=0, timeout=1
        ).acquire()
        with io.StringIO() as stdout:
            call_command("crontask", stdout=stdout)
            assert "Starting scheduler…" in stdout.getvalue()

    def test_handle(self, monkeypatch):
        scheduler = Mock()
        monkeypatch.setattr(crontask, "scheduler", scheduler)
        with io.StringIO() as stdout:
            call_command("crontask", stdout=stdout)
            assert "Starting scheduler…" in stdout.getvalue()
        scheduler.start.assert_called_once()

    def test_handle__keyboard_interrupt(self, monkeypatch):
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)
        with io.StringIO() as stdout:
            call_command("crontask", stdout=stdout)
            assert "Shutting down scheduler…" in stdout.getvalue()
        scheduler.shutdown.assert_called_once()
        scheduler.start.assert_called_once()

    @pytest.mark.skipif(sys.platform == "win32", reason="Unix-specific test")
    def test_launch_scheduler_unix_signals(self, monkeypatch):
        """Test signal registration on Unix-like systems."""
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)

        signal_calls = []
        original_signal = signal.signal

        def mock_signal(signum, handler):
            signal_calls.append(signum)
            return original_signal(signum, handler)

        with (
            patch("signal.signal", side_effect=mock_signal),
            io.StringIO() as stdout,
        ):
            call_command("crontask", stdout=stdout)
            assert signal.SIGHUP in signal_calls
            assert signal.SIGTERM in signal_calls
            assert signal.SIGINT in signal_calls

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-specific test")
    def test_launch_scheduler_windows_signals(self, monkeypatch):
        """Test signal registration on Windows."""
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)

        signal_calls = []
        original_signal = signal.signal

        def mock_signal(signum, handler):
            signal_calls.append(signum)
            return original_signal(signum, handler)

        with (
            patch("signal.signal", side_effect=mock_signal),
            io.StringIO() as stdout,
        ):
            call_command("crontask", stdout=stdout)
            assert signal.SIGTERM in signal_calls
            assert signal.SIGINT in signal_calls
            if hasattr(signal, "SIGBREAK"):
                assert signal.SIGBREAK in signal_calls
            assert not any(
                sig == getattr(signal, "SIGHUP", None) for sig in signal_calls
            )

    @pytest.mark.parametrize(
        ("stdin_tty", "stdout_tty", "expected"),
        [
            (True, True, "launch_inspector"),
            (True, False, "launch_scheduler"),
            (False, True, "launch_scheduler"),
            (False, False, "launch_scheduler"),
        ],
    )
    def test_tty_routing(self, monkeypatch, stdin_tty, stdout_tty, expected):
        launched = []
        monkeypatch.setattr(
            crontask.Command,
            "launch_inspector",
            lambda self, lock, scheduler: launched.append("launch_inspector"),
        )
        monkeypatch.setattr(
            crontask.Command,
            "launch_scheduler",
            lambda self, lock, scheduler: launched.append("launch_scheduler"),
        )
        monkeypatch.setattr(sys, "stdin", FakeTTY() if stdin_tty else io.StringIO())
        monkeypatch.setattr(sys, "stdout", FakeTTY() if stdout_tty else io.StringIO())
        with io.StringIO() as stdout:
            call_command("crontask", stdout=stdout)
        assert launched == [expected]

    @pytest.mark.parametrize("shutdown_during_run", [False, True])
    def test_launch_inspector(self, monkeypatch, shutdown_during_run):
        source = BlockingScheduler()
        source.add_job(
            my_task.enqueue,
            IntervalTrigger(minutes=5),
            id="my-task",
            name="my-task",
        )
        created = []

        class StubInspectorApp:
            def __init__(self, scheduler):
                self.scheduler = scheduler
                self.jobs = scheduler.get_jobs()
                created.append(self)

            def run(self):
                self.running_during_run = self.scheduler.running
                if shutdown_during_run:
                    self.scheduler.shutdown()

        monkeypatch.setattr("crontask.inspector.app.InspectorApp", StubInspectorApp)
        crontask.Command().launch_inspector(utils.FakeLock(), source)
        (app,) = created
        assert app.running_during_run is True
        assert app.scheduler.running is False
        jobs = {job.name: job for job in app.jobs}
        assert jobs["my-task"].id == "my-task"
        assert "crontask.utils.lock.extend" in jobs


class FakeTTY(io.StringIO):
    def isatty(self):
        return True


def block_textual_import(monkeypatch, missing_name):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "textual" or name.startswith("textual."):
            raise ModuleNotFoundError(name=missing_name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.delitem(sys.modules, "crontask.inspector.app", raising=False)


def test_launch_inspector__textual_missing(monkeypatch):
    block_textual_import(monkeypatch, "textual")
    launched = []
    monkeypatch.setattr(
        crontask.Command,
        "launch_scheduler",
        lambda self, lock, scheduler: launched.append(scheduler),
    )
    command = crontask.Command()
    stderr = io.StringIO()
    command.stderr = stderr
    source = BlockingScheduler()
    command.launch_inspector(utils.FakeLock(), source)
    assert "Textual is not installed." in stderr.getvalue()
    assert "django-crontask[tui]" in stderr.getvalue()
    assert launched == [source]


def test_launch_inspector__other_import_error(monkeypatch):
    block_textual_import(monkeypatch, "other")
    monkeypatch.setattr(
        crontask.Command,
        "launch_scheduler",
        lambda self, lock, scheduler: pytest.fail("unexpected fallback"),
    )
    command = crontask.Command()
    with pytest.raises(ModuleNotFoundError) as exc_info:
        command.launch_inspector(utils.FakeLock(), BlockingScheduler())
    assert exc_info.value.name == "other"
