import io
import signal
import sys
from unittest.mock import Mock, patch

import pytest
from crontask import utils
from crontask.management.commands import crontask
from django.core.management import call_command


def test_kill_softly():
    with pytest.raises(KeyboardInterrupt) as e:
        crontask.kill_softly(15, None)
    assert "Received SIGTERM (15), shutting down…" in str(e.value)


class Testcrontask:
    @pytest.fixture()
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

    def test_locked(self):
        """A lock was already acquired by another process."""
        pytest.importorskip("redis", reason="redis is not installed")
        with utils.redis_client.lock("crontask-lock", blocking_timeout=0):
            with io.StringIO() as stderr:
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

    def test_launch_scheduler_unix_signals(self, monkeypatch):
        """Test signal registration on Unix-like systems."""
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)

        # Mock sys.platform to be non-Windows
        with patch.object(sys, "platform", "linux"):
            signal_calls = []
            original_signal = signal.signal

            def mock_signal(signum, handler):
                signal_calls.append(signum)
                return original_signal(signum, handler)

            with patch("signal.signal", side_effect=mock_signal):
                with io.StringIO() as stdout:
                    call_command("crontask", stdout=stdout)
                    # Verify SIGHUP, SIGTERM, and SIGINT were registered
                    assert signal.SIGTERM in signal_calls
                    assert signal.SIGINT in signal_calls
                    if hasattr(signal, "SIGHUP"):
                        assert signal.SIGHUP in signal_calls

    def test_launch_scheduler_windows_signals(self, monkeypatch):
        """Test signal registration on Windows."""
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)

        # Mock sys.platform to be Windows
        with patch.object(sys, "platform", "win32"):
            signal_calls = []
            original_signal = signal.signal

            def mock_signal(signum, handler):
                signal_calls.append(signum)
                return original_signal(signum, handler)

            with patch("signal.signal", side_effect=mock_signal):
                with io.StringIO() as stdout:
                    call_command("crontask", stdout=stdout)
                    # Verify SIGTERM and SIGINT were registered
                    assert signal.SIGTERM in signal_calls
                    assert signal.SIGINT in signal_calls
                    # SIGHUP should NOT be registered on Windows
                    if hasattr(signal, "SIGHUP"):
                        assert signal.SIGHUP not in signal_calls
                    # SIGBREAK might be registered if available
                    if hasattr(signal, "SIGBREAK"):
                        assert signal.SIGBREAK in signal_calls

    def test_launch_scheduler_missing_sighup(self, monkeypatch):
        """Test graceful handling when SIGHUP is not available."""
        scheduler = Mock()
        scheduler.start.side_effect = KeyboardInterrupt()
        monkeypatch.setattr(crontask, "scheduler", scheduler)

        # Mock signal.signal to raise AttributeError for SIGHUP
        original_signal = signal.signal

        def mock_signal(signum, handler):
            # Simulate SIGHUP not being available
            if hasattr(signal, "SIGHUP") and signum == signal.SIGHUP:
                raise AttributeError("SIGHUP not available")
            return original_signal(signum, handler)

        with patch("signal.signal", side_effect=mock_signal):
            with io.StringIO() as stdout:
                # Should not raise an exception
                call_command("crontask", stdout=stdout)
                assert "Starting scheduler…" in stdout.getvalue()
