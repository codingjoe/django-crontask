from unittest.mock import Mock

import pytest
from crontask import utils


def test_extend_lock():
    lock = Mock()
    scheduler = Mock()
    utils.extend_lock(lock, scheduler)
    assert lock.extend.call_count == 1
    assert scheduler.shutdown.call_count == 0


def test_extend_lock__error():
    lock = Mock()
    lock.extend.side_effect = utils.LockError()
    scheduler = Mock()
    with pytest.raises(utils.LockError):
        utils.extend_lock(lock, scheduler)
    assert lock.extend.call_count == 1
    assert scheduler.shutdown.call_count == 1
    # ``extend_lock`` runs inside a scheduler worker thread; shutdown must be
    # signalled asynchronously to avoid ``RuntimeError: cannot join current
    # thread`` from BlockingScheduler's pool executor.
    scheduler.shutdown.assert_called_once_with(wait=False)


class TestFakeLock:
    def test_enter(self):
        fake_lock = utils.FakeLock()
        assert fake_lock.__enter__() is fake_lock

    def test_exit(self):
        fake_lock = utils.FakeLock()
        assert fake_lock.__exit__(None, None, None) is None

    def test_extend(self):
        fake_lock = utils.FakeLock()
        assert fake_lock.extend(additional_time=10, replace_ttl=True)
