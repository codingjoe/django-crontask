from crontask.conf import get_settings

__all__ = ["LockError", "lock"]


class FakeLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extend(self, additional_time=None, replace_ttl=False):
        return True


if redis_url := get_settings().REDIS_URL:
    import redis
    from redis.exceptions import LockError, LockNotOwnedError  # noqa

    redis_client = redis.Redis.from_url(redis_url)
    lock = redis_client.lock(
        "crontask-lock",
        blocking_timeout=get_settings().LOCK_BLOCKING_TIMEOUT,
        timeout=get_settings().LOCK_TIMEOUT,
        thread_local=False,
    )
else:

    class LockError(Exception):
        pass

    class LockNotOwnedError(LockError):
        pass

    lock = FakeLock()


def extend_lock(lock, scheduler):
    """Extend the lock for a scheduler or shut it down.

    ``extend_lock`` is itself an APScheduler job and therefore runs inside one
    of the scheduler's executor threads. ``scheduler.shutdown()`` defaults to
    ``wait=True``, which makes the threadpool join every worker thread —
    including the one this function is running in — raising
    ``RuntimeError: cannot join current thread``. We pass ``wait=False`` so
    shutdown is signalled asynchronously and the current thread is allowed to
    finish unwinding.
    """
    try:
        lock.extend(get_settings().LOCK_TIMEOUT, True)
    except LockError:
        scheduler.shutdown(wait=False)
        raise
