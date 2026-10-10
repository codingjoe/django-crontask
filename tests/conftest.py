"""Pytest configuration for the crontask test suite."""


def _install_fake_redis() -> None:
    """Route every Redis client to one in-process fake server."""
    import fakeredis
    import redis

    server = fakeredis.FakeServer()

    class FakeRedis(fakeredis.FakeStrictRedis):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("server", server)
            super().__init__(*args, **kwargs)

    redis.Redis = FakeRedis
    redis.StrictRedis = FakeRedis


# Patch at import time: ``crontask.utils`` builds its client while the test
# modules are collected, so a fixture would be too late.
_install_fake_redis()
