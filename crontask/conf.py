from django.conf import settings


def get_settings():
    return type(
        "Settings",
        (),
        {
            "REDIS_URL": None,
            "LOCK_REFRESH_INTERVAL": 5,
            "LOCK_TIMEOUT": 10,
            "LOCK_BLOCKING_TIMEOUT": 15,
            **getattr(settings, "CRONTASK", {}),
        },
    )
