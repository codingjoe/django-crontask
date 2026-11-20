from django.apps import AppConfig


class CronTaskAdminConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "crontask.contrib.admin"
    label = "crontask_admin"
    verbose_name = "Cron Task"
