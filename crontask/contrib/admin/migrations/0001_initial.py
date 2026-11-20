from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="CronTask",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
            ],
            options={
                "verbose_name": "cron task",
                "verbose_name_plural": "task schedules",
                "managed": False,
                "default_permissions": ("view",),
                "permissions": (("enqueue_crontask", "Can enqueue cron task"),),
            },
        ),
    ]
