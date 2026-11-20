import logging
from unittest.mock import Mock

import pytest
from apscheduler.triggers.cron import CronTrigger
from crontask import cron, scheduler
from crontask.contrib.admin.admin import CronTaskAdmin, CronTaskOverview
from crontask.contrib.admin.models import CronTask
from django.contrib import admin
from django.contrib.auth.models import Permission
from django.tasks import task
from django.test import RequestFactory
from django.urls import NoReverseMatch, reverse

logger = logging.getLogger(__name__)


@task
def overview_task():
    """Log that the overview task ran."""
    logger.info("Overview task ran.")


def scheduler_job():
    """Do nothing so the overview ignores the job."""


@task
def documented_task():
    """**Bold** documentation."""


@task
def undocumented_task():
    pass


@pytest.fixture
def overview_job():
    cron("0 0 * * *")(overview_task)
    yield overview_task
    scheduler.remove_all_jobs()


def grant_permissions(user, *codenames):
    user.user_permissions.add(
        *Permission.objects.filter(
            content_type__app_label="crontask_admin",
            codename__in=codenames,
        )
    )


class TestCronTaskOverview:
    def test_docstring_html__rst(self):
        overview = CronTaskOverview(
            task=documented_task,
            trigger=CronTrigger.from_crontab("* * * * *"),
        )

        assert "<strong>Bold</strong> documentation." in overview.docstring_html

    def test_docstring_html__without_docutils(self, monkeypatch):
        monkeypatch.setattr("crontask.contrib.admin.admin.docutils_is_available", False)
        overview = CronTaskOverview(
            task=documented_task,
            trigger=CronTrigger.from_crontab("* * * * *"),
        )

        assert "**Bold** documentation." in overview.docstring_html

    def test_docstring_html__without_admindocs_urls(self, monkeypatch):
        monkeypatch.setattr(
            "crontask.contrib.admin.admin.parse_rst",
            Mock(side_effect=NoReverseMatch),
        )
        overview = CronTaskOverview(
            task=documented_task,
            trigger=CronTrigger.from_crontab("* * * * *"),
        )

        assert "**Bold** documentation." in overview.docstring_html

    def test_docstring_html__empty(self):
        overview = CronTaskOverview(
            task=undocumented_task,
            trigger=CronTrigger.from_crontab("* * * * *"),
        )

        assert overview.docstring_html == ""


class TestCronTaskAdmin:
    CHANGELIST_URL = "admin:crontask_admin_crontask_changelist"
    DETAIL_URL = "admin:crontask_admin_crontask_detail"
    ENQUEUE_ACTION = "enqueue_tasks"

    @pytest.mark.django_db
    def test_admin_index__lists_cron_tasks(self, client, admin_user):
        client.force_login(admin_user)

        response = client.get(reverse("admin:index"))

        assert response.status_code == 200
        assert b"Task schedules" in response.content
        assert b"/crontask/add/" not in response.content

    @pytest.mark.django_db
    def test_admin__no_add_change_delete(self, admin_user):
        request = RequestFactory().get("/")
        request.user = admin_user
        model_admin = CronTaskAdmin(CronTask, admin.site)

        assert not model_admin.has_add_permission(request)
        assert not model_admin.has_change_permission(request)
        assert not model_admin.has_delete_permission(request)

    @pytest.mark.django_db
    def test_changelist_view__lists_tasks(self, client, admin_user, overview_job):
        client.force_login(admin_user)

        response = client.get(reverse(self.CHANGELIST_URL))

        assert response.status_code == 200
        assert b"tests.test_admin." in response.content
        assert b"overview_task" in response.content
        assert b"cron[" in response.content
        assert b"<time" in response.content
        assert b"Enqueue selected tasks" in response.content
        assert (
            reverse(self.DETAIL_URL, args=[overview_job.module_path]).encode()
            in response.content
        )

    @pytest.mark.django_db
    def test_detail_view__renders_docstring(self, client, admin_user, overview_job):
        client.force_login(admin_user)

        response = client.get(reverse(self.DETAIL_URL, args=[overview_job.module_path]))

        assert response.status_code == 200
        assert b"Log that the overview task ran." in response.content
        assert b"cron[" in response.content
        assert b"<time" in response.content

    @pytest.mark.django_db
    def test_detail_view__unknown_task(self, client, admin_user, overview_job):
        client.force_login(admin_user)

        response = client.get(
            reverse(self.DETAIL_URL, args=["tests.test_admin.missing"])
        )

        assert response.status_code == 404

    @pytest.mark.django_db
    def test_detail_view__requires_permission(
        self, client, django_user_model, overview_job
    ):
        user = django_user_model.objects.create_user(
            username="staff",
            password="secret",
            is_staff=True,
        )
        client.force_login(user)

        response = client.get(reverse(self.DETAIL_URL, args=[overview_job.module_path]))

        assert response.status_code == 403

    @pytest.mark.django_db
    def test_enqueue_tasks__enqueues_selected_tasks(
        self, client, admin_user, overview_job, caplog
    ):
        client.force_login(admin_user)

        with caplog.at_level(logging.INFO):
            response = client.post(
                reverse(self.CHANGELIST_URL),
                {
                    "action": self.ENQUEUE_ACTION,
                    "_selected_action": overview_job.module_path,
                },
            )

        assert response.status_code == 302
        assert "Overview task ran." in caplog.text

    @pytest.mark.django_db
    def test_enqueue_tasks__requires_selection(
        self, client, admin_user, overview_job, caplog
    ):
        client.force_login(admin_user)

        with caplog.at_level(logging.INFO):
            response = client.post(
                reverse(self.CHANGELIST_URL), {"action": self.ENQUEUE_ACTION}
            )

        assert response.status_code == 200
        assert "Overview task ran." not in caplog.text

    @pytest.mark.django_db
    def test_enqueue_tasks__unknown_action(self, client, admin_user, overview_job):
        client.force_login(admin_user)

        response = client.post(
            reverse(self.CHANGELIST_URL),
            {
                "action": "unknown",
                "_selected_action": overview_job.module_path,
            },
        )

        assert response.status_code == 200

    @pytest.mark.django_db
    def test_changelist_view__requires_permission(
        self, client, django_user_model, overview_job
    ):
        user = django_user_model.objects.create_user(
            username="staff",
            password="secret",
            is_staff=True,
        )
        client.force_login(user)

        response = client.get(reverse(self.CHANGELIST_URL))

        assert response.status_code == 403

    @pytest.mark.django_db
    def test_changelist_view__view_permission_hides_actions(
        self, client, django_user_model, overview_job
    ):
        user = django_user_model.objects.create_user(
            username="staff",
            password="secret",
            is_staff=True,
        )
        grant_permissions(user, "view_crontask")
        client.force_login(user)

        response = client.get(reverse(self.CHANGELIST_URL))

        assert response.status_code == 200
        assert b"Enqueue selected tasks" not in response.content

    @pytest.mark.django_db
    def test_changelist_view__enqueue_permission_shows_actions(
        self, client, django_user_model, overview_job
    ):
        user = django_user_model.objects.create_user(
            username="staff",
            password="secret",
            is_staff=True,
        )
        grant_permissions(user, "enqueue_crontask")
        client.force_login(user)

        response = client.get(reverse(self.CHANGELIST_URL))

        assert response.status_code == 200
        assert b"Enqueue selected tasks" in response.content

    @pytest.mark.django_db
    def test_enqueue_tasks__requires_enqueue_permission(
        self, client, django_user_model, overview_job, caplog
    ):
        user = django_user_model.objects.create_user(
            username="staff",
            password="secret",
            is_staff=True,
        )
        grant_permissions(user, "view_crontask")
        client.force_login(user)

        with caplog.at_level(logging.INFO):
            response = client.post(
                reverse(self.CHANGELIST_URL),
                {
                    "action": self.ENQUEUE_ACTION,
                    "_selected_action": overview_job.module_path,
                },
            )

        assert response.status_code == 200
        assert "Overview task ran." not in caplog.text

    @pytest.mark.django_db
    def test_changelist_view__ignores_non_task_jobs(
        self, client, admin_user, overview_job
    ):
        scheduler.add_job(scheduler_job, "interval", seconds=60)
        client.force_login(admin_user)

        response = client.get(reverse(self.CHANGELIST_URL))

        assert response.status_code == 200
        assert b"scheduler_job" not in response.content

    @pytest.mark.django_db
    def test_changelist_view__extra_context(self, admin_user):
        request = RequestFactory().get("/")
        request.user = admin_user
        model_admin = CronTaskAdmin(CronTask, admin.site)

        response = model_admin.changelist_view(
            request, extra_context={"extra": "context"}
        )

        assert response.context_data["extra"] == "context"

    @pytest.mark.django_db
    def test_permissions__view_and_enqueue_only(self):
        assert set(
            Permission.objects.filter(
                content_type__app_label="crontask_admin",
            ).values_list("codename", flat=True)
        ) == {"view_crontask", "enqueue_crontask"}
