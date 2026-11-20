import dataclasses
import datetime as dt
import importlib
import inspect

from apscheduler.triggers.base import BaseTrigger
from django.contrib import admin
from django.contrib.admin import helpers
from django.contrib.admindocs.utils import docutils_is_available, parse_rst
from django.contrib.auth import get_permission_codename
from django.core.exceptions import PermissionDenied
from django.http import Http404, HttpResponseBase, HttpResponseRedirect
from django.tasks import Task
from django.template.defaultfilters import linebreaks
from django.template.response import TemplateResponse
from django.urls import NoReverseMatch, path, reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy, ngettext

from ... import load_tasks, scheduler
from .models import CronTask


@dataclasses.dataclass(frozen=True)
class CronTaskOverview:
    task: Task
    trigger: BaseTrigger

    @property
    def task_id(self) -> str:
        return self.task.module_path

    @property
    def task_id_prefix(self) -> str:
        return self.task_id.removesuffix(self.task.name)

    @property
    def next_enqueue_at(self) -> dt.datetime | None:
        return self.trigger.get_next_fire_time(None, timezone.now())

    @property
    def docstring(self) -> str:
        return inspect.getdoc(self.task.func) or ""

    @property
    def docstring_html(self) -> str:
        if not self.docstring:
            return ""
        if docutils_is_available:
            try:
                return parse_rst(self.docstring, "model", self.task_id)
            except NoReverseMatch:
                return linebreaks(self.docstring)
        return linebreaks(self.docstring)


@admin.register(CronTask)
class CronTaskAdmin(admin.ModelAdmin):
    """List registered cron tasks and enqueue them on demand."""

    actions = ["enqueue_tasks"]
    change_list_template = "admin/crontask_admin/cron_task/change_list.html"
    detail_template = "admin/crontask_admin/cron_task/detail.html"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_view_permission(self, request, obj=None):
        return self.has_enqueue_permission(request) or super().has_view_permission(
            request, obj
        )

    def has_enqueue_permission(self, request, obj=None):
        codename = get_permission_codename("enqueue", self.opts)
        return request.user.has_perm(f"{self.opts.app_label}.{codename}")

    @admin.action(
        description=gettext_lazy("Enqueue selected tasks"),
        permissions=["enqueue"],
    )
    def enqueue_tasks(self, request, overviews):
        for overview in overviews:
            overview.task.enqueue()
        count = len(overviews)
        self.message_user(
            request,
            ngettext(
                "Enqueued %(count)d task.",
                "Enqueued %(count)d tasks.",
                count,
            )
            % {"count": count},
        )

    def changelist_view(self, request, extra_context=None):
        if not self.has_view_or_change_permission(request):
            raise PermissionDenied
        if request.method == "POST" and (response := self.run_action(request)):
            return response
        action_form = self.action_form(auto_id=None)
        action_form.fields["action"].choices = self.get_action_choices(request)
        context = {
            **self.admin_site.each_context(request),
            "opts": self.opts,
            "overviews": self.get_overviews(),
            "title": self.opts.verbose_name_plural.capitalize(),
            "has_enqueue_permission": self.has_enqueue_permission(request),
            "action_form": action_form,
            "detail_url_name": self.get_admin_url_name("detail"),
        }
        if extra_context:
            context.update(extra_context)
        return TemplateResponse(request, self.change_list_template, context)

    def get_urls(self):
        return [
            path(
                "<str:task_id>/change/",
                self.admin_site.admin_view(self.detail_view),
                name=self.get_url_name("detail"),
            ),
            *super().get_urls(),
        ]

    def detail_view(self, request, task_id):
        if not self.has_view_or_change_permission(request):
            raise PermissionDenied
        overview = next(
            (
                overview
                for overview in self.get_overviews()
                if overview.task_id == task_id
            ),
            None,
        )
        if overview is None:
            raise Http404
        context = {
            **self.admin_site.each_context(request),
            "opts": self.opts,
            "overview": overview,
            "title": overview.task.name,
            "changelist_url": reverse(self.get_admin_url_name("changelist")),
        }
        return TemplateResponse(request, self.detail_template, context)

    def get_action_func(self, action):
        # Django 6.0 returns tuples, Django 6.1 returns Action objects.
        return action.func if hasattr(action, "func") else action[0]

    def run_action(self, request):
        action = request.POST.get("action")
        actions = self.get_actions(request)
        response = None
        if action in actions:
            selected_task_ids = set(request.POST.getlist(helpers.ACTION_CHECKBOX_NAME))
            if overviews := [
                overview
                for overview in self.get_overviews()
                if overview.task_id in selected_task_ids
            ]:
                response = self.get_action_func(actions[action])(
                    self, request, overviews
                )
                if not isinstance(response, HttpResponseBase):
                    response = HttpResponseRedirect(request.get_full_path())
        return response

    def get_overviews(self) -> list[CronTaskOverview]:
        load_tasks()
        importlib.import_module("crontask.tasks")
        overviews = [
            CronTaskOverview(task=task, trigger=job.trigger)
            for job in scheduler.get_jobs()
            if isinstance(task := getattr(job.func, "__self__", None), Task)
        ]
        return sorted(overviews, key=lambda overview: overview.task_id)

    def get_url_name(self, action: str) -> str:
        return f"{self.opts.app_label}_{self.opts.model_name}_{action}"

    def get_admin_url_name(self, action: str) -> str:
        return f"{self.admin_site.name}:{self.get_url_name(action)}"
