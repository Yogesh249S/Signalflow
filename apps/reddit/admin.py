"""
apps/reddit/admin.py — Phase 2
================================
Phase 2: Register SubredditConfig with a customised Admin interface.

Ops can now:
  - Add a new subreddit via the browser form (no code change needed)
  - Toggle is_active to pause/resume polling
  - Change interval_seconds or priority on the fly
  - See the full Django admin change-log for audit trail

The ingestion scheduler picks up config changes within SCHEDULER_CONFIG_POLL_S
seconds (default 60) via a periodic DB poll — no container restart required.
"""

from django.contrib import admin
from .models import SubredditConfig, Subreddit, Post


@admin.register(SubredditConfig)
class SubredditConfigAdmin(admin.ModelAdmin):
    list_display = ("name", "priority", "interval_seconds", "is_active", "updated_at", "added_by")
    list_filter  = ("is_active", "priority")
    search_fields = ("name", "added_by")
    list_editable = ("is_active", "priority", "interval_seconds")
    readonly_fields = ("created_at", "updated_at")
    ordering = ("name",)

    fieldsets = (
        ("Subreddit", {
            "fields": ("name", "is_active", "added_by"),
        }),
        ("Polling", {
            "fields": ("interval_seconds", "priority"),
            "description": (
                "Changes here take effect within ~60 seconds. "
                "The scheduler polls this table periodically and restarts "
                "affected asyncio tasks automatically."
            ),
        }),
        ("Timestamps", {
            "fields": ("created_at", "updated_at"),
            "classes": ("collapse",),
        }),
    )

    actions = ["activate_selected", "deactivate_selected"]

    @admin.action(description="Activate selected subreddits")
    def activate_selected(self, request, queryset):
        updated = queryset.update(is_active=True)
        self.message_user(request, f"{updated} subreddit(s) activated.")

    @admin.action(description="Deactivate selected subreddits (pause ingestion)")
    def deactivate_selected(self, request, queryset):
        updated = queryset.update(is_active=False)
        self.message_user(request, f"{updated} subreddit(s) deactivated.")


@admin.register(Subreddit)
class SubredditAdmin(admin.ModelAdmin):
    list_display  = ("name", "created_at")
    search_fields = ("name",)
    readonly_fields = ("id", "created_at")


@admin.register(Post)
class PostAdmin(admin.ModelAdmin):
    list_display   = ("id", "title", "subreddit", "current_score", "trending_score", "is_active")
    list_filter    = ("subreddit", "is_active", "poll_priority")
    search_fields  = ("id", "title", "author")
    readonly_fields = ("id", "created_utc", "first_seen_at", "last_polled_at")
