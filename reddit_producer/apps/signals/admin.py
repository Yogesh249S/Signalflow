"""
apps/signals/admin.py
======================
Replaces apps/reddit/admin.py.

SourceConfigAdmin replaces SubredditConfigAdmin — same hot-reload
pattern but covers all 4 platforms from one admin table.
Signal and PlatformDivergence are read-only (written by processing service).
"""

from django.contrib import admin
from .models import Signal, Community, PlatformDivergence, SourceConfig


@admin.register(SourceConfig)
class SourceConfigAdmin(admin.ModelAdmin):
    """
    Replaces SubredditConfigAdmin.
    Ops can add/toggle/tune any source across all 4 platforms.
    Changes hot-reload in the ingestion scheduler within ~60 seconds.
    """
    list_display  = ("platform", "identifier", "label",
                     "interval_seconds", "is_active", "added_by", "added_at")
    list_filter   = ("platform", "is_active")
    search_fields = ("identifier", "label", "added_by")
    list_editable = ("is_active", "interval_seconds")
    readonly_fields = ("added_at",)
    ordering = ("platform", "identifier")

    fieldsets = (
        ("Source", {
            "fields": ("platform", "identifier", "label", "is_active", "added_by"),
        }),
        ("Polling", {
            "fields": ("interval_seconds",),
            "description": (
                "Changes take effect within ~60 seconds. "
                "The ingestion scheduler polls this table and restarts "
                "affected tasks automatically."
            ),
        }),
        ("Timestamps", {
            "fields": ("added_at",),
            "classes": ("collapse",),
        }),
    )

    actions = ["activate_selected", "deactivate_selected"]

    @admin.action(description="Activate selected sources")
    def activate_selected(self, request, queryset):
        updated = queryset.update(is_active=True)
        self.message_user(request, f"{updated} source(s) activated.")

    @admin.action(description="Deactivate selected sources (pause ingestion)")
    def deactivate_selected(self, request, queryset):
        updated = queryset.update(is_active=False)
        self.message_user(request, f"{updated} source(s) deactivated.")


@admin.register(Community)
class CommunityAdmin(admin.ModelAdmin):
    list_display  = ("platform", "name", "created_at")
    list_filter   = ("platform",)
    search_fields = ("name",)
    readonly_fields = ("id", "created_at")
    ordering = ("platform", "name")


@admin.register(Signal)
class SignalAdmin(admin.ModelAdmin):
    list_display  = ("id", "platform", "short_text", "community",
                     "raw_score", "sentiment_label", "is_trending", "published_at")
    list_filter   = ("platform", "sentiment_label", "is_trending")
    search_fields = ("title", "body", "author", "id")
    readonly_fields = (
        "id", "platform", "source_id", "community",
        "title", "body", "url", "author",
        "published_at", "first_seen_at", "last_updated_at",
        "raw_score", "comment_count", "normalised_score",
        "score_velocity", "comment_velocity",
        "trending_score", "is_trending",
        "sentiment_compound", "sentiment_label",
        "keywords", "topics", "extra",
    )
    ordering = ("-published_at",)

    def short_text(self, obj):
        return (obj.title or obj.body or "")[:60]
    short_text.short_description = "Title / Body"

    def has_add_permission(self, request):    return False
    def has_change_permission(self, request, obj=None): return False


@admin.register(PlatformDivergence)
class PlatformDivergenceAdmin(admin.ModelAdmin):
    list_display  = ("topic", "platform_a", "platform_b",
                     "divergence_score", "origin_platform",
                     "origin_lag_minutes", "is_resolved", "detected_at")
    list_filter   = ("platform_a", "platform_b", "is_resolved")
    search_fields = ("topic",)
    readonly_fields = (
        "topic", "detected_at", "platform_a", "platform_b",
        "sentiment_a", "sentiment_b", "divergence_score",
        "origin_platform", "origin_lag_minutes",
        "sample_signal_ids", "resolved_at", "is_resolved",
    )
    ordering = ("-detected_at",)

    def has_add_permission(self, request):    return False
    def has_change_permission(self, request, obj=None): return False
