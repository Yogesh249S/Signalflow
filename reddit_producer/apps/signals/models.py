"""
apps/signals/models.py
=======================
Single unified model layer for all 4 platforms.
Replaces the entire apps/reddit/ model set (Post, Subreddit,
Comment, KeywordTrend, SubredditStats, SubredditConfig).

All tables are managed=False — owned by storage/migrations/V5.
Django reads and queries, never runs DDL.

Models:
  Community       — any grouping within a platform (subreddit / channel / feed)
  Signal          — unified post/story/comment from any platform
  PlatformDivergence — cross-platform sentiment disagreement events
  SourceConfig    — replaces SubredditConfig, controls all 4 sources
"""

from django.db import models


class Community(models.Model):
    """
    Replaces: Subreddit
    A community is any grouping within a platform.
      reddit    → r/technology
      hackernews→ hackernews (single community)
      bluesky   → bluesky or a specific feed
      youtube   → channel name (Fireship, ThePrimeagen)
    """
    platform   = models.TextField()
    name       = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed  = False
        db_table = "communities"
        unique_together = [("platform", "name")]

    def __str__(self):
        return f"{self.platform}:{self.name}"


class Signal(models.Model):
    """
    Replaces: Post (and partially Comment)
    One row per post/story/comment from any platform.
    id = "platform:source_id" e.g. "reddit:abc123", "hackernews:456"
    """
    # Identity
    id         = models.TextField(primary_key=True)
    platform   = models.TextField()               # reddit / hackernews / bluesky / youtube
    source_id  = models.TextField()               # platform-native ID
    community  = models.ForeignKey(
        Community, on_delete=models.SET_NULL,
        null=True, db_column="community_id"
    )

    # Content
    title      = models.TextField(blank=True, default="")   # empty for bluesky/youtube
    body       = models.TextField(blank=True, default="")
    url        = models.TextField(blank=True, default="")
    author     = models.TextField(blank=True, default="")

    # Timing
    published_at    = models.DateTimeField(null=True)
    first_seen_at   = models.DateTimeField(auto_now_add=True)
    last_updated_at = models.DateTimeField(auto_now=True)

    # Engagement — raw platform-native units
    raw_score     = models.IntegerField(default=0)   # upvotes / points / likes
    comment_count = models.IntegerField(default=0)

    # Engagement — normalised 0-1, computed by processing service
    normalised_score = models.FloatField(null=True)

    # Velocity — computed by processing service
    score_velocity   = models.FloatField(null=True)
    comment_velocity = models.FloatField(null=True)

    # Trending — computed by processing service
    trending_score = models.FloatField(default=0.0)
    is_trending    = models.BooleanField(default=False)

    # NLP — computed by processing service
    sentiment_compound = models.FloatField(null=True)
    sentiment_label    = models.TextField(null=True)   # positive / neutral / negative
    keywords           = models.JSONField(default=list)
    topics             = models.JSONField(default=list)

    # Platform-specific fields preserved as JSONB
    # reddit:  {"upvote_ratio": 0.95, "poll_priority": "slow"}
    # youtube: {"video_id": "xxx", "channel_id": "yyy"}
    # bluesky: {"repost_count": 12, "langs": ["en"]}
    extra = models.JSONField(default=dict)

    schema_version = models.IntegerField(default=1)

    class Meta:
        managed  = False
        db_table = "signals"
        unique_together = [("platform", "source_id")]

    def __str__(self):
        return f"{self.id} — {(self.title or self.body)[:60]}"


class PlatformDivergence(models.Model):
    """
    Cross-platform sentiment divergence events.
    Written by processing/divergence_detector.py every 15 minutes.
    This is the core of the /api/v1/compare/ product feature.
    """
    topic              = models.TextField()
    detected_at        = models.DateTimeField()
    platform_a         = models.TextField()
    platform_b         = models.TextField()
    sentiment_a        = models.FloatField()
    sentiment_b        = models.FloatField()
    divergence_score   = models.FloatField()
    origin_platform    = models.TextField(null=True)
    origin_lag_minutes = models.IntegerField(null=True)
    sample_signal_ids  = models.JSONField(default=list)
    resolved_at        = models.DateTimeField(null=True)
    is_resolved        = models.BooleanField(default=False)

    class Meta:
        managed  = False
        db_table = "platform_divergence"

    def __str__(self):
        return f"{self.topic}: {self.platform_a} vs {self.platform_b} ({self.divergence_score:.2f})"


class SourceConfig(models.Model):
    """
    Replaces: SubredditConfig
    Controls what each source ingests — hot-reloaded by ingestion scheduler.
    Covers all 4 platforms from one admin table.
    """
    PLATFORM_CHOICES = [
        ("reddit",      "Reddit"),
        ("hackernews",  "Hacker News"),
        ("bluesky",     "Bluesky"),
        ("youtube",     "YouTube"),
    ]

    platform         = models.TextField(choices=PLATFORM_CHOICES)
    identifier       = models.TextField()   # subreddit name / channel_id / feed / keyword
    label            = models.TextField(blank=True)
    interval_seconds = models.PositiveIntegerField(default=300)
    is_active        = models.BooleanField(default=True)
    added_by         = models.TextField(blank=True, default="system")
    added_at         = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed      = False
        db_table     = "source_config"
        unique_together = [("platform", "identifier")]
        ordering     = ["platform", "identifier"]

    def __str__(self):
        status = "✓" if self.is_active else "✗"
        return f"{status} [{self.platform}] {self.identifier} ({self.interval_seconds}s)"
