"""
apps/reddit/models.py — Phase 2
=================================
Phase 2 additions:
  1. SubredditConfig  — DB-driven scheduler config (replaces hard-coded list)
  2. Post             — velocity & trending_score fields uncommented; managed=False kept
  3. Admin registration moved to admin.py (see that file)
"""

from django.db import models


# ── Subreddit ─────────────────────────────────────────────────────────────────

class Subreddit(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField()

    def __str__(self):
        return self.name

    class Meta:
        db_table = "subreddits"
        managed = False


# ── Post ──────────────────────────────────────────────────────────────────────

class Post(models.Model):
    id = models.CharField(primary_key=True, max_length=20)
    subreddit = models.ForeignKey(Subreddit, on_delete=models.CASCADE)

    title = models.TextField()
    author = models.CharField(max_length=100)
    created_utc = models.DateTimeField()

    first_seen_at = models.DateTimeField(null=True, blank=True)
    last_polled_at = models.DateTimeField(null=True, blank=True)

    current_score = models.IntegerField()
    current_comments = models.IntegerField()
    current_ratio = models.FloatField()

    poll_priority = models.CharField(max_length=20, null=True, blank=True)
    is_active = models.BooleanField()

    # Phase 2: expose fields computed by the processing service
    velocity = models.FloatField(null=True, blank=True)
    trending_score = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "posts"
        managed = False


# ── Comment ───────────────────────────────────────────────────────────────────

class Comment(models.Model):
    reddit_id = models.CharField(max_length=50, unique=True)
    post = models.ForeignKey(Post, on_delete=models.CASCADE)
    author = models.CharField(max_length=100)
    body = models.TextField()
    score = models.IntegerField()
    sentiment = models.FloatField(null=True, blank=True)
    created_utc = models.DateTimeField()

    class Meta:
        db_table = "comments"
        managed = False


# ── KeywordTrend ──────────────────────────────────────────────────────────────

class KeywordTrend(models.Model):
    keyword = models.CharField(max_length=100)
    subreddit = models.ForeignKey(Subreddit, on_delete=models.CASCADE)
    score = models.FloatField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "keyword_trends"
        managed = False


# ── SubredditStats ────────────────────────────────────────────────────────────

class SubredditStats(models.Model):
    subreddit = models.ForeignKey(Subreddit, on_delete=models.CASCADE)
    date = models.DateField()
    total_posts = models.IntegerField()
    total_comments = models.IntegerField()
    top_user = models.CharField(max_length=100, null=True, blank=True)

    class Meta:
        db_table = "subreddit_stats"
        managed = False


# ── SubredditConfig — Phase 2 (new) ──────────────────────────────────────────

class SubredditConfig(models.Model):
    """
    DB-driven configuration for the ingestion scheduler.

    Previously, the list of subreddits to poll was hard-coded in the ingestion
    service as TOP_SUBREDDITS = [...]. Adding a new subreddit required:
      1. Editing the source file
      2. Rebuilding the ingestion Docker image
      3. Redeploying the container

    Phase 2: The scheduler reads this table on startup and on SIGHUP.
    Ops can add/remove/pause subreddits, change poll intervals, and adjust
    priority entirely from Django Admin — no code changes, no container restart.

    Fields:
      name             — subreddit name without r/ prefix, e.g. "technology"
      interval_seconds — how often the ingestion scheduler polls this subreddit
      priority         — "fast" / "medium" / "slow" tier for the processing queue
      is_active        — set False to pause polling without deleting the record
      added_by         — free-text audit field; Django Admin logs full history

    Hot reload:
      The ingestion scheduler checks this table every `SCHEDULER_CONFIG_POLL_S`
      seconds (default 60). Changed or new records trigger asyncio.Task
      cancellation + recreation without a container restart.
    """

    PRIORITY_CHOICES = [
        ("fast",   "Fast   (~30s interval)"),
        ("medium", "Medium (~2min interval)"),
        ("slow",   "Slow   (~10min interval)"),
    ]

    name = models.CharField(
        max_length=100,
        unique=True,
        help_text="Subreddit name without the r/ prefix, e.g. 'technology'",
    )
    interval_seconds = models.PositiveIntegerField(
        default=120,
        help_text="How often (seconds) the scheduler fetches new posts from this subreddit",
    )
    priority = models.CharField(
        max_length=10,
        choices=PRIORITY_CHOICES,
        default="medium",
        help_text="Processing priority tier passed to the Kafka producer",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Uncheck to pause ingestion without deleting the record",
    )
    added_by = models.CharField(
        max_length=100,
        blank=True,
        help_text="Free-text: who added this subreddit (informal audit trail)",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "subreddit_config"
        verbose_name = "Subreddit Config"
        verbose_name_plural = "Subreddit Configs"
        ordering = ["name"]

    def __str__(self):
        status = "✓" if self.is_active else "✗"
        return f"{status} r/{self.name} ({self.priority}, {self.interval_seconds}s)"
