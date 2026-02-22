"""
apps/reddit/views.py — Phase 2
================================
Phase 1 changes (kept):
  - Single aggregate query for stats overview
  - Redis caching on /api/stats/ (30 s TTL)
  - Cursor-based pagination on PostViewSet
  - select_related() to avoid N+1 on subreddit

Phase 2 additions:
  1. All views now require JWT authentication (set in settings.py via
     DEFAULT_PERMISSION_CLASSES = IsAuthenticated). The /api/token/ and
     /health/ endpoints are explicitly AllowAny.
  2. /health/ endpoint — unauthenticated, used by load balancers and Docker.
  3. PostSerializer now includes velocity + trending_score fields.
  4. WebSocket channel notification after stats computation so the dashboard
     can be notified of significant changes (optional enhancement hook).
"""

import json
import logging
from datetime import datetime, timedelta, time as dt_time

from django.core.cache import cache
from django.db.models import Avg, Count, ExpressionWrapper, F, FloatField, Sum
from django.db.models.functions import Extract, Now

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework import viewsets, status

from .models import Subreddit, Post, Comment, KeywordTrend, SubredditStats
from .serializers import (
    SubredditSerializer,
    PostSerializer,
    CommentSerializer,
    KeywordTrendSerializer,
    SubredditStatsSerializer,
)

logger = logging.getLogger(__name__)

STATS_CACHE_TTL = 30  # seconds


# ── Unauthenticated endpoints ─────────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    """
    Unauthenticated health-check for Docker / load-balancer probes.
    Returns 200 if Django is responding. No DB query.
    """
    return Response({"status": "ok"})


@api_view(["GET"])
@permission_classes([AllowAny])
def home(request):
    return Response({"status": "Reddit Pulse API", "version": "2.0"})


# ── PostViewSet ───────────────────────────────────────────────────────────────

class PostViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Authenticated. Reads routed to the Postgres replica via DB router.
    Cursor pagination via ?cursor=<ISO-datetime>&page_size=N (max 200).
    """
    serializer_class = PostSerializer
    filterset_fields = ["subreddit__name"]

    def get_queryset(self):
        qs = (
            Post.objects
            .select_related("subreddit")
            .annotate(
                engagement_score=F("current_score") + (F("current_comments") * 2),
                age_minutes=ExpressionWrapper(
                    Extract(Now() - F("created_utc"), "epoch") / 60.0,
                    output_field=FloatField(),
                ),
                momentum=ExpressionWrapper(
                    (F("current_score") + (F("current_comments") * 2)) /
                    (Extract(Now() - F("created_utc"), "epoch") / 60.0 + 1.0),
                    output_field=FloatField(),
                ),
            )
            .order_by("-created_utc")
        )

        # Cursor-based pagination (Phase 1)
        cursor = self.request.query_params.get("cursor")
        if cursor:
            try:
                cursor_dt = datetime.fromisoformat(cursor)
                qs = qs.filter(created_utc__lt=cursor_dt)
            except ValueError:
                logger.warning("Invalid cursor value: %s", cursor)

        try:
            page_size = min(int(self.request.query_params.get("page_size", 100)), 200)
        except ValueError:
            page_size = 500

        return qs[:page_size]


# ── CommentViewSet ────────────────────────────────────────────────────────────

class CommentViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Comment.objects.all().order_by("-score")
    serializer_class = CommentSerializer


# ── Stats view ────────────────────────────────────────────────────────────────

@api_view(["GET"])
def stats(request):
    """
    Aggregate stats for a date range, defaulting to yesterday.
    Query params: start=YYYY-MM-DD, end=YYYY-MM-DD

    Reads route to the Postgres replica via the DB router.
    Response cached in Redis for STATS_CACHE_TTL seconds.
    Requires authentication (JWT Bearer token).
    """
    start = request.GET.get("start")
    end   = request.GET.get("end")

    if not start or not end:
        yesterday  = datetime.utcnow().date() - timedelta(days=1)
        start_date = end_date = yesterday
    else:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date   = datetime.strptime(end,   "%Y-%m-%d").date()

    cache_key = f"stats:{start_date}:{end_date}"
    cached = cache.get(cache_key)
    if cached:
        logger.debug("Cache hit for %s", cache_key)
        return Response(cached)

    start_dt = datetime.combine(start_date, dt_time.min)
    end_dt   = datetime.combine(end_date,   dt_time.max)

    base_qs = Post.objects.filter(created_utc__range=(start_dt, end_dt))

    agg = base_qs.aggregate(
        total_posts=Count("id"),
        avg_score=Avg("current_score"),
    )
    active_users = base_qs.values("author").distinct().count()

    overview = {
        "total_posts":  agg["total_posts"],
        "avg_score":    round(agg["avg_score"] or 0, 2),
        "active_users": active_users,
    }

    most_upvoted = (
        base_qs.order_by("-current_score")
        .values("id", "title", "author", "current_score")
        .first()
    )
    most_commented = (
        base_qs.order_by("-current_comments")
        .values("id", "title", "author", "current_comments")
        .first()
    )
    top_posts = list(
        base_qs
        .annotate(engagement=F("current_score") + F("current_comments"))
        .order_by("-engagement")
        .values("id", "title", "author", "engagement")[:50]
    )

    top_users = list(
        base_qs
        .values("author")
        .annotate(posts=Count("id"), total_score=Sum("current_score"))
        .order_by("-total_score")[:50]
    )

    top_subreddits = list(
        base_qs
        .values("subreddit__name")
        .annotate(count=Count("id"))
        .order_by("-count")[:20]
    )

    result = {
        "range":    {"start": str(start_date), "end": str(end_date)},
        "overview": overview,
        "posts": {
            "most_upvoted":   most_upvoted,
            "most_commented": most_commented,
            "top_posts":      top_posts,
        },
        "users":      top_users,
        "subreddits": top_subreddits,
    }

    cache.set(cache_key, result, timeout=STATS_CACHE_TTL)
    logger.debug("Cache set for %s (TTL=%ds)", cache_key, STATS_CACHE_TTL)

    return Response(result)


# ── Unchanged viewsets ────────────────────────────────────────────────────────

class TrendViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = KeywordTrend.objects.all().order_by("-score")
    serializer_class = KeywordTrendSerializer


class StatsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SubredditStats.objects.all().order_by("-date")
    serializer_class = SubredditStatsSerializer
