"""
apps/signals/views.py
======================
Replaces: apps/reddit/views.py entirely.

All endpoints live here — Reddit-specific ones now just filter
by platform="reddit" on the unified Signal model.

Endpoints:
  /api/v1/             — health + home
  /api/v1/signals/     — filtered feed (replaces /api/posts/)
  /api/v1/pulse/       — topic cross-platform summary
  /api/v1/trending/    — trending topics across platforms
  /api/v1/compare/     — platform divergence events
  /api/v1/stats/       — aggregate stats (replaces /api/stats/)
  /api/v1/stats/timeline/ — activity timeline
  /api/v1/stats/keywords/ — keyword frequency
"""

import logging
from datetime import datetime, timedelta, timezone
from statistics import mean

from django.core.cache import cache
from django.db import connection
from django.db.models import (
    Avg, Count, ExpressionWrapper, F, FloatField, Max, Q, Sum
)
from django.db.models.functions import Extract, Now

from rest_framework import status, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Signal, Community, PlatformDivergence, SourceConfig
from .serializers import (
    SignalSerializer, CommunitySerializer,
    PlatformDivergenceSerializer, SourceConfigSerializer,
)

logger = logging.getLogger(__name__)

CACHE_TTL_SIGNALS  = 30
CACHE_TTL_PULSE    = 60
CACHE_TTL_TRENDING = 60
CACHE_TTL_STATS    = 30
CACHE_TTL_TIMELINE = 15
CACHE_TTL_KEYWORDS = 30


def _now():
    return datetime.now(timezone.utc)


def _since(minutes: int):
    return _now() - timedelta(minutes=minutes)


def _momentum_label(velocity: float) -> str:
    if velocity > 2.0:  return "rising"
    if velocity < -1.0: return "falling"
    return "stable"


# ── Unauthenticated ───────────────────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    return Response({"status": "ok"})


@api_view(["GET"])
@permission_classes([AllowAny])
def home(request):
    return Response({"status": "SignalFlow API", "version": "3.0"})


# ── Signal feed (replaces PostViewSet) ───────────────────────────────────────

class SignalViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/v1/signals/
    GET /api/v1/signals/{id}/

    Replaces /api/posts/ — now covers all 4 platforms.
    Filter to Reddit only with ?platform=reddit to replicate old behaviour.

    Query params:
      platform    reddit / hackernews / bluesky / youtube (comma-separated for multiple)
      community   r/technology / Fireship / hackernews etc.
      topics      comma-separated entity topics: openai,kafka
      keywords    full-text search in title + body
      trending    true — only trending signals
      min_score   minimum normalised_score 0.0–1.0
      start       ISO datetime
      end         ISO datetime
      cursor      pagination cursor (published_at ISO)
      page_size   default 50, max 200
    """
    serializer_class = SignalSerializer

    def get_queryset(self):
        qs = (
            Signal.objects
            .select_related("community")
            .annotate(
                engagement_score=ExpressionWrapper(
                    F("raw_score") + (F("comment_count") * 2),
                    output_field=FloatField(),
                ),
                age_minutes=ExpressionWrapper(
                    Extract(Now() - F("published_at"), "epoch") / 60.0,
                    output_field=FloatField(),
                ),
                momentum=ExpressionWrapper(
                    (F("raw_score") + (F("comment_count") * 2)) /
                    (Extract(Now() - F("published_at"), "epoch") / 60.0 + 1.0),
                    output_field=FloatField(),
                ),
            )
            .filter(published_at__isnull=False)
            .order_by("-published_at")
        )

        p = self.request.query_params

        # Platform filter
        platforms = p.get("platform")
        if platforms:
            qs = qs.filter(platform__in=platforms.split(","))

        # Community filter
        community = p.get("community")
        if community:
            qs = qs.filter(community__name__iexact=community)

        # Topic filter
        topics = p.get("topics")
        if topics:
            q = Q()
            for t in topics.split(","):
                q |= Q(topics__contains=[t.lower().strip()])
            qs = qs.filter(q)

        # Full-text keyword search
        keywords = p.get("keywords")
        if keywords:
            qs = qs.filter(
                Q(title__icontains=keywords) | Q(body__icontains=keywords)
            )

        # Trending only
        if p.get("trending") == "true":
            qs = qs.filter(is_trending=True)

        # Minimum normalised score
        min_score = p.get("min_score")
        if min_score:
            try:
                qs = qs.filter(normalised_score__gte=float(min_score))
            except ValueError:
                pass

        # Time range
        if p.get("start"):
            qs = qs.filter(published_at__gte=p["start"])
        if p.get("end"):
            qs = qs.filter(published_at__lte=p["end"])

        # Cursor pagination
        cursor = p.get("cursor")
        if cursor:
            try:
                qs = qs.filter(published_at__lt=cursor)
            except Exception:
                pass

        page_size = min(int(p.get("page_size", 50)), 200)
        return qs[:page_size]


# ── Pulse — topic summary ─────────────────────────────────────────────────────

class PulseView(APIView):
    """
    GET /api/v1/pulse/?topic=openai&window=60

    Cross-platform sentiment summary for one topic.
    The core product endpoint.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        topic  = request.query_params.get("topic", "").lower().strip()
        window = int(request.query_params.get("window", 60))

        if not topic:
            return Response(
                {"error": "topic parameter is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        cache_key = f"pulse:{topic}:{window}"
        cached = cache.get(cache_key)
        if cached:
            return Response(cached)

        since = _since(window)
        now   = _now()

        qs = Signal.objects.filter(
            topics__contains=[topic],
            published_at__gte=since,
            sentiment_compound__isnull=False,
        )

        total = qs.count()
        if total == 0:
            return Response({
                "topic": topic, "as_of": now,
                "overall_sentiment": None, "overall_momentum": "stable",
                "signal_count": 0, "platforms": {}, "divergence": {"score": 0.0, "alert": False},
            })

        platform_stats = (
            qs.values("platform")
            .annotate(
                avg_sentiment=Avg("sentiment_compound"),
                signal_count =Count("id"),
                avg_velocity =Avg("score_velocity"),
            )
        )

        platforms_data = {}
        sentiments     = []

        for stat in platform_stats:
            p        = stat["platform"]
            avg_sent = round(stat["avg_sentiment"] or 0.0, 4)
            sentiments.append(avg_sent)
            top = (
                qs.filter(platform=p)
                .order_by("-raw_score")
                .values("id", "title", "url", "raw_score")
                .first()
            )
            platforms_data[p] = {
                "avg_sentiment": avg_sent,
                "signal_count":  stat["signal_count"],
                "momentum":      _momentum_label(stat["avg_velocity"] or 0.0),
                "top_signal":    top,
            }

        overall_sentiment = round(mean(sentiments), 4) if sentiments else None

        divergence_score = 0.0
        divergence_alert = False
        interpretation   = "Only one platform has data"
        if len(sentiments) >= 2:
            divergence_score = round(max(sentiments) - min(sentiments), 4)
            divergence_alert = divergence_score >= 0.3
            if divergence_alert:
                pos = max(platforms_data, key=lambda x: platforms_data[x]["avg_sentiment"])
                neg = min(platforms_data, key=lambda x: platforms_data[x]["avg_sentiment"])
                interpretation = f"{pos} significantly more positive than {neg}"
            else:
                interpretation = "Platforms broadly agree"

        # Momentum from volume split
        half     = window // 2
        recent   = qs.filter(published_at__gte=_since(half)).count()
        older    = total - recent
        if recent > older * 1.5:   overall_momentum = "rising"
        elif recent < older * 0.5: overall_momentum = "falling"
        else:                      overall_momentum = "stable"

        result = {
            "topic": topic, "as_of": now, "window_minutes": window,
            "overall_sentiment": overall_sentiment,
            "overall_momentum":  overall_momentum,
            "signal_count":      total,
            "platforms":         platforms_data,
            "divergence": {
                "score":          divergence_score,
                "alert":          divergence_alert,
                "interpretation": interpretation,
            },
        }
        cache.set(cache_key, result, CACHE_TTL_PULSE)
        return Response(result)


# ── Trending ──────────────────────────────────────────────────────────────────

class TrendingView(APIView):
    """
    GET /api/v1/trending/?platform=all&window=60&limit=20

    Topics trending right now, ranked by signal velocity × platform spread.
    Cross-platform topics rank higher than single-platform ones.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        platform = request.query_params.get("platform", "all")
        window   = int(request.query_params.get("window", 60))
        limit    = min(int(request.query_params.get("limit", 20)), 50)
        since    = _since(window)

        cache_key = f"trending:{platform}:{window}:{limit}"
        cached = cache.get(cache_key)
        if cached:
            return Response(cached)

        qs = Signal.objects.filter(published_at__gte=since).exclude(topics=[])
        if platform != "all":
            qs = qs.filter(platform=platform)

        signals = qs.values(
            "id", "platform", "topics", "sentiment_compound",
            "score_velocity", "raw_score", "published_at", "title", "url"
        )

        topic_map: dict = {}
        for sig in signals:
            for topic in (sig["topics"] or []):
                if topic not in topic_map:
                    topic_map[topic] = {
                        "topic":          topic,
                        "signal_count":   0,
                        "platforms":      set(),
                        "sentiments":     [],
                        "velocities":     [],
                        "sample_signals": [],
                    }
                d = topic_map[topic]
                d["signal_count"] += 1
                d["platforms"].add(sig["platform"])
                if sig["sentiment_compound"] is not None:
                    d["sentiments"].append(sig["sentiment_compound"])
                if sig["score_velocity"] is not None:
                    d["velocities"].append(sig["score_velocity"])
                if len(d["sample_signals"]) < 2:
                    d["sample_signals"].append({
                        "id": sig["id"], "title": sig["title"],
                        "url": sig["url"], "platform": sig["platform"],
                    })

        scored = []
        for topic, d in topic_map.items():
            if d["signal_count"] < 2:
                continue
            spread       = len(d["platforms"])
            avg_velocity = mean(d["velocities"]) if d["velocities"] else 0.0
            avg_sentiment= round(mean(d["sentiments"]), 4) if d["sentiments"] else 0.0
            trend_score  = d["signal_count"] * spread * max(avg_velocity + 1, 0.1)
            scored.append({
                "topic":          topic,
                "signal_count":   d["signal_count"],
                "platform_count": spread,
                "platforms":      list(d["platforms"]),
                "avg_sentiment":  avg_sentiment,
                "avg_velocity":   round(avg_velocity, 4),
                "trend_score":    round(trend_score, 2),
                "cross_platform": spread > 1,
                "sample_signals": d["sample_signals"],
            })

        scored.sort(key=lambda x: x["trend_score"], reverse=True)
        result = {
            "window_minutes": window,
            "platform":       platform,
            "generated_at":   _now(),
            "topics":         scored[:limit],
        }
        cache.set(cache_key, result, CACHE_TTL_TRENDING)
        return Response(result)


# ── Compare — divergence events ───────────────────────────────────────────────

class CompareView(APIView):
    """
    GET /api/v1/compare/?topic=openai&platform_a=reddit&platform_b=hackernews
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        p          = request.query_params
        hours      = int(p.get("hours", 24))
        since      = _now() - timedelta(hours=hours)
        show_resolved = p.get("resolved", "false") == "true"

        qs = PlatformDivergence.objects.filter(detected_at__gte=since)

        topic = p.get("topic", "").lower().strip()
        if topic:
            qs = qs.filter(topic=topic)

        pa = p.get("platform_a")
        pb = p.get("platform_b")
        if pa and pb:
            qs = qs.filter(
                Q(platform_a=pa, platform_b=pb) | Q(platform_a=pb, platform_b=pa)
            )

        if not show_resolved:
            qs = qs.filter(is_resolved=False)

        events = list(qs.order_by("-divergence_score")[:100])
        avg_div = mean([e.divergence_score for e in events]) if events else 0.0
        max_div = max([e.divergence_score for e in events], default=0.0)
        topic_counts: dict = {}
        for e in events:
            topic_counts[e.topic] = topic_counts.get(e.topic, 0) + 1
        hottest = max(topic_counts, key=topic_counts.get) if topic_counts else None

        return Response({
            "query":   {"topic": topic or "all", "platform_a": pa or "all",
                        "platform_b": pb or "all", "hours": hours},
            "summary": {"total_events": len(events), "avg_divergence": round(avg_div, 4),
                        "max_divergence": round(max_div, 4), "hottest_topic": hottest},
            "events":  PlatformDivergenceSerializer(events, many=True).data,
        })


# ── Stats (replaces /api/stats/) ──────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def stats(request):
    """
    GET /api/v1/stats/?platform=reddit&start=2026-01-01&end=2026-01-31

    Replaces /api/stats/ — now works across all platforms.
    Filter to ?platform=reddit to replicate original behaviour.
    """
    p     = request.query_params
    start = p.get("start")
    end   = p.get("end")
    platform = p.get("platform")

    if not start or not end:
        yesterday = datetime.utcnow().date() - timedelta(days=1)
        start = end = str(yesterday)

    cache_key = f"stats:{platform}:{start}:{end}"
    cached = cache.get(cache_key)
    if cached:
        return Response(cached)

    qs = Signal.objects.filter(published_at__date__range=(start, end))
    if platform:
        qs = qs.filter(platform=platform)

    agg = qs.aggregate(
        total_signals=Count("id"),
        avg_score=Avg("raw_score"),
        avg_sentiment=Avg("sentiment_compound"),
    )

    top_by_score = (
        qs.order_by("-raw_score")
        .values("id", "title", "platform", "author", "raw_score", "community__name")
        .first()
    )
    top_by_comments = (
        qs.order_by("-comment_count")
        .values("id", "title", "platform", "author", "comment_count", "community__name")
        .first()
    )
    top_authors = list(
        qs.values("author", "platform")
        .annotate(signals=Count("id"), total_score=Sum("raw_score"))
        .order_by("-total_score")[:20]
    )
    by_platform = list(
        qs.values("platform")
        .annotate(count=Count("id"), avg_sentiment=Avg("sentiment_compound"))
        .order_by("-count")
    )

    result = {
        "range":       {"start": start, "end": end, "platform": platform or "all"},
        "overview":    {
            "total_signals": agg["total_signals"],
            "avg_score":     round(agg["avg_score"] or 0, 2),
            "avg_sentiment": round(agg["avg_sentiment"] or 0, 4),
        },
        "top_signals": {"most_scored": top_by_score, "most_discussed": top_by_comments},
        "top_authors": top_authors,
        "by_platform": by_platform,
    }
    cache.set(cache_key, result, CACHE_TTL_STATS)
    return Response(result)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def stats_timeline(request):
    """
    GET /api/v1/stats/timeline/?hours=24&platform=reddit

    Replaces /api/stats/timeline/ — buckets signal_metrics_history.captured_at
    by hour. Works for all platforms; filter with ?platform=reddit.
    """
    try:
        hours = max(0.5, min(float(request.query_params.get("hours", 24)), 168))
    except (ValueError, TypeError):
        hours = 24

    platform = request.query_params.get("platform", "all")
    cache_key = f"timeline:{platform}:{hours}"
    cached = cache.get(cache_key)
    if cached:
        return Response(cached)

    cutoff = datetime.utcnow() - timedelta(hours=hours)

    platform_filter = "AND s.platform = %s" if platform != "all" else ""
    params = [cutoff, platform] if platform != "all" else [cutoff]

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                date_trunc('hour', h.captured_at) AS hour,
                s.platform,
                COUNT(DISTINCT h.signal_id)        AS signals,
                ROUND(AVG(h.raw_score)::numeric, 2) AS avg_score,
                ROUND(AVG(h.sentiment_compound)::numeric, 4) AS avg_sentiment
            FROM signal_metrics_history h
            JOIN signals s ON s.id = h.signal_id
            WHERE h.captured_at >= %s
            {platform_filter}
            GROUP BY 1, 2
            ORDER BY 1
            """,
            params,
        )
        rows = cur.fetchall()

    result = [
        {
            "hour":          row[0].isoformat() if row[0] else None,
            "label":         row[0].strftime("%H:%M") if row[0] else "",
            "platform":      row[1],
            "signals":       row[2],
            "avg_score":     float(row[3]) if row[3] else 0.0,
            "avg_sentiment": float(row[4]) if row[4] else 0.0,
        }
        for row in rows
    ]
    cache.set(cache_key, result, CACHE_TTL_TIMELINE)
    return Response(result)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def stats_keywords(request):
    """
    GET /api/v1/stats/keywords/?hours=6&platform=reddit

    Replaces /api/stats/keywords/ — keyword frequency from signals.keywords JSONB.
    """
    try:
        hours = max(0.5, min(float(request.query_params.get("hours", 6)), 168))
    except (ValueError, TypeError):
        hours = 6

    platform = request.query_params.get("platform", "all")
    cache_key = f"keywords:{platform}:{hours}"
    cached = cache.get(cache_key)
    if cached:
        return Response(cached)

    cutoff = datetime.utcnow() - timedelta(hours=hours)
    platform_filter = "AND s.platform = %s" if platform != "all" else ""
    params = [cutoff, platform] if platform != "all" else [cutoff]

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT kw.word, COUNT(*) AS freq
            FROM signals s
            CROSS JOIN LATERAL jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(s.keywords) = 'array'
                     THEN s.keywords ELSE '[]'::jsonb END
            ) AS kw(word)
            WHERE s.first_seen_at >= %s
              {platform_filter}
              AND length(kw.word) > 2
            GROUP BY kw.word
            ORDER BY freq DESC
            LIMIT 50
            """,
            params,
        )
        rows = cur.fetchall()

    result = [{"word": row[0], "count": row[1]} for row in rows]
    cache.set(cache_key, result, CACHE_TTL_KEYWORDS)
    return Response(result)
