import re
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

from .models import Signal, Community, PlatformDivergence, SourceConfig, TopicSummary, WatchedTopic, AlertDelivery
from .serializers import (
    SignalSerializer, CommunitySerializer,
    PlatformDivergenceSerializer, SourceConfigSerializer,
)

logger = logging.getLogger(__name__)

CACHE_TTL_SIGNALS    = 30
CACHE_TTL_PULSE      = 300
CACHE_TTL_TRENDING   = 120
CACHE_TTL_STATS      = 300
CACHE_TTL_TIMELINE   = 15
CACHE_TTL_KEYWORDS   = 30
CACHE_TTL_DIVERGENCE_LB = 60   # divergence leaderboard — 1 min


def _now():
    return datetime.now(timezone.utc)


def _since(minutes: int):
    return _now() - timedelta(minutes=minutes)


def _momentum_label(velocity: float) -> str:
    if velocity > 2.0:  return "rising"
    if velocity < -1.0: return "falling"
    return "stable"



def _get_intelligence(topic: str, window_minutes: int) -> dict | None:
    """
    Fetch the most recent LLM-generated summary for a topic.
    Enriches dominant_narrative with source attribution — e.g.
    "Driven by Reddit · <narrative>" — so consumers know which
    platform is shaping the discourse without a separate query.
    Never raises — intelligence is enrichment, not load-bearing.
    """
    try:
        summary = (
            TopicSummary.objects
            .filter(topic__iexact=topic)
            .order_by("-generated_at")
            .first()
        )
        if not summary:
            return None

        # ── Addition 4: source attribution ──────────────────────────────────
        # Determine the dominant platform by signal volume in the window.
        # We do this in Python to avoid a second DB round-trip — signals are
        # already available in the caller's queryset, but _get_intelligence
        # is called from multiple places so we keep it self-contained with
        # a lightweight aggregation query.
        narrative = summary.dominant_narrative
        try:
            since = _since(window_minutes)
            platform_volumes = (
                Signal.objects
                .filter(topics__contains=[topic], published_at__gte=since)
                .values("platform")
                .annotate(n=Count("id"))
                .order_by("-n")
            )
            if platform_volumes:
                top_platform = platform_volumes[0]["platform"]
                total        = sum(r["n"] for r in platform_volumes)
                top_n        = platform_volumes[0]["n"]
                share        = top_n / total if total else 0
                # Only badge if one platform contributes ≥ 50 % of volume
                # so we don't mislead on evenly distributed topics.
                if share >= 0.5 and narrative:
                    platform_label = {
                        "reddit":      "Reddit",
                        "hackernews":  "Hacker News",
                        "bluesky":     "Bluesky",
                        "youtube":     "YouTube",
                    }.get(top_platform, top_platform.title())
                    narrative = f"{platform_label}-dominant · {narrative}"
        except Exception as attr_exc:
            logger.debug("source attribution failed for %r: %s", topic, attr_exc)
        # ── end addition 4 ───────────────────────────────────────────────────

        return {
            "summary":                summary.summary_text,
            "dominant_narrative":     narrative,
            "emerging_angle":         summary.emerging_angle,
            "divergence_explanation": summary.divergence_explanation,
            "generated_at":           summary.generated_at,
            "model":                  summary.model_used,
        }
    except Exception as exc:
        logger.warning("_get_intelligence failed for %r: %s", topic, exc)
        return None


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
    permission_classes = [AllowAny]

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
                "topic":             topic,
                "as_of":             now,
                "window_minutes":    window,
                "overall_sentiment": None,
                "overall_momentum":  "stable",
                "signal_count":      0,
                "platforms":         {},
                "divergence":        {"score": 0.0, "alert": False, "interpretation": "No data"},
                "intelligence":      _get_intelligence(topic, window),
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

            # Top 3 signals per platform
            # Reddit: exclude comments, show posts only
            # YouTube/Bluesky/HN: comments are primary content, keep them
            top_qs = (
                qs.filter(platform=p)
                .exclude(url="")
                .exclude(title="", body="")
            )
            if p == "reddit":
                top_qs = top_qs.exclude(extra__is_comment=True)
            top_qs = (
                top_qs
                .order_by(
                    F("trending_score").desc(nulls_last=True),
                    F("raw_score").desc(nulls_last=True),
                )
                .values("id", "title", "body", "url", "raw_score",
                        "trending_score", "author", "published_at")[:3]
            )
            top_signals = []
            for sig in top_qs:
                sig = dict(sig)
                if sig.get("id", "").startswith("bluesky:CBORTag"):
                    url = sig.get("url", "")
                    sig["id"] = url.replace("https://bsky.app/", "bsky://") if url else sig["id"]
                if not sig.get("title") and sig.get("body"):
                    sig["title"] = sig["body"][:120]
                top_signals.append(sig)

            avg_vel = stat["avg_velocity"] or 0.0
            momentum_label = _momentum_label(avg_vel)
            if momentum_label == "rising":
                momentum_detail = f"rising — volume accelerating (velocity: +{avg_vel:.2f})"
            elif momentum_label == "falling":
                momentum_detail = f"falling — volume decelerating (velocity: {avg_vel:.2f})"
            else:
                momentum_detail = "stable — volume consistent with recent average"

            platforms_data[p] = {
                "avg_sentiment":   avg_sent,
                "signal_count":    stat["signal_count"],
                "momentum":        momentum_label,
                "momentum_detail": momentum_detail,
                "top_signals":     top_signals,
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
            "intelligence":      _get_intelligence(topic, window),
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
    permission_classes = [AllowAny]

    def get(self, request):
        platform = request.query_params.get("platform", "all")
        window = int(request.query_params.get("window_minutes", request.query_params.get("window", 240)))
        limit = min(int(request.query_params.get("limit", 20)), 100)
        since    = _since(window)

        cache_key = f"trending:{platform}:{window}:{limit}"
        cached = cache.get(cache_key)
        if cached:
            return Response(cached)

        # ── Fast path: windows >= 24h read from topic_timeseries, not signals ──
        # Avoids scanning millions of signal rows in Python for long windows.
        if window >= 1440:
            return self._trending_from_timeseries(request, platform, window, limit, cache_key)

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
                # skip hashtags, quoted fragments, hex hashes, single chars
                if not topic or topic.startswith('#') or topic.startswith('"'):
                    continue
                if len(topic) < 2:
                    continue
                if len([c for c in topic if c.isalnum()]) < 2:
                    continue
                if re.search(r'\d.*&|&.*\d|attachments|\d+ \w+ & \d+', topic):
                    continue
                if '\n' in topic or '\r' in topic:
                    continue
                topic = topic.strip()
                if topic in {'handheld electric', 'legacy indie radio', 'iembot', 'dm', 'signal'}:
                    continue
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
            if d["signal_count"] < 5:
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
        top_topics = scored[:limit]

        # ── Addition 2: 7-day sparklines ─────────────────────────────────────
        # Pull per-day signal counts from topic_timeseries for each top topic.
        # 7 data points = enough to tell "spiked today" vs "trending 3 days".
        # One query for all topics via IN — not N queries.
        if top_topics:
            topic_names = [t["topic"] for t in top_topics]
            sparkline_map: dict = {t: [] for t in topic_names}
            try:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            topic,
                            DATE_TRUNC('day', bucket)  AS day,
                            SUM(signal_count)          AS daily_count
                        FROM topic_timeseries
                        WHERE
                            bucket  > NOW() - INTERVAL '7 days'
                            AND topic = ANY(%s)
                        GROUP BY topic, day
                        ORDER BY topic, day
                        """,
                        [topic_names],
                    )
                    rows = cur.fetchall()

                # Build a dense 7-slot list (oldest → newest, 0 for missing days)
                from datetime import date
                today = _now().date()
                days  = [(today - timedelta(days=i)) for i in range(6, -1, -1)]
                day_idx = {d: i for i, d in enumerate(days)}

                for topic_name, day_dt, count in rows:
                    d = day_dt.date() if hasattr(day_dt, "date") else day_dt
                    idx = day_idx.get(d)
                    if idx is not None:
                        sl = sparkline_map.setdefault(topic_name, [0] * 7)
                        if len(sl) < 7:
                            sl.extend([0] * (7 - len(sl)))
                        sl[idx] = int(count)

                for topic_name in topic_names:
                    if sparkline_map[topic_name] == []:
                        sparkline_map[topic_name] = [0] * 7

            except Exception as spark_exc:
                logger.warning("sparkline query failed: %s", spark_exc)

            for t in top_topics:
                t["sparkline_7d"] = sparkline_map.get(t["topic"], [0] * 7)
        # ── end addition 2 ───────────────────────────────────────────────────

        result = {
            "window_minutes": window,
            "platform":       platform,
            "generated_at":   _now(),
            "topics":         top_topics,
        }
        cache.set(cache_key, result, CACHE_TTL_TRENDING)

        # Fire webhook alerts for any subscribed users — non-blocking best-effort.
        # We do this after caching so the HTTP response is already on its way.
        try:
            _dispatch_topic_alerts(top_topics)
        except Exception:
            pass

    def _trending_from_timeseries(self, request, platform, window, limit, cache_key):
        """Fast trending for 24h+ windows — reads topic_timeseries hypertable."""
        from django.db import connection
        import re

        CACHE_TTL = 300  # 5 min for 24h, still fresh enough

        pf_filter = "" if platform == "all" else "AND platform = %s"
        # Scale minimum signal threshold with window size to avoid
        # scanning millions of low-frequency topics on long windows
        min_signals = 5
        if window >= 43200:   # 30d
            min_signals = 500
        elif window >= 10080: # 7d
            min_signals = 100
        elif window >= 1440:  # 24h
            min_signals = 20

        params = [window]
        if platform != "all":
            params.append(platform)
        params.append(min_signals)
        params.append(limit)

        sql = """
            SELECT
                topic,
                COUNT(DISTINCT platform)            AS platform_count,
                array_agg(DISTINCT platform)        AS platforms,
                SUM(signal_count)                   AS signal_count,
                ROUND(AVG(avg_sentiment)::numeric, 4) AS avg_sentiment,
                MAX(max_trending)                   AS trend_score
            FROM topic_timeseries
            WHERE bucket > NOW() - (%s * INTERVAL '1 minute')
            """ + pf_filter + """
            GROUP BY topic
            HAVING SUM(signal_count) >= %s
              AND topic IS NOT NULL
              AND LENGTH(topic) >= 2
            ORDER BY SUM(signal_count) DESC
            LIMIT %s
        """

        try:
            with connection.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        except Exception as e:
            logger.error("timeseries trending query failed: %s", e)
            rows = []

        top_topics = []
        for topic, platform_count, platforms, signal_count, avg_sentiment, trend_score in rows:
            # Skip noise topics
            if not topic or topic.startswith('#') or topic.startswith('"'):
                continue
            if '\n' in topic or '\r' in topic:
                continue
            top_topics.append({
                "topic":          topic,
                "signal_count":   int(signal_count or 0),
                "platform_count": int(platform_count or 0),
                "platforms":      list(platforms or []),
                "avg_sentiment":  float(avg_sentiment or 0),
                "avg_velocity":   0.0,
                "trend_score":    float(trend_score or 0),
                "cross_platform": int(platform_count or 0) > 1,
                "sample_signals": [],
                "sparkline_7d":   [0] * 7,
            })

        # Add sparklines
        if top_topics:
            topic_names = [t["topic"] for t in top_topics]
            sparkline_map = {t: [0] * 7 for t in topic_names}
            try:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT topic,
                               DATE_TRUNC('day', bucket) AS day,
                               SUM(signal_count)         AS daily_count
                        FROM topic_timeseries
                        WHERE bucket > NOW() - INTERVAL '7 days'
                          AND topic = ANY(%s)
                        GROUP BY topic, day
                        ORDER BY topic, day
                        """,
                        [topic_names],
                    )
                    spark_rows = cur.fetchall()

                from datetime import date
                today = _now().date()
                days = [(today - timedelta(days=i)) for i in range(6, -1, -1)]
                day_idx = {d: i for i, d in enumerate(days)}

                for topic_name, day_dt, count in spark_rows:
                    d = day_dt.date() if hasattr(day_dt, "date") else day_dt
                    idx = day_idx.get(d)
                    if idx is not None:
                        sparkline_map[topic_name][idx] = int(count)

            except Exception as spark_exc:
                logger.warning("sparkline query failed: %s", spark_exc)

            for t in top_topics:
                t["sparkline_7d"] = sparkline_map.get(t["topic"], [0] * 7)

        result = {
            "window_minutes": window,
            "platform":       platform,
            "generated_at":   _now(),
            "topics":         top_topics,
        }
        cache.set(cache_key, result, CACHE_TTL)
        try:
            _dispatch_topic_alerts(top_topics)
        except Exception as alert_exc:
            logger.warning("alert dispatch failed: %s", alert_exc)
        return Response(result)


# ── Compare — divergence events ───────────────────────────────────────────────

class CompareView(APIView):
    """
    GET /api/v1/compare/?topic=openai&platform_a=reddit&platform_b=hackernews
    """
    permission_classes = [AllowAny]

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


# ── Addition 3: Divergence leaderboard ───────────────────────────────────────

class DivergenceLeaderboardView(APIView):
    """
    GET /api/v1/divergence/leaderboard/?hours=24&limit=10

    Top 10 topics with the highest cross-platform divergence score right now.
    One DB query over platform_divergence — no aggregation on the hot path.

    Returns each topic with its worst active divergence event, the two
    platforms in disagreement, and the raw scores so the consumer can
    build their own interpretation.

    This endpoint is intentionally denormalised — it's designed to be
    polled every 60 s by dashboards and media monitoring clients.
    """
    permission_classes = [AllowAny]

    def get(self, request):
        hours = int(request.query_params.get("hours", 24))
        limit = min(int(request.query_params.get("limit", 10)), 50)

        cache_key = f"divergence_lb:{hours}:{limit}"
        cached = cache.get(cache_key)
        if cached:
            return Response(cached)

        since = _now() - timedelta(hours=hours)

        # One query: per topic, pick the single worst (highest divergence_score)
        # active event.  Using a raw query to leverage DISTINCT ON which is
        # more efficient than a Python groupby on potentially large result sets.
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (topic)
                    topic,
                    divergence_score,
                    platform_a,
                    platform_b,
                    sentiment_a,
                    sentiment_b,
                    detected_at,
                    origin_platform,
                    origin_lag_minutes
                FROM platform_divergence
                WHERE
                    detected_at >= %s
                    AND is_resolved = FALSE
                ORDER BY topic, divergence_score DESC
                """,
                [since],
            )
            rows = cur.fetchall()
            cols = [
                "topic", "divergence_score", "platform_a", "platform_b",
                "sentiment_a", "sentiment_b", "detected_at",
                "origin_platform", "origin_lag_minutes",
            ]

        events = [dict(zip(cols, r)) for r in rows]
        events.sort(key=lambda e: e["divergence_score"], reverse=True)
        top    = events[:limit]

        # Classify divergence severity for consumers who don't want to
        # implement their own thresholds.
        def _severity(score: float) -> str:
            if score >= 0.7: return "high"
            if score >= 0.4: return "medium"
            return "low"

        for e in top:
            e["severity"] = _severity(e["divergence_score"])
            # Humanise: which platform is more positive?
            if e["sentiment_a"] is not None and e["sentiment_b"] is not None:
                if e["sentiment_a"] > e["sentiment_b"]:
                    e["positive_platform"] = e["platform_a"]
                    e["negative_platform"] = e["platform_b"]
                else:
                    e["positive_platform"] = e["platform_b"]
                    e["negative_platform"] = e["platform_a"]
            e["divergence_score"] = round(e["divergence_score"], 4)

        result = {
            "generated_at":  _now(),
            "window_hours":  hours,
            "total_active":  len(events),
            "leaderboard":   top,
        }
        cache.set(cache_key, result, CACHE_TTL_DIVERGENCE_LB)
        return Response(result)


# ── Addition 1: Topic alert webhooks ─────────────────────────────────────────

class TopicAlertView(APIView):
    """
    Topic webhook subscriptions.  Authenticated — requires Token header.

    POST   /api/v1/alerts/watch/
        Body: {
            "topic":           "openai",
            "webhook_url":     "https://your.service/hook",
            "min_trend_score": 50.0,    // optional, default 0
            "min_platforms":   2,        // optional, default 1
            "cooldown_minutes": 60       // optional, default 60
        }
        → 201 Created  {id, topic, webhook_url, created}
        → 200 OK       {id, topic, webhook_url, reactivated: true}  (if exists but inactive)

    DELETE /api/v1/alerts/watch/
        Body: {"topic": "openai", "webhook_url": "https://your.service/hook"}
        → 200 OK  {deactivated: true}

    GET    /api/v1/alerts/watch/
        → 200 OK  [{id, topic, webhook_url, is_active, last_fired_at, ...}]

    Webhook payload (POST to webhook_url when triggered):
        {
            "event":        "topic_alert",
            "topic":        "openai",
            "trend_score":  142.5,
            "platforms":    ["reddit", "hackernews"],
            "triggered_at": "2026-03-19T14:00:00Z"
        }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        """List the authenticated user's active subscriptions."""
        watches = (
            WatchedTopic.objects
            .filter(user=request.user, is_active=True)
            .order_by("-created_at")
            .values(
                "id", "topic", "webhook_url",
                "min_trend_score", "min_platforms", "cooldown_minutes",
                "is_active", "created_at", "last_fired_at",
            )
        )
        return Response(list(watches))

    def post(self, request):
        """Subscribe to alerts for a topic."""
        topic       = (request.data.get("topic") or "").lower().strip()
        webhook_url = (request.data.get("webhook_url") or "").strip()

        if not topic:
            return Response({"error": "topic is required"}, status=400)
        if not webhook_url or not webhook_url.startswith(("http://", "https://")):
            return Response({"error": "webhook_url must be a valid http(s) URL"}, status=400)

        min_trend  = float(request.data.get("min_trend_score", 0.0))
        min_plat   = int(request.data.get("min_platforms", 1))
        cooldown   = int(request.data.get("cooldown_minutes", 60))

        existing = WatchedTopic.objects.filter(
            user=request.user, topic=topic, webhook_url=webhook_url
        ).first()

        if existing:
            if existing.is_active:
                return Response({
                    "id":          existing.id,
                    "topic":       existing.topic,
                    "webhook_url": existing.webhook_url,
                    "message":     "Already watching this topic.",
                }, status=200)
            # Reactivate
            existing.is_active       = True
            existing.min_trend_score = min_trend
            existing.min_platforms   = min_plat
            existing.cooldown_minutes= cooldown
            existing.save()
            return Response({
                "id":          existing.id,
                "topic":       existing.topic,
                "webhook_url": existing.webhook_url,
                "reactivated": True,
            }, status=200)

        watch = WatchedTopic.objects.create(
            user=request.user,
            topic=topic,
            webhook_url=webhook_url,
            min_trend_score=min_trend,
            min_platforms=min_plat,
            cooldown_minutes=cooldown,
        )
        return Response({
            "id":          watch.id,
            "topic":       watch.topic,
            "webhook_url": watch.webhook_url,
            "created":     True,
        }, status=201)

    def delete(self, request):
        """Unsubscribe from a topic alert."""
        topic       = (request.data.get("topic") or "").lower().strip()
        webhook_url = (request.data.get("webhook_url") or "").strip()

        if not topic or not webhook_url:
            return Response({"error": "topic and webhook_url are required"}, status=400)

        updated = WatchedTopic.objects.filter(
            user=request.user, topic=topic, webhook_url=webhook_url, is_active=True
        ).update(is_active=False)

        if not updated:
            return Response({"error": "Subscription not found."}, status=404)

        return Response({"deactivated": True})


# ── Alert dispatcher — called from the trending pipeline ─────────────────────

def _dispatch_topic_alerts(trending_topics: list[dict]) -> None:
    """
    Called at the end of TrendingView (or from an async task/cron) after
    trending topics are scored.  Fires webhooks for any subscribed users
    whose threshold is met and whose cooldown has expired.

    trending_topics is the scored list from TrendingView:
        [{"topic": "openai", "trend_score": 142.5, "platforms": [...], ...}]

    This function is deliberately synchronous and fire-and-forget for the
    MVP — it uses urllib (stdlib, no extra deps) so it works inside the
    Django process.  When volume grows, move to a Celery task or the
    existing Kafka pipeline.
    """
    import urllib.request
    import json as _json

    if not trending_topics:
        return

    topic_index = {t["topic"]: t for t in trending_topics}
    topic_names = list(topic_index.keys())

    now = _now()

    # Load all active subscriptions that cover any of the trending topics
    watches = list(
        WatchedTopic.objects
        .filter(topic__in=topic_names, is_active=True)
        .select_related("user")
    )
    if not watches:
        return

    for watch in watches:
        td = topic_index[watch.topic]

        # Threshold checks
        if td["trend_score"] < watch.min_trend_score:
            continue
        if td["platform_count"] < watch.min_platforms:
            continue

        # Cooldown check
        if watch.last_fired_at:
            elapsed = (now - watch.last_fired_at).total_seconds() / 60
            if elapsed < watch.cooldown_minutes:
                continue

        # Build payload
        payload = _json.dumps({
            "event":        "topic_alert",
            "topic":        watch.topic,
            "trend_score":  td["trend_score"],
            "platform_count": td["platform_count"],
            "platforms":    td["platforms"],
            "triggered_at": now.isoformat(),
        }).encode()

        t0          = _now()
        http_status = None
        error_msg   = None
        try:
            req = urllib.request.Request(
                watch.webhook_url,
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "SignalFlow/3.0"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                http_status = resp.status
        except Exception as exc:
            error_msg = str(exc)[:200]

        latency_ms = int((_now() - t0).total_seconds() * 1000)
        success    = http_status is not None and 200 <= http_status < 300

        # Write audit record
        try:
            AlertDelivery.objects.create(
                watched_topic=watch,
                topic=watch.topic,
                trend_score=td["trend_score"],
                platform_count=td["platform_count"],
                platforms=td["platforms"],
                http_status=http_status,
                success=success,
                error_message=error_msg,
                latency_ms=latency_ms,
            )
            # Update last_fired_at regardless of success — we don't want to
            # spam a broken endpoint.  Users can check alert_deliveries.
            WatchedTopic.objects.filter(pk=watch.pk).update(last_fired_at=now)
        except Exception as db_exc:
            logger.warning("alert delivery audit write failed: %s", db_exc)

        if success:
            logger.info("alert fired: %s → %s (%d ms)", watch.topic, watch.webhook_url, latency_ms)
        else:
            logger.warning(
                "alert failed: %s → %s status=%s err=%s",
                watch.topic, watch.webhook_url, http_status, error_msg,
            )


# ── Stats (replaces /api/stats/) ──────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([AllowAny])
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
@permission_classes([AllowAny])
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
@permission_classes([AllowAny])
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


# ── Platform Totals ───────────────────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([AllowAny])
def platform_totals(request):
    """
    GET /api/v1/stats/totals/
    Returns all-time signal counts per platform + last 24h counts.
    Same source as Grafana dashboard.
    """
    cached = cache.get("platform_totals")
    if cached:
        return Response(cached)

    with connection.cursor() as cur:
        # all-time totals
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE platform='reddit') as reddit,
                COUNT(*) FILTER (WHERE platform='hackernews') as hackernews,
                COUNT(*) FILTER (WHERE platform='youtube') as youtube,
                COUNT(*) FILTER (WHERE platform='bluesky') as bluesky
            FROM signals
        """)
        row = cur.fetchone()

        # last 24h totals
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE platform='reddit') as reddit,
                COUNT(*) FILTER (WHERE platform='hackernews') as hackernews,
                COUNT(*) FILTER (WHERE platform='youtube') as youtube,
                COUNT(*) FILTER (WHERE platform='bluesky') as bluesky
            FROM signals
            WHERE last_updated_at >= NOW() - INTERVAL '24 hours'
        """)
        row24 = cur.fetchone()

    result = {
        "as_of": _now(),
        "all_time": {
            "total":      row[0],
            "reddit":     row[1],
            "hackernews": row[2],
            "youtube":    row[3],
            "bluesky":    row[4],
        },
        "last_24h": {
            "total":      row24[0],
            "reddit":     row24[1],
            "hackernews": row24[2],
            "youtube":    row24[3],
            "bluesky":    row24[4],
        },
    }
    cache.set("platform_totals", result, 300)  # cache 5 min
    return Response(result)


# ── Dashboard — single endpoint replacing 7 parallel calls ───────────────────

@api_view(["GET"])
@permission_classes([AllowAny])
def dashboard(request):
    """
    GET /api/v1/dashboard/?window=240&limit=25

    Bundles everything the homepage needs into one response:
      - platform totals (all-time + 24h)
      - trending topics with sparklines
      - pulse for the top 5 trending topics

    One call replaces 7+ parallel calls. Cached for 60 seconds.
    This endpoint exists purely to reduce frontend request count and
    eliminate 502s caused by 7 simultaneous Django worker hits on page load.
    """
    window = int(request.query_params.get("window", 240))
    limit  = min(int(request.query_params.get("limit", 25)), 50)

    cache_key = f"dashboard:{window}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return Response(cached)

    # ── 1. Platform totals ────────────────────────────────────────────────────
    totals = cache.get("platform_totals")
    if not totals:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE platform='reddit') as reddit,
                    COUNT(*) FILTER (WHERE platform='hackernews') as hackernews,
                    COUNT(*) FILTER (WHERE platform='youtube') as youtube,
                    COUNT(*) FILTER (WHERE platform='bluesky') as bluesky
                FROM signals
            """)
            row = cur.fetchone()
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE platform='reddit') as reddit,
                    COUNT(*) FILTER (WHERE platform='hackernews') as hackernews,
                    COUNT(*) FILTER (WHERE platform='youtube') as youtube,
                    COUNT(*) FILTER (WHERE platform='bluesky') as bluesky
                FROM signals
                WHERE last_updated_at >= NOW() - INTERVAL '24 hours'
            """)
            row24 = cur.fetchone()
        totals = {
            "all_time": {
                "total": row[0], "reddit": row[1],
                "hackernews": row[2], "youtube": row[3], "bluesky": row[4],
            },
            "last_24h": {
                "total": row24[0], "reddit": row24[1],
                "hackernews": row24[2], "youtube": row24[3], "bluesky": row24[4],
            },
        }
        cache.set("platform_totals", totals, 300)

    # ── 2. Trending topics ────────────────────────────────────────────────────
    trending_key = f"trending:all:{window}:{limit}"
    trending = cache.get(trending_key)
    if not trending:
        # Reuse TrendingView logic via internal request simulation
        from rest_framework.test import APIRequestFactory
        factory = APIRequestFactory()
        fake_req = factory.get(
            f"/api/v1/trending/?platform=all&window={window}&limit={limit}"
        )
        from rest_framework.request import Request
        fake_req = Request(fake_req)
        view = TrendingView()
        view.request = fake_req
        resp = view.get(fake_req)
        trending = resp.data
    top_topics = trending.get("topics", [])[:limit]

    # ── 3. Pulse for top 5 topics ─────────────────────────────────────────────
    pulse_data = {}
    for topic_entry in top_topics[:5]:
        topic = topic_entry["topic"]
        pulse_cache_key = f"pulse:{topic}:{window}"
        pulse = cache.get(pulse_cache_key)
        if pulse:
            pulse_data[topic] = pulse
            continue

        since = _since(window)
        qs = Signal.objects.filter(
            topics__contains=[topic],
            published_at__gte=since,
            sentiment_compound__isnull=False,
        )
        total = qs.count()
        if total == 0:
            pulse_data[topic] = {"signal_count": 0, "overall_sentiment": None}
            continue

        platform_stats = qs.values("platform").annotate(
            avg_sentiment=Avg("sentiment_compound"),
            signal_count=Count("id"),
            avg_velocity=Avg("score_velocity"),
        )
        platforms_data = {}
        sentiments = []
        for stat in platform_stats:
            avg_sent = round(stat["avg_sentiment"] or 0.0, 4)
            sentiments.append(avg_sent)
            platforms_data[stat["platform"]] = {
                "avg_sentiment": avg_sent,
                "signal_count":  stat["signal_count"],
                "momentum":      _momentum_label(stat["avg_velocity"] or 0.0),
            }

        overall_sentiment = round(mean(sentiments), 4) if sentiments else None
        divergence_score  = round(max(sentiments) - min(sentiments), 4) if len(sentiments) >= 2 else 0.0

        pulse_result = {
            "topic":              topic,
            "signal_count":       total,
            "overall_sentiment":  overall_sentiment,
            "platforms":          platforms_data,
            "divergence_score":   divergence_score,
            "divergence_alert":   divergence_score >= 0.3,
            "intelligence":       _get_intelligence(topic, window),
        }
        cache.set(pulse_cache_key, pulse_result, CACHE_TTL_PULSE)
        pulse_data[topic] = pulse_result

    result = {
        "generated_at": _now(),
        "window_minutes": window,
        "platform_totals": totals,
        "trending": {
            "window_minutes": window,
            "topics": top_topics,
        },
        "pulse": pulse_data,
    }
    cache.set(cache_key, result, 60)  # 60s cache — fresh enough, fast enough
    return Response(result)


# ── API Access Request ────────────────────────────────────────────────────────

@api_view(["POST"])
@permission_classes([AllowAny])
def request_api_access(request):
    """
    POST /api/v1/access/request/
    Body: {"name": "...", "email": "..."}
    Creates a Django user + DRF token, returns token immediately.
    If email already exists, returns the existing token.
    """
    name  = (request.data.get("name")  or "").strip()
    email = (request.data.get("email") or "").strip().lower()

    if not name:
        return Response({"error": "Name is required."}, status=400)
    if not email or "@" not in email:
        return Response({"error": "Valid email is required."}, status=400)

    from django.contrib.auth.models import User
    from rest_framework.authtoken.models import Token

    # get or create user
    user, created = User.objects.get_or_create(
        email=email,
        defaults={
            "username": email.split("@")[0][:30] + "_" + email.split("@")[1].split(".")[0][:10],
            "first_name": name.split()[0][:30] if name else "",
        }
    )

    # get or create token
    token, _ = Token.objects.get_or_create(user=user)

    return Response({
        "token":   token.key,
        "created": created,
        "message": "Key generated successfully." if created else "An API key already exists for this email.",
    })
