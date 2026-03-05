"""
processing/topic_aggregator.py
================================
Phase 2: Topic traction aggregator.

Consumes signals.normalised from Kafka and writes to topic_timeseries
in 15-minute buckets. This is the analytical foundation for:
  - Cross-platform topic detection
  - Topic traction over time charts in Grafana
  - Leading indicator detection (which platform sees topics first)

How it works:
  1. Buffer incoming signals in memory for FLUSH_INTERVAL seconds
  2. On flush: extract topics from each signal (already computed by processor)
  3. Upsert into topic_timeseries — one row per (topic, platform, bucket)
  4. Run cross-platform detection on the freshly updated buckets

Runs as a separate process alongside the main processor workers.
One instance is sufficient — it's lightweight (read + aggregate + write).

Start with:
  docker compose up -d topic-aggregator
"""

import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

FLUSH_INTERVAL   = int(os.environ.get("TOPIC_AGG_FLUSH_INTERVAL", "60"))   # seconds
BUCKET_MINUTES   = int(os.environ.get("TOPIC_AGG_BUCKET_MINUTES", "15"))   # bucket size
MIN_TOPIC_LENGTH = 2    # skip single-char topics
MAX_TOPICS_PER_SIGNAL = 8

DSN = (
    f"dbname={os.environ.get('POSTGRES_DB', 'reddit')} "
    f"user={os.environ.get('POSTGRES_USER', 'reddit')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'reddit')} "
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')}"
)

_pool: pool.ThreadedConnectionPool | None = None


def get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(minconn=1, maxconn=5, dsn=DSN)
        logger.info("Topic aggregator DB pool ready.")
    return _pool


def _bucket(dt: datetime) -> datetime:
    """Floor a datetime to the nearest BUCKET_MINUTES interval."""
    minutes = (dt.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    return dt.replace(minute=minutes, second=0, microsecond=0)


# ── In-memory accumulator ─────────────────────────────────────────────────────
# Key: (topic, platform, bucket_str)
# Value: {signal_count, sentiment_sum, sentiment_count, max_trending, total_score}
_accumulator: dict[tuple, dict] = defaultdict(lambda: {
    "signal_count":    0,
    "sentiment_sum":   0.0,
    "sentiment_count": 0,
    "max_trending":    0.0,
    "total_score":     0,
})


def _accumulate(signal: dict) -> None:
    """Add a signal's topics to the in-memory accumulator."""
    platform  = signal.get("platform", "unknown")
    topics    = signal.get("topics")

    # topics is stored as JSONB list — may arrive as list or JSON string
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except Exception:
            topics = []

    if not topics:
        return

    # Determine bucket from signal's first_seen_at or now
    ts = signal.get("first_seen_at") or signal.get("published_at")
    if ts:
        try:
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    bkt = _bucket(dt).isoformat()

    sentiment  = signal.get("sentiment_compound") or 0.0
    trending   = signal.get("trending_score") or 0.0
    raw_score  = signal.get("raw_score") or 0

    for topic in topics[:MAX_TOPICS_PER_SIGNAL]:
        topic = topic.strip().lower()
        if len(topic) < MIN_TOPIC_LENGTH:
            continue

        key = (topic, platform, bkt)
        acc = _accumulator[key]
        acc["signal_count"]    += 1
        acc["total_score"]     += raw_score
        acc["max_trending"]     = max(acc["max_trending"], trending)
        if sentiment != 0.0:
            acc["sentiment_sum"]   += sentiment
            acc["sentiment_count"] += 1


def _flush_to_db() -> int:
    """
    Write accumulated topic buckets to topic_timeseries.
    Uses UPSERT — safe to call repeatedly, adds to existing counts.
    Returns number of rows written.
    """
    if not _accumulator:
        return 0

    snapshot = dict(_accumulator)
    _accumulator.clear()

    rows = []
    for (topic, platform, bucket_str), acc in snapshot.items():
        avg_sentiment = (
            acc["sentiment_sum"] / acc["sentiment_count"]
            if acc["sentiment_count"] > 0
            else None
        )
        rows.append((
            topic,
            platform,
            bucket_str,
            acc["signal_count"],
            avg_sentiment,
            acc["max_trending"],
            acc["total_score"],
        ))

    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO topic_timeseries
                    (topic, platform, bucket, signal_count,
                     avg_sentiment, max_trending, total_score)
                VALUES %s
                ON CONFLICT (topic, platform, bucket) DO UPDATE SET
                    signal_count  = topic_timeseries.signal_count + EXCLUDED.signal_count,
                    avg_sentiment = CASE
                        WHEN EXCLUDED.avg_sentiment IS NULL
                        THEN topic_timeseries.avg_sentiment
                        WHEN topic_timeseries.avg_sentiment IS NULL
                        THEN EXCLUDED.avg_sentiment
                        ELSE (topic_timeseries.avg_sentiment + EXCLUDED.avg_sentiment) / 2
                    END,
                    max_trending  = GREATEST(topic_timeseries.max_trending, EXCLUDED.max_trending),
                    total_score   = topic_timeseries.total_score + EXCLUDED.total_score
                """,
                rows,
            )
        conn.commit()
        logger.info("topic_aggregator: flushed %d topic-bucket rows.", len(rows))
        return len(rows)
    except Exception:
        conn.rollback()
        logger.exception("topic_aggregator: flush failed.")
        return 0
    finally:
        p.putconn(conn)


# ── Cross-platform detector ───────────────────────────────────────────────────

def _detect_cross_platform_events() -> None:
    """
    Find topics that appeared on 2+ platforms in the last 2 hours.
    Writes new events to cross_platform_events.
    Runs after every DB flush.
    """
    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:

            # Step 1 — Find topics on 2+ platforms in last 2 hours
            cur.execute("""
                SELECT
                    topic,
                    array_agg(DISTINCT platform ORDER BY platform)  AS platforms,
                    COUNT(DISTINCT platform)                         AS platform_count,
                    SUM(signal_count)                               AS total_signals,
                    AVG(avg_sentiment)                              AS avg_sentiment,
                    MAX(max_trending)                               AS peak_trending,
                    MIN(bucket)                                     AS first_seen,
                    MAX(bucket)                                     AS last_seen
                FROM topic_timeseries
                WHERE bucket > NOW() - INTERVAL '2 hours'
                GROUP BY topic
                HAVING COUNT(DISTINCT platform) >= 2
                   AND SUM(signal_count) >= 3
                ORDER BY COUNT(DISTINCT platform) DESC, SUM(signal_count) DESC
                LIMIT 100
            """)
            multi_platform_topics = cur.fetchall()

            if not multi_platform_topics:
                return

            # Step 2 — For each topic, get per-platform breakdown
            for row in multi_platform_topics:
                (topic, platforms, platform_count,
                 total_signals, avg_sentiment,
                 peak_trending, first_seen, last_seen) = row

                # Get per-platform details
                cur.execute("""
                    SELECT
                        platform,
                        MIN(bucket)             AS first_seen,
                        SUM(signal_count)       AS signal_count,
                        AVG(avg_sentiment)      AS avg_sentiment,
                        MAX(max_trending)       AS peak_trending
                    FROM topic_timeseries
                    WHERE topic = %s
                      AND bucket > NOW() - INTERVAL '2 hours'
                    GROUP BY platform
                    ORDER BY MIN(bucket)
                """, (topic,))
                platform_details = cur.fetchall()

                # Build platform list ordered by first_seen
                platforms_json = [
                    {
                        "platform":   r[0],
                        "first_seen": r[1].isoformat() if r[1] else None,
                        "signal_count": r[2],
                        "avg_sentiment": round(float(r[3]), 3) if r[3] else None,
                    }
                    for r in platform_details
                ]

                sentiment_by_platform = {
                    r[0]: round(float(r[3]), 3) if r[3] else None
                    for r in platform_details
                }

                # Origin = platform with earliest first_seen
                origin = platform_details[0] if platform_details else None
                origin_platform  = origin[0] if origin else None
                origin_first_seen = origin[1] if origin else None

                # Spread = minutes between first and last platform
                if len(platform_details) >= 2:
                    t_first = platform_details[0][1]
                    t_last  = platform_details[-1][1]
                    spread_minutes = int((t_last - t_first).total_seconds() / 60)
                else:
                    spread_minutes = 0

                # Step 3 — Upsert the event (match on topic + same 2h window)
                cur.execute("""
                    INSERT INTO cross_platform_events (
                        topic, detected_at,
                        platforms, origin_platform, origin_first_seen,
                        spread_minutes, avg_sentiment,
                        sentiment_by_platform, peak_signal_count,
                        is_active
                    )
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, TRUE)
                    ON CONFLICT DO NOTHING
                """, (
                    topic,
                    json.dumps(platforms_json),
                    origin_platform,
                    origin_first_seen,
                    spread_minutes,
                    round(float(avg_sentiment), 3) if avg_sentiment else None,
                    json.dumps(sentiment_by_platform),
                    int(total_signals),
                ))

        conn.commit()
        logger.info(
            "cross_platform_detector: found %d multi-platform topics.",
            len(multi_platform_topics),
        )

    except Exception:
        conn.rollback()
        logger.exception("cross_platform_detector failed.")
    finally:
        p.putconn(conn)


# ── Main loop ─────────────────────────────────────────────────────────────────

def _poll_signals_from_db(since: datetime) -> list[dict]:
    """
    Fetch signals that have been NLP-enriched (topics populated) since `since`.
    The main_processor writes topics to the signals table — we read from there
    rather than from Kafka where messages are NOT yet enriched.
    """
    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, platform, topics,
                    first_seen_at, published_at,
                    sentiment_compound, trending_score, raw_score
                FROM signals
                WHERE last_updated_at >= %s
                  AND topics IS NOT NULL
                  AND jsonb_array_length(topics) > 0
                ORDER BY last_updated_at
                LIMIT 5000
            """, (since,))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        logger.exception("_poll_signals_from_db failed.")
        return []
    finally:
        p.putconn(conn)


def run_aggregator() -> None:
    logger.info("Topic aggregator starting. bucket=%dmin flush=%ds",
                BUCKET_MINUTES, FLUSH_INTERVAL)

    # Poll the signals table (where topics are written by main_processor's NLP
    # enrichment step) rather than reading raw Kafka messages which arrive
    # BEFORE topic extraction has run.
    poll_since = datetime.now(timezone.utc) - timedelta(minutes=BUCKET_MINUTES)

    while True:
        cycle_start = time.monotonic()

        signals = _poll_signals_from_db(poll_since)
        poll_since = datetime.now(timezone.utc)   # advance watermark

        for sig in signals:
            _accumulate(sig)

        rows_written = _flush_to_db()
        if rows_written > 0:
            _detect_cross_platform_events()

        logger.info("Aggregator cycle: %d signals → %d topic rows", len(signals), rows_written)

        # Sleep for the remainder of FLUSH_INTERVAL
        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, FLUSH_INTERVAL - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    run_aggregator()
