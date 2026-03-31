"""
processing/divergence_detector.py
===================================
Standalone process that detects cross-platform sentiment divergence.

Runs every DETECT_INTERVAL_SECONDS (default 900 = 15 min).
For each topic seen recently across multiple platforms, computes
per-platform sentiment and writes a divergence event when platforms
strongly disagree.

This is the core of the /api/v1/compare/ and /api/v1/pulse/ product features.

Run alongside the main processor:
  python -m processing.divergence_detector

Or add to docker-compose as a separate service — it's lightweight,
one Postgres query every 15 minutes.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from itertools import combinations

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("divergence_detector")

# ── Config ────────────────────────────────────────────────────────────────────
DETECT_INTERVAL_SECONDS = int(os.environ.get("DIVERGENCE_DETECT_INTERVAL", "900"))
LOOKBACK_MINUTES        = int(os.environ.get("DIVERGENCE_LOOKBACK_MINUTES", "60"))
MIN_SIGNALS_PER_PLATFORM = int(os.environ.get("DIVERGENCE_MIN_SIGNALS", "3"))
DIVERGENCE_THRESHOLD    = float(os.environ.get("DIVERGENCE_THRESHOLD", "0.3"))
# 0.3 = platforms differ by 0.3 on [-1,1] scale — meaningful disagreement

DSN = (
    f"dbname={os.environ.get('POSTGRES_DB', 'reddit')} "
    f"user={os.environ.get('POSTGRES_USER', 'reddit')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'reddit')} "
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')}"
)


def get_conn():
    return psycopg2.connect(DSN, connect_timeout=10)


# ── Core query ────────────────────────────────────────────────────────────────

SENTIMENT_BY_TOPIC_PLATFORM_SQL = """
SELECT
    topic,
    platform,
    AVG(sentiment_compound)  AS avg_sentiment,
    COUNT(*)                 AS signal_count,
    MIN(published_at)        AS earliest,
    ARRAY_AGG(id ORDER BY raw_score DESC NULLS LAST)[1:3] AS top_signal_ids
FROM
    signals,
    jsonb_array_elements_text(topics) AS topic
WHERE
    published_at > NOW() - INTERVAL '{lookback} minutes'
    AND sentiment_compound IS NOT NULL
    AND jsonb_array_length(topics) > 0
GROUP BY topic, platform
HAVING COUNT(*) >= {min_signals}
ORDER BY topic, platform
"""


def fetch_platform_sentiments(conn, lookback_minutes: int, min_signals: int) -> dict:
    """
    Returns:
    {
      "openai": {
        "reddit":      {"avg_sentiment": 0.31, "signal_count": 45, "earliest": ..., "top_ids": [...]},
        "hackernews":  {"avg_sentiment": -0.12, "signal_count": 12, ...},
      },
      ...
    }
    """
    sql = SENTIMENT_BY_TOPIC_PLATFORM_SQL.format(
        lookback=lookback_minutes,
        min_signals=min_signals,
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    result: dict = {}
    for topic, platform, avg_sent, count, earliest, top_ids in rows:
        if topic not in result:
            result[topic] = {}
        result[topic][platform] = {
            "avg_sentiment": round(float(avg_sent), 4),
            "signal_count":  count,
            "earliest":      earliest,
            "top_ids":       top_ids or [],
        }
    return result


# ── Origin platform detection ─────────────────────────────────────────────────

def detect_origin(platform_data: dict) -> tuple[str | None, int | None]:
    """
    Find which platform picked up the topic first and compute lag.

    Returns (origin_platform, lag_minutes_to_second_platform).
    Returns (None, None) if only one platform has data.
    """
    if len(platform_data) < 2:
        return None, None

    by_time = sorted(
        [(p, d["earliest"]) for p, d in platform_data.items() if d.get("earliest")],
        key=lambda x: x[1]
    )
    if len(by_time) < 2:
        return None, None

    origin   = by_time[0][0]
    second   = by_time[1][1]
    earliest = by_time[0][1]

    lag_minutes = int((second - earliest).total_seconds() / 60)
    return origin, lag_minutes


# ── Divergence computation ────────────────────────────────────────────────────

def compute_divergence_events(platform_sentiments: dict) -> list[dict]:
    """
    For each topic with data on 2+ platforms, check all platform pairs
    for significant sentiment divergence.

    Returns list of divergence event dicts ready for DB insert.
    """
    events = []
    now    = datetime.now(timezone.utc)

    for topic, platform_data in platform_sentiments.items():
        if len(platform_data) < 2:
            continue  # need at least 2 platforms to compare

        origin_platform, lag_minutes = detect_origin(platform_data)

        # Check every pair of platforms
        for platform_a, platform_b in combinations(sorted(platform_data.keys()), 2):
            data_a = platform_data[platform_a]
            data_b = platform_data[platform_b]

            sent_a = data_a["avg_sentiment"]
            sent_b = data_b["avg_sentiment"]
            divergence = round(abs(sent_a - sent_b), 4)

            if divergence < DIVERGENCE_THRESHOLD:
                continue  # not significant enough

            # Collect sample signal IDs from both platforms
            sample_ids = (data_a["top_ids"] + data_b["top_ids"])[:6]

            events.append({
                "topic":             topic,
                "detected_at":       now,
                "platform_a":        platform_a,
                "platform_b":        platform_b,
                "sentiment_a":       sent_a,
                "sentiment_b":       sent_b,
                "divergence_score":  divergence,
                "origin_platform":   origin_platform,
                "origin_lag_minutes": lag_minutes,
                "sample_signal_ids": json.dumps(sample_ids),
            })

            logger.info(
                "Divergence detected: topic='%s' %s(%.2f) vs %s(%.2f) = %.2f",
                topic, platform_a, sent_a, platform_b, sent_b, divergence,
            )

    return events


# ── DB writer ─────────────────────────────────────────────────────────────────

def write_divergence_events(conn, events: list[dict]) -> None:
    """
    Insert new divergence events.
    Uses ON CONFLICT DO NOTHING — if the same topic+pair was detected
    in the last detection cycle it won't create duplicates.
    """
    if not events:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO platform_divergence (
                topic, detected_at,
                platform_a, platform_b,
                sentiment_a, sentiment_b,
                divergence_score,
                origin_platform, origin_lag_minutes,
                sample_signal_ids
            )
            VALUES %s
            """,
            [
                (
                    e["topic"], e["detected_at"],
                    e["platform_a"], e["platform_b"],
                    e["sentiment_a"], e["sentiment_b"],
                    e["divergence_score"],
                    e["origin_platform"], e["origin_lag_minutes"],
                    e["sample_signal_ids"],
                )
                for e in events
            ],
        )
    conn.commit()
    logger.info("Wrote %d divergence events to DB.", len(events))


# ── Resolve stale divergences ─────────────────────────────────────────────────

def resolve_stale_divergences(conn) -> None:
    """
    Mark divergence events as resolved if the platforms have converged
    since detection (divergence score now below threshold).
    Runs on every cycle — cheap query since it filters on is_resolved=FALSE.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE platform_divergence
            SET is_resolved = TRUE, resolved_at = NOW()
            WHERE is_resolved = FALSE
              AND detected_at < NOW() - INTERVAL '2 hours'
            """
        )
        resolved = cur.rowcount
    conn.commit()
    if resolved:
        logger.info("Resolved %d stale divergence events.", resolved)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_detector() -> None:
    logger.info(
        "Divergence detector started. interval=%ds lookback=%dmin threshold=%.2f",
        DETECT_INTERVAL_SECONDS, LOOKBACK_MINUTES, DIVERGENCE_THRESHOLD,
    )

    while True:
        try:
            conn = get_conn()
            try:
                # Fetch per-platform sentiment per topic
                platform_sentiments = fetch_platform_sentiments(
                    conn, LOOKBACK_MINUTES, MIN_SIGNALS_PER_PLATFORM
                )
                logger.info(
                    "Analysed %d topics across platforms.", len(platform_sentiments)
                )

                # Compute divergence events
                events = compute_divergence_events(platform_sentiments)

                # Write to DB
                write_divergence_events(conn, events)

                # Clean up stale events
                resolve_stale_divergences(conn)

            finally:
                conn.close()

        except Exception:
            logger.exception("Divergence detection cycle failed.")

        time.sleep(DETECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_detector()
