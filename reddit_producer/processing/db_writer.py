"""
processing/db_writer.py
=======================
CHANGES vs original:
  1. Added bulk_upsert_signals() — writes the common Signal schema to the
     new `signals` table. This is the primary write path for all new sources
     (HN, Bluesky, YouTube) and will eventually replace bulk_upsert_posts().

  2. Added bulk_insert_signal_metrics_history() — appends to the new
     `signal_metrics_history` hypertable (TimescaleDB).

  3. bulk_upsert_posts() and bulk_insert_metrics_history() preserved unchanged
     for backward compatibility with the Reddit-specific pipeline. These can
     be removed once the Reddit source is fully migrated to Signal schema.

  4. _ensure_community() replaces _ensure_subreddit() — same pattern but
     stores platform + community name, not just subreddit name. A subreddit
     is just a community with platform="reddit".
"""

import json
import logging
import os
import time

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

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
    if _pool is not None:
        return _pool
    delay = 5.0
    for attempt in range(1, 11):
        try:
            _pool = pool.ThreadedConnectionPool(minconn=2, maxconn=20, dsn=DSN)
            logger.info("Postgres connection pool created.")
            return _pool
        except psycopg2.OperationalError as exc:
            logger.warning("Postgres not ready (attempt %d/10): %s — retrying in %.0fs.", attempt, exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("Cannot connect to Postgres after 10 attempts.")


# ── Community cache (replaces _subreddit_id_cache) ───────────────────────────
# Key: (platform, community_name) → integer PK in communities table
_community_id_cache: dict[tuple[str, str], int] = {}


def _ensure_community(cur, platform: str, community: str) -> int:
    """
    Return the community's integer PK, inserting if it doesn't exist.
    communities table has a UNIQUE(platform, name) constraint.
    """
    key = (platform, community)
    if key in _community_id_cache:
        return _community_id_cache[key]

    cur.execute(
        """
        INSERT INTO communities (platform, name)
        VALUES (%s, %s)
        ON CONFLICT (platform, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        """,
        (platform, community),
    )
    cid = cur.fetchone()[0]
    _community_id_cache[key] = cid
    return cid


# ── Legacy: subreddit cache (used by bulk_upsert_posts) ──────────────────────
_subreddit_id_cache: dict[str, int] = {}


def _ensure_subreddit(cur, name: str) -> int:
    if name in _subreddit_id_cache:
        return _subreddit_id_cache[name]
    cur.execute(
        "INSERT INTO subreddits (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    sid = cur.fetchone()[0]
    _subreddit_id_cache[name] = sid
    return sid


# ── NEW: Unified signal writer ────────────────────────────────────────────────

def bulk_upsert_signals(signals: list[dict]) -> None:
    """
    Write normalised Signal dicts to the `signals` table.
    Works for all platforms — Reddit, HN, Bluesky, YouTube.

    The signals table uses (platform, source_id) as the logical key.
    The `id` field ("reddit:abc123") is the primary key.
    """
    if not signals:
        return

    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            rows = []
            for s in signals:
                cid = _ensure_community(cur, s["platform"], s["community"])
                rows.append((
                    s["id"],                          # "reddit:abc123"
                    s["platform"],
                    s["source_id"],                   # platform-native ID
                    cid,
                    s.get("title", ""),
                    s.get("body", ""),
                    s.get("url", ""),
                    s.get("author", ""),
                    s.get("published_at"),            # float epoch
                    s.get("raw_score", 0),
                    s.get("comment_count", 0),
                    s.get("normalised_score"),        # None until enrichment
                    s.get("score_velocity"),
                    s.get("comment_velocity"),
                    s.get("trending_score"),
                    s.get("is_trending"),
                    s.get("sentiment_compound"),
                    s.get("sentiment_label"),
                    json.dumps(s.get("keywords") or []),
                    json.dumps(s.get("topics") or []),
                    json.dumps(s.get("extra") or {}),
                    s.get("schema_version", 1),
                ))

            execute_values(
                cur,
                """
                INSERT INTO signals (
                    id, platform, source_id, community_id,
                    title, body, url, author,
                    published_at,
                    raw_score, comment_count,
                    normalised_score,
                    score_velocity, comment_velocity,
                    trending_score, is_trending,
                    sentiment_compound, sentiment_label,
                    keywords, topics, extra,
                    schema_version
                )
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    raw_score         = EXCLUDED.raw_score,
                    comment_count     = EXCLUDED.comment_count,
                    score_velocity    = COALESCE(EXCLUDED.score_velocity,    signals.score_velocity),
                    comment_velocity  = COALESCE(EXCLUDED.comment_velocity,  signals.comment_velocity),
                    trending_score    = COALESCE(EXCLUDED.trending_score,    signals.trending_score),
                    is_trending       = COALESCE(EXCLUDED.is_trending,       signals.is_trending),
                    sentiment_compound= COALESCE(EXCLUDED.sentiment_compound,signals.sentiment_compound),
                    sentiment_label   = COALESCE(EXCLUDED.sentiment_label,   signals.sentiment_label),
                    keywords          = COALESCE(EXCLUDED.keywords,          signals.keywords),
                    topics            = COALESCE(EXCLUDED.topics,            signals.topics),
                    last_updated_at   = NOW()
                """,
                rows,
                template=(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,"
                    "to_timestamp(%s),"           # published_at
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                ),
            )
        conn.commit()
        logger.debug("bulk_upsert_signals: %d rows committed.", len(signals))

    except Exception:
        conn.rollback()
        logger.exception("bulk_upsert_signals failed — batch rolled back.")
        raise
    finally:
        p.putconn(conn)


def bulk_insert_signal_metrics_history(signals: list[dict]) -> None:
    """
    Append to signal_metrics_history (TimescaleDB hypertable).
    Records raw_score and comment_count at the time of refresh.
    Works for all platforms.
    """
    if not signals:
        return

    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            rows = [
                (s["id"], s.get("raw_score", 0), s.get("comment_count", 0),
                 s.get("score_velocity"), s.get("comment_velocity"),
                 s.get("trending_score"), s.get("sentiment_compound"))
                for s in signals
            ]
            execute_values(
                cur,
                """
                INSERT INTO signal_metrics_history
                    (signal_id, raw_score, comment_count,
                     score_velocity, comment_velocity,
                     trending_score, sentiment_compound)
                VALUES %s
                """,
                rows,
            )
        conn.commit()
        logger.debug("bulk_insert_signal_metrics_history: %d rows.", len(signals))

    except Exception:
        conn.rollback()
        logger.exception("bulk_insert_signal_metrics_history failed.")
        raise
    finally:
        p.putconn(conn)


def bulk_upsert_signal_nlp(items: list[dict]) -> None:
    """
    Write NLP enrichment back to the signals table.
    Called after sentiment + keyword extraction.

    items: list of signal dicts with sentiment_compound, sentiment_label,
           keywords, topics already populated.
    """
    if not items:
        return

    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            rows = [
                (
                    s["id"],
                    s.get("sentiment_compound"),
                    s.get("sentiment_label"),
                    json.dumps(s.get("keywords") or []),
                    json.dumps(s.get("topics") or []),
                )
                for s in items
            ]
            execute_values(
                cur,
                """
                UPDATE signals SET
                    sentiment_compound = data.sentiment_compound,
                    sentiment_label    = data.sentiment_label,
                    keywords           = data.keywords::jsonb,
                    topics             = data.topics::jsonb,
                    last_updated_at    = NOW()
                FROM (VALUES %s) AS data(id, sentiment_compound, sentiment_label, keywords, topics)
                WHERE signals.id = data.id
                """,
                rows,
            )
        conn.commit()
        logger.debug("bulk_upsert_signal_nlp: %d rows.", len(items))

    except Exception:
        conn.rollback()
        logger.exception("bulk_upsert_signal_nlp failed.")
        raise
    finally:
        p.putconn(conn)


# ── Legacy Reddit writers (preserved unchanged) ───────────────────────────────

def bulk_upsert_posts(posts: list[dict]) -> None:
    """Legacy Reddit-specific writer. Preserved for backward compatibility."""
    if not posts:
        return
    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            rows = []
            for post in posts:
                sid = _ensure_subreddit(cur, post["subreddit"])
                rows.append((
                    post["id"], sid, post["title"], post["author"],
                    post["created_utc"], post["score"], post["num_comments"],
                    post["upvote_ratio"], post.get("poll_priority"),
                    post.get("score_velocity"), post.get("comment_velocity"),
                    post.get("trending_score"), post.get("is_trending"),
                ))
            execute_values(
                cur,
                """
                INSERT INTO posts (
                    id, subreddit_id, title, author, created_utc,
                    current_score, current_comments, current_ratio,
                    poll_priority, is_active,
                    score_velocity, comment_velocity, trending_score, is_trending
                )
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    current_score    = EXCLUDED.current_score,
                    current_comments = EXCLUDED.current_comments,
                    current_ratio    = EXCLUDED.current_ratio,
                    poll_priority    = EXCLUDED.poll_priority,
                    last_polled_at   = NOW(),
                    score_velocity   = COALESCE(EXCLUDED.score_velocity,   posts.score_velocity),
                    comment_velocity = COALESCE(EXCLUDED.comment_velocity, posts.comment_velocity),
                    trending_score   = COALESCE(EXCLUDED.trending_score,   posts.trending_score),
                    is_trending      = COALESCE(EXCLUDED.is_trending,      posts.is_trending)
                """,
                rows,
                template="(%s,%s,%s,%s,to_timestamp(%s),%s,%s,%s,%s,TRUE,%s,%s,%s,%s)",
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("bulk_upsert_posts failed.")
        raise
    finally:
        p.putconn(conn)


def bulk_insert_metrics_history(posts: list[dict]) -> None:
    """Legacy Reddit-specific metrics history writer."""
    if not posts:
        return
    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            rows = [(p["id"], p["score"], p["num_comments"], p["upvote_ratio"]) for p in posts]
            execute_values(
                cur,
                "INSERT INTO post_metrics_history (post_id, score, num_comments, upvote_ratio) VALUES %s",
                rows,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("bulk_insert_metrics_history failed.")
        raise
    finally:
        p.putconn(conn)


def bulk_upsert_nlp_features(items: list[tuple]) -> None:
    """Legacy Reddit NLP writer."""
    if not items:
        return
    p = get_pool()
    conn = p.getconn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO post_nlp_features (post_id, sentiment_score, keywords)
                VALUES %s
                ON CONFLICT (post_id) DO UPDATE SET
                    sentiment_score = EXCLUDED.sentiment_score,
                    keywords        = EXCLUDED.keywords
                """,
                items,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("bulk_upsert_nlp_features failed.")
        raise
    finally:
        p.putconn(conn)
