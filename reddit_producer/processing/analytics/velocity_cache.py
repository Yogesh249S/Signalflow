"""
processing/analytics/velocity_cache.py
========================================
PHASE 1 IMPROVEMENT: Redis Velocity Cache
------------------------------------------
PROBLEM (previous version):
  The velocity cache was an in-memory Python dict. This meant:
  - Running 2+ processing replicas caused split-brain: replica A and replica B
    each had separate caches. Post X's velocity in replica B was computed
    against stale data (its own last observation), not the true last observation.
    This produced wildly incorrect velocity values — some posts would appear to
    spike in trending score simply due to which replica happened to process them.
  - The dict was local to the process — a container restart wiped the entire
    cache, causing every post to report 0.0 velocity until it was seen twice.
  - Memory leaked slowly (despite TTL eviction) because eviction only ran every
    500 writes, not continuously.

SOLUTION:
  Replace the dict with a Redis HASH. Each post gets one key:
      vel:{post_id}  →  HASH { score: N, comments: N, ts: float }

  All processing replicas share the same Redis instance, so velocity is always
  computed against the TRUE previous snapshot regardless of which replica saw
  the post last. Expiry is delegated to Redis TTL (EXPIRE command) — no manual
  eviction code needed.

  Fallback: if Redis is unavailable (connection error, timeout), the module
  falls back to the local in-memory dict so the processor does not crash.
  A warning is logged on every Redis miss-due-to-error.
"""

import logging
import os
import time
from typing import Optional

import redis

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

REDIS_URL     = os.environ.get("REDIS_URL", "redis://redis:6379/0")
TTL_SECONDS   = 25 * 3_600   # 25 h — posts live 24 h; 1 h buffer

# ── Redis client (lazy, with reconnect on failure) ─────────────────────────────

_redis_client: Optional[redis.Redis] = None


def _get_redis() -> Optional[redis.Redis]:
    """
    Return a Redis client, creating it on first call.
    Returns None (and logs a warning) if connection fails.
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        client = redis.Redis.from_url(
            REDIS_URL,
            socket_connect_timeout=2,
            socket_timeout=1,
            decode_responses=True,
        )
        client.ping()   # fail fast if Redis isn't up
        _redis_client = client
        logger.info("Redis velocity cache connected at %s.", REDIS_URL)
        return _redis_client
    except redis.RedisError as exc:
        logger.warning("Redis unavailable — falling back to in-memory cache: %s", exc)
        return None


# ── In-memory fallback ─────────────────────────────────────────────────────────
# Used when Redis is unreachable. Same structure as before but now clearly
# labelled as the fallback path, not the primary path.

_fallback_cache: dict[str, tuple[int, int, float]] = {}


# ── Public API ─────────────────────────────────────────────────────────────────

def get_previous(post_id: str) -> Optional[tuple[int, int, float]]:
    """
    Return (score, num_comments, timestamp) for the previous snapshot,
    or None if no valid entry exists.

    Reads from Redis first; falls back to the in-memory dict on error.
    Redis TTL guarantees freshness — no manual TTL check needed here.
    """
    r = _get_redis()
    key = f"vel:{post_id}"

    if r is not None:
        try:
            raw = r.hgetall(key)
            if not raw:
                return None
            return (
                int(raw["score"]),
                int(raw["comments"]),
                float(raw["ts"]),
            )
        except redis.RedisError as exc:
            logger.warning("Redis read error for %s — using fallback: %s", key, exc)
            # Fall through to in-memory fallback

    # ── In-memory fallback ────────────────────────────────────────────────────
    entry = _fallback_cache.get(post_id)
    if entry is None:
        return None
    _, _, ts = entry
    if time.time() - ts > TTL_SECONDS:
        del _fallback_cache[post_id]
        return None
    return entry


def update_cache(post_id: str, score: int, num_comments: int, timestamp: float) -> None:
    """
    Store a new velocity snapshot for a post.

    Writes to Redis with a TTL; falls back to in-memory dict on error.
    Redis EXPIRE replaces the manual eviction sweep from the old version.
    """
    r = _get_redis()
    key = f"vel:{post_id}"

    if r is not None:
        try:
            pipe = r.pipeline()
            pipe.hset(key, mapping={
                "score":    score,
                "comments": num_comments,
                "ts":       timestamp,
            })
            # TTL set on every write — refreshes the expiry on update
            pipe.expire(key, TTL_SECONDS)
            pipe.execute()
            return
        except redis.RedisError as exc:
            logger.warning("Redis write error for %s — using fallback: %s", key, exc)

    # ── In-memory fallback ────────────────────────────────────────────────────
    _fallback_cache[post_id] = (score, num_comments, timestamp)
