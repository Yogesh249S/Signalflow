"""
processing/analytics/normalised_score.py
=========================================
Computes normalised_score (0.0–1.0) for each signal by comparing
raw_score against a rolling per-platform baseline.

Why this matters:
  Reddit post with 1,000 upvotes ≠ HN story with 1,000 points.
  Reddit's 95th percentile score is ~5,000. HN's is ~300.
  Without normalisation, cross-platform comparisons are meaningless.

Approach:
  - Keep a rolling window of the last N scores per platform in Redis.
  - Baseline = mean of that window (or a hardcoded fallback on startup).
  - normalised_score = raw_score / (2 × baseline), clamped to [0, 1].
    Dividing by 2× baseline means "twice the average" = 1.0.

Redis key: norm:baseline:{platform}  →  JSON list of last 500 scores
Falls back to in-memory dict if Redis is unavailable.
"""

import json
import logging
import os
from collections import defaultdict
from statistics import mean
from typing import Optional

logger = logging.getLogger(__name__)

# ── Hardcoded fallback baselines (from empirical observation) ─────────────────
# Used on first startup before enough real data accumulates.
# Update these as your platform data grows.
_FALLBACK_BASELINES: dict[str, float] = {
    "reddit":      150.0,   # average score for a 7-day-old post
    "hackernews":  50.0,    # HN points are harder to accumulate
    "bluesky":     10.0,    # early-stage platform, lower engagement
    "youtube":     20.0,    # comment likes
}
_DEFAULT_BASELINE = 100.0

# Rolling window size per platform
_WINDOW_SIZE = 500


# ── In-memory fallback (used if Redis unavailable) ────────────────────────────
_memory_windows: dict[str, list[float]] = defaultdict(list)


# ── Redis client (optional) ───────────────────────────────────────────────────
_redis = None


def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    try:
        import redis
        _redis = redis.Redis(
            host=os.environ.get("REDIS_HOST", "redis"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            db=3,  # DB 3 — separate from velocity cache (DB 0) and Django (DB 1,2)
            socket_connect_timeout=2,
            decode_responses=True,
        )
        _redis.ping()
        logger.info("Normaliser connected to Redis DB 3.")
    except Exception as e:
        logger.warning("Redis unavailable for normaliser (%s) — using in-memory.", e)
        _redis = None
    return _redis


def _get_window(platform: str) -> list[float]:
    """Retrieve rolling window from Redis or in-memory fallback."""
    r = _get_redis()
    if r:
        try:
            raw = r.get(f"norm:baseline:{platform}")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return _memory_windows[platform].copy()


def _save_window(platform: str, window: list[float]) -> None:
    """Persist rolling window to Redis or in-memory fallback."""
    r = _get_redis()
    if r:
        try:
            r.set(f"norm:baseline:{platform}", json.dumps(window[-_WINDOW_SIZE:]))
            return
        except Exception:
            pass
    _memory_windows[platform] = window[-_WINDOW_SIZE:]


def _get_baseline(platform: str) -> float:
    """Compute current baseline for a platform from its rolling window."""
    window = _get_window(platform)
    if len(window) >= 10:  # need at least 10 samples to trust the baseline
        return max(mean(window), 1.0)
    return _FALLBACK_BASELINES.get(platform, _DEFAULT_BASELINE)


def compute_normalised_score(platform: str, raw_score: int) -> float:
    """
    Normalise raw_score against the platform's rolling baseline.

    Formula: raw_score / (2 × baseline), clamped to [0.0, 1.0]
    - Score equal to baseline → 0.5
    - Score twice baseline    → 1.0
    - Score below baseline    → < 0.5
    """
    if raw_score <= 0:
        return 0.0
    baseline = _get_baseline(platform)
    return round(min(raw_score / (2.0 * baseline), 1.0), 4)


def update_baseline(platform: str, raw_score: int) -> None:
    """
    Add raw_score to platform's rolling window.
    Called after computing normalised_score so the score
    contributes to future baselines.
    Only adds scores > 0 — zero scores from new/unseen signals
    would drag the baseline down unrealistically.
    """
    if raw_score <= 0:
        return
    window = _get_window(platform)
    window.append(float(raw_score))
    if len(window) > _WINDOW_SIZE:
        window = window[-_WINDOW_SIZE:]
    _save_window(platform, window)


def enrich_normalised_scores(signals: list[dict]) -> None:
    """
    Compute and attach normalised_score to each signal in-place.
    Also updates the rolling baseline window for each platform.

    Called from flush_signal_batch() after sentiment and topics.
    """
    for sig in signals:
        platform  = sig.get("platform", "unknown")
        raw_score = sig.get("raw_score", 0)

        sig["normalised_score"] = compute_normalised_score(platform, raw_score)
        update_baseline(platform, raw_score)
