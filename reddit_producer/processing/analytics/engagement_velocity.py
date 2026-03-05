"""
processing/analytics/engagement_velocity.py
============================================
CHANGE vs original:
  Field names updated from Reddit-specific (post["score"], post["num_comments"])
  to Signal schema (signal["raw_score"], signal["comment_count"]).

  The velocity calculation itself is unchanged — it still computes
  per-second rate of change from cached previous snapshot.

  Works for all platforms because every Signal has raw_score and comment_count
  regardless of whether they're Reddit upvotes, HN points, or YT likes.
"""

import logging
import time

from processing.analytics.velocity_cache import get_previous, update_cache

logger = logging.getLogger(__name__)


def calculate_velocity(signal: dict) -> tuple[float, float]:
    """
    Compute per-second score and comment velocity for a signal.

    Works on the common Signal schema — raw_score and comment_count
    are present for all platforms (Reddit, HN, Bluesky, YouTube).

    Returns (score_velocity, comment_velocity) in units/second.
    Returns (0.0, 0.0) on first observation.
    """
    sig_id = signal["id"]   # "reddit:abc123", "hackernews:456", etc.
    now    = time.time()

    prev = get_previous(sig_id)

    if prev is None:
        update_cache(sig_id, signal["raw_score"], signal["comment_count"], now)
        return 0.0, 0.0

    old_score, old_comments, old_time = prev
    delta_time = max(now - old_time, 1.0)

    score_velocity   = (signal["raw_score"]    - old_score)    / delta_time
    comment_velocity = (signal["comment_count"] - old_comments) / delta_time

    update_cache(sig_id, signal["raw_score"], signal["comment_count"], now)

    logger.debug(
        "Signal %s velocity: score=%.4f/s comments=%.4f/s",
        sig_id, score_velocity, comment_velocity,
    )
    return score_velocity, comment_velocity
