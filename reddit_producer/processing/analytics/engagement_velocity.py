"""
processing/analytics/engagement_velocity.py
============================================
OPTIMISATION CHANGES vs original
----------------------------------
1. REMOVED STALE DB CONNECTION
   - Original: opened `psycopg2.connect(...)` at the TOP of this file — a
     completely unused database connection that was never referenced anywhere
     in the function body. It was dead code that silently consumed a Postgres
     connection slot on every container start and would raise an
     OperationalError if Postgres was not yet ready.
   - New: connection import removed entirely. Velocity is computed purely
     from the in-memory cache — no DB needed here.

2. Uses the TTL-aware velocity_cache (see velocity_cache.py changes).
"""

import logging
import time

from processing.analytics.velocity_cache import get_previous, update_cache

logger = logging.getLogger(__name__)


def calculate_velocity(post: dict) -> tuple[float, float]:
    """
    Compute per-second score and comment velocity for a post by comparing
    its current metrics to the previous cached snapshot.

    Returns (score_velocity, comment_velocity) — both in units/second.
    Returns (0.0, 0.0) when no prior snapshot exists (first observation).
    """
    post_id = post["id"]
    now     = time.time()

    prev = get_previous(post_id)

    if prev is None:
        # First time we've seen this post — store baseline, no velocity yet
        update_cache(post_id, post["score"], post["num_comments"], now)
        return 0.0, 0.0

    old_score, old_comments, old_time = prev
    # max(..., 1) guards against division-by-zero if called twice in quick succession
    delta_time = max(now - old_time, 1.0)

    score_velocity   = (post["score"]        - old_score)    / delta_time
    comment_velocity = (post["num_comments"] - old_comments) / delta_time

    # Update cache with current snapshot for the NEXT refresh calculation
    update_cache(post_id, post["score"], post["num_comments"], now)

    logger.debug(
        "Post %s velocity: score=%.4f/s comments=%.4f/s",
        post_id, score_velocity, comment_velocity,
    )
    return score_velocity, comment_velocity
