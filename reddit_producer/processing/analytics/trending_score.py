"""
processing/analytics/trending_score.py
========================================
OPTIMISATION CHANGES vs original
----------------------------------
1. CONFIGURABLE THRESHOLDS via environment variables
   - Original: magic numbers (`50`, `10`, `0.5`, `100`, `0.5`) were hardcoded.
     Tuning required a code change + container rebuild.
   - New: all thresholds are read from environment variables with safe defaults.
     Adjust via `.env` or docker-compose `environment:` without code changes.

2. EXPLICIT RETURN TYPE and docstring
   - Makes the function's contract clear, especially the range of trending_score
     (0.0 – 1.0) and what is_trending means.
"""

import os

# Thresholds read from env so they can be tuned without rebuilding the image
_VEL_HIGH   = float(os.environ.get("TRENDING_VELOCITY_HIGH",   "50"))
_VEL_LOW    = float(os.environ.get("TRENDING_VELOCITY_LOW",    "10"))
_SENT_THRESH = float(os.environ.get("TRENDING_SENTIMENT_THRESH", "0.5"))
_COMMENT_MIN = int(os.environ.get("TRENDING_COMMENT_MIN",      "100"))
_IS_TRENDING = float(os.environ.get("TRENDING_CUTOFF",          "0.5"))


def compute_trending(
    post: dict,
    score_velocity: float,
    sentiment_score: float,
) -> tuple[float, bool]:
    """
    Compute a composite trending score in [0.0, 1.0] from multiple signals.

    Signals and weights
    -------------------
    score_velocity > _VEL_HIGH  → +0.4
    score_velocity > _VEL_LOW   → +0.2
    |sentiment|    > _SENT_THRESH → +0.2
    num_comments   > _COMMENT_MIN → +0.2

    Returns
    -------
    trending_score : float — composite score
    is_trending    : bool  — True if score >= _IS_TRENDING cutoff
    """
    score = 0.0

    if score_velocity > _VEL_HIGH:
        score += 0.4
    elif score_velocity > _VEL_LOW:
        score += 0.2

    if abs(sentiment_score) > _SENT_THRESH:
        score += 0.2

    if post.get("num_comments", 0) > _COMMENT_MIN:
        score += 0.2

    return round(score, 4), score >= _IS_TRENDING
