"""
processing/analytics/trending_score.py
========================================
Platform-aware trending score computation.

Problem with original:
  - Formula: velocity + sentiment + comment_count
  - Bluesky has raw_score=0, comment_count=0 → max score 0.2 (never trends)
  - YouTube/HN have low velocity → max score 0.4
  - TRENDING_CUTOFF=0.5 means NOTHING ever qualifies
  - All platforms capped at 0.4 in production data

Fix:
  Each platform uses signals that actually exist for that platform.

  Reddit     → velocity-based (upvote momentum + comment growth)
  HackerNews → normalised_score + sentiment (points accumulate slowly)
  YouTube    → normalised_score + comment ratio (views/comments)
  Bluesky    → sentiment strength + volume spike (no engagement data)

  All scores still in [0.0, 1.0]. is_trending = score >= TRENDING_CUTOFF.
  Default cutoff lowered to 0.3 (was 0.5 — too high for real data).
"""

import os
import logging

logger = logging.getLogger(__name__)

# ── Thresholds — tunable via .env without rebuild ─────────────────────────────
_VEL_HIGH       = float(os.environ.get("TRENDING_VELOCITY_HIGH",    "50"))
_VEL_LOW        = float(os.environ.get("TRENDING_VELOCITY_LOW",     "5"))
_SENT_THRESH    = float(os.environ.get("TRENDING_SENTIMENT_THRESH", "0.3"))
_COMMENT_MIN    = int(  os.environ.get("TRENDING_COMMENT_MIN",      "10"))
_IS_TRENDING    = float(os.environ.get("TRENDING_CUTOFF",           "0.3"))


def _score_reddit(post: dict, score_velocity: float, sentiment: float) -> float:
    """
    Reddit: rich engagement data — velocity is the primary signal.

    Components (max 1.0):
      score_velocity > VEL_HIGH  → 0.4  (viral momentum)
      score_velocity > VEL_LOW   → 0.2  (growing)
      |sentiment|    > SENT_THRESH → 0.2  (strong opinion)
      comment_count  > COMMENT_MIN → 0.2  (discussion happening)
    """
    score = 0.0

    if score_velocity > _VEL_HIGH:
        score += 0.4
    elif score_velocity > _VEL_LOW:
        score += 0.2

    if abs(sentiment) > _SENT_THRESH:
        score += 0.2

    comments = post.get("comment_count", post.get("num_comments", 0))
    if comments > _COMMENT_MIN:
        score += 0.2

    # Bonus: normalised_score captures posts with high absolute score
    norm = post.get("normalised_score", 0.0)
    if norm > 0.6:
        score += 0.2

    return min(score, 1.0)


def _score_hackernews(post: dict, score_velocity: float, sentiment: float) -> float:
    """
    HackerNews: points accumulate slowly, discussion is the real signal.

    Components (max 1.0):
      normalised_score > 0.3  → 0.3  (above-average HN story)
      normalised_score > 0.6  → 0.2  (bonus for high score)
      comment_count    > 5    → 0.2  (discussion started)
      comment_count    > 20   → 0.1  (bonus for active thread)
      |sentiment|      > SENT_THRESH → 0.2
    """
    score = 0.0

    norm = post.get("normalised_score", 0.0)
    if norm > 0.6:
        score += 0.5
    elif norm > 0.3:
        score += 0.3

    comments = post.get("comment_count", 0)
    if comments > 20:
        score += 0.3
    elif comments > 5:
        score += 0.2

    if abs(sentiment) > _SENT_THRESH:
        score += 0.2

    return min(score, 1.0)


def _score_youtube(post: dict, score_velocity: float, sentiment: float) -> float:
    """
    YouTube: view counts are high but we only have comment likes.
    Use normalised_score + sentiment + comment activity.

    Components (max 1.0):
      normalised_score > 0.3  → 0.3
      normalised_score > 0.6  → 0.2 bonus
      comment_count    > 5    → 0.2
      comment_count    > 50   → 0.1 bonus
      |sentiment|      > SENT_THRESH → 0.2
    """
    score = 0.0

    norm = post.get("normalised_score", 0.0)
    if norm > 0.6:
        score += 0.5
    elif norm > 0.3:
        score += 0.3

    comments = post.get("comment_count", 0)
    if comments > 50:
        score += 0.3
    elif comments > 5:
        score += 0.2

    if abs(sentiment) > _SENT_THRESH:
        score += 0.2

    return min(score, 1.0)


def _score_bluesky(post: dict, score_velocity: float, sentiment: float) -> float:
    """
    Bluesky: no engagement data at all (raw_score=0, comment_count=0).
    Use sentiment strength as the sole signal — strong opinion posts
    are the most interesting content from the firehose.

    Components (max 1.0):
      |sentiment| > 0.6  → 0.6  (very strong opinion)
      |sentiment| > 0.3  → 0.4  (moderate opinion)
      |sentiment| > 0.1  → 0.2  (slight opinion)
      sentiment positive → +0.1 bonus (positive posts engage more)
    """
    score = 0.0
    abs_sent = abs(sentiment)

    if abs_sent > 0.6:
        score += 0.6
    elif abs_sent > 0.3:
        score += 0.4
    elif abs_sent > 0.1:
        score += 0.2

    # Small bonus for positive sentiment
    if sentiment > 0.2:
        score += 0.1

    return min(score, 1.0)


# ── Platform dispatch table ───────────────────────────────────────────────────
_SCORERS = {
    "reddit":     _score_reddit,
    "hackernews": _score_hackernews,
    "youtube":    _score_youtube,
    "bluesky":    _score_bluesky,
}


def compute_trending(
    post: dict,
    score_velocity: float,
    sentiment_score: float,
) -> tuple[float, bool]:
    """
    Compute a platform-aware composite trending score in [0.0, 1.0].

    Dispatches to a platform-specific scorer based on post["platform"].
    Falls back to the Reddit scorer for unknown platforms.

    Returns
    -------
    trending_score : float  — composite score in [0.0, 1.0]
    is_trending    : bool   — True if score >= TRENDING_CUTOFF (default 0.3)
    """
    platform = post.get("platform", "reddit")
    scorer   = _SCORERS.get(platform, _score_reddit)
    score    = scorer(post, score_velocity, sentiment_score)
    result   = round(score, 4)

    logger.debug(
        "[%s] trending: score=%.3f velocity=%.3f sentiment=%.3f is_trending=%s",
        platform, result, score_velocity, sentiment_score, result >= _IS_TRENDING,
    )

    return result, result >= _IS_TRENDING
