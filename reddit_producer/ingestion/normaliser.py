"""
ingestion/normaliser.py
=======================
Transforms raw dicts from each source into a common Signal schema.

This is the most important file in the multi-source architecture.
Everything downstream (processing service, TimescaleDB, Django API)
only ever sees Signal objects — never Reddit-specific or HN-specific shapes.

Adding a new source = add one function here + register it in NORMALISERS.
"""

import hashlib
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── Common Signal schema ──────────────────────────────────────────────────────
# Every source produces this shape. Fields marked None are populated
# by the processing service (sentiment, topics, trending_score).

def _make_signal(
    platform: str,
    source_id: str,
    title: str,
    body: str,
    url: str,
    author: str,
    community: str,       # subreddit / "hackernews" / yt channel / bsky feed
    published_at: float,  # unix timestamp
    raw_score: int,       # platform-native engagement (upvotes/points/likes)
    comment_count: int,
    extra: Optional[dict] = None,  # platform-specific fields, preserved as-is
) -> dict:
    """Construct a normalised Signal dict."""
    return {
        # Identity
        "id":           f"{platform}:{source_id}",
        "platform":     platform,
        "source_id":    source_id,

        # Content
        "title":        title[:512],   # cap length for all platforms
        "body":         body[:2048],
        "url":          url,
        "author":       author,
        "community":    community,

        # Timing
        "published_at": published_at,
        "ingested_at":  time.time(),

        # Engagement — raw (platform-native)
        "raw_score":    raw_score,
        "comment_count": comment_count,

        # Engagement — computed by processing service, null at ingestion
        "normalised_score":  None,
        "score_velocity":    None,
        "comment_velocity":  None,
        "trending_score":    None,
        "is_trending":       None,

        # NLP — computed by processing service, null at ingestion
        "sentiment_compound": None,
        "sentiment_label":    None,
        "keywords":           None,
        "topics":             None,

        # Platform-specific fields preserved for consumers that need them
        "extra": extra or {},

        # Schema version — allows downstream to handle breaking changes
        "schema_version": 1,
    }


# ── Per-source normalisers ────────────────────────────────────────────────────

def _normalise_reddit(raw: dict) -> Optional[dict]:
    """
    raw shape from reddit.py:
      id, title, subreddit, author, created_utc,
      score, num_comments, upvote_ratio
    """
    try:
        return _make_signal(
            platform     = "reddit",
            source_id    = raw["id"],
            title        = raw.get("title", ""),
            body         = raw.get("selftext", ""),
            url          = f"https://reddit.com/r/{raw['subreddit']}/comments/{raw['id']}",
            author       = raw.get("author", ""),
            community    = f"r/{raw['subreddit']}",
            published_at = raw["created_utc"],
            raw_score    = raw.get("score", 0),
            comment_count= raw.get("num_comments", 0),
            extra        = {
                "upvote_ratio":      raw.get("upvote_ratio"),
                "poll_priority":     raw.get("poll_priority"),
                "parent_post_id":    raw.get("parent_post_id"),
                "parent_post_title": raw.get("parent_post_title"),
                "is_comment":        raw.get("parent_post_id") is not None,
            },
        )
    except Exception as e:
        logger.warning("Reddit normalise failed: %s | raw=%s", e, raw.get("id"))
        return None


def _normalise_hackernews(raw: dict) -> Optional[dict]:
    """
    raw shape from HN Algolia API:
      objectID, title, url, author, created_at_i,
      points, num_comments, story_text
    """
    try:
        hn_url = raw.get("url") or f"https://news.ycombinator.com/item?id={raw['objectID']}"
        return _make_signal(
            platform     = "hackernews",
            source_id    = str(raw["objectID"]),
            title        = raw.get("title", ""),
            body         = raw.get("story_text") or "",
            url          = hn_url,
            author       = raw.get("author", ""),
            community    = "hackernews",
            published_at = raw["created_at_i"],
            raw_score    = raw.get("points", 0),
            comment_count= raw.get("num_comments", 0),
            extra        = {
                "hn_id": raw["objectID"],
                "tags":  raw.get("_tags", []),
            },
        )
    except Exception as e:
        logger.warning("HN normalise failed: %s | raw=%s", e, raw.get("objectID"))
        return None


def _normalise_bluesky(raw: dict) -> Optional[dict]:
    """
    raw shape from AT Protocol firehose commit event:
      uri, cid, author (did), record.text, record.createdAt,
      likeCount, replyCount, repostCount
    """
    try:
        uri      = raw.get("uri", "")
        post_id  = uri.split("/")[-1] if uri else raw.get("cid", "")
        handle   = raw.get("author_handle", raw.get("author", ""))
        bsky_url = f"https://bsky.app/profile/{handle}/post/{post_id}"

        # Use CID as stable ID — URI can change on re-indexing
        stable_id = raw.get("cid", post_id)

        published_raw = raw.get("record", {}).get("createdAt", "")
        published_at  = _parse_iso(published_raw)

        return _make_signal(
            platform     = "bluesky",
            source_id    = stable_id,
            title        = "",  # Bluesky has no titles — use body
            body         = raw.get("record", {}).get("text", ""),
            url          = bsky_url,
            author       = handle,
            community    = raw.get("feed", "bluesky"),
            published_at = published_at,
            raw_score    = raw.get("likeCount", 0),
            comment_count= raw.get("replyCount", 0),
            extra        = {
                "repost_count": raw.get("repostCount", 0),
                "cid":          raw.get("cid"),
                "langs":        raw.get("record", {}).get("langs", []),
            },
        )
    except Exception as e:
        logger.warning("Bluesky normalise failed: %s | raw=%s", e, raw.get("cid"))
        return None


def _normalise_youtube(raw: dict) -> Optional[dict]:
    """
    raw shape from YouTube Data API v3 comment thread:
      id (commentThreadId), snippet.topLevelComment.snippet:
        textOriginal, authorDisplayName, publishedAt, likeCount
      snippet.videoId, snippet.totalReplyCount
      video_title (injected by youtube.py)
    """
    try:
        snippet     = raw.get("snippet", {})
        comment_s   = snippet.get("topLevelComment", {}).get("snippet", {})
        video_id    = snippet.get("videoId", "")
        published   = _parse_iso(comment_s.get("publishedAt", ""))

        return _make_signal(
            platform     = "youtube",
            source_id    = raw["id"],
            title        = raw.get("video_title", ""),   # injected by youtube.py
            body         = comment_s.get("textOriginal", ""),
            url          = f"https://youtube.com/watch?v={video_id}&lc={raw['id']}",
            author       = comment_s.get("authorDisplayName", ""),
            community    = raw.get("channel_title", video_id),  # injected by youtube.py
            published_at = published,
            raw_score    = comment_s.get("likeCount", 0),
            comment_count= snippet.get("totalReplyCount", 0),
            extra        = {
                "video_id":    video_id,
                "channel_id":  raw.get("channel_id", ""),
            },
        )
    except Exception as e:
        logger.warning("YouTube normalise failed: %s | raw=%s", e, raw.get("id"))
        return None


# ── Registry ──────────────────────────────────────────────────────────────────

NORMALISERS = {
    "reddit":      _normalise_reddit,
    "hackernews":  _normalise_hackernews,
    "bluesky":     _normalise_bluesky,
    "youtube":     _normalise_youtube,
}


def normalise(source: str, raw: dict) -> Optional[dict]:
    """
    Entry point. Called by BaseIngester after each poll().
    Returns a normalised Signal dict or None if normalisation fails.
    """
    fn = NORMALISERS.get(source)
    if not fn:
        logger.error("No normaliser registered for source: %s", source)
        return None
    return fn(raw)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_iso(s: str) -> float:
    """Parse ISO 8601 string to unix timestamp. Returns 0.0 on failure."""
    if not s:
        return 0.0
    try:
        from datetime import datetime, timezone
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0
