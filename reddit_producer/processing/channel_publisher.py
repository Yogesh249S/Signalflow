"""
processing/channel_publisher.py — Phase 2 (new file)
======================================================
After each batch DB flush, the processing service publishes updated post
data to the Redis channel layer so Django Channels can push it to connected
WebSocket clients.

This is a pure Redis publish — the processing service does NOT need Django
installed. It speaks directly to the channels_redis channel layer protocol:
  channel_layer.group_send("posts_feed", {"type": "post.update", "data": [...]})

The message format mirrors what channels_redis expects for group_send.
It is then picked up by PostFeedConsumer.post_update() in the Django app.

Why asyncio.run() and not a persistent event loop?
  The main_processor loop is synchronous. We don't want to restructure it
  into async just to publish a Redis message. asyncio.run() spins up a loop,
  runs one coroutine, and tears it down — overhead is negligible (~0.5 ms)
  compared to the DB write that just completed.
"""

import asyncio
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Redis DB 2 is reserved for the channel layer (see settings.py CHANNEL_LAYERS)
_CHANNEL_LAYER_URL = os.environ.get("CHANNEL_LAYER_URL", "redis://redis:6379/2")
_GROUP_NAME = "posts_feed"
_MESSAGE_TYPE = "post.update"

# Minimal serialisation: only send fields the dashboard needs to update its state.
# Keeps WebSocket frames small even for large batches.
_DASHBOARD_FIELDS = {
    "id", "title", "author", "current_score", "current_comments",
    "current_ratio", "velocity", "trending_score", "subreddit_id",
    "last_polled_at",
}


def _slim(post: dict) -> dict:
    """Return only dashboard-relevant fields from a post dict."""
    return {k: v for k, v in post.items() if k in _DASHBOARD_FIELDS}


async def _async_publish(posts: list[dict]) -> None:
    """
    Publish a group_send message to the Redis channel layer.

    channels_redis stores group messages under keys of the form:
        asgi:group:<group_name>
    and encodes them as JSON in a Redis list. We replicate this directly
    rather than importing channels_redis (which would pull in Django).
    """
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(_CHANNEL_LAYER_URL, decode_responses=True)

        payload = json.dumps({
            "type":    _MESSAGE_TYPE,
            "data":    [_slim(p) for p in posts],
        })

        # channels_redis group key format
        group_key = f"asgi:group:{_GROUP_NAME}"
        await r.publish(group_key, payload)
        await r.aclose()

        logger.debug("Published %d post updates to channel layer group %s", len(posts), _GROUP_NAME)
    except Exception as exc:
        # Never let a publish failure crash the processing loop
        logger.warning("Channel layer publish failed (non-fatal): %s", exc)


def publish_post_updates(posts: list[dict]) -> None:
    """
    Synchronous entry point called from the main processor loop.
    Runs the async publish coroutine in a temporary event loop.
    """
    if not posts:
        return
    try:
        asyncio.run(_async_publish(posts))
    except Exception as exc:
        logger.warning("asyncio.run publish_post_updates failed: %s", exc)
