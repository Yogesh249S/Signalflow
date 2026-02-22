"""
apps/reddit/consumers.py — Phase 2 (new file)
===============================================
Django Channels async WebSocket consumer for the live posts feed.

How it works:
  1. Browser opens ws://host/ws/posts/
  2. Consumer joins the "posts_feed" channel group on connect.
  3. After each DB batch flush, the processing service calls:
       channel_layer.group_send("posts_feed", {"type": "post.update", "data": [...]})
  4. This consumer receives the message and forwards it to the browser as JSON.
  5. On disconnect the consumer leaves the group (no further messages sent).

Result: browsers receive data only when it changes. At 50 concurrent users
this drops idle traffic from ~600 HTTP req/min (5 s polling) to ~0.

The consumer is registered in config/routing.py at ws://.../ws/posts/
"""

import logging
from channels.generic.websocket import AsyncJsonWebsocketConsumer

logger = logging.getLogger(__name__)

POSTS_FEED_GROUP = "posts_feed"


class PostFeedConsumer(AsyncJsonWebsocketConsumer):
    """Push live post updates to subscribed browser clients."""

    async def connect(self):
        """Subscribe this socket to the shared posts_feed group."""
        await self.channel_layer.group_add(POSTS_FEED_GROUP, self.channel_name)
        await self.accept()
        logger.info("WebSocket connected: %s", self.channel_name)

    async def disconnect(self, close_code):
        """Leave the group so no further messages are delivered."""
        await self.channel_layer.group_discard(POSTS_FEED_GROUP, self.channel_name)
        logger.info("WebSocket disconnected: %s (code=%s)", self.channel_name, close_code)

    # ── Incoming messages from the browser ────────────────────────────────────

    async def receive_json(self, content, **kwargs):
        """
        Browsers don't send data to this feed (read-only push channel).
        Ignore any incoming frames to keep the consumer robust.
        """
        pass

    # ── Messages from the channel layer (sent by the processing service) ──────

    async def post_update(self, event):
        """
        Handler for {"type": "post.update", "data": [...]} messages.

        The processing service publishes this event after every batch DB flush.
        We forward the payload directly to the WebSocket client as JSON.

        event["data"] is expected to be a list of serialised post dicts, e.g.:
            [{"id": "abc", "title": "...", "current_score": 1234, ...}, ...]
        """
        await self.send_json(event["data"])
        logger.debug(
            "Pushed %d post(s) to %s", len(event.get("data", [])), self.channel_name
        )
