"""
apps/signals/consumers.py
==========================
Replaces apps/reddit/consumers.py.

One base class, two concrete consumers — no code duplication.
Both are registered in config/routing.py.

PostFeedConsumer   — ws://.../ws/posts/   (kept for any existing frontend)
SignalFeedConsumer — ws://.../ws/signals/ (new cross-platform feed)
"""

import logging
from channels.generic.websocket import AsyncJsonWebsocketConsumer

logger = logging.getLogger(__name__)


class BaseFeedConsumer(AsyncJsonWebsocketConsumer):
    group_name: str = ""

    async def connect(self):
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()
        logger.info("[%s] connected: %s", self.group_name, self.channel_name)

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(self.group_name, self.channel_name)
        logger.info("[%s] disconnected: %s", self.group_name, self.channel_name)

    async def receive_json(self, content, **kwargs):
        pass  # read-only push channel

    async def post_update(self, event):
        await self.send_json(event["data"])

    async def signal_update(self, event):
        await self.send_json(event["data"])


class PostFeedConsumer(BaseFeedConsumer):
    """Kept for backward compatibility with existing frontend."""
    group_name = "posts_feed"


class SignalFeedConsumer(BaseFeedConsumer):
    """Cross-platform live signal feed."""
    group_name = "signals_feed"
