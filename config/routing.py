"""
config/routing.py — Phase 2 (new file)
========================================
Channels routing layer — replaces the plain ASGI application for WebSocket
support while keeping all HTTP traffic handled by Django as normal.

How it works:
  - URLRouter maps ws:// paths to Channels consumers.
  - All other requests fall through to Django's standard HTTP handler.
  - ASGI_APPLICATION in settings.py points here instead of config.asgi.

WebSocket endpoint:
  ws://host/ws/posts/
    → apps.reddit.consumers.PostFeedConsumer

The consumer joins the "posts_feed" group on connect. The processing service
publishes to this group after each batch flush via the channel layer, causing
the consumer to push serialised post updates to every connected browser.
"""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.urls import re_path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# Import consumers after Django setup
django_asgi_app = get_asgi_application()

from apps.reddit.consumers import PostFeedConsumer  # noqa: E402

application = ProtocolTypeRouter(
    {
        # HTTP → standard Django ASGI handler
        "http": django_asgi_app,
        # WebSocket → Channels router with session/auth middleware
        "websocket": AuthMiddlewareStack(
            URLRouter(
                [
                    re_path(r"^ws/posts/$", PostFeedConsumer.as_asgi()),
                ]
            )
        ),
    }
)
