"""
config/routing.py — Phase 3 (clean slate)
==========================================
Both WebSocket consumers now live in apps.signals.consumers.
apps.reddit removed.
"""

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.urls import re_path

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

django_asgi_app = get_asgi_application()

from apps.signals.consumers import PostFeedConsumer, SignalFeedConsumer  # noqa: E402

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AuthMiddlewareStack(
        URLRouter([
            re_path(r"^ws/posts/$",   PostFeedConsumer.as_asgi()),
            re_path(r"^ws/signals/$", SignalFeedConsumer.as_asgi()),
        ])
    ),
})
