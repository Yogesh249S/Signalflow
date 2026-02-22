"""
apps/reddit/urls.py — Phase 2
===============================
Phase 2 additions:
  - /api/health/   — unauthenticated health-check (load balancer probe)

Note: /api/token/ and /api/token/refresh/ are registered in config/urls.py,
not here, to keep auth concerns at the project level.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import home, health, TrendViewSet, PostViewSet, CommentViewSet, StatsViewSet, stats

router = DefaultRouter()
router.register(r"posts",    PostViewSet,   basename="post")
router.register(r"comments", CommentViewSet, basename="comment")
router.register(r"trends",   TrendViewSet,   basename="trend")

urlpatterns = [
    path("",        home,   name="home"),
    path("health/", health, name="health"),   # Phase 2 — unauthenticated
    path("",        include(router.urls)),
    path("stats/",  stats,  name="stats"),
]
