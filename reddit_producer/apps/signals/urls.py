"""
apps/signals/urls.py
=====================
Single URL file for the entire API.
Replaces apps/reddit/urls.py entirely.

All endpoints under /api/v1/ — clean versioned namespace.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    home, health,
    SignalViewSet,
    PulseView, TrendingView, CompareView,
    stats, stats_timeline, stats_keywords,
)

router = DefaultRouter()
router.register(r"signals", SignalViewSet, basename="signal")

urlpatterns = [
    path("",              home,   name="home"),
    path("health/",       health, name="health"),

    # Signal feed (replaces /api/posts/)
    path("", include(router.urls)),

    # Cross-platform intelligence
    path("pulse/",    PulseView.as_view(),    name="pulse"),
    path("trending/", TrendingView.as_view(), name="trending"),
    path("compare/",  CompareView.as_view(),  name="compare"),

    # Stats (replaces /api/stats/)
    path("stats/",           stats,           name="stats"),
    path("stats/timeline/",  stats_timeline,  name="stats-timeline"),
    path("stats/keywords/",  stats_keywords,  name="stats-keywords"),
]
