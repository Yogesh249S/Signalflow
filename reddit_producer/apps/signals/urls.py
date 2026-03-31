"""
apps/signals/urls.py
=====================
Single URL file for the entire API.
Replaces apps/reddit/urls.py entirely.

All endpoints under /api/v1/ — clean versioned namespace.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (request_api_access, 
    home, health,
    SignalViewSet,
    PulseView, TrendingView, CompareView,
    DivergenceLeaderboardView, TopicAlertView,
    stats, stats_timeline, stats_keywords, platform_totals,
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

    # Addition 3: Divergence leaderboard
    path("divergence/leaderboard/", DivergenceLeaderboardView.as_view(), name="divergence-leaderboard"),

    # Addition 1: Topic alert webhooks (authenticated)
    path("alerts/watch/", TopicAlertView.as_view(), name="topic-alert-watch"),

    # Stats (replaces /api/stats/)
    path("stats/",           stats,           name="stats"),
    path("stats/timeline/",  stats_timeline,  name="stats-timeline"),
    path("stats/keywords/",  stats_keywords,  name="stats-keywords"),
    path("stats/totals/",   platform_totals, name="stats-totals"),
    #path ("stats/totals/",  platform_totals   name ="platform-totals")
    path("access/request/", request_api_access, name="access-request"),
]
