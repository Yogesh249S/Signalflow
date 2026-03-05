"""
config/urls.py — Phase 3 (clean slate)
========================================
Single URL root. All API endpoints under /api/v1/.
apps.reddit removed entirely.
"""

from django.contrib import admin
from django.urls import path, include
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)

urlpatterns = [
    path("admin/",             admin.site.urls),
    path("api/v1/",            include("apps.signals.urls")),
    path("api/token/",         TokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("api/token/refresh/", TokenRefreshView.as_view(),    name="token_refresh"),
    path("api/token/verify/",  TokenVerifyView.as_view(),     name="token_verify"),
]
