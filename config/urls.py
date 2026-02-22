"""
config/urls.py — Phase 2
=========================
Phase 2 additions:
  - /api/token/          — obtain JWT access + refresh tokens (POST)
  - /api/token/refresh/  — refresh an access token (POST)
  - /api/token/verify/   — verify a token is still valid (POST)
  - /health/             — unauthenticated health-check for load balancers

/api/token/ and /health/ are explicitly unauthenticated (see views.py).
All other /api/* endpoints require a valid Bearer token.
"""

from django.contrib import admin
from django.urls import path, include
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include("apps.reddit.urls")),

    # Phase 2 — JWT token endpoints (unauthenticated)
    path("api/token/",         TokenObtainPairView.as_view(),  name="token_obtain_pair"),
    path("api/token/refresh/", TokenRefreshView.as_view(),     name="token_refresh"),
    path("api/token/verify/",  TokenVerifyView.as_view(),      name="token_verify"),
]
