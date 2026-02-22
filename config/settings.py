"""
config/settings.py — Phase 2
==============================
Phase 1 changes (kept):
  - Redis cache backend (django-redis)
  - CONN_MAX_AGE=600 persistent connections
  - Secrets from environment variables
  - CorsMiddleware at position 0

Phase 2 additions:
  1. DATABASES['replica']  — Postgres streaming replica for all reads
  2. DATABASE_ROUTERS      — ReadReplicaRouter sends SELECTs to replica
  3. INSTALLED_APPS        — channels, rest_framework_simplejwt added
  4. CHANNEL_LAYERS        — Redis-backed Django Channels
  5. ASGI_APPLICATION      — Channels routing replaces plain ASGI
  6. REST_FRAMEWORK auth   — JWTAuthentication on all endpoints
  7. REST_FRAMEWORK throttle— 100 req/min per authenticated user
  8. SIMPLE_JWT config     — token lifetimes
"""

import os
from pathlib import Path
from datetime import timedelta

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "django-insecure-dev-key-change-in-production"
)

DEBUG = os.environ.get("DJANGO_DEBUG", "true").lower() == "true"

ALLOWED_HOSTS = os.environ.get(
    "DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1"
).split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "channels",               # Phase 2 — WebSocket support
    "rest_framework_simplejwt",  # Phase 2 — JWT tokens
    "apps.reddit",
    #"django_extensions",

]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",   # must be first
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

# Phase 2: Channels replaces plain ASGI for WebSocket support
ASGI_APPLICATION = "config.routing.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE":   "django.db.backends.postgresql",
        "NAME":     os.environ.get("POSTGRES_DB",       "reddit"),
        "USER":     os.environ.get("POSTGRES_USER",     "reddit"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "reddit"),
        "HOST":     os.environ.get("POSTGRES_HOST",     "localhost"),  
        "PORT":     os.environ.get("POSTGRES_PORT",     "5433"),        
        "CONN_MAX_AGE": 600,
    },
    "replica": {
        "ENGINE":   "django.db.backends.postgresql",
        "NAME":     os.environ.get("POSTGRES_DB",           "reddit"),
        "USER":     os.environ.get("POSTGRES_USER",         "reddit"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD",     "reddit"),
        #"HOST":     os.environ.get("POSTGRES_REPLICA_HOST", "localhost"),
        #"PORT":     os.environ.get("POSTGRES_REPLICA_PORT", "5434"),
        "HOST":     os.environ.get("POSTGRES_HOST", "localhost"),
        "PORT":     os.environ.get("POSTGRES_PORT", "5433"),
        "CONN_MAX_AGE": 600,
    },
}


# Phase 2: route reads → replica, writes → default
#DATABASE_ROUTERS = ["config.db_router.ReadReplicaRouter"]

# ── Redis Cache ───────────────────────────────────────────────────────────────
_redis_base = os.environ.get("REDIS_URL", "redis://localhost:6379")

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": f"{_redis_base}/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            "IGNORE_EXCEPTIONS": True,
        },
        "KEY_PREFIX": "reddit",
    }
}

# Phase 2: Redis channel layer for Django Channels (DB 2 keeps keys separate)
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [f"{_redis_base}/2"],
        },
    }
}

# ── REST Framework — Phase 2 ──────────────────────────────────────────────────
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        "rest_framework.authentication.SessionAuthentication",  # browsable API
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.AllowAny",
        #"rest_framework.permissions.IsAuthenticated",
    ],
    # 100 requests/min per user; counters live in Redis
    "DEFAULT_THROTTLE_CLASSES": [
        "rest_framework.throttling.UserRateThrottle",
    ],
    "DEFAULT_THROTTLE_RATES": {
        "user": "100/min",
    },
}

# Phase 2: JWT token config
SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME":  timedelta(minutes=60),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=7),
    "ROTATE_REFRESH_TOKENS":  True,
    "ALGORITHM": "HS256",
    "AUTH_HEADER_TYPES": ("Bearer",),
}

# ── Auth ──────────────────────────────────────────────────────────────────────
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE     = "UTC"
USE_I18N      = True
USE_TZ        = True

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

CORS_ALLOW_ALL_ORIGINS = DEBUG
CORS_ALLOWED_ORIGINS = os.environ.get(
    "CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"
).split(",") if not DEBUG else []
