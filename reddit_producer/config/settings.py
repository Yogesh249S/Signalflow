"""
config/settings.py — Phase 3 (clean slate)
===========================================
apps.reddit removed entirely.
apps.signals is the single app handling all 4 platforms.

All Phase 2 infrastructure kept:
  - Redis cache, CHANNEL_LAYERS, CONN_MAX_AGE
  - JWT auth, throttle, SIMPLE_JWT
  - ASGI_APPLICATION → config.routing
  - DATABASES replica (commented, ready to enable)
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
    "channels",
    "rest_framework_simplejwt",
    "apps.signals",       # single app — all 4 platforms
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF      = "config.urls"
WSGI_APPLICATION  = "config.wsgi.application"
ASGI_APPLICATION  = "config.routing.application"

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
        "NAME":     os.environ.get("POSTGRES_DB",       "reddit"),
        "USER":     os.environ.get("POSTGRES_USER",     "reddit"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "reddit"),
        "HOST":     os.environ.get("POSTGRES_HOST",     "localhost"),
        "PORT":     os.environ.get("POSTGRES_PORT",     "5433"),
        "CONN_MAX_AGE": 600,
    },
}

# DATABASE_ROUTERS = ["config.db_router.ReadReplicaRouter"]

_redis_base = os.environ.get("REDIS_URL", "redis://localhost:6379")

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": f"{_redis_base}/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
            "IGNORE_EXCEPTIONS": True,
        },
        "KEY_PREFIX": "signalflow",
    }
}

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {"hosts": [f"{_redis_base}/2"]},
    }
}

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_THROTTLE_CLASSES": [
        "rest_framework.throttling.UserRateThrottle",
    ],
    "DEFAULT_THROTTLE_RATES": {
        "user": "100/min",
    },
}

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME":  timedelta(minutes=60),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=7),
    "ROTATE_REFRESH_TOKENS":  True,
    "ALGORITHM": "HS256",
    "AUTH_HEADER_TYPES": ("Bearer",),
}

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
