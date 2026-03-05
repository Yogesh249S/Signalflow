"""
ingestion/rate_limiter.py
==========================
Token bucket rate limiter for ingestion sources.

Smooths out bursty platform behaviour (YouTube 3k/s spikes,
Bluesky firehose floods) into a controlled, predictable flow
that keeps Grafana graphs readable and workers healthy.

Usage in BaseIngester — automatic, no changes needed per-source.
Config via environment variables or PLATFORM_LIMITS dict below.

Token Bucket Algorithm:
  - Bucket holds up to `burst` tokens
  - `rate` tokens are added per second (refill)
  - Each message consumes 1 token
  - If bucket empty → message is dropped (non-blocking)

Example:
  rate=10, burst=20 means:
    → steady state: 10 messages/sec
    → can absorb a burst of 20 before dropping
    → recovers at 10 tokens/sec after a quiet period
"""

import logging
import os
import time

logger = logging.getLogger(__name__)

# ── Per-platform limits ───────────────────────────────────────────────────────
# Tune these to control how much each platform can push per second.
# Set rate=-1 to disable rate limiting for that platform.
#
# These can also be overridden via environment variables:
#   RATE_LIMIT_YOUTUBE=10
#   RATE_LIMIT_BLUESKY=20
#   RATE_LIMIT_REDDIT=10
#   RATE_LIMIT_HACKERNEWS=5

PLATFORM_LIMITS: dict[str, dict] = {
    "youtube":    {"rate": 10,  "burst": 30},   # quota is precious — keep low
    "bluesky":    {"rate": 20,  "burst": 60},   # firehose — must throttle hard
    "reddit":     {"rate": 10,  "burst": 30},   # poll-based, moderate
    "hackernews": {"rate": 5,   "burst": 15},   # low volume anyway
}

_DEFAULT = {"rate": 10, "burst": 30}


class TokenBucket:
    """
    Thread-safe (asyncio-safe) token bucket rate limiter.

    acquire() returns True if the message is allowed, False if dropped.
    Non-blocking — never sleeps, never delays the firehose loop.
    """

    def __init__(self, rate: float, burst: float, source_name: str = ""):
        self.rate        = rate         # tokens added per second
        self.burst       = burst        # max bucket capacity
        self.tokens      = burst        # start full
        self._last_refill = time.monotonic()
        self._source     = source_name
        self._allowed    = 0
        self._dropped    = 0
        self._last_log   = time.monotonic()

    def acquire(self) -> bool:
        """Return True = message allowed, False = message dropped."""
        if self.rate < 0:
            return True  # disabled

        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now

        # Refill tokens proportional to elapsed time
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            self._allowed += 1
            self._maybe_log(now)
            return True
        else:
            self._dropped += 1
            self._maybe_log(now)
            return False

    def _maybe_log(self, now: float) -> None:
        """Log stats every 60 seconds so you can see what's being dropped."""
        if now - self._last_log >= 60:
            total = self._allowed + self._dropped
            drop_pct = (self._dropped / total * 100) if total else 0
            logger.info(
                "[%s] rate_limiter: allowed=%d dropped=%d (%.1f%% drop) tokens=%.1f",
                self._source, self._allowed, self._dropped, drop_pct, self.tokens,
            )
            self._allowed  = 0
            self._dropped  = 0
            self._last_log = now


def make_limiter(source_name: str) -> TokenBucket:
    """
    Build a TokenBucket for the given source.
    Checks env vars first, falls back to PLATFORM_LIMITS, then _DEFAULT.
    """
    env_rate = os.environ.get(f"RATE_LIMIT_{source_name.upper()}")
    if env_rate is not None:
        try:
            rate = float(env_rate)
            burst = rate * 3
            logger.info("[%s] rate_limiter: using env override rate=%.0f burst=%.0f",
                        source_name, rate, burst)
            return TokenBucket(rate=rate, burst=burst, source_name=source_name)
        except ValueError:
            logger.warning("[%s] Invalid RATE_LIMIT env var: %s", source_name, env_rate)

    cfg = PLATFORM_LIMITS.get(source_name, _DEFAULT)
    logger.info("[%s] rate_limiter: rate=%.0f/s burst=%.0f", source_name, cfg["rate"], cfg["burst"])
    return TokenBucket(rate=cfg["rate"], burst=cfg["burst"], source_name=source_name)
