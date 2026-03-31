"""
ingestion/sources/youtube.py
=============================
YouTube ingestion with:
  - Multi-key rotation (YOUTUBE_API_KEY_1, _2, etc.) for maximum quota
  - Dead channel detection and automatic skip with logging
  - Daily channel ID refresh via handle → ID resolution
    (handles are stable; raw IDs go dead when YouTube migrates channels)
  - Channels loaded from env: YOUTUBE_CHANNEL_HANDLES (comma-separated @handles)
    Falls back to DEFAULT_CHANNEL_HANDLES if env var not set.

Channel ID refresh runs once on startup and every 24 hours thereafter.
Dead channels are logged clearly and skipped — never crash the poll cycle.

API key rotation:
  Set YOUTUBE_API_KEY_1, YOUTUBE_API_KEY_2, etc. in .env
  Falls back to YOUTUBE_API_KEY if no numbered keys found.
  Keys are rotated round-robin per channel — when one exhausts its
  10,000 unit/day quota, the next key takes over automatically.
"""

import asyncio
import logging
import os
import time
from typing import AsyncIterator

import aiohttp
from ingestion.base import BaseIngester

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Default channel handles — stable across YouTube migrations.
# Override with YOUTUBE_CHANNEL_HANDLES env var (comma-separated @handles).
DEFAULT_CHANNEL_HANDLES = [
    "@Fireship",
    "@TechWithTim",
    "@ThePrimeTimeagen",
    "@LinusTechTips",
    "@TheCodingTrain",
    "@GoogleDevelopers",
    "@t3dotgg",
    "@ByteByteGo",
    "@NetworkChuck",
    "@ContinuousDelivery",
]

# How often to re-resolve handles → channel IDs (seconds)
CHANNEL_REFRESH_INTERVAL = 86400  # 24 hours


def _load_api_keys() -> list[str]:
    """
    Load all YOUTUBE_API_KEY_N keys from env, falling back to YOUTUBE_API_KEY.
    Returns list of valid (non-empty) keys.
    """
    keys = []
    i = 1
    while True:
        k = os.environ.get(f"YOUTUBE_API_KEY_{i}", "")
        if not k:
            break
        keys.append(k)
        i += 1
    if not keys:
        k = os.environ.get("YOUTUBE_API_KEY", "")
        if k:
            keys.append(k)
    return keys


class YouTubeIngester(BaseIngester):
    source_name   = "youtube"
    kafka_topic   = "youtube.comments.raw"
    poll_interval = 21600.0  # 6 hours — quota-conscious

    def __init__(self):
        super().__init__()
        self.session = None

        # API key pool — round-robin per channel
        self._api_keys: list[str] = _load_api_keys()
        self._key_index: int = 0                    # current key pointer
        self._exhausted_keys: set[str] = set()      # keys that hit quota today
        self._key_reset_ts: float = 0.0             # midnight UTC reset timestamp

        # Channel state
        self._handles: list[str] = self._load_handles()
        self._channel_ids: list[str] = []           # resolved at startup + daily
        self._dead_channels: set[str] = set()       # channel IDs confirmed dead
        self._last_channel_refresh: float = 0.0
        self._uploads_playlist_cache: dict[str, str] = {}

        # Dedup
        self._seen_comments: set[str] = set()

    def _load_handles(self) -> list[str]:
        env = os.environ.get("YOUTUBE_CHANNEL_HANDLES", "")
        if env:
            handles = [h.strip().lstrip("@") for h in env.split(",") if h.strip()]
            logger.info("YouTube: loaded %d handles from env", len(handles))
            return handles
        # Strip @ for consistent storage, re-add when querying
        return [h.lstrip("@") for h in DEFAULT_CHANNEL_HANDLES]

    def _next_key(self) -> str | None:
        """
        Round-robin across non-exhausted keys.
        Returns None if all keys are exhausted for the day.
        """
        # Reset exhausted set at midnight UTC
        midnight_utc = (time.time() // 86400 + 1) * 86400
        if time.time() > self._key_reset_ts and self._key_reset_ts > 0:
            if self._exhausted_keys:
                logger.info("YouTube: midnight reset — clearing %d exhausted key(s)", len(self._exhausted_keys))
            self._exhausted_keys.clear()
        self._key_reset_ts = midnight_utc

        available = [k for k in self._api_keys if k not in self._exhausted_keys]
        if not available:
            return None

        key = available[self._key_index % len(available)]
        self._key_index = (self._key_index + 1) % len(available)
        return key

    def _mark_key_exhausted(self, key: str):
        self._exhausted_keys.add(key)
        remaining = len(self._api_keys) - len(self._exhausted_keys)
        logger.warning(
            "YouTube: key ...%s quota exhausted — %d key(s) remaining today",
            key[-6:], remaining
        )

    async def setup(self) -> None:
        if not self._api_keys:
            raise RuntimeError(
                "No YouTube API keys found. Set YOUTUBE_API_KEY_1 "
                "(and optionally _2, _3) in .env"
            )
        self.session = aiohttp.ClientSession()
        logger.info(
            "YouTube ingester ready — %d API key(s), %d handles to resolve",
            len(self._api_keys), len(self._handles)
        )
        # Resolve handles → IDs on startup
        await self._refresh_channel_ids()

    async def _resolve_handle(self, handle: str, api_key: str) -> str | None:
        """
        Resolve a @handle to a channel ID via the YouTube channels API.
        Cost: 1 unit. Handles are stable; raw IDs are not.
        """
        params = {
            "key":        api_key,
            "forHandle":  f"@{handle}",
            "part":       "id",
            "maxResults": 1,
        }
        try:
            async with self.session.get(
                f"{YOUTUBE_API_BASE}/channels",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"].get("message", "")
                    if "quota" in err.lower():
                        self._mark_key_exhausted(api_key)
                    else:
                        logger.warning("Handle resolve error for @%s: %s", handle, err)
                    return None
                items = data.get("items", [])
                if not items:
                    logger.warning("YouTube: @%s not found — handle may be wrong or channel deleted", handle)
                    return None
                channel_id = items[0].get("id")
                logger.info("YouTube: @%s → %s", handle, channel_id)
                return channel_id
        except Exception as e:
            logger.warning("YouTube: handle resolve failed for @%s: %s", handle, e)
            return None

    async def _refresh_channel_ids(self):
        """
        Re-resolve all handles to channel IDs.
        Runs on startup and every 24 hours.
        Dead channels are detected here when resolve returns None.
        """
        logger.info("YouTube: refreshing channel IDs for %d handles...", len(self._handles))
        key = self._next_key()
        if not key:
            logger.error("YouTube: no API keys available for channel refresh")
            return

        resolved = []
        newly_dead = []

        for handle in self._handles:
            channel_id = await self._resolve_handle(handle, key)
            if channel_id:
                resolved.append(channel_id)
                # Clear from dead set if it came back alive
                self._dead_channels.discard(channel_id)
            else:
                newly_dead.append(handle)
            await asyncio.sleep(0.2)  # gentle pacing — 1 unit per resolve

        self._channel_ids = resolved
        self._last_channel_refresh = time.time()

        if newly_dead:
            logger.warning(
                "YouTube: %d handle(s) failed to resolve (dead/renamed): %s",
                len(newly_dead), newly_dead
            )
        logger.info(
            "YouTube: channel refresh complete — %d/%d channels active",
            len(resolved), len(self._handles)
        )

        # Clear the uploads playlist cache so stale IDs are re-fetched
        self._uploads_playlist_cache.clear()

    async def poll(self) -> AsyncIterator[dict]:
        # Daily channel refresh cron — runs inside poll cycle
        if time.time() - self._last_channel_refresh > CHANNEL_REFRESH_INTERVAL:
            logger.info("YouTube: 24h channel refresh triggered")
            await self._refresh_channel_ids()

        if not self._channel_ids:
            logger.warning("YouTube: no active channel IDs — skipping poll")
            return

        key = self._next_key()
        if not key:
            logger.warning("YouTube: all API keys exhausted for today — skipping poll")
            return

        for i, channel_id in enumerate(self._channel_ids):
            if channel_id in self._dead_channels:
                logger.debug("YouTube: skipping known-dead channel %s", channel_id)
                continue

            # Rotate key per channel for even quota distribution
            current_key = self._api_keys[i % len(self._api_keys)]
            if current_key in self._exhausted_keys:
                current_key = self._next_key()
                if not current_key:
                    logger.warning("YouTube: all keys exhausted mid-poll — stopping")
                    return

            async for comment in self._fetch_channel_comments(channel_id, current_key):
                yield comment
            await asyncio.sleep(0.5)

    async def _get_uploads_playlist_id(self, channel_id: str, api_key: str) -> str | None:
        if channel_id in self._uploads_playlist_cache:
            return self._uploads_playlist_cache[channel_id]

        params = {
            "key":  api_key,
            "id":   channel_id,
            "part": "contentDetails",
        }
        try:
            async with self.session.get(
                f"{YOUTUBE_API_BASE}/channels",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"].get("message", "")
                    if "quota" in err.lower():
                        self._mark_key_exhausted(api_key)
                    else:
                        # Channel not found or deleted — mark dead
                        logger.warning(
                            "YouTube: channel %s returned error '%s' — marking dead",
                            channel_id, err
                        )
                        self._dead_channels.add(channel_id)
                    return None
                items = data.get("items", [])
                if not items:
                    logger.warning(
                        "YouTube: channel %s returned no items — marking dead", channel_id
                    )
                    self._dead_channels.add(channel_id)
                    return None
                playlist_id = (
                    items[0]
                    .get("contentDetails", {})
                    .get("relatedPlaylists", {})
                    .get("uploads")
                )
                if playlist_id:
                    self._uploads_playlist_cache[channel_id] = playlist_id
                return playlist_id
        except Exception as e:
            logger.warning("YouTube: uploads playlist fetch failed for %s: %s", channel_id, e)
            return None

    async def _get_latest_videos(
        self, channel_id: str, api_key: str, max_results: int = 5
    ) -> list:
        """
        Fetch most recent videos via uploads playlist.
        Increased to 5 videos (was 3) for more comment coverage.
        Cost: 1 unit per call.
        """
        playlist_id = await self._get_uploads_playlist_id(channel_id, api_key)
        if not playlist_id:
            return []

        params = {
            "key":        api_key,
            "playlistId": playlist_id,
            "part":       "snippet",
            "maxResults": max_results,
        }
        try:
            async with self.session.get(
                f"{YOUTUBE_API_BASE}/playlistItems",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if "error" in data:
                    err = data["error"].get("message", "")
                    if "quota" in err.lower():
                        self._mark_key_exhausted(api_key)
                    else:
                        logger.warning("YouTube: playlistItems error for %s: %s", channel_id, err)
                    return []

                videos = []
                for item in data.get("items", []):
                    snippet  = item.get("snippet", {})
                    video_id = snippet.get("resourceId", {}).get("videoId")
                    if not video_id:
                        continue
                    videos.append({
                        "id":      {"videoId": video_id},
                        "snippet": {
                            "title":        snippet.get("title", ""),
                            "channelTitle": snippet.get("channelTitle", ""),
                        },
                    })
                return videos
        except Exception as e:
            logger.warning("YouTube: video list failed for %s: %s", channel_id, e)
            return []

    async def _fetch_channel_comments(
        self, channel_id: str, api_key: str
    ) -> AsyncIterator[dict]:
        try:
            videos = await self._get_latest_videos(channel_id, api_key, max_results=5)
        except Exception as e:
            logger.warning("YouTube: fetch failed for channel %s: %s", channel_id, e)
            return

        if not videos:
            return

        for video in videos:
            video_id      = video["id"]["videoId"]
            video_title   = video["snippet"]["title"]
            channel_title = video["snippet"]["channelTitle"]

            async for comment in self._fetch_video_comments(
                video_id, video_title, channel_id, channel_title, api_key
            ):
                yield comment

            await asyncio.sleep(0.2)

    async def _fetch_video_comments(
        self,
        video_id: str,
        video_title: str,
        channel_id: str,
        channel_title: str,
        api_key: str,
        max_pages: int = 2,
    ) -> AsyncIterator[dict]:
        page_token    = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {
                "key":        api_key,
                "videoId":    video_id,
                "part":       "id,snippet",
                "order":      "relevance",
                "maxResults": 100,
            }
            if page_token:
                params["pageToken"] = page_token

            try:
                async with self.session.get(
                    f"{YOUTUBE_API_BASE}/commentThreads",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    data = await resp.json()

                    if "error" in data:
                        err = data["error"].get("message", "")
                        if "commentsDisabled" in err or "disabled comments" in err:
                            logger.debug("YouTube: comments disabled for video %s", video_id)
                        elif "quota" in err.lower():
                            self._mark_key_exhausted(api_key)
                            logger.warning("YouTube: quota hit mid-video %s — halting", video_id)
                        elif "videoNotFound" in err or "forbidden" in err.lower():
                            logger.warning(
                                "YouTube: video %s inaccessible (%s) — skipping",
                                video_id, err[:60]
                            )
                        else:
                            logger.warning("YouTube API error for %s: %s", video_id, err)
                        return

                    for item in data.get("items", []):
                        comment_id = item.get("id", "")
                        if comment_id in self._seen_comments:
                            continue
                        self._seen_comments.add(comment_id)

                        if len(self._seen_comments) > 50_000:
                            self._seen_comments = set(list(self._seen_comments)[25_000:])

                        item["video_title"]   = video_title
                        item["channel_id"]    = channel_id
                        item["channel_title"] = channel_title
                        yield item

                    page_token = data.get("nextPageToken")
                    pages_fetched += 1
                    if not page_token:
                        break

            except aiohttp.ClientError as e:
                logger.warning("YouTube: request error for %s: %s", video_id, e)
                break

    async def teardown(self) -> None:
        if self.session:
            await self.session.close()
