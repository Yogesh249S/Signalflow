"""
ingestion/sources/youtube.py
=============================
YouTube comment ingestion via YouTube Data API v3.

Strategy:
  - Maintain a list of tracked channels (tech-focused)
  - Every poll cycle: fetch latest videos from each channel
  - For each new video: fetch top-level comments
  - Quota: 10,000 units/day. Each commentThreads page = 1 unit.
    At 100 comments/page: 10,000 pages/day = 1M comments/day theoretical max.
    In practice, track ~50 channels, fetch comments on latest 3 videos each
    per 4-hour cycle = manageable quota usage.

Channels are configurable via YOUTUBE_CHANNELS env var (comma-separated IDs)
or hardcoded defaults below.
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

# Default tech channels to track — override with YOUTUBE_CHANNELS env var
DEFAULT_CHANNELS = [
    "UCsBjURrPoezykLs9EqgamOA",  # Fireship
    "UCVhQ2NnY5Rskt6UjCUkJ_DA",  # Tech with Tim
    "UCWX3yG9WUQQ4pFMxfDSEBOQ",  # ThePrimeagen
    "UCXuqSBlHAE6Xw-yeJA0Tunw",  # Linus Tech Tips
    "UC295-Dw4tztFUTpCHoEjVAQ",  # The Coding Train
    "UCnUYZLuoy1rq1aVMwx4aTzw",  # Google for Developers
    "UCddiUEpeqJcYeBxX1IVBKvQ",  # Theo
    "UCsBjURrPoezykLs9EqgamOA",  # ByteByteGo
]


class YouTubeIngester(BaseIngester):
    source_name   = "youtube"
    kafka_topic   = "youtube.comments.raw"
    poll_interval = 21600.0  # 6 hours — quota-conscious

    def __init__(self):
        super().__init__()
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")
        self.session = None
        self._seen_videos:   set[str] = set()
        self._seen_comments: set[str] = set()

        # Load channel list from env or use defaults
        channels_env = os.environ.get("YOUTUBE_CHANNELS", "")
        self.channels = (
            [c.strip() for c in channels_env.split(",") if c.strip()]
            if channels_env else DEFAULT_CHANNELS
        )

    async def setup(self) -> None:
        if not self.api_key:
            raise RuntimeError("YOUTUBE_API_KEY environment variable not set.")
        self.session = aiohttp.ClientSession()
        logger.info("YouTube ingester ready — tracking %d channels.", len(self.channels))

    async def poll(self) -> AsyncIterator[dict]:
        for channel_id in self.channels:
            async for comment in self._fetch_channel_comments(channel_id):
                yield comment
            await asyncio.sleep(0.5)  # brief pause between channels

    async def _fetch_channel_comments(self, channel_id: str) -> AsyncIterator[dict]:
        """Fetch latest videos from a channel then pull comments for each."""
        try:
            videos = await self._get_latest_videos(channel_id, max_results=3)
        except Exception as e:
            logger.warning("Failed to fetch videos for channel %s: %s", channel_id, e)
            return

        for video in videos:
            video_id    = video["id"]["videoId"]
            video_title = video["snippet"]["title"]
            channel_title = video["snippet"]["channelTitle"]

            async for comment in self._fetch_video_comments(
                video_id, video_title, channel_id, channel_title
            ):
                yield comment

            await asyncio.sleep(0.2)

    async def _get_latest_videos(self, channel_id: str, max_results: int = 3) -> list:
        """
        Fetch most recent videos from a channel.
        Cost: 100 units per call.
        """
        params = {
            "key":        self.api_key,
            "channelId":  channel_id,
            "part":       "id,snippet",
            "order":      "date",
            "type":       "video",
            "maxResults": max_results,
        }
        async with self.session.get(
            f"{YOUTUBE_API_BASE}/search",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            data = await resp.json()
            if "error" in data:
                raise RuntimeError(data["error"].get("message", "API error"))
            return [
                item for item in data.get("items", [])
                if item.get("id", {}).get("kind") == "youtube#video"
            ]

    async def _fetch_video_comments(
        self,
        video_id: str,
        video_title: str,
        channel_id: str,
        channel_title: str,
        max_pages: int = 2,
    ) -> AsyncIterator[dict]:
        """
        Fetch top-level comment threads for a video.
        Cost: 1 unit per page, 100 comments per page.
        max_pages=2 means max 200 comments per video, 2 units spent.
        """
        page_token = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {
                "key":        self.api_key,
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
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    data = await resp.json()

                    if "error" in data:
                        err = data["error"].get("message", "")
                        if "commentsDisabled" in err:
                            logger.debug("Comments disabled for video %s", video_id)
                        else:
                            logger.warning("YouTube API error for %s: %s", video_id, err)
                        return

                    for item in data.get("items", []):
                        comment_id = item.get("id", "")
                        if comment_id in self._seen_comments:
                            continue
                        self._seen_comments.add(comment_id)

                        # Evict if set gets large
                        if len(self._seen_comments) > 50_000:
                            self._seen_comments = set(list(self._seen_comments)[25_000:])

                        # Inject video/channel metadata for the normaliser
                        item["video_title"]   = video_title
                        item["channel_id"]    = channel_id
                        item["channel_title"] = channel_title

                        yield item

                    page_token = data.get("nextPageToken")
                    pages_fetched += 1
                    if not page_token:
                        break

            except aiohttp.ClientError as e:
                logger.warning("YouTube request error for %s: %s", video_id, e)
                break

    async def teardown(self) -> None:
        if self.session:
            await self.session.close()
