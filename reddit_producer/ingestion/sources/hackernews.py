"""
ingestion/sources/hackernews.py
================================
Hacker News ingestion via Algolia API.
No auth, no rate limits worth worrying about, completely free.

Polls for new stories every 5 minutes.
Algolia search_by_date returns stories sorted by recency.
"""

import logging
import time
from typing import AsyncIterator

import aiohttp
from ingestion.base import BaseIngester

logger = logging.getLogger(__name__)

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"

# Only ingest these HN tags — skip comments, polls, job posts
VALID_TAGS = {"story", "ask_hn", "show_hn"}


class HackerNewsIngester(BaseIngester):
    source_name   = "hackernews"
    kafka_topic   = "hackernews.stories.raw"
    poll_interval = 300.0  # 5 minutes — HN moves slower than Reddit

    def __init__(self):
        super().__init__()
        self.session = None
        self._seen: set[str] = set()   # in-memory dedup within session
        self._seen_maxsize = 10_000    # evict when set gets large

    async def setup(self) -> None:
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "SignalFlow/1.0 (social intelligence platform)"}
        )
        logger.info("HackerNews ingester ready.")

    async def poll(self) -> AsyncIterator[dict]:
        if not self.session:
            return

        params = {
            "tags":        "story",
            "hitsPerPage": 100,
            "numericFilters": "created_at_i>0",
        }

        try:
            async with self.session.get(ALGOLIA_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("HN API returned %d", resp.status)
                    return
                data = await resp.json()

            hits = data.get("hits", [])
            logger.info("HN poll: %d stories fetched", len(hits))

            for hit in hits:
                story_id = str(hit.get("objectID", ""))
                if not story_id or story_id in self._seen:
                    continue

                # Filter to valid content types
                tags = set(hit.get("_tags", []))
                if not tags.intersection(VALID_TAGS):
                    continue

                # Skip dead/deleted stories
                if hit.get("dead") or hit.get("deleted"):
                    continue

                self._seen.add(story_id)

                # Evict oldest entries if set is too large
                if len(self._seen) > self._seen_maxsize:
                    # Sets don't have order so just clear half
                    self._seen = set(list(self._seen)[self._seen_maxsize // 2:])

                yield hit

        except aiohttp.ClientError as e:
            logger.warning("HN API request failed: %s", e)
        except Exception:
            logger.exception("HN poll unexpected error")

    async def teardown(self) -> None:
        if self.session:
            await self.session.close()
