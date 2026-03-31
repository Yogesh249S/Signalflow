"""
ingestion/sources/hackernews.py
================================
Hacker News ingestion via Algolia API.
No auth, no rate limits worth worrying about, completely free.

Polls for new stories every 5 minutes.
Also ingests top comments per story to boost signal volume.
Algolia search_by_date returns stories sorted by recency.
"""

import logging
import time
from typing import AsyncIterator

import aiohttp
from ingestion.base import BaseIngester

logger = logging.getLogger(__name__)

ALGOLIA_URL      = "https://hn.algolia.com/api/v1/search_by_date"
ALGOLIA_ITEM_URL = "https://hn.algolia.com/api/v1/items"

# Only ingest these HN tags — skip polls, job posts
VALID_TAGS = {"story", "ask_hn", "show_hn"}


class HackerNewsIngester(BaseIngester):
    source_name   = "hackernews"
    kafka_topic   = "hackernews.stories.raw"
    poll_interval = 300.0  # 5 minutes

    def __init__(self):
        super().__init__()
        self.session = None
        self._seen: set[str] = set()
        self._seen_maxsize = 20_000

    async def setup(self) -> None:
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "SignalFlow/1.0 (social intelligence platform)"}
        )
        logger.info("HackerNews ingester ready.")

    async def poll(self) -> AsyncIterator[dict]:
        if not self.session:
            return

        # Widen window to 6 hours to catch more stories per poll
        cutoff = int(time.time()) - 21600
        params = {
            "tags":           "story",
            "hitsPerPage":    200,           # was 100 — Algolia max is 1000
            "numericFilters": f"created_at_i>{cutoff}",
        }

        try:
            async with self.session.get(
                ALGOLIA_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logger.warning("HN API returned %d", resp.status)
                    return
                data = await resp.json()

            hits = data.get("hits", [])
            new_stories = 0
            logger.info("HN poll: %d stories in window", len(hits))

            for hit in hits:
                story_id = str(hit.get("objectID", ""))
                if not story_id or story_id in self._seen:
                    continue

                tags = set(hit.get("_tags", []))
                if not tags.intersection(VALID_TAGS):
                    continue

                if hit.get("dead") or hit.get("deleted"):
                    continue

                # Only ingest stories with some traction (>= 3 points or comments)
                points   = hit.get("points") or 0
                num_cmts = hit.get("num_comments") or 0
                if points < 3 and num_cmts < 2:
                    continue

                self._seen.add(story_id)
                new_stories += 1

                if len(self._seen) > self._seen_maxsize:
                    self._seen = set(list(self._seen)[self._seen_maxsize // 2:])

                yield hit

                # For high-engagement stories, also ingest top comments
                # This significantly boosts HN signal volume
                if num_cmts >= 10:
                    async for comment in self._fetch_top_comments(story_id, max_comments=25):
                        yield comment

            logger.info("HN poll: %d new stories yielded", new_stories)

        except aiohttp.ClientError as e:
            logger.warning("HN API request failed: %s", e)
        except Exception:
            logger.exception("HN poll unexpected error")

    async def _fetch_top_comments(
        self, story_id: str, max_comments: int = 25
    ) -> AsyncIterator[dict]:
        """
        Fetch top-level comments for a story via Algolia items endpoint.
        Only called for high-engagement stories (>= 10 comments).
        No extra quota cost — Algolia is free and unlimited.
        """
        try:
            async with self.session.get(
                f"{ALGOLIA_ITEM_URL}/{story_id}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()

            children = data.get("children", [])[:max_comments]
            for child in children:
                comment_id = str(child.get("id", ""))
                if not comment_id or comment_id in self._seen:
                    continue
                if child.get("type") != "comment":
                    continue
                if not child.get("text"):
                    continue
                if child.get("deleted") or child.get("dead"):
                    continue

                self._seen.add(comment_id)

                # Reshape comment to match story schema downstream
                # created_at_i must be present — normaliser expects unix timestamp
                import calendar, email.utils
                raw_ts = child.get("created_at", "")
                try:
                    created_at_i = int(calendar.timegm(
                        email.utils.parsedate(raw_ts)
                    )) if raw_ts else int(time.time())
                except Exception:
                    created_at_i = int(time.time())

                yield {
                    "objectID":     comment_id,
                    "parent_id":    story_id,
                    "title":        data.get("title", ""),
                    "body":         child.get("text", ""),
                    "author":       child.get("author", ""),
                    "points":       child.get("points") or 0,
                    "created_at_i": created_at_i,
                    "story_id":     story_id,
                    "is_comment":   True,
                    "_tags":        ["comment"],
                }

        except Exception as e:
            logger.debug("HN comment fetch failed for story %s: %s", story_id, e)

    async def teardown(self) -> None:
        if self.session:
            await self.session.close()
