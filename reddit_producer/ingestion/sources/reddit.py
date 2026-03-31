"""
ingestion/sources/reddit.py
============================
Reddit ingestion source. Logic extracted from scheduler.py — unchanged.
Now inherits from BaseIngester so the poll loop, Kafka publishing,
and error handling are handled by the base class.

The only Reddit-specific code here is:
  - asyncpraw setup in setup()
  - fetching posts in poll()
  - the eviction + refresh workers (Reddit-specific because other
    sources don't have a "refresh" concept)
"""

import asyncio
import logging
import os
import time
from typing import AsyncIterator

import asyncpraw
import asyncprawcore
import psycopg2
from dotenv import load_dotenv

from ingestion.base import BaseIngester
from ingestion.priority_rules import calculate_priority

load_dotenv()
logger = logging.getLogger(__name__)

MAX_POST_AGE_SECONDS = 86_400

PRIORITY_INTERVALS = {
    "aggressive": 300,
    "normal":     1800,
    "slow":       7200,
}

_DEFAULT_SUBREDDITS = [
    {"name": "technology",  "interval": 120, "priority": "fast"},
    {"name": "worldnews",   "interval": 120, "priority": "fast"},
    {"name": "science",     "interval": 300, "priority": "medium"},
    {"name": "programming", "interval": 300, "priority": "medium"},
]


def _fetch_subreddit_config() -> list[dict]:
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ.get("POSTGRES_DB", "reddit"),
            user=os.environ.get("POSTGRES_USER", "reddit"),
            password=os.environ.get("POSTGRES_PASSWORD", "reddit"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT name, interval_seconds, priority FROM subreddit_config WHERE is_active ORDER BY name"
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
        result = [{"name": r[0], "interval": r[1], "priority": r[2]} for r in rows]
        logger.info("Loaded %d active subreddits from DB.", len(result))
        return result or _DEFAULT_SUBREDDITS
    except Exception as exc:
        logger.warning("Could not load subreddit_config (%s). Using defaults.", exc)
        return _DEFAULT_SUBREDDITS


def _serialize(submission, subreddit_name: str) -> dict:
    return {
        "id":           submission.id,
        "title":        submission.title,
        "selftext":     getattr(submission, "selftext", ""),
        "subreddit":    subreddit_name,
        "author":       str(submission.author) if submission.author else "deleted",
        "created_utc":  float(submission.created_utc or 0),
        "score":        int(submission.score or 0),
        "num_comments": int(submission.num_comments or 0),
        "upvote_ratio": float(submission.upvote_ratio or 0),
    }


def _serialize_comment(comment, submission, subreddit_name: str) -> dict:
    return {
        "id":                f"reddit_comment_{comment.id}",
        "title":             "",
        "selftext":          comment.body[:2048],
        "subreddit":         subreddit_name,
        "author":            str(comment.author) if comment.author else "deleted",
        "created_utc":       float(comment.created_utc or 0),
        "score":             int(comment.score or 0),
        "num_comments":      0,
        "upvote_ratio":      None,
        "parent_post_id":    submission.id,
        "parent_post_title": submission.title[:300],
        "is_comment":        True,
    }


class RedditIngester(BaseIngester):
    """
    Manages per-subreddit polling tasks, active post tracking,
    eviction, and refresh.
    """
    source_name  = "reddit"
    kafka_topic  = "reddit.posts.raw"
    poll_interval = 0

    def __init__(self):
        super().__init__()
        self.reddit = None
        self.active_posts: dict[str, dict] = {}
        self._lock = None
        self.task_map: dict[str, asyncio.Task] = {}
        self.subreddits = _fetch_subreddit_config()

    def _make_reddit_client(self) -> asyncpraw.Reddit:
        return asyncpraw.Reddit(
            client_id=os.environ["REDDIT_CLIENT_ID"],
            client_secret=os.environ["REDDIT_CLIENT_SECRET"],
            user_agent=os.environ["REDDIT_USER_AGENT"],
        )

    async def setup(self) -> None:
        self._lock = asyncio.Lock()
        self.reddit = self._make_reddit_client()
        logger.info("Reddit client initialised.")

    async def _recreate_client(self) -> None:
        try:
            await self.reddit.close()
        except Exception:
            pass
        self.reddit = self._make_reddit_client()
        logger.info("Reddit client recreated after session failure.")

    async def poll(self) -> AsyncIterator[dict]:
        return
        yield

    async def teardown(self) -> None:
        if self.reddit:
            await self.reddit.close()

    async def run(self) -> None:
        self.producer = await __import__(
            "ingestion.kafka_client", fromlist=["get_async_producer"]
        ).get_async_producer()
        await self.setup()

        self._spawn_poll_tasks(self.subreddits)
        tasks = list(self.task_map.values())
        tasks.append(asyncio.create_task(self._eviction_worker(), name="reddit-eviction"))
        tasks.append(asyncio.create_task(self._refresh_worker(), name="reddit-refresh"))
        tasks.append(asyncio.create_task(self._config_watcher(), name="reddit-config"))

        logger.info("Reddit ingester started — %d subreddits", len(self.subreddits))

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Reddit ingester shutdown cleanly.")
        finally:
            await self.teardown()

    def _spawn_poll_tasks(self, subs: list[dict]) -> None:
        active_names = {s["name"] for s in subs}
        for name, task in list(self.task_map.items()):
            if name not in active_names and not task.done():
                task.cancel()
                logger.info("Cancelled poll task for r/%s", name)
                del self.task_map[name]
        for sub in subs:
            if sub["name"] not in self.task_map or self.task_map[sub["name"]].done():
                t = asyncio.create_task(
                    self._poll_subreddit(sub["name"], sub["interval"]),
                    name=f"poll-{sub['name']}"
                )
                self.task_map[sub["name"]] = t

    async def _poll_subreddit(self, name: str, interval: int) -> None:
        logger.info("Poll task started for r/%s (interval=%ds)", name, interval)
        while True:
            now = time.time()
            try:
                sub = await self.reddit.subreddit(name)
                async for submission in sub.new(limit=25):
                    post = _serialize(submission, name)
                    post["poll_priority"]  = calculate_priority(post["created_utc"], now)
                    post["last_polled_at"] = None

                    async with self._lock:
                        is_new = post["id"] not in self.active_posts
                        self.active_posts[post["id"]] = post

                    if is_new:
                        from ingestion.normaliser import normalise
                        signal = normalise("reddit", post)
                        if signal:
                            await self.producer.send("reddit.posts.raw", post)
                            await self.producer.send("signals.normalised", signal)

                        # Top comments — requires load() since asyncpraw 7.7.1
                        try:
                            await submission.load()
                            for comment in submission.comments[:25]:
                                if not hasattr(comment, "body"):
                                    continue
                                if comment.body in ("[deleted]", "[removed]", ""):
                                    continue
                                if int(comment.score or 0) < 5:
                                    continue
                                raw_comment = _serialize_comment(comment, submission, name)
                                comment_signal = normalise("reddit", raw_comment)
                                if comment_signal:
                                    await self.producer.send("signals.normalised", comment_signal)
                        except asyncprawcore.exceptions.TooManyRequests:
                            logger.debug("[r/%s] Comment load rate limited — skipping", name)
                            await asyncio.sleep(30)
                        except Exception as exc:
                            logger.debug("[r/%s] Comment skipped: %s", name, exc)

            except asyncprawcore.exceptions.TooManyRequests as exc:
                wait = max(getattr(exc, "retry_after", None) or 60, 30)
                logger.warning("[r/%s] Rate limited — sleeping %ss", name, wait)
                await asyncio.sleep(wait)
                continue
            except asyncprawcore.exceptions.ResponseException as exc:
                logger.error("[r/%s] Reddit API error: %s", name, exc)
            except Exception as exc:
                err_str = str(exc)
                if "Session is closed" in err_str or "ClientConnectorError" in err_str:
                    logger.warning("[r/%s] Session closed — recreating Reddit client", name)
                    await self._recreate_client()
                    await asyncio.sleep(5)
                    continue
                logger.exception("[r/%s] Unexpected error", name)

            await asyncio.sleep(interval)

    async def _eviction_worker(self) -> None:
        while True:
            await asyncio.sleep(60)
            cutoff = time.time() - MAX_POST_AGE_SECONDS
            async with self._lock:
                expired = [pid for pid, p in self.active_posts.items()
                           if p["created_utc"] < cutoff]
                for pid in expired:
                    del self.active_posts[pid]
            if expired:
                logger.debug("Evicted %d posts. Active: %d", len(expired), len(self.active_posts))

    async def _refresh_worker(self) -> None:
        while True:
            await asyncio.sleep(10)
            now = time.time()
            async with self._lock:
                candidates = list(self.active_posts.values())
            for post in candidates:
                priority = post.get("poll_priority", "inactive")
                interval = PRIORITY_INTERVALS.get(priority)
                if not interval:
                    continue
                last = post.get("last_polled_at")
                if last is not None and (now - last) <= interval:
                    continue
                try:
                    submission = await self.reddit.submission(id=post["id"])
                    await submission.load()
                    updated = _serialize(submission, post["subreddit"])
                    updated["poll_priority"]  = calculate_priority(updated["created_utc"], now)
                    updated["last_polled_at"] = now
                    async with self._lock:
                        self.active_posts[post["id"]] = updated
                    await self.producer.send("reddit.posts.refresh", updated)
                except asyncprawcore.exceptions.TooManyRequests as exc:
                    wait = max(getattr(exc, "retry_after", None) or 60, 30)
                    await asyncio.sleep(wait)
                except Exception as exc:
                    if "Session is closed" in str(exc):
                        logger.warning("[refresh] Session closed — recreating Reddit client")
                        await self._recreate_client()
                        await asyncio.sleep(5)
                        break
                    logger.exception("[refresh] Failed post %s", post.get("id"))

    async def _config_watcher(self) -> None:
        poll_s = int(os.environ.get("SCHEDULER_CONFIG_POLL_S", "60"))
        while True:
            await asyncio.sleep(poll_s)
            try:
                new_config = await asyncio.to_thread(_fetch_subreddit_config)
                self._spawn_poll_tasks(new_config)
            except Exception as exc:
                logger.warning("Config hot-reload failed: %s", exc)
