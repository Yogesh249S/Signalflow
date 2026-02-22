"""
ingestion/scheduler.py
======================
OPTIMISATION CHANGES vs original
---------------------------------
1. ASYNC CONCURRENT POLLING (biggest change)
   - Original: single `while True` loop, 20 subreddits polled *sequentially*.
     Each `safe_fetch_new_posts()` call blocked the entire thread waiting for a
     Reddit HTTP response before the next subreddit could start. With 20
     subreddits each taking ~1s per network round-trip, one full cycle took
     20+ seconds, making fast-tier (5-min) polling meaningless in practice.
   - New: each subreddit gets its own independent `asyncio.Task` via asyncpraw.
     All 20 subreddits poll *concurrently*. A rate-limit sleep on r/funny no
     longer delays r/gaming at all.

2. DICT-BASED active_posts — O(1) dedup, no list rebuilds
   - Original: `ACTIVE_POSTS` was a plain list. New posts were blindly appended
     each poll cycle, accumulating duplicates. Eviction was a full O(n) list
     comprehension every 10-second tick.
   - New: `active_posts` is a `dict[post_id -> post]`. Upsert by ID is O(1),
     duplicates are impossible, and eviction only runs every 60 s.

3. SEPARATE REFRESH TASK
   - Original: refresh scanning ran inside the same sequential new-post loop,
     so a burst of 500 posts due for refresh would block new ingestion entirely.
   - New: `refresh_worker` is an independent asyncio Task on its own cadence.

4. GRACEFUL SHUTDOWN via signal handlers
   - Original: Ctrl-C killed the process mid-write with no cleanup.
   - New: SIGINT/SIGTERM cancel all tasks cleanly, closing the Reddit session
     and flushing the Kafka producer before exit.
"""

import asyncio
import logging
import signal
import time
import os

import asyncpraw
import asyncprawcore
from dotenv import load_dotenv

from ingestion.kafka_client import get_async_producer
from ingestion.priority_rules import calculate_priority

load_dotenv()
logger = logging.getLogger(__name__)

# ── Subreddit config — Phase 2: DB-driven (hot-reload) ───────────────────────
# Previously TOP_SUBREDDITS and MID_SUBREDDITS were hard-coded here.
# Phase 2: the ingestion scheduler reads from the subreddit_config Postgres table
# (managed via Django Admin). Any ops change takes effect within
# SCHEDULER_CONFIG_POLL_S seconds (default 60) without container restart.

import psycopg2

SCHEDULER_CONFIG_POLL_S = int(os.environ.get("SCHEDULER_CONFIG_POLL_S", "60"))

# Fallback defaults used only if the DB is unreachable on first startup
_DEFAULT_SUBREDDITS = [
    {"name": "technology",  "interval": 120, "priority": "fast"},
    {"name": "worldnews",   "interval": 120, "priority": "fast"},
    {"name": "science",     "interval": 300, "priority": "medium"},
    {"name": "programming", "interval": 300, "priority": "medium"},
]


def _fetch_subreddit_config() -> list[dict]:
    """
    Read active rows from the subreddit_config table.
    Returns a list of dicts with keys: name, interval (=interval_seconds), priority.
    Falls back to _DEFAULT_SUBREDDITS if the DB is not reachable.
    """
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
        cur.close()
        conn.close()
        result = [{"name": r[0], "interval": r[1], "priority": r[2]} for r in rows]
        logger.info("Loaded %d active subreddits from DB config.", len(result))
        return result or _DEFAULT_SUBREDDITS
    except Exception as exc:
        logger.warning("Could not load subreddit_config from DB (%s). Using defaults.", exc)
        return _DEFAULT_SUBREDDITS


# Load on module import (synchronous, before the event loop starts)
SUBREDDITS = _fetch_subreddit_config()

MAX_POST_AGE_SECONDS = 86_400   # evict posts older than 24 h

# Seconds between refreshes per priority tier
PRIORITY_INTERVALS = {
    "aggressive": 300,
    "normal":     1800,
    "slow":       7200,
}

# CHANGE: was a plain list; now a dict for O(1) upsert and dedup
active_posts: dict[str, dict] = {}
# asyncio.Lock protects the shared dict from concurrent task writes
_lock = None   # initialised inside the event loop in _run()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _serialize(submission, subreddit_name: str) -> dict:
    """Convert an asyncpraw Submission into a plain dict for Kafka."""
    return {
        "id":           submission.id,
        "title":        submission.title,
        "subreddit":    subreddit_name,
        "author":       str(submission.author) if submission.author else "deleted",
        "created_utc":  submission.created_utc,
        "score":        submission.score,
        "num_comments": submission.num_comments,
        "upvote_ratio": submission.upvote_ratio,
    }


# ── Per-subreddit polling task ─────────────────────────────────────────────────

async def poll_subreddit(reddit, name: str, interval: int, producer):
    """
    CHANGE: replaces the inner `for sub in SUBREDDITS` block that previously
    ran sequentially. Each subreddit now runs as its own asyncio Task.
    Network I/O is non-blocking; concurrent fetches do not block each other.
    """
    logger.info("Polling task started for r/%s  (interval=%ds)", name, interval)

    while True:
        now = time.time()
        try:
            sub = await reddit.subreddit(name)
            async for submission in sub.new(limit=25):
                post = _serialize(submission, name)
                post["poll_priority"]  = calculate_priority(post["created_utc"], now)
                post["last_polled_at"] = None

                async with _lock:
                    is_new = post["id"] not in active_posts
                    # O(1) upsert — no duplicates possible
                    active_posts[post["id"]] = post

                if is_new:
                    # Only publish to Kafka when the post is genuinely new
                    await producer.send("reddit.posts.raw", post)

        except asyncprawcore.exceptions.TooManyRequests as exc:
            # CHANGE: proper asyncprawcore rate-limit exception (was `prawcore`)
            wait = getattr(exc, "retry_after", 10)
            logger.warning("[r/%s] Rate limited — sleeping %ss", name, wait)
            await asyncio.sleep(wait)
            continue   # skip the normal interval sleep after a rate-limit pause

        except asyncprawcore.exceptions.ResponseException as exc:
            logger.error("[r/%s] Reddit API error: %s", name, exc)

        except Exception:
            logger.exception("[r/%s] Unexpected error during fetch", name)

        # Non-blocking sleep — other tasks continue during this wait
        await asyncio.sleep(interval)


# ── Eviction task ─────────────────────────────────────────────────────────────

async def eviction_worker():
    """
    CHANGE: was an O(n) list comprehension every 10-second main-loop tick.
    Now a separate task running every 60 s — much cheaper and doesn't
    interfere with polling cadence.
    """
    while True:
        await asyncio.sleep(60)
        cutoff = time.time() - MAX_POST_AGE_SECONDS
        async with _lock:
            expired = [pid for pid, p in active_posts.items()
                       if p["created_utc"] < cutoff]
            for pid in expired:
                del active_posts[pid]
        if expired:
            logger.debug("Evicted %d expired posts. Active: %d",
                         len(expired), len(active_posts))


# ── Refresh task ──────────────────────────────────────────────────────────────

async def refresh_worker(reddit, producer):
    """
    CHANGE: was interleaved with new-post fetching in the same blocking loop.
    A burst of refreshes previously blocked all new ingestion for the duration.
    Now an independent asyncio Task with its own 10-second tick. Uses
    asyncpraw so the I/O is non-blocking.
    """
    while True:
        await asyncio.sleep(10)
        now = time.time()

        async with _lock:
            # Snapshot the dict to avoid holding the lock during async I/O
            candidates = list(active_posts.values())

        for post in candidates:
            priority = post.get("poll_priority", "inactive")
            interval = PRIORITY_INTERVALS.get(priority)
            if not interval:
                continue

            last = post.get("last_polled_at")
            if last is not None and (now - last) <= interval:
                continue   # not due yet

            try:
                submission = await reddit.submission(id=post["id"])
                await submission.load()   # force fetch of latest data

                updated = _serialize(submission, post["subreddit"])
                updated["poll_priority"]  = calculate_priority(updated["created_utc"], now)
                updated["last_polled_at"] = now

                async with _lock:
                    active_posts[post["id"]] = updated

                await producer.send("reddit.posts.refresh", updated)
                logger.debug("Refreshed post %s (%s)", post["id"], priority)

            except asyncprawcore.exceptions.TooManyRequests as exc:
                wait = getattr(exc, "retry_after", 10)
                logger.warning("[refresh] Rate limited — sleeping %ss", wait)
                await asyncio.sleep(wait)

            except Exception:
                logger.exception("[refresh] Failed to refresh post %s", post.get("id"))


# ── Main coroutine ────────────────────────────────────────────────────────────

async def _run():
    global _lock
    _lock = asyncio.Lock()   # must be created inside the running event loop

    reddit = asyncpraw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )
    producer = await get_async_producer()

    # CHANGE: was a single while loop; now N+2 concurrent independent tasks
    # Phase 2: task_map allows the config hot-reload coroutine to cancel/recreate
    # individual poll tasks when subreddit_config rows change in the DB.
    task_map: dict[str, asyncio.Task] = {}

    def _spawn_poll_tasks(subs: list[dict]) -> None:
        """Create poll tasks for subs not already running; cancel removed ones."""
        active_names = {s["name"] for s in subs}
        # Cancel tasks for subreddits no longer in config
        for name, task in list(task_map.items()):
            if name not in active_names and not task.done():
                task.cancel()
                logger.info("Cancelled poll task for r/%s (removed from config).", name)
                del task_map[name]
        # Spawn tasks for new / not-yet-running subreddits
        for sub in subs:
            if sub["name"] not in task_map or task_map[sub["name"]].done():
                t = asyncio.create_task(
                    poll_subreddit(reddit, sub["name"], sub["interval"], producer),
                    name=f"poll-{sub['name']}"
                )
                task_map[sub["name"]] = t

    async def config_watcher() -> None:
        """Phase 2: poll subreddit_config every SCHEDULER_CONFIG_POLL_S seconds."""
        while True:
            await asyncio.sleep(SCHEDULER_CONFIG_POLL_S)
            try:
                new_config = await asyncio.to_thread(_fetch_subreddit_config)
                _spawn_poll_tasks(new_config)
            except Exception as exc:
                logger.warning("Config hot-reload failed: %s", exc)

    # Initial spawn
    _spawn_poll_tasks(SUBREDDITS)
    tasks = list(task_map.values())
    tasks.append(asyncio.create_task(eviction_worker(), name="eviction"))
    tasks.append(asyncio.create_task(refresh_worker(reddit, producer), name="refresh"))
    tasks.append(asyncio.create_task(config_watcher(), name="config-watcher"))

    logger.info(
        "Scheduler started — %d subreddit tasks + eviction + refresh + config-watcher",
        len(SUBREDDITS)
    )

    # Graceful shutdown: cancel all tasks on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except asyncio.CancelledError:
        logger.info("Scheduler shutdown cleanly.")
    finally:
        await reddit.close()
        await producer.stop()


def run_scheduler():
    """Synchronous entry point — called from main.py."""
    asyncio.run(_run())
