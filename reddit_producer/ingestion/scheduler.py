"""
ingestion/scheduler.py
=======================
Thin orchestrator. Launches all source ingesters as concurrent asyncio tasks.

Adding a new source:
  1. Create ingestion/sources/your_source.py with a class inheriting BaseIngester
  2. Import it here and add it to SOURCES list
  3. That's it — nothing else changes.
"""

import asyncio
import logging
import signal
import os

logger = logging.getLogger(__name__)


def _get_enabled_sources():
    """
    Return list of ingester instances based on environment config.
    Sources are enabled by the presence of their required env vars.
    """
    sources = []

    # Reddit — always enabled if credentials present
    from ingestion.sources.reddit import RedditIngester
    if os.environ.get("REDDIT_CLIENT_ID"):
        sources.append(RedditIngester())
        logger.info("Reddit ingester enabled.")
    else:
        logger.warning("REDDIT_CLIENT_ID not set — Reddit ingester disabled.")

    # Hacker News — always enabled (no auth required)
    from ingestion.sources.hackernews import HackerNewsIngester
    sources.append(HackerNewsIngester())
    logger.info("HackerNews ingester enabled.")

    # Bluesky — enabled if cbor2 is installed (optional dependency)
    try:
        import cbor2
        from ingestion.sources.bluesky import BlueskyIngester
        sources.append(BlueskyIngester())
        logger.info("Bluesky ingester enabled.")
    except ImportError:
        logger.warning("cbor2 not installed — Bluesky ingester disabled. pip install cbor2")

    # YouTube — enabled if API key present
    from ingestion.sources.youtube import YouTubeIngester
    if os.environ.get("YOUTUBE_API_KEY") or os.environ.get("YOUTUBE_API_KEY_1"):
        sources.append(YouTubeIngester())
        logger.info("YouTube ingester enabled.")
    else:
        logger.warning("YOUTUBE_API_KEY not set — YouTube ingester disabled.")

    return sources


async def _run():
    sources = _get_enabled_sources()

    if not sources:
        logger.error("No ingestion sources enabled. Check environment variables.")
        return

    logger.info("Starting %d ingestion sources.", len(sources))

    tasks = [
        asyncio.create_task(source.run(), name=source.source_name)
        for source in sources
    ]

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except asyncio.CancelledError:
        logger.info("All ingestion sources shutdown cleanly.")


def run_scheduler():
    asyncio.run(_run())
