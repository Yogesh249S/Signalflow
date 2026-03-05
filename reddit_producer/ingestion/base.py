"""
ingestion/base.py
=================
Abstract base class for all ingestion sources.

Every source (Reddit, HN, Bluesky, YouTube) inherits from BaseIngester
and implements two methods:
  - setup()   : async, called once before polling starts (auth, session init)
  - poll()    : async, called on each tick, yields raw dicts

The base class handles:
  - Kafka publishing to the correct raw topic
  - Error handling and logging
  - Graceful shutdown
  - The poll loop with configurable interval

Adding a new source = subclass BaseIngester, implement setup() + poll().
Nothing else needs to change.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import AsyncIterator

from ingestion.kafka_client import get_async_producer
from ingestion.normaliser import normalise
from ingestion.rate_limiter import make_limiter

logger = logging.getLogger(__name__)


class BaseIngester(ABC):
    """
    Inherit from this and implement setup() + poll().
    Call run() to start the poll loop.
    """

    # Subclasses set these as class attributes
    source_name: str = ""        # "reddit", "hackernews", "bluesky", "youtube"
    kafka_topic: str = ""        # "hackernews.stories.raw" etc.
    poll_interval: float = 60.0  # seconds between polls

    def __init__(self):
        self.producer = None
        self._running = False
        self.log = logging.getLogger(f"ingestion.{self.source_name}")
        self._limiter = make_limiter(self.source_name)

    @abstractmethod
    async def setup(self) -> None:
        """
        Called once before polling starts.
        Initialise API clients, authenticate, open sessions here.
        """

    @abstractmethod
    async def poll(self) -> AsyncIterator[dict]:
        """
        Called on each tick. Yield raw dicts — one per item fetched.
        The base class normalises and publishes each yielded item.
        Shape varies per source; normaliser handles the transformation.
        """

    async def teardown(self) -> None:
        """Optional cleanup. Override if your source needs it."""

    async def run(self) -> None:
        """Main loop. Call this from the scheduler."""
        self.producer = await get_async_producer()
        await self.setup()
        self._running = True
        self.log.info("Started — topic=%s interval=%.0fs", self.kafka_topic, self.poll_interval)

        try:
            while self._running:
                try:
                    async for raw in self.poll():
                        # Rate limit before normalising — drop excess early
                        if not self._limiter.acquire():
                            continue
                        signal = normalise(self.source_name, raw)
                        if signal:
                            await self.producer.send(self.kafka_topic, signal)
                            await self.producer.send("signals.normalised", signal)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.log.exception("Poll cycle error — will retry in %.0fs", self.poll_interval)

                await asyncio.sleep(self.poll_interval)

        except asyncio.CancelledError:
            self.log.info("Shutting down cleanly.")
        finally:
            await self.teardown()

    def stop(self) -> None:
        self._running = False
