# """
# ingestion/sources/bluesky.py
# =============================
# Bluesky ingestion via AT Protocol firehose WebSocket.
#
# The firehose streams every public post on Bluesky in real-time.
# We filter to posts only (ignoring reposts, likes, follows) and
# apply keyword/topic filtering to avoid ingesting everything.
#
# No auth required for the public firehose.
# Volume is lower than Reddit but growing — currently ~5-20 posts/sec.
# """
#
# import asyncio
# import io
# import logging
# import time
# from typing import AsyncIterator
#
# import aiohttp
# import cbor2
# from ingestion.base import BaseIngester
#
# logger = logging.getLogger(__name__)
#
# # Public firehose — no auth needed
# FIREHOSE_URL = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
#
# # Filter to posts mentioning these topics
# # Keep this list tight — the firehose is high volume
#
# '''
# TOPIC_KEYWORDS = [
#     "python", "kafka", "data engineering", "machine learning", "ai", "llm",
#     "openai", "anthropic", "gpt", "software", "programming", "developer",
#     "startup", "tech", "database", "docker", "kubernetes", "aws",
#     "reddit", "hacker news", "github"
# ]
#
# '''
#
# TOPIC_KEYWORDS = [
#     "wall street", "stock markets"
# ]
# # Bluesky profile resolver for handle lookup
# PROFILE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"
#
#
# class BlueskyIngester(BaseIngester):
#     source_name   = "bluesky"
#     kafka_topic   = "bluesky.posts.raw"
#     poll_interval = 0  # firehose is push-based, not poll-based
#
#     def __init__(self):
#         super().__init__()
#         self.session = None
#         self._handle_cache: dict[str, str] = {}  # did -> handle cache
#
#     async def setup(self) -> None:
#         self.session = aiohttp.ClientSession()
#         logger.info("Bluesky ingester ready. Connecting to firehose...")
#
#     async def poll(self) -> AsyncIterator[dict]:
#         # Unused — run() is overridden for WebSocket
#         return
#         yield
#
#     async def teardown(self) -> None:
#         if self.session:
#             await self.session.close()
#
#     # ── Override run() — firehose is WebSocket, not HTTP poll ─────────────────
#
#     async def run(self) -> None:
#         from ingestion.kafka_client import get_async_producer
#         from ingestion.normaliser import normalise
#
#         self.producer = await get_async_producer()
#         await self.setup()
#
#         logger.info("Bluesky firehose connecting...")
#
#         while True:
#             try:
#                 async with aiohttp.ClientSession() as ws_session:
#                     async with ws_session.ws_connect(
#                         FIREHOSE_URL,
#                         heartbeat=30,
#                         timeout=aiohttp.ClientTimeout(total=None),
#                     ) as ws:
#                         logger.info("Bluesky firehose connected.")
#                         async for msg in ws:
#                             if msg.type == aiohttp.WSMsgType.BINARY:
#                                 await self._handle_message(msg.data, normalise)
#                             elif msg.type == aiohttp.WSMsgType.ERROR:
#                                 logger.warning("Firehose WS error: %s", ws.exception())
#                                 break
#
#             except asyncio.CancelledError:
#                 logger.info("Bluesky ingester shutdown cleanly.")
#                 break
#             except Exception as e:
#                 logger.warning("Firehose disconnected (%s). Reconnecting in 5s...", e)
#                 await asyncio.sleep(5)
#
#         await self.teardown()
#
#     @staticmethod
#     def _extract_post_from_car(blocks: bytes) -> dict:
#         """Parse CAR file and return the first block containing 'text' (the post record)."""
#         def read_varint(buf):
#             result, shift = 0, 0
#             while True:
#                 b = buf.read(1)
#                 if not b: return None
#                 byte = b[0]
#                 result |= (byte & 0x7F) << shift
#                 if not (byte & 0x80): return result
#                 shift += 7
#
#         buf = io.BytesIO(blocks)
#         header_len = read_varint(buf)
#         if not header_len: return {}
#         buf.read(header_len)  # skip CAR header
#
#         while buf.tell() < len(blocks):
#             block_len = read_varint(buf)
#             if not block_len: break
#             raw = buf.read(block_len)
#             try:
#                 rec = cbor2.loads(raw[36:])  # skip 36-byte CID
#                 if isinstance(rec, dict) and "text" in rec:
#                     return rec
#             except Exception:
#                 pass
#         return {}
#
#     async def _handle_message(self, data: bytes, normalise_fn) -> None:
#         try:
#             buf    = io.BytesIO(data)
#             header = cbor2.load(buf)
#             body   = cbor2.load(buf)
#
#             if header.get("t") != "#commit":
#                 return
#
#             did = body.get("repo", "")
#             ops = body.get("ops", [])
#
#             for op in ops:
#                 if op.get("action") != "create":
#                     continue
#                 path = op.get("path", "")
#                 if not path.startswith("app.bsky.feed.post/"):
#                     continue
#
#                 # Extract post record from CAR blocks
#                 record = self._extract_post_from_car(body.get("blocks", b""))
#                 if not record:
#                     continue
#
#                 text = record.get("text", "")
#                 if not text:
#                     continue
#
#                 # Topic filter
#                 text_lower = text.lower()
#                 if not any(kw in text_lower for kw in TOPIC_KEYWORDS):
#                     continue
#
#                 rkey   = path.split("/")[-1]
#                 handle = self._handle_cache.get(did, did)
#
#                 raw = {
#                     "uri":           f"at://{did}/app.bsky.feed.post/{rkey}",
#                     "cid":           str(op.get("cid", "")),
#                     "author":        did,
#                     "author_handle": handle,
#                     "record":        record,
#                     "likeCount":     0,
#                     "replyCount":    0,
#                     "repostCount":   0,
#                     "feed":          "bluesky",
#                     "ingested_at":   time.time(),
#                 }
#
#                 signal = normalise_fn("bluesky", raw)
#                 if signal:
#                     await self.producer.send("bluesky.posts.raw", raw)
#                     await self.producer.send("signals.normalised", signal)
#                     logger.debug("Bluesky post ingested: %s", text[:60])
#
#         except Exception:
#             logger.debug("Failed to parse firehose message", exc_info=True)





"""
ingestion/sources/bluesky.py
=============================
Bluesky ingestion via AT Protocol firehose WebSocket.

The firehose streams every public post on Bluesky in real-time.
We filter to posts only (ignoring reposts, likes, follows) and
apply keyword/topic filtering to avoid ingesting everything.

No auth required for the public firehose.
Volume is lower than Reddit but growing — currently ~5-20 posts/sec.
"""

import asyncio
import io
import logging
import time
from typing import AsyncIterator

import aiohttp
import cbor2
from ingestion.base import BaseIngester
from ingestion.rate_limiter import make_limiter

logger = logging.getLogger(__name__)

# Public firehose — no auth needed
FIREHOSE_URL = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"

# Filter to posts mentioning these topics
# Keep this list tight — the firehose is high volume

'''
TOPIC_KEYWORDS = [
    "python", "kafka", "data engineering", "machine learning", "ai", "llm",
    "openai", "anthropic", "gpt", "software", "programming", "developer",
    "startup", "tech", "database", "docker", "kubernetes", "aws",
    "reddit", "hacker news", "github"
]

'''

TOPIC_KEYWORDS = [
    "wall street", "stock markets"
]
# Bluesky profile resolver for handle lookup
PROFILE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"


class BlueskyIngester(BaseIngester):
    source_name   = "bluesky"
    kafka_topic   = "bluesky.posts.raw"
    poll_interval = 0  # firehose is push-based, not poll-based

    def __init__(self):
        super().__init__()
        self.session = None
        self._handle_cache: dict[str, str] = {}  # did -> handle cache
        self._limiter = make_limiter(self.source_name)

    async def setup(self) -> None:
        self.session = aiohttp.ClientSession()
        logger.info("Bluesky ingester ready. Connecting to firehose...")

    async def poll(self) -> AsyncIterator[dict]:
        # Unused — run() is overridden for WebSocket
        return
        yield

    async def teardown(self) -> None:
        if self.session:
            await self.session.close()

    # ── Override run() — firehose is WebSocket, not HTTP poll ─────────────────

    async def run(self) -> None:
        from ingestion.kafka_client import get_async_producer
        from ingestion.normaliser import normalise

        self.producer = await get_async_producer()
        await self.setup()

        logger.info("Bluesky firehose connecting...")

        while True:
            try:
                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(
                        FIREHOSE_URL,
                        heartbeat=30,
                        timeout=aiohttp.ClientTimeout(total=None),
                    ) as ws:
                        logger.info("Bluesky firehose connected.")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.BINARY:
                                await self._handle_message(msg.data, normalise)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.warning("Firehose WS error: %s", ws.exception())
                                break

            except asyncio.CancelledError:
                logger.info("Bluesky ingester shutdown cleanly.")
                break
            except Exception as e:
                logger.warning("Firehose disconnected (%s). Reconnecting in 5s...", e)
                await asyncio.sleep(5)

        await self.teardown()

    @staticmethod
    def _extract_post_from_car(blocks: bytes) -> dict:
        """Parse CAR file and return the first block containing 'text' (the post record)."""
        def read_varint(buf):
            result, shift = 0, 0
            while True:
                b = buf.read(1)
                if not b: return None
                byte = b[0]
                result |= (byte & 0x7F) << shift
                if not (byte & 0x80): return result
                shift += 7

        buf = io.BytesIO(blocks)
        header_len = read_varint(buf)
        if not header_len: return {}
        buf.read(header_len)  # skip CAR header

        while buf.tell() < len(blocks):
            block_len = read_varint(buf)
            if not block_len: break
            raw = buf.read(block_len)
            try:
                rec = cbor2.loads(raw[36:])  # skip 36-byte CID
                if isinstance(rec, dict) and "text" in rec:
                    return rec
            except Exception:
                pass
        return {}

    async def _handle_message(self, data: bytes, normalise_fn) -> None:
        try:
            buf    = io.BytesIO(data)
            header = cbor2.load(buf)
            body   = cbor2.load(buf)

            if header.get("t") != "#commit":
                return

            did = body.get("repo", "")
            ops = body.get("ops", [])

            for op in ops:
                if op.get("action") != "create":
                    continue
                path = op.get("path", "")
                if not path.startswith("app.bsky.feed.post/"):
                    continue

                # Extract post record from CAR blocks
                record = self._extract_post_from_car(body.get("blocks", b""))
                if not record:
                    continue

                text = record.get("text", "")
                if not text:
                    continue

                # Filter 1 — English only
                langs = record.get("langs", [])
                if langs and "en" not in langs:
                    continue

                # Filter 2 — minimum length, skip noise/spam
                if len(text) < 40:
                    continue

                # Filter 3 — keyword match
                text_lower = text.lower()
                if not any(kw in text_lower for kw in TOPIC_KEYWORDS):
                    continue

                # Filter 4 — hard rate cap: max 200 posts per minute
                _now = time.time()
                if not hasattr(self, '_rate_window'):
                    self._rate_window = _now
                    self._rate_count  = 0
                if _now - self._rate_window < 60:
                    self._rate_count += 1
                    if self._rate_count > 200:
                        continue
                else:
                    self._rate_window = _now
                    self._rate_count  = 0

                rkey   = path.split("/")[-1]
                handle = self._handle_cache.get(did, did)

                raw = {
                    "uri":           f"at://{did}/app.bsky.feed.post/{rkey}",
                    "cid":           str(op.get("cid", "")),
                    "author":        did,
                    "author_handle": handle,
                    "record":        record,
                    "likeCount":     0,
                    "replyCount":    0,
                    "repostCount":   0,
                    "feed":          "bluesky",
                    "ingested_at":   time.time(),
                }

                # Rate limit — drop excess before hitting Kafka
                if not self._limiter.acquire():
                    continue

                signal = normalise_fn("bluesky", raw)
                if signal:
                    await self.producer.send("bluesky.posts.raw", raw)
                    await self.producer.send("signals.normalised", signal)
                    logger.debug("Bluesky post ingested: %s", text[:60])

        except Exception:
            logger.debug("Failed to parse firehose message", exc_info=True)
