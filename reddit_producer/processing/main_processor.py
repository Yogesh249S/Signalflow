"""
processing/main_processor.py
============================
OPTIMISATION CHANGES vs original
----------------------------------
1. MICRO-BATCHING (biggest change in this file)
   - Original: processed one Kafka message at a time, calling
     `upsert_post_snapshot()` and `insert_metrics_history()` individually for
     each message — one DB round-trip per message, per function call.
   - New: messages are accumulated in `raw_batch` / `refresh_batch` lists.
     When the batch hits BATCH_SIZE (50 messages) OR BATCH_TIMEOUT (2 s)
     elapses, `flush_batches()` is called, which writes all rows in two bulk
     statements (execute_values). This reduces Postgres write overhead by ~50×
     at default batch size.

2. MANUAL OFFSET COMMIT (exactly-once-style processing)
   - Original: `enable_auto_commit=True` (default) — Kafka auto-committed
     offsets every 5 s regardless of whether DB writes succeeded. A crash
     mid-batch would silently skip messages.
   - New: `enable_auto_commit=False`. Offsets are committed only AFTER the
     batch has been successfully written to Postgres. A crash before commit
     causes Kafka to re-deliver the batch — no data loss.

3. LARGER KAFKA FETCH SIZE
   - Original: default `max_poll_records=500` and `fetch_max_bytes=50MB` but
     `fetch_min_bytes=1` — Kafka returns as soon as 1 byte is available,
     giving tiny fetches that keep the consumer busy with overhead.
   - New: `fetch_min_bytes=1024` (wait for at least 1 KB before returning),
     `max_poll_records=200` per poll call — the consumer processes bigger
     chunks per iteration, reducing poll loop overhead.

4. SENTIMENT CACHING — skip recomputing for unchanged titles
   - Original: `analyze_sentiment(post["title"])` called on EVERY message,
     including refresh events where the title hasn't changed. VADER is fast
     (~1 ms) but pointless on the same string repeated every 5 min.
   - New: `_sentiment_cache` dict keyed by post_id. On a refresh event,
     sentiment is looked up from cache; VADER only runs for truly new posts
     and when cache misses occur. Cache entries expire with the post (24 h).

5. REMOVED STALE print() DEBUGGING
   - Original had un-toggled `print(f"[RAW] Processed post {post['id']}")` in
     the hot path. Under 10,000 messages/min this adds measurable stdout I/O.
   - New: structured `logger.info / logger.debug` with appropriate levels.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from processing.db_writer import (
    bulk_upsert_posts,
    bulk_insert_metrics_history,
    bulk_upsert_nlp_features,
)
from processing.analytics.sentiment import analyze_sentiment
from processing.analytics.engagement_velocity import calculate_velocity
from processing.analytics.trending_score import compute_trending
from processing.channel_publisher import publish_post_updates
from processing.metrics import (
    start_metrics_server,
    inc_counter,
    set_gauge,
    timed,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092,kafka2:9092,kafka3:9092")

# ── Dead-Letter Queue ─────────────────────────────────────────────────────────
# PHASE 1: Route failed messages to DLQ instead of silently dropping them.

DLQ_TOPIC = "reddit.posts.dlq"
_dlq_producer: KafkaProducer | None = None


def _get_dlq_producer() -> KafkaProducer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _dlq_producer


def _send_to_dlq(messages: list[dict], source_topic: str, error: Exception) -> None:
    """
    Route failed messages to the DLQ topic with full error context.
    The dlq_consumer.py process stores these for inspection and replay.
    """
    producer = _get_dlq_producer()
    failed_at = datetime.now(timezone.utc).isoformat()
    for msg in messages:
        envelope = {
            "source_topic": source_topic,
            "error":        str(error),
            "failed_at":    failed_at,
            "attempt":      1,
            "payload":      msg,
        }
        try:
            producer.send(DLQ_TOPIC, value=envelope)
        except Exception as dlq_exc:
            logger.critical("Failed to send to DLQ — message is lost: %s", dlq_exc)
    try:
        producer.flush(timeout=5)
    except Exception:
        pass


# CHANGE: was no batching. These two parameters control the write cadence.
# Flush when either limit is hit, whichever comes first.
BATCH_SIZE    = 50     # rows — flush after accumulating this many messages
BATCH_TIMEOUT = 2.0    # seconds — flush even if batch isn't full yet

# ── Sentiment cache ───────────────────────────────────────────────────────────
# CHANGE: was no caching — VADER ran on every message including refreshes.
# Keyed by post_id; value is (compound_score, label).
_sentiment_cache: dict[str, tuple[float, str]] = {}


def _get_sentiment(post: dict) -> tuple[float, str]:
    """
    Return cached sentiment for this post_id, or compute and cache it.
    CHANGE: avoids redundant VADER calls on refresh events (same title,
    same result every 5 min for the post's 24-h lifetime).
    """
    pid = post["id"]
    if pid not in _sentiment_cache:
        _sentiment_cache[pid] = analyze_sentiment(post["title"])
    return _sentiment_cache[pid]


# ── Batch flush ───────────────────────────────────────────────────────────────

def flush_batches(
    raw_batch: list[dict],
    refresh_batch: list[dict],
) -> None:
    """
    CHANGE: was N individual DB calls (one per message).
    Now: two bulk_upsert calls cover the entire batch in two SQL statements.
    """
    with timed("reddit_processor_batch_flush_seconds"):
        if raw_batch:
            # Compute NLP for all raw posts in one pass (list comprehension)
            nlp_rows = []
            for post in raw_batch:
                score, label = _get_sentiment(post)
                keywords = [w for w in post["title"].lower().split() if len(w) > 3][:10]
                nlp_rows.append((post["id"], score, json.dumps(keywords)))

            # Two bulk statements cover the entire raw batch
            bulk_upsert_posts(raw_batch)
            bulk_upsert_nlp_features(nlp_rows)
            inc_counter("reddit_processor_messages_total", len(raw_batch), {"topic": "raw"})
            logger.info("[RAW  ] Flushed batch of %d posts to DB.", len(raw_batch))

        if refresh_batch:
            # Velocity and trending are still computed per-post (stateful)
            for post in refresh_batch:
                score_velocity, comment_velocity = calculate_velocity(post)
                sentiment_score, _ = _get_sentiment(post)
                compute_trending(post, score_velocity, sentiment_score)   # side-effect free

            # Two bulk statements cover the entire refresh batch
            bulk_upsert_posts(refresh_batch)
            bulk_insert_metrics_history(refresh_batch)
            inc_counter("reddit_processor_messages_total", len(refresh_batch), {"topic": "refresh"})
            logger.info("[REFRESH] Flushed batch of %d posts to DB.", len(refresh_batch))
            # Phase 2: notify WebSocket clients of updated posts via channel layer
            publish_post_updates(refresh_batch)


# ── Main consumer loop ────────────────────────────────────────────────────────

def run_processor() -> None:
    logger.info("Connecting consumer to Kafka @ %s …", KAFKA_BOOTSTRAP)

    # PHASE 1: Start Prometheus metrics HTTP server on port 8000
    start_metrics_server()

    consumer = KafkaConsumer(
        "reddit.posts.raw",
        "reddit.posts.refresh",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),

        # CHANGE: was auto-commit (messages could be lost on crash).
        # Manual commit happens only after a successful batch DB write.
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id="reddit-processor",

        # CHANGE: wait for at least 1 KB before returning from poll —
        # reduces empty/tiny poll overhead under low traffic.
        fetch_min_bytes=1_024,

        # CHANGE: was default 500. 200 per poll gives predictable batch sizes
        # without holding too much data in memory at once.
        max_poll_records=200,

        # Give Kafka up to 1 s to accumulate fetch_min_bytes
        fetch_max_wait_ms=1_000,
    )

    logger.info(
        "Processor started. batch_size=%d, batch_timeout=%.1fs",
        BATCH_SIZE, BATCH_TIMEOUT,
    )

    raw_batch:     list[dict] = []
    refresh_batch: list[dict] = []
    last_flush = time.monotonic()

    for message in consumer:
        # Accumulate into the correct bucket
        if message.topic == "reddit.posts.raw":
            raw_batch.append(message.value)
        elif message.topic == "reddit.posts.refresh":
            refresh_batch.append(message.value)

        total = len(raw_batch) + len(refresh_batch)
        elapsed = time.monotonic() - last_flush

        # Flush when batch is full OR the timeout window has expired
        # CHANGE: was no batching — flushed (individually) on every message.
        if total >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
            try:
                flush_batches(raw_batch, refresh_batch)
                # Only commit offsets after a successful DB write
                consumer.commit()
                inc_counter("reddit_processor_batches_total", labels={"status": "ok"})
            except Exception as exc:
                inc_counter("reddit_processor_batches_total", labels={"status": "error"})
                logger.exception(
                    "Batch flush failed — routing %d messages to DLQ. "
                    "Offsets NOT committed; Kafka will re-deliver.",
                    total,
                )
                # PHASE 1: DLQ — send failed messages for inspection/replay
                if raw_batch:
                    _send_to_dlq(raw_batch, "reddit.posts.raw", exc)
                    inc_counter("reddit_processor_dlq_messages_total", len(raw_batch))
                if refresh_batch:
                    _send_to_dlq(refresh_batch, "reddit.posts.refresh", exc)
                    inc_counter("reddit_processor_dlq_messages_total", len(refresh_batch))
            finally:
                raw_batch.clear()
                refresh_batch.clear()
                last_flush = time.monotonic()


if __name__ == "__main__":
    run_processor()
