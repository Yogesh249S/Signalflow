"""
processing/main_processor.py
============================
CHANGES vs original:
  1. Now consumes `signals.normalised` in addition to Reddit-specific topics.
     The signals topic carries normalised Signal dicts from all sources.

  2. Three batch buckets instead of two:
     - raw_batch     : Reddit raw posts (legacy path, unchanged)
     - refresh_batch : Reddit refresh events (legacy path, unchanged)
     - signal_batch  : all normalised signals from signals.normalised topic

  3. flush_signal_batch() is the new unified flush path.
     It handles NLP enrichment, velocity, trending, and DB writes
     using Signal field names (raw_score, comment_count) not Reddit ones.

  4. Legacy flush_batches() preserved unchanged for Reddit topics.

  5. DLQ topic updated to `signals.dlq` for the new path.
     Reddit-specific errors still go to `reddit.posts.dlq`.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from processing.db_writer import (
    # Legacy Reddit writers
    bulk_upsert_posts,
    bulk_insert_metrics_history,
    bulk_upsert_nlp_features,
    # New unified writers
    bulk_upsert_signals,
    bulk_insert_signal_metrics_history,
    bulk_upsert_signal_nlp,
)
from processing.analytics.sentiment import analyze_sentiment
from processing.analytics.engagement_velocity import calculate_velocity
from processing.analytics.trending_score import compute_trending
from processing.analytics.topic_extractor import extract_topics_batch
from processing.analytics.normalised_score import enrich_normalised_scores
from processing.channel_publisher import publish_post_updates
from processing.metrics import (
    start_metrics_server,
    inc_counter,
    timed,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092,kafka2:9092,kafka3:9092")
BATCH_SIZE      = int(os.environ.get("BATCH_SIZE", "50"))
BATCH_TIMEOUT   = float(os.environ.get("BATCH_TIMEOUT", "2.0"))

# Topics to consume
REDDIT_RAW_TOPIC     = "reddit.posts.raw"
REDDIT_REFRESH_TOPIC = "reddit.posts.refresh"
SIGNALS_TOPIC        = "signals.normalised"

REDDIT_DLQ  = "reddit.posts.dlq"
SIGNALS_DLQ = "signals.dlq"

_dlq_producer: KafkaProducer | None = None


def _get_dlq_producer() -> KafkaProducer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _dlq_producer


def _send_to_dlq(messages: list[dict], topic: str, error: Exception) -> None:
    producer  = _get_dlq_producer()
    failed_at = datetime.now(timezone.utc).isoformat()
    for msg in messages:
        try:
            producer.send(topic, value={
                "source_topic": topic,
                "error":        str(error),
                "failed_at":    failed_at,
                "payload":      msg,
            })
        except Exception as e:
            logger.critical("DLQ send failed — message lost: %s", e)
    try:
        producer.flush(timeout=5)
    except Exception:
        pass


# ── Sentiment cache ───────────────────────────────────────────────────────────
# Keyed by signal id ("reddit:abc123", "hackernews:456", etc.)
_sentiment_cache: dict[str, tuple[float, str]] = {}


def _get_sentiment(signal: dict) -> tuple[float, str]:
    """
    Use title for Reddit/HN, body for Bluesky/YouTube (which have no titles).
    Cache by signal id — runs VADER once per signal lifetime.
    """
    sid = signal["id"]
    if sid not in _sentiment_cache:
        text = signal.get("title") or signal.get("body") or ""
        _sentiment_cache[sid] = analyze_sentiment(text)
    return _sentiment_cache[sid]


# ── NEW: Unified signal flush ─────────────────────────────────────────────────

def flush_signal_batch(signal_batch: list[dict]) -> None:
    """
    Process and persist a batch of normalised signals from any platform.

    Steps:
      1. Sentiment (VADER, cached per signal)
      2. Topic extraction (spaCy NER, batched for performance)
      3. Keyword extraction (simple word frequency, still useful for search)
      4. Bulk DB writes
      5. Velocity + trending
      6. WebSocket publish for trending signals
    """
    if not signal_batch:
        return

    with timed("signal_processor_batch_flush_seconds"):

        # ── Step 1: Sentiment ─────────────────────────────────────────────────
        for sig in signal_batch:
            compound, label = _get_sentiment(sig)
            sig["sentiment_compound"] = compound
            sig["sentiment_label"]    = label

        # ── Step 2: Topic extraction (spaCy NER, one pass over entire batch) ──
        # Prefer title for Reddit/HN, fall back to body for Bluesky/YouTube.
        # Concatenate both when both exist — more context = better NER.
        texts = [
            (sig.get("title", "") + " " + sig.get("body", "")).strip()
            for sig in signal_batch
        ]
        topic_results = extract_topics_batch(texts)  # single nlp.pipe() call
        for sig, topics in zip(signal_batch, topic_results):
            sig["topics"] = topics

        # ── Step 3: Keyword extraction (simple, fast, complements NER) ────────
        # Topics = named entities (who/what).
        # Keywords = content words (what it's about, useful for search).
        # They're different — keep both.
        _STOPWORDS = {
            "the","a","an","and","or","but","in","on","at","to","for",
            "of","with","by","from","as","is","was","are","were","be",
            "been","have","has","had","do","does","did","will","would",
            "could","should","may","might","that","this","these","those",
            "it","its","i","you","he","she","we","they","what","which",
            "who","how","when","where","why","not","no","so","if","says",
            "said","also","after","before","along","about","just","more",
        }
        for sig in signal_batch:
            text = (sig.get("title","") + " " + sig.get("body","")).lower()
            keywords = [
                w for w in text.split()
                if len(w) > 3 and w.isalpha() and w not in _STOPWORDS
            ]
            sig["keywords"] = list(dict.fromkeys(keywords))[:10]

        # ── Step 4: Write initial state ───────────────────────────────────────
        enrich_normalised_scores(signal_batch)   # adds normalised_score per platform
        bulk_upsert_signals(signal_batch)
        bulk_upsert_signal_nlp(signal_batch)

        # ── Step 5: Velocity + trending ───────────────────────────────────────
        enriched = []
        for sig in signal_batch:
            score_vel, comment_vel = calculate_velocity(sig)
            compound = sig.get("sentiment_compound", 0.0)

            # compute_trending expects comment_count key
            trending_input = {**sig, "num_comments": sig.get("comment_count", 0)}
            trending_score, is_trending = compute_trending(trending_input, score_vel, compound)

            sig["score_velocity"]   = score_vel
            sig["comment_velocity"] = comment_vel
            sig["trending_score"]   = trending_score
            sig["is_trending"]      = is_trending

            if score_vel != 0.0 or comment_vel != 0.0:
                enriched.append(sig)

        # ── Step 6: Write enriched signals + metrics history ──────────────────
        if enriched:
            bulk_upsert_signals(enriched)          # update velocity/trending
            bulk_insert_signal_metrics_history(enriched)

        # ── Step 7: WebSocket publish for trending signals ────────────────────
        trending = [s for s in signal_batch if s.get("is_trending")]
        if trending:
            publish_post_updates(trending)

        inc_counter("signal_processor_messages_total", len(signal_batch), {"topic": "signals"})
        logger.info("[SIGNALS] Flushed batch of %d signals (%d trending).",
                    len(signal_batch), len(trending))


# ── Legacy Reddit flush (unchanged) ──────────────────────────────────────────

def flush_batches(raw_batch: list[dict], refresh_batch: list[dict]) -> None:
    with timed("reddit_processor_batch_flush_seconds"):
        if raw_batch:
            nlp_rows = []
            for post in raw_batch:
                score, label = analyze_sentiment(post["title"])
                keywords = [w for w in post["title"].lower().split() if len(w) > 3][:10]
                nlp_rows.append((post["id"], score, json.dumps(keywords)))
            bulk_upsert_posts(raw_batch)
            bulk_upsert_nlp_features(nlp_rows)
            inc_counter("reddit_processor_messages_total", len(raw_batch), {"topic": "raw"})
            logger.info("[RAW  ] Flushed %d posts.", len(raw_batch))

        if refresh_batch:
            for post in refresh_batch:
                # Legacy uses post["score"] and post["num_comments"]
                from processing.analytics.velocity_cache import get_previous, update_cache
                prev = get_previous(post["id"])
                if prev:
                    old_score, old_comments, old_time = prev
                    dt = max(time.time() - old_time, 1.0)
                    post["score_velocity"]   = (post["score"]        - old_score)    / dt
                    post["comment_velocity"] = (post["num_comments"] - old_comments) / dt
                else:
                    post["score_velocity"]   = 0.0
                    post["comment_velocity"] = 0.0
                update_cache(post["id"], post["score"], post["num_comments"], time.time())
                compound, _ = analyze_sentiment(post["title"])
                t_score, is_trending = compute_trending(post, post["score_velocity"], compound)
                post["trending_score"] = t_score
                post["is_trending"]    = is_trending

            bulk_upsert_posts(refresh_batch)
            bulk_insert_metrics_history(refresh_batch)
            inc_counter("reddit_processor_messages_total", len(refresh_batch), {"topic": "refresh"})
            logger.info("[REFRESH] Flushed %d posts.", len(refresh_batch))
            publish_post_updates(refresh_batch)


# ── Main consumer loop ────────────────────────────────────────────────────────

def run_processor() -> None:
    logger.info("Connecting to Kafka @ %s", KAFKA_BOOTSTRAP)
    start_metrics_server()

    consumer = KafkaConsumer(
        REDDIT_RAW_TOPIC,
        REDDIT_REFRESH_TOPIC,
        SIGNALS_TOPIC,            # new — all normalised signals
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id="signal-processor",
        fetch_min_bytes=1_024,
        max_poll_records=200,
        fetch_max_wait_ms=1_000,
    )

    logger.info("Processor started. batch_size=%d timeout=%.1fs", BATCH_SIZE, BATCH_TIMEOUT)

    raw_batch:     list[dict] = []
    refresh_batch: list[dict] = []
    signal_batch:  list[dict] = []
    last_flush = time.monotonic()

    for message in consumer:
        # Route to correct bucket by topic
        if message.topic == REDDIT_RAW_TOPIC:
            raw_batch.append(message.value)
        elif message.topic == REDDIT_REFRESH_TOPIC:
            refresh_batch.append(message.value)
        elif message.topic == SIGNALS_TOPIC:
            signal_batch.append(message.value)

        total   = len(raw_batch) + len(refresh_batch) + len(signal_batch)
        elapsed = time.monotonic() - last_flush

        if total >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT:
            try:
                # Flush legacy Reddit batches
                flush_batches(raw_batch, refresh_batch)

                # Flush unified signal batch
                flush_signal_batch(signal_batch)

                consumer.commit()
                inc_counter("signal_processor_batches_total", labels={"status": "ok"})

            except Exception as exc:
                inc_counter("signal_processor_batches_total", labels={"status": "error"})
                logger.exception("Batch flush failed — routing to DLQ.")

                if raw_batch:
                    _send_to_dlq(raw_batch, REDDIT_DLQ, exc)
                    inc_counter("signal_processor_dlq_messages_total", len(raw_batch))
                if refresh_batch:
                    _send_to_dlq(refresh_batch, REDDIT_DLQ, exc)
                    inc_counter("signal_processor_dlq_messages_total", len(refresh_batch))
                if signal_batch:
                    _send_to_dlq(signal_batch, SIGNALS_DLQ, exc)
                    inc_counter("signal_processor_dlq_messages_total", len(signal_batch))

            finally:
                raw_batch.clear()
                refresh_batch.clear()
                signal_batch.clear()
                last_flush = time.monotonic()


if __name__ == "__main__":
    run_processor()
