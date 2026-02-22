"""
processing/dlq_consumer.py
===========================
PHASE 1 IMPROVEMENT: Dead-Letter Queue Consumer
-------------------------------------------------
PROBLEM:
  The original processor had no dead-letter queue. When a Kafka message failed
  to process (malformed JSON, missing field, DB constraint error), it was logged
  and silently dropped. There was no way to:
  - Inspect what went wrong after the fact
  - Replay the message once the bug was fixed
  - Alert on an elevated failure rate

SOLUTION:
  1. main_processor.py now routes failed messages to `reddit.posts.dlq` instead
     of dropping them (see the updated flush_batches() function).

  2. This module (dlq_consumer.py) is a separate process that:
     - Consumes from reddit.posts.dlq
     - Logs every failed message with its error context
     - Exposes a /replay endpoint (via a simple HTTP server on port 8001)
       that re-publishes selected DLQ messages back to their original topic
     - Tracks failure counts per topic in Redis for alerting

RUNNING:
  Add to docker-compose.yml:
    dlq-consumer:
      build: { context: ., dockerfile: processing/Dockerfile }
      command: python -m processing.dlq_consumer
      environment: { KAFKA_BOOTSTRAP_SERVERS: kafka:9092, REDIS_URL: redis://redis:6379/0 }
      depends_on: [kafka-init, redis]

  Or run locally:
    python -m processing.dlq_consumer
"""

import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import redis
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL       = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DLQ_TOPIC       = "reddit.posts.dlq"
REPLAY_PORT     = int(os.environ.get("DLQ_REPLAY_PORT", "8001"))

# How long to keep DLQ failure counts in Redis
DLQ_COUNTER_TTL = 7 * 24 * 3_600   # 7 days

# ── Redis client for failure metrics ──────────────────────────────────────────

_redis: Optional[redis.Redis] = None


def get_redis() -> Optional[redis.Redis]:
    global _redis
    if _redis is not None:
        return _redis
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        _redis = r
        return _redis
    except redis.RedisError as exc:
        logger.warning("DLQ consumer: Redis unavailable — metrics will not be tracked: %s", exc)
        return None


# ── DLQ message store (in-memory ring buffer for the replay API) ──────────────

_dlq_buffer: list[dict] = []
_dlq_lock = threading.Lock()
MAX_BUFFER = 1_000   # keep last 1000 failed messages in memory


def _store_message(envelope: dict) -> None:
    """Store a DLQ message in the ring buffer and increment Redis counter."""
    with _dlq_lock:
        _dlq_buffer.append(envelope)
        if len(_dlq_buffer) > MAX_BUFFER:
            _dlq_buffer.pop(0)

    # Track failure count in Redis for alerting (Prometheus can scrape this)
    r = get_redis()
    if r:
        try:
            source_topic = envelope.get("source_topic", "unknown")
            key = f"dlq:count:{source_topic}"
            pipe = r.pipeline()
            pipe.incr(key)
            pipe.expire(key, DLQ_COUNTER_TTL)
            pipe.execute()
        except redis.RedisError:
            pass


# ── Replay HTTP server ────────────────────────────────────────────────────────
# A minimal HTTP server that allows operations to replay DLQ messages.
# Endpoints:
#   GET  /dlq         — list buffered DLQ messages (last MAX_BUFFER)
#   POST /dlq/replay  — replay all buffered messages to their original topics
#   POST /dlq/replay/{index} — replay a single message by buffer index

_producer: Optional[KafkaProducer] = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


class DLQHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logger.debug("DLQ HTTP: " + format % args)

    def _send_json(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/dlq":
            with _dlq_lock:
                messages = list(_dlq_buffer)
            self._send_json(200, {
                "count": len(messages),
                "buffer_limit": MAX_BUFFER,
                "messages": messages,
            })
        elif self.path == "/dlq/stats":
            r = get_redis()
            stats = {}
            if r:
                try:
                    for key in r.scan_iter("dlq:count:*"):
                        topic = key.split(":")[-1]
                        stats[topic] = int(r.get(key) or 0)
                except redis.RedisError:
                    pass
            self._send_json(200, {"failure_counts_by_topic": stats})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/dlq/replay":
            with _dlq_lock:
                to_replay = list(_dlq_buffer)

            replayed = 0
            errors = 0
            producer = get_producer()
            for envelope in to_replay:
                topic   = envelope.get("source_topic", "reddit.posts.raw")
                payload = envelope.get("payload")
                if payload:
                    try:
                        producer.send(topic, value=payload)
                        replayed += 1
                    except Exception as exc:
                        logger.error("Replay send failed: %s", exc)
                        errors += 1
            producer.flush()
            self._send_json(200, {"replayed": replayed, "errors": errors})

        elif self.path.startswith("/dlq/replay/"):
            try:
                idx = int(self.path.split("/")[-1])
                with _dlq_lock:
                    envelope = _dlq_buffer[idx]
            except (ValueError, IndexError):
                self._send_json(400, {"error": "invalid index"})
                return

            topic   = envelope.get("source_topic", "reddit.posts.raw")
            payload = envelope.get("payload")
            if payload:
                try:
                    get_producer().send(topic, value=payload)
                    get_producer().flush()
                    self._send_json(200, {"replayed": 1, "topic": topic})
                except Exception as exc:
                    self._send_json(500, {"error": str(exc)})
            else:
                self._send_json(400, {"error": "no payload in envelope"})
        else:
            self._send_json(404, {"error": "not found"})


def _run_http_server() -> None:
    server = HTTPServer(("0.0.0.0", REPLAY_PORT), DLQHandler)
    logger.info("DLQ replay HTTP server listening on port %d.", REPLAY_PORT)
    server.serve_forever()


# ── Main DLQ consumer loop ────────────────────────────────────────────────────

def run_dlq_consumer() -> None:
    logger.info("Starting DLQ consumer — topic: %s", DLQ_TOPIC)

    # Start the replay HTTP server in a background thread
    t = threading.Thread(target=_run_http_server, daemon=True)
    t.start()

    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id="reddit-dlq-consumer",
    )

    logger.info("DLQ consumer connected. Waiting for failed messages…")

    for message in consumer:
        envelope = message.value

        # Expected envelope structure (set by main_processor.py on failure):
        # {
        #   "source_topic": "reddit.posts.raw" | "reddit.posts.refresh",
        #   "error": "exception string",
        #   "failed_at": ISO timestamp,
        #   "attempt": int,
        #   "payload": { original message dict }
        # }
        source_topic = envelope.get("source_topic", "unknown")
        error        = envelope.get("error", "unknown error")
        failed_at    = envelope.get("failed_at", "unknown time")

        logger.error(
            "DLQ message received | topic=%s | failed_at=%s | error=%s",
            source_topic, failed_at, error,
        )

        _store_message(envelope)

        # Future: auto-retry logic could go here
        # e.g. if envelope.get("attempt", 0) < 3: re-publish to source_topic


if __name__ == "__main__":
    run_dlq_consumer()
