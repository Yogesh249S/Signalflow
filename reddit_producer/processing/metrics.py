"""
processing/metrics.py
======================
PHASE 1 IMPROVEMENT: Prometheus Observability
----------------------------------------------
PROBLEM:
  The processing service had zero observability. The only signal that something
  was wrong was:
  - Container logs (if anyone was watching)
  - Kafka consumer lag accumulating silently
  - The Grafana "no data" dashboard

  You could not answer: "Why did throughput drop at 3am?" or "How long do
  batch flushes take under load?" or "Is the DB connection pool saturated?"

SOLUTION:
  This module exposes a Prometheus /metrics HTTP endpoint on port 8000 from
  the processing service. Metrics tracked:

  Counters (always increasing):
    reddit_processor_messages_total{topic}   — messages consumed by topic
    reddit_processor_batches_total{status}   — batch flush outcomes (ok/error)
    reddit_processor_dlq_messages_total      — messages sent to DLQ

  Histograms (latency distributions):
    reddit_processor_batch_flush_seconds     — time per batch DB write
    reddit_processor_kafka_poll_seconds      — time per Kafka poll call

  Gauges (current state):
    reddit_processor_velocity_cache_size     — entries in Redis velocity cache
    reddit_processor_batch_size{topic}       — current batch accumulation size
    reddit_processor_db_pool_available       — available DB connections in pool

RUNNING:
  The metrics server starts automatically when the processor imports this module.
  Access at: http://localhost:8000/metrics

GRAFANA:
  See monitoring/grafana/dashboards/processing.json for the pre-built dashboard.
  Pre-built panels include:
  - Messages/sec by topic (rate of reddit_processor_messages_total)
  - Batch flush latency P50/P95/P99
  - DLQ error rate
  - DB pool saturation
  - Kafka consumer lag (scraped via Kafka JMX exporter)
"""

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

logger = logging.getLogger(__name__)

METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

# ── Metric stores ─────────────────────────────────────────────────────────────
# Simple Python dicts instead of the full prometheus_client library — no
# extra dependency, compatible with Prometheus text format.
#
# For production with high cardinality, switch to the official library:
#   pip install prometheus-client
#   from prometheus_client import Counter, Histogram, Gauge, start_http_server

_counters: dict[str, float] = {}
_gauges:   dict[str, float] = {}
_histograms: dict[str, list[float]] = {}


def _counter(name: str, labels: dict | None = None) -> str:
    """Return a fully qualified metric name with labels."""
    if not labels:
        return name
    label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f'{name}{{{label_str}}}'


def inc_counter(name: str, value: float = 1.0, labels: dict | None = None) -> None:
    key = _counter(name, labels)
    _counters[key] = _counters.get(key, 0.0) + value


def set_gauge(name: str, value: float, labels: dict | None = None) -> None:
    key = _counter(name, labels)
    _gauges[key] = value


def observe_histogram(name: str, value: float) -> None:
    if name not in _histograms:
        _histograms[name] = []
    _histograms[name].append(value)
    # Keep last 10,000 observations (memory bound)
    if len(_histograms[name]) > 10_000:
        _histograms[name] = _histograms[name][-10_000:]


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * pct / 100)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


# ── Prometheus text format renderer ──────────────────────────────────────────

def _render_metrics() -> str:
    lines = []

    # Counters
    lines.append("# Counters")
    for key, value in sorted(_counters.items()):
        base_name = key.split("{")[0]
        lines.append(f"# TYPE {base_name} counter")
        lines.append(f"{key} {value:.2f}")

    # Gauges
    lines.append("\n# Gauges")
    for key, value in sorted(_gauges.items()):
        base_name = key.split("{")[0]
        lines.append(f"# TYPE {base_name} gauge")
        lines.append(f"{key} {value:.4f}")

    # Histograms (emit p50/p95/p99 + count + sum as summary)
    lines.append("\n# Histograms (summary)")
    for name, values in sorted(_histograms.items()):
        lines.append(f"# TYPE {name} summary")
        lines.append(f'{name}{{quantile="0.5"}} {_percentile(values, 50):.6f}')
        lines.append(f'{name}{{quantile="0.95"}} {_percentile(values, 95):.6f}')
        lines.append(f'{name}{{quantile="0.99"}} {_percentile(values, 99):.6f}')
        lines.append(f"{name}_count {len(values)}")
        lines.append(f"{name}_sum {sum(values):.6f}")

    return "\n".join(lines) + "\n"


# ── HTTP server ───────────────────────────────────────────────────────────────

class MetricsHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress access logs

    def do_GET(self):
        if self.path in ("/metrics", "/"):
            body = _render_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def start_metrics_server() -> None:
    """
    Start the Prometheus metrics HTTP server in a daemon thread.
    Called once at processor startup — non-blocking.
    """
    def _serve():
        server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
        logger.info("Prometheus metrics server started on port %d.", METRICS_PORT)
        server.serve_forever()

    t = threading.Thread(target=_serve, daemon=True, name="metrics-server")
    t.start()


# ── Convenience context manager for timing ────────────────────────────────────

import time as _time
from contextlib import contextmanager


@contextmanager
def timed(histogram_name: str):
    """
    Context manager that records wall-clock time of a block into a histogram.

    Usage:
        with timed("reddit_processor_batch_flush_seconds"):
            flush_batches(raw_batch, refresh_batch)
    """
    start = _time.monotonic()
    try:
        yield
    finally:
        observe_histogram(histogram_name, _time.monotonic() - start)
