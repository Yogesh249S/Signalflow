
"""
ingestion/main.py
=================
CHANGE: added structured logging configuration so all output is timestamped
and levelled, making log aggregation (e.g. Datadog / CloudWatch) trivial.
"""

import logging
import sys

from ingestion.scheduler import run_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    run_scheduler()
