"""
DAG: timescaledb_to_s3_archive
--------------------------------
Runs nightly at 01:00 UTC. Identifies 1-day chunks in post_metrics_history
that are within ARCHIVE_LOOKAHEAD_DAYS of the 90-day TimescaleDB retention
drop, exports each chunk to Parquet, and uploads to S3.

Why this exists:
  TimescaleDB retention policy drops entire chunk files (not row-level DELETEs)
  for post_metrics_history. Once dropped, that data is gone. This DAG archives
  cold chunks to S3 before the drop window, giving you a permanent cold store
  queryable via Athena, Trino, or Spark.

Hot path:  TimescaleDB  (last 90 days, fast time-range queries via chunk exclusion)
Cold path: S3 / Parquet (90+ days, partitioned by year/month/day)

S3 key layout (Hive-compatible, works with Athena/Glue out of the box):
  s3://{BUCKET}/reddit-metrics/year=YYYY/month=MM/day=DD/metrics.parquet

Idempotency:
  Re-running the same logical_date overwrites the same S3 key.
  The identify_chunks task skips dates already present in S3.

Connections (set in Airflow UI or airflow.cfg):
  postgres_signalflow  — Postgres conn pointing to postgres:5432
  aws_default          — AWS conn with key/secret that has s3:PutObject

Environment variables (via .env → airflow service env_file):
  S3_BUCKET_NAME         Target bucket (e.g. reddit-signalflow-archive)
  ARCHIVE_LOOKAHEAD_DAYS Days before drop to archive (default: 5)
  RETENTION_DAYS         Must match TimescaleDB policy (default: 90)
"""

import os
import io
import logging
from datetime import datetime, timedelta, date

import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — driven by env vars so .env controls everything
# ---------------------------------------------------------------------------

RETENTION_DAYS   = int(os.getenv("RETENTION_DAYS", 90))
LOOKAHEAD_DAYS   = int(os.getenv("ARCHIVE_LOOKAHEAD_DAYS", 5))
S3_BUCKET        = os.getenv("S3_BUCKET_NAME", "reddit-signalflow-archive")
S3_PREFIX        = "reddit-metrics"

# Parquet schema — matches post_metrics_history + enrichment join
PARQUET_SCHEMA = pa.schema([
    ("time",             pa.timestamp("us", tz="UTC")),
    ("post_id",          pa.string()),
    ("score",            pa.int32()),
    ("num_comments",     pa.int32()),
    ("score_velocity",   pa.float32()),
    ("comment_velocity", pa.float32()),
    ("trending_score",   pa.float32()),
    ("is_trending",      pa.bool_()),
    ("subreddit_id",     pa.int32()),
    ("title",            pa.string()),
    ("author",           pa.string()),
    ("poll_priority",    pa.string()),
    ("first_seen_at",    pa.timestamp("us", tz="UTC")),
    ("sentiment_score",  pa.float32()),
    ("sentiment_label",  pa.string()),
    ("keywords",         pa.string()),   # JSONB serialised as string
])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_db_conn():
    """
    Get psycopg2 connection via Airflow connection 'postgres_signalflow'.
    Falls back to direct env vars if connection not registered — useful
    during local development before Airflow UI is configured.
    """
    try:
        conn_info = BaseHook.get_connection("postgres_signalflow")
        return psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port or 5432,
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )
    except Exception:
        log.warning("Airflow connection 'postgres_signalflow' not found — using env vars.")
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "reddit"),
            user=os.getenv("POSTGRES_USER", "reddit"),
            password=os.getenv("POSTGRES_PASSWORD", "reddit"),
        )


def _get_s3_client():
    """
    Get boto3 S3 client via Airflow connection 'aws_default'.
    Falls back to env vars (AWS_ACCESS_KEY_ID etc.) if not registered.
    """
    try:
        conn_info = BaseHook.get_connection("aws_default")
        return boto3.client(
            "s3",
            aws_access_key_id=conn_info.login,
            aws_secret_access_key=conn_info.password,
            region_name=conn_info.extra_dejson.get("region_name", "ap-south-1"),
        )
    except Exception:
        log.warning("Airflow connection 'aws_default' not found — using env vars.")
        return boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
        )


def _s3_key(target_date: date) -> str:
    """Hive-partitioned S3 key for a given date."""
    return (
        f"{S3_PREFIX}/"
        f"year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
        "metrics.parquet"
    )


def _already_in_s3(s3, target_date: date) -> bool:
    """Return True if this date partition already exists in S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=_s3_key(target_date))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# ---------------------------------------------------------------------------
# Task: identify chunks approaching retention drop
# ---------------------------------------------------------------------------

def identify_chunks(**context):
    """
    Query timescaledb_information.chunks to find 1-day chunks in
    post_metrics_history whose range_end is within LOOKAHEAD_DAYS of
    the retention drop boundary.

    TimescaleDB retention drops chunks where:
        range_end <= now() - RETENTION_DAYS

    We archive chunks where:
        range_end <= now() - (RETENTION_DAYS - LOOKAHEAD_DAYS)

    i.e. chunks that will be dropped within the next LOOKAHEAD_DAYS days.
    Pushes list of date strings to XCom.
    """
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT range_start::date AS chunk_date
                FROM   timescaledb_information.chunks
                WHERE  hypertable_name = 'post_metrics_history'
                  AND  range_end <= NOW() - INTERVAL '%s days'
                ORDER  BY range_start ASC;
            """, (RETENTION_DAYS - LOOKAHEAD_DAYS,))
            rows = cur.fetchall()
    finally:
        conn.close()

    chunk_dates = [str(row[0]) for row in rows]
    log.info("Chunks approaching retention drop (%d found): %s", len(chunk_dates), chunk_dates)

    context["ti"].xcom_push(key="chunk_dates", value=chunk_dates)


# ---------------------------------------------------------------------------
# Task: export each chunk to Parquet and upload to S3
# ---------------------------------------------------------------------------

def archive_to_s3(**context):
    """
    For each chunk date from XCom:
      1. Query post_metrics_history for that day (chunk exclusion via WHERE on time)
         joined with posts and post_nlp_features for enrichment
      2. Convert to Parquet in memory via pyarrow (no temp files on disk)
      3. Upload to S3 under Hive-partitioned key
      4. Log row count, S3 URI, and Parquet size

    Skips dates already present in S3 — safe to re-run.
    Commits are per-chunk so a failure mid-run doesn't lose already-archived chunks.
    """
    chunk_dates = context["ti"].xcom_pull(key="chunk_dates", task_ids="identify_chunks")

    if not chunk_dates:
        log.info("No chunks to archive. Nothing to do.")
        return

    s3   = _get_s3_client()
    conn = _get_db_conn()

    archived = []
    skipped  = []
    failed   = []

    # Enrichment query — one per chunk day.
    # WHERE h.time >= date AND h.time < date + 1 day
    # hits TimescaleDB chunk exclusion: only the relevant 1-day chunk is scanned.
    QUERY = """
        SELECT
            h.time,
            h.post_id,
            h.score,
            h.num_comments,
            h.score_velocity,
            h.comment_velocity,
            h.trending_score,
            h.is_trending,
            p.subreddit_id,
            p.title,
            p.author,
            p.poll_priority,
            p.first_seen_at,
            COALESCE(n.sentiment_score, 0.0)   AS sentiment_score,
            COALESCE(n.sentiment_label, 'neutral') AS sentiment_label,
            COALESCE(n.keywords::text, '[]')   AS keywords
        FROM post_metrics_history h
        JOIN posts p
            ON p.id = h.post_id
        LEFT JOIN post_nlp_features n
            ON n.post_id = h.post_id
        WHERE h.time >= %s::date
          AND h.time <  %s::date + INTERVAL '1 day'
        ORDER BY h.time ASC;
    """

    try:
        for date_str in chunk_dates:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            s3_key = _s3_key(target_date)

            if _already_in_s3(s3, target_date):
                log.info("Skipping %s — already in S3 at s3://%s/%s", date_str, S3_BUCKET, s3_key)
                skipped.append(date_str)
                continue

            try:
                # ----------------------------------------------------------
                # Fetch data for this chunk day
                # ----------------------------------------------------------
                with conn.cursor() as cur:
                    cur.execute(QUERY, (date_str, date_str))
                    rows = cur.fetchall()
                    col_names = [desc[0] for desc in cur.description]

                if not rows:
                    log.warning("Chunk %s returned 0 rows — skipping.", date_str)
                    skipped.append(date_str)
                    continue

                # ----------------------------------------------------------
                # Convert to Parquet in memory
                # pandas → pyarrow table → parquet bytes
                # We avoid writing temp files to disk — important on t2.micro
                # ----------------------------------------------------------
                df = pd.DataFrame(rows, columns=col_names)

                # Ensure timestamp columns are tz-aware (UTC) for schema compliance
                for ts_col in ("time", "first_seen_at"):
                    if ts_col in df.columns:
                        df[ts_col] = pd.to_datetime(df[ts_col], utc=True)

                table  = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, safe=False)
                buf    = io.BytesIO()
                pq.write_table(
                    table, buf,
                    compression="snappy",        # consistent with Kafka producer compression
                    write_statistics=True,        # enables Parquet predicate pushdown
                    row_group_size=50_000,
                )
                buf.seek(0)
                parquet_bytes = buf.getvalue()

                # ----------------------------------------------------------
                # Upload to S3
                # ----------------------------------------------------------
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=parquet_bytes,
                    ContentType="application/octet-stream",
                    Metadata={
                        "chunk_date":  date_str,
                        "row_count":   str(len(rows)),
                        "archived_at": datetime.utcnow().isoformat(),
                        "source":      "post_metrics_history",
                    },
                )

                size_kb = len(parquet_bytes) / 1024
                log.info(
                    "Archived %s → s3://%s/%s  [%d rows, %.1f KB]",
                    date_str, S3_BUCKET, s3_key, len(rows), size_kb,
                )
                archived.append(date_str)

            except Exception as chunk_err:
                log.error("Failed to archive chunk %s: %s", date_str, chunk_err, exc_info=True)
                failed.append(date_str)
                # Continue to next chunk — don't abort the entire run
                # Failed chunks will be retried on the next DAG run

    finally:
        conn.close()

    # ------------------------------------------------------------------
    # Summary log — visible in Airflow task logs and easy to grep
    # ------------------------------------------------------------------
    log.info(
        "Archive run complete. archived=%d  skipped=%d  failed=%d",
        len(archived), len(skipped), len(failed),
    )
    if archived:
        log.info("Archived:  %s", archived)
    if skipped:
        log.info("Skipped:   %s", skipped)
    if failed:
        log.warning("Failed:    %s", failed)
        # Raise so Airflow marks this task as failed and triggers retry/alert
        raise RuntimeError(f"Failed to archive {len(failed)} chunk(s): {failed}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner":            "signalflow",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

with DAG(
    dag_id="timescaledb_to_s3_archive",
    default_args=default_args,
    description="Archive post_metrics_history chunks to S3 before TimescaleDB retention drop",
    schedule_interval="0 1 * * *",   # 01:00 UTC nightly — well before the retention job
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # don't backfill — chunks already dropped can't be archived
    tags=["signalflow", "archive", "timescaledb", "s3"],
    doc_md=__doc__,
) as dag:

    t_identify = PythonOperator(
        task_id="identify_chunks",
        python_callable=identify_chunks,
        doc_md="""
        Queries timescaledb_information.chunks to find post_metrics_history
        partitions within ARCHIVE_LOOKAHEAD_DAYS of the retention drop.
        Pushes a list of date strings to XCom for the archive task.
        """,
    )

    t_archive = PythonOperator(
        task_id="archive_to_s3",
        python_callable=archive_to_s3,
        doc_md="""
        For each chunk date: fetches enriched metrics from TimescaleDB,
        converts to Snappy-compressed Parquet in memory, uploads to S3
        under a Hive-partitioned key. Idempotent — skips already-archived dates.
        """,
    )

    t_identify >> t_archive
