-- =====================================================================
-- V3__timescaledb_hypertable.sql
-- Phase 1 addition: TimescaleDB for post_metrics_history
--
-- PROBLEM:
--   post_metrics_history is append-only time-series data that grows forever.
--   At 20 subreddits × 25 posts × 12 refreshes/day = 6,000 rows/day.
--   At scale (1M posts/day) this becomes 1 billion rows/year with no purge.
--   Standard Postgres btree indexes degrade as the table grows — vacuum
--   becomes expensive, and time-range queries slow down linearly.
--
-- SOLUTION:
--   Convert post_metrics_history to a TimescaleDB hypertable with:
--   - Daily chunks: Postgres only touches the relevant chunk for time queries
--   - Automatic retention: DROP chunks older than 90 days via pg_cron job
--   - Continuous aggregates (see below) for fast historical summaries
--
-- PREREQ:
--   The postgres service in docker-compose.yml must use timescaledb image:
--     image: timescale/timescaledb:latest-pg15
--   The extension must be enabled (done below).
-- =====================================================================

-- Enable TimescaleDB extension (no-op if already enabled)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Convert the existing table to a hypertable partitioned by captured_at.
-- chunk_time_interval = 1 day: each day's data lives in its own chunk.
-- Postgres only scans the relevant chunk(s) for time-range queries.
-- migrate_data = TRUE: retains all existing rows.
-- TimescaleDB requires the partitioning column (captured_at) to be part of
-- any unique index or primary key on the table. The existing PK is on id alone,
-- which blocks hypertable creation. Two options:
--   (a) Drop the PK entirely — fine for append-only time-series with no FK refs
--       pointing at post_metrics_history.id
--   (b) Replace with composite PK (id, captured_at)
-- Option (a) is cleaner here: nothing references this table's id column.
ALTER TABLE post_metrics_history DROP CONSTRAINT IF EXISTS post_metrics_history_pkey;

SELECT create_hypertable(
    'post_metrics_history',
    by_range('captured_at', INTERVAL '1 day'),
    migrate_data  => TRUE,
    if_not_exists => TRUE
);

-- ── Automatic data retention ─────────────────────────────────────────────────
-- Drop chunks older than 90 days. Timescale drops the entire chunk file
-- from disk — vastly faster than DELETE and doesn't bloat the WAL.
-- Adjust the retention interval via the TIMESCALE_RETENTION env var.
SELECT add_retention_policy(
    'post_metrics_history',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

-- ── Continuous aggregate: hourly summary ─────────────────────────────────────
-- A materialised view that Timescale refreshes automatically.
-- Powers the "hourly trend" chart without touching the raw hypertable.
--
-- NOTE: continuous aggregates cannot reference a second table (JOIN) because
-- Timescale needs to incrementally maintain the view using only the hypertable.
-- subreddit_id is available on post_metrics_history via the post_id FK lookup
-- at query time — keep this aggregate simple (per-post) and join at read time.
CREATE MATERIALIZED VIEW IF NOT EXISTS post_metrics_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', captured_at) AS bucket,
    post_id,
    AVG(score)        AS avg_score,
    MAX(score)        AS max_score,
    AVG(num_comments) AS avg_comments,
    COUNT(*)          AS sample_count
FROM post_metrics_history
GROUP BY bucket, post_id
WITH NO DATA;

-- Refresh every 30 minutes, covering a 4-hour window ending 1 hour ago.
-- window = start_offset - end_offset = 4h - 1h = 3h = 3 buckets.
-- Strictly greater than the 2-bucket minimum required by TimescaleDB 2.x.
SELECT add_continuous_aggregate_policy(
    'post_metrics_hourly',
    start_offset      => INTERVAL '4 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE
);

-- ── Record this migration ─────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, description)
VALUES ('V3', 'Convert post_metrics_history to TimescaleDB hypertable with 90d retention')
ON CONFLICT (version) DO NOTHING;