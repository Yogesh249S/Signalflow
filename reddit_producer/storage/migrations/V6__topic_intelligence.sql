-- =====================================================================
-- V6__topic_intelligence.sql
-- =====================================================================
-- Phase 2: Topic traction + cross-platform intelligence tables.
--
-- New tables:
--   topic_timeseries         — 15-min bucketed topic volume + sentiment
--                              per platform. The analytical foundation.
--   cross_platform_events    — topics appearing on 2+ platforms,
--                              with lead/lag times between platforms.
--
-- These two tables answer:
--   "Which topics gained traction over time?"
--   "Did this topic cross platforms? Who saw it first?"
--   "What was the sentiment on each platform for the same topic?"
-- =====================================================================


-- ── Topic timeseries ─────────────────────────────────────────────────────────
-- One row per (topic, platform, 15-min bucket).
-- Populated by the topic_aggregator consumer (processing/topic_aggregator.py).
-- Read by Grafana and the cross-platform detector.
--
-- Example rows:
--   ("openai",    "reddit",     "2026-03-04 14:00", 12, 0.45, 0.8)
--   ("openai",    "hackernews", "2026-03-04 14:00",  3, 0.10, 0.3)
--   ("openai",    "bluesky",    "2026-03-04 15:15",  8, 0.60, 0.7)
CREATE TABLE IF NOT EXISTS topic_timeseries (
    topic           TEXT        NOT NULL,
    platform        TEXT        NOT NULL,
    bucket          TIMESTAMPTZ NOT NULL,   -- truncated to 15-min intervals
    signal_count    INT         DEFAULT 0,  -- how many signals mentioned this topic
    avg_sentiment   FLOAT,                  -- avg sentiment_compound in this bucket
    max_trending    FLOAT,                  -- max trending_score in this bucket
    total_score     BIGINT      DEFAULT 0,  -- sum of raw_score (engagement weight)
    PRIMARY KEY (topic, platform, bucket)
);

CREATE INDEX IF NOT EXISTS idx_topic_ts_topic_time
    ON topic_timeseries (topic, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_topic_ts_platform_time
    ON topic_timeseries (platform, bucket DESC);

CREATE INDEX IF NOT EXISTS idx_topic_ts_bucket
    ON topic_timeseries (bucket DESC);

-- Convert to TimescaleDB hypertable if available
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'topic_timeseries',
            'bucket',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        PERFORM add_retention_policy(
            'topic_timeseries',
            INTERVAL '90 days',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'topic_timeseries converted to hypertable';
    END IF;
END $$;


-- ── Cross-platform events ─────────────────────────────────────────────────────
-- Records when the same topic appears on multiple platforms.
-- Populated by the cross_platform_detector job (runs every 15 min).
--
-- Answers:
--   "Topic X appeared on HN first, then Reddit 47 min later"
--   "Topic Y trended on all 4 platforms simultaneously"
--   "Bluesky is consistently 30 min ahead of Reddit on AI topics"
CREATE TABLE IF NOT EXISTS cross_platform_events (
    id              BIGSERIAL   PRIMARY KEY,
    topic           TEXT        NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),

    -- Which platforms saw this topic and when
    platforms       JSONB       NOT NULL DEFAULT '[]',
    -- e.g. [{"platform": "hackernews", "first_seen": "...", "peak_count": 5},
    --        {"platform": "reddit",     "first_seen": "...", "peak_count": 23}]

    -- The platform that saw it first
    origin_platform TEXT,
    origin_first_seen TIMESTAMPTZ,

    -- Max lag between first and last platform to pick it up (minutes)
    spread_minutes  INT,

    -- Aggregate sentiment across all platforms
    avg_sentiment   FLOAT,

    -- Per-platform sentiment snapshot at time of detection
    sentiment_by_platform JSONB DEFAULT '{}',
    -- e.g. {"reddit": 0.45, "hackernews": -0.1, "bluesky": 0.6}

    -- Peak signal count across all platforms combined
    peak_signal_count INT DEFAULT 0,

    -- Whether this is still active (topic still being discussed)
    is_active       BOOLEAN     DEFAULT TRUE,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_cpe_topic_time
    ON cross_platform_events (topic, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_cpe_detected
    ON cross_platform_events (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_cpe_active
    ON cross_platform_events (detected_at DESC)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_cpe_origin
    ON cross_platform_events (origin_platform, detected_at DESC);


-- ── Convenience views ─────────────────────────────────────────────────────────

-- Top topics in last 24 hours, ranked by cross-platform spread
CREATE OR REPLACE VIEW trending_topics_24h AS
SELECT
    tt.topic,
    COUNT(DISTINCT tt.platform)                     AS platform_count,
    array_agg(DISTINCT tt.platform ORDER BY tt.platform) AS platforms,
    SUM(tt.signal_count)                            AS total_signals,
    ROUND(AVG(tt.avg_sentiment)::numeric, 3)        AS avg_sentiment,
    MAX(tt.max_trending)                            AS peak_trending,
    MIN(tt.bucket)                                  AS first_seen,
    MAX(tt.bucket)                                  AS last_seen
FROM topic_timeseries tt
WHERE tt.bucket > NOW() - INTERVAL '24 hours'
GROUP BY tt.topic
HAVING SUM(tt.signal_count) >= 3          -- at least 3 signals
ORDER BY platform_count DESC, total_signals DESC;


-- Per-platform topic leaderboard for a rolling window
CREATE OR REPLACE VIEW topic_leaderboard_2h AS
SELECT
    topic,
    platform,
    SUM(signal_count)                           AS signal_count,
    ROUND(AVG(avg_sentiment)::numeric, 3)       AS avg_sentiment,
    MAX(max_trending)                           AS peak_trending,
    MIN(bucket)                                 AS first_seen,
    MAX(bucket)                                 AS last_seen
FROM topic_timeseries
WHERE bucket > NOW() - INTERVAL '2 hours'
GROUP BY topic, platform
ORDER BY signal_count DESC;
