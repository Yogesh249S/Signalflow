-- =====================================================================
-- V5__multi_platform_signals.sql
-- =====================================================================
-- Adds the unified multi-platform schema alongside the existing Reddit
-- tables. Reddit tables (posts, subreddits, post_metrics_history,
-- post_nlp_features) are LEFT INTACT for backward compatibility.
-- The new tables are the canonical store for all sources going forward.
--
-- New tables:
--   communities              — replaces subreddits, covers all platforms
--   signals                  — replaces posts, platform-agnostic
--   signal_metrics_history   — replaces post_metrics_history (hypertable)
--   platform_divergence      — cross-platform sentiment divergence events
-- =====================================================================


-- ── Communities (replaces subreddits) ────────────────────────────────────────
-- A community is any grouping within a platform:
--   Reddit   → r/technology
--   HN       → hackernews (single community, no sub-grouping)
--   Bluesky  → bluesky or a specific feed
--   YouTube  → channel name
CREATE TABLE IF NOT EXISTS communities (
    id          SERIAL PRIMARY KEY,
    platform    TEXT NOT NULL,        -- "reddit","hackernews","bluesky","youtube"
    name        TEXT NOT NULL,        -- "r/technology", "hackernews", "Fireship"
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform, name)
);


-- ── Signals (unified post/story/comment table) ────────────────────────────────
CREATE TABLE IF NOT EXISTS signals (
    -- Identity
    id              TEXT PRIMARY KEY,   -- "platform:source_id" e.g. "reddit:abc123"
    platform        TEXT NOT NULL,
    source_id       TEXT NOT NULL,      -- platform-native ID
    community_id    INT REFERENCES communities(id),

    -- Content
    title           TEXT,               -- empty for Bluesky/YouTube (body-only)
    body            TEXT,
    url             TEXT,
    author          TEXT,

    -- Timing
    published_at    TIMESTAMPTZ,        -- when it was posted on the platform
    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Engagement (raw — platform native units)
    raw_score       INT DEFAULT 0,      -- upvotes / points / likes
    comment_count   INT DEFAULT 0,

    -- Engagement (normalised 0-1, computed by enrichment service)
    normalised_score FLOAT,

    -- Velocity (computed by processing service)
    score_velocity   FLOAT,
    comment_velocity FLOAT,

    -- Trending (computed by processing service)
    trending_score   FLOAT DEFAULT 0.0,
    is_trending      BOOLEAN DEFAULT FALSE,

    -- NLP (computed by processing service)
    sentiment_compound FLOAT,
    sentiment_label    TEXT,            -- "positive","neutral","negative"
    keywords           JSONB DEFAULT '[]',
    topics             JSONB DEFAULT '[]',

    -- Platform-specific fields preserved as JSONB
    -- Reddit: {"upvote_ratio": 0.95, "poll_priority": "slow"}
    -- YouTube: {"video_id": "xxx", "channel_id": "yyy"}
    -- Bluesky: {"repost_count": 12, "langs": ["en"]}
    extra           JSONB DEFAULT '{}',

    schema_version  INT DEFAULT 1,

    UNIQUE (platform, source_id)
);

-- Indexes on signals
CREATE INDEX IF NOT EXISTS idx_signals_platform
    ON signals (platform);

CREATE INDEX IF NOT EXISTS idx_signals_community
    ON signals (community_id);

CREATE INDEX IF NOT EXISTS idx_signals_published
    ON signals (published_at DESC)
    WHERE published_at IS NOT NULL;

-- Partial index for trending queries — very selective, small index
CREATE INDEX IF NOT EXISTS idx_signals_trending
    ON signals (trending_score DESC)
    WHERE is_trending = TRUE;

-- Covering index for cross-platform sentiment queries
-- Covers: WHERE platform = X AND published_at BETWEEN a AND b
--         ORDER BY sentiment_compound, trending_score
CREATE INDEX IF NOT EXISTS idx_signals_platform_time_sentiment
    ON signals (platform, published_at DESC, sentiment_compound)
    WHERE published_at IS NOT NULL;

-- GIN index for keyword and topic search
CREATE INDEX IF NOT EXISTS idx_signals_keywords
    ON signals USING GIN (keywords);

CREATE INDEX IF NOT EXISTS idx_signals_topics
    ON signals USING GIN (topics);


-- ── Signal metrics history (TimescaleDB hypertable) ───────────────────────────
-- Append-only time-series — one row per refresh per signal.
-- Works for all platforms: Reddit refresh, HN re-poll, YouTube comment re-fetch.
CREATE TABLE IF NOT EXISTS signal_metrics_history (
    signal_id        TEXT NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
    captured_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_score        INT,
    comment_count    INT,
    score_velocity   FLOAT,
    comment_velocity FLOAT,
    trending_score   FLOAT,
    sentiment_compound FLOAT,
    PRIMARY KEY (signal_id, captured_at)
);

-- Convert to TimescaleDB hypertable if TimescaleDB extension is available
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable(
            'signal_metrics_history',
            'captured_at',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );

        -- 90-day retention policy
        PERFORM add_retention_policy(
            'signal_metrics_history',
            INTERVAL '90 days',
            if_not_exists => TRUE
        );

        -- Continuous aggregate: hourly rollup per signal
        -- Used by the API for trend charts without hitting raw hypertable
        BEGIN
            CREATE MATERIALIZED VIEW signal_metrics_hourly
            WITH (timescaledb.continuous) AS
            SELECT
                signal_id,
                time_bucket('1 hour', captured_at)    AS bucket,
                AVG(raw_score)                         AS avg_score,
                MAX(raw_score)                         AS max_score,
                AVG(comment_count)                     AS avg_comments,
                AVG(score_velocity)                    AS avg_velocity,
                AVG(sentiment_compound)                AS avg_sentiment,
                COUNT(*)                               AS sample_count
            FROM signal_metrics_history
            GROUP BY signal_id, bucket
            WITH NO DATA;
        EXCEPTION WHEN duplicate_table THEN NULL;
        END;

        PERFORM add_continuous_aggregate_policy(
            'signal_metrics_hourly',
            start_offset  => INTERVAL '3 hours',
            end_offset    => INTERVAL '1 hour',
            schedule_interval => INTERVAL '30 minutes',
            if_not_exists => TRUE
        );

        RAISE NOTICE 'TimescaleDB hypertable created for signal_metrics_history';
    ELSE
        RAISE NOTICE 'TimescaleDB not available — signal_metrics_history is a plain table';
    END IF;
END $$;


-- ── Platform divergence events ────────────────────────────────────────────────
-- Records moments when two platforms significantly disagree on a topic.
-- Populated by the divergence detection job (future Phase 3).
-- This is the core of the "sentiment_divergence" product feature.
CREATE TABLE IF NOT EXISTS platform_divergence (
    id              BIGSERIAL PRIMARY KEY,
    topic           TEXT NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),

    -- The two platforms that diverged
    platform_a      TEXT NOT NULL,
    platform_b      TEXT NOT NULL,

    -- Their sentiment scores at the moment of divergence
    sentiment_a     FLOAT NOT NULL,
    sentiment_b     FLOAT NOT NULL,

    -- How far apart they are: abs(sentiment_a - sentiment_b)
    -- 0.0 = identical, 2.0 = maximum possible divergence
    divergence_score FLOAT NOT NULL,

    -- Which platform picked up the topic first
    origin_platform TEXT,
    origin_lag_minutes INT,   -- how many minutes B lagged behind A

    -- Sample signals that triggered the divergence detection
    sample_signal_ids JSONB DEFAULT '[]',

    -- Whether this divergence persisted or was a transient spike
    resolved_at     TIMESTAMPTZ,
    is_resolved     BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_divergence_topic_time
    ON platform_divergence (topic, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_divergence_score
    ON platform_divergence (divergence_score DESC)
    WHERE is_resolved = FALSE;


-- ── Source config (extends subreddit_config pattern to all sources) ───────────
-- Allows hot-reload control of what each source ingests.
-- Reddit already uses subreddit_config — this is the multi-platform equivalent.
CREATE TABLE IF NOT EXISTS source_config (
    id              SERIAL PRIMARY KEY,
    platform        TEXT NOT NULL,      -- "hackernews","bluesky","youtube"
    identifier      TEXT NOT NULL,      -- channel_id / feed / keyword
    label           TEXT,               -- human-readable name
    interval_seconds INT DEFAULT 300,
    is_active       BOOLEAN DEFAULT TRUE,
    added_at        TIMESTAMPTZ DEFAULT NOW(),
    added_by        TEXT DEFAULT 'system',
    UNIQUE (platform, identifier)
);

-- Seed with defaults
INSERT INTO source_config (platform, identifier, label, interval_seconds)
VALUES
    ('hackernews', 'story',   'HN Stories',    300),
    ('hackernews', 'ask_hn',  'Ask HN',         300),
    ('hackernews', 'show_hn', 'Show HN',        300),
    ('youtube',    'UCsBjURrPoezykLs9EqgamOA', 'Fireship',   14400),
    ('youtube',    'UCVhQ2NnY5Rskt6UjCUkJ_DA', 'Tech w Tim', 14400),
    ('bluesky',    'firehose','Bluesky Public', 0)
ON CONFLICT (platform, identifier) DO NOTHING;


-- ── Summary view (convenience for Django ORM and API queries) ─────────────────
-- Joins signals with communities so queries don't need explicit JOINs.
CREATE OR REPLACE VIEW signals_with_community AS
SELECT
    s.*,
    c.platform   AS community_platform,
    c.name       AS community_name
FROM signals s
LEFT JOIN communities c ON s.community_id = c.id;
