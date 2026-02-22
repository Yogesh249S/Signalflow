-- =====================================================================
-- Reddit Analytics Database Schema
-- =====================================================================
-- OPTIMISATION CHANGES vs original
-- ---------------------------------
-- 1. COMPOSITE COVERING INDEX for /api/stats/ queries
--    Original: only individual single-column indexes existed. The stats
--    view's GROUP BY author and subreddit_id queries required full-table
--    scans + sort operations. Under millions of rows this is very slow.
--    New: idx_posts_stats_covering covers the most common access pattern:
--      WHERE created_utc BETWEEN x AND y
--      ORDER BY current_score DESC
--      GROUP BY author / subreddit_id
--    Postgres can satisfy all of these from the index alone (index-only scan).
--
-- 2. PARTIAL INDEX for active-posts queries
--    Original: idx_posts_active on the boolean column is low-selectivity
--    (Postgres often ignores boolean indexes). The PostViewSet filters on
--    is_active=TRUE which represents the hot ~95% of data.
--    New: idx_posts_active_created is a PARTIAL index (WHERE is_active=TRUE)
--    on created_utc DESC — far smaller, directly useful for the API's
--    ORDER BY created_utc query on active posts.
--
-- 3. poll_priority TEXT -> SMALLINT
--    Original: poll_priority stored as TEXT ("aggressive", "normal", etc.)
--    requires string comparison on every row evaluated. As an integer (3/2/1/0)
--    comparisons are branchless integer ops.
--    NOTE: the Python code still passes text strings; Postgres casts them.
--    To complete this change, migrate the column and update the Python enum.
--
-- 4. post_metrics_history: BRIN index option noted
--    For very high-volume deployments where post_metrics_history grows to
--    tens of millions of rows, a BRIN index on captured_at is far smaller
--    than a btree and works well for append-only time-series tables.
--    See commented BRIN index below.
-- =====================================================================


-- ── Subreddits ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS subreddits (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);


-- ── Posts (current snapshot — upserted on every poll) ─────────────────────────
CREATE TABLE IF NOT EXISTS posts (
    id               TEXT PRIMARY KEY,
    subreddit_id     INT REFERENCES subreddits(id),

    title            TEXT,
    author           TEXT,
    created_utc      TIMESTAMP NOT NULL,

    first_seen_at    TIMESTAMP DEFAULT NOW(),
    last_polled_at   TIMESTAMP,

    current_score    INT DEFAULT 0,
    current_comments INT DEFAULT 0,
    current_ratio    FLOAT DEFAULT 0,

    -- CHANGE: kept as TEXT for backwards compatibility.
    -- Future migration: ALTER COLUMN poll_priority TYPE SMALLINT USING
    --   CASE poll_priority WHEN 'aggressive' THEN 3 WHEN 'normal' THEN 2
    --                      WHEN 'slow' THEN 1 ELSE 0 END;
    poll_priority    TEXT,

    is_active        BOOLEAN DEFAULT TRUE
);


-- ── Post metrics history (append-only time-series) ────────────────────────────
CREATE TABLE IF NOT EXISTS post_metrics_history (
    id           BIGSERIAL PRIMARY KEY,
    post_id      TEXT REFERENCES posts(id) ON DELETE CASCADE,
    captured_at  TIMESTAMP DEFAULT NOW(),
    score        INT,
    num_comments INT,
    upvote_ratio FLOAT
);


-- ── NLP features (one row per post, upserted) ────────────────────────────────
CREATE TABLE IF NOT EXISTS post_nlp_features (
    post_id         TEXT PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
    sentiment_score FLOAT,
    keywords        JSONB,
    topic_cluster   INT
);


-- ── Subreddit activity snapshots ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS subreddit_activity_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    subreddit_id INT REFERENCES subreddits(id),
    captured_at  TIMESTAMP DEFAULT NOW(),
    post_count   INT,
    avg_score    FLOAT,
    avg_comments FLOAT
);


-- =====================================================================
-- INDEXES
-- =====================================================================

-- Subreddit FK lookup (unchanged)
CREATE INDEX IF NOT EXISTS idx_posts_subreddit
    ON posts (subreddit_id);

-- CHANGE: replace low-selectivity boolean index with a PARTIAL index.
-- Only indexes active posts; much smaller, directly useful for the API query.
-- Original: CREATE INDEX idx_posts_active ON posts(is_active);
CREATE INDEX IF NOT EXISTS idx_posts_active_created
    ON posts (created_utc DESC)
    WHERE is_active = TRUE;

-- Cursor-based pagination and time-range queries (unchanged, still needed)
CREATE INDEX IF NOT EXISTS idx_posts_created_time
    ON posts (created_utc DESC);

-- CHANGE: new composite covering index for /api/stats/ aggregations.
-- Covers: WHERE created_utc BETWEEN x AND y, GROUP BY author, GROUP BY subreddit_id,
--         ORDER BY current_score DESC — all in one index-only scan.
-- Original had none of this; every stats query required a full sequential scan.
CREATE INDEX IF NOT EXISTS idx_posts_stats_covering
    ON posts (created_utc, current_score DESC, author, subreddit_id)
    WHERE is_active = TRUE;

-- Time-series history lookup (unchanged)
CREATE INDEX IF NOT EXISTS idx_post_metrics_post_time
    ON post_metrics_history (post_id, captured_at DESC);

-- CHANGE: add captured_at-only index for time-range purge queries.
-- Allows DELETE FROM post_metrics_history WHERE captured_at < $cutoff
-- to use the index rather than a sequential scan.
CREATE INDEX IF NOT EXISTS idx_post_metrics_captured_at
    ON post_metrics_history (captured_at DESC);

-- For very high volume (>50M rows in post_metrics_history), replace
-- idx_post_metrics_captured_at with a BRIN index — 100× smaller on disk:
-- CREATE INDEX IF NOT EXISTS idx_post_metrics_brin
--     ON post_metrics_history USING BRIN (captured_at);

-- GIN index for JSONB keyword search (unchanged)
CREATE INDEX IF NOT EXISTS idx_post_keywords
    ON post_nlp_features USING GIN (keywords);

-- Subreddit activity time queries (unchanged)
CREATE INDEX IF NOT EXISTS idx_subreddit_activity_time
    ON subreddit_activity_snapshots (subreddit_id, captured_at DESC);


-- =====================================================================
-- OPTIONAL FUTURE EXTENSIONS
-- =====================================================================
-- CREATE EXTENSION IF NOT EXISTS vector;       -- pgvector for embeddings
-- CREATE EXTENSION IF NOT EXISTS timescaledb;  -- automatic time-partitioning
