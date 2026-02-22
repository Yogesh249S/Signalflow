-- =====================================================
-- Reddit Analytics Database Schema
-- Designed for real-time ingestion + analytics
-- =====================================================

-- ===============================
-- 🟢 SUBREDDITS TABLE
-- ===============================
CREATE TABLE IF NOT EXISTS subreddits (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ===============================
-- 🟢 POSTS (CURRENT SNAPSHOT)
-- Stores latest state of each post
-- ===============================
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,                  -- Reddit post ID
    subreddit_id INT REFERENCES subreddits(id),

    title TEXT,
    author TEXT,
    created_utc TIMESTAMP NOT NULL,

    first_seen_at TIMESTAMP DEFAULT NOW(),
    last_polled_at TIMESTAMP,

    current_score INT DEFAULT 0,
    current_comments INT DEFAULT 0,
    current_ratio FLOAT DEFAULT 0,

    poll_priority TEXT,
    is_active BOOLEAN DEFAULT TRUE
);

-- ===============================
-- 🔵 POST METRICS HISTORY
-- Time-series table (MOST IMPORTANT)
-- Each refresh inserts a row
-- ===============================
CREATE TABLE IF NOT EXISTS post_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    post_id TEXT REFERENCES posts(id) ON DELETE CASCADE,

    captured_at TIMESTAMP DEFAULT NOW(),

    score INT,
    num_comments INT,
    upvote_ratio FLOAT
);

-- ===============================
-- 🟡 NLP / ANALYTICS FEATURES
-- Sentiment, keywords, clustering
-- ===============================
CREATE TABLE IF NOT EXISTS post_nlp_features (
    post_id TEXT PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,

    sentiment_score FLOAT,
    keywords JSONB,
    topic_cluster INT
);

-- ===============================
-- 🟣 SUBREDDIT ACTIVITY SNAPSHOTS
-- Heatmaps & subreddit comparisons
-- ===============================
CREATE TABLE IF NOT EXISTS subreddit_activity_snapshots (
    id BIGSERIAL PRIMARY KEY,
    subreddit_id INT REFERENCES subreddits(id),

    captured_at TIMESTAMP DEFAULT NOW(),

    post_count INT,
    avg_score FLOAT,
    avg_comments FLOAT
);

-- =====================================================
-- ⚡ PERFORMANCE INDEXES
-- =====================================================

-- Fast subreddit lookups
CREATE INDEX IF NOT EXISTS idx_posts_subreddit
ON posts(subreddit_id);

-- Active posts filter
CREATE INDEX IF NOT EXISTS idx_posts_active
ON posts(is_active);

-- Latest posts queries (DRF / dashboards)
CREATE INDEX IF NOT EXISTS idx_posts_created_time
ON posts(created_utc DESC);

-- Time-series queries
CREATE INDEX IF NOT EXISTS idx_post_metrics_post_time
ON post_metrics_history(post_id, captured_at DESC);

-- JSON keyword search support
CREATE INDEX IF NOT EXISTS idx_post_keywords
ON post_nlp_features USING GIN (keywords);

-- Subreddit activity queries
CREATE INDEX IF NOT EXISTS idx_subreddit_activity_time
ON subreddit_activity_snapshots(subreddit_id, captured_at DESC);

-- =====================================================
-- OPTIONAL FUTURE EXTENSIONS (Commented)
-- =====================================================
-- CREATE EXTENSION IF NOT EXISTS vector;     -- for embeddings later
-- CREATE EXTENSION IF NOT EXISTS timescaledb;







