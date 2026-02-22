-- =====================================================================
-- V4__subreddit_config.sql  — Phase 2 migration
-- =====================================================================
-- Creates the subreddit_config table used by:
--   1. Django Admin (SubredditConfig model) — ops add/remove/pause subreddits
--   2. Ingestion scheduler — reads active rows on startup and every
--      SCHEDULER_CONFIG_POLL_S seconds to hot-reload config without restart
--
-- Why a table instead of an env variable list?
--   The original approach hard-coded TOP_SUBREDDITS = [...] in the ingestion
--   service. Any change required editing source code, rebuilding the Docker
--   image, and redeploying. This table makes it a ~10-second browser operation.
-- =====================================================================

CREATE TABLE IF NOT EXISTS subreddit_config (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(100) NOT NULL UNIQUE,
    interval_seconds INTEGER      NOT NULL DEFAULT 120 CHECK (interval_seconds >= 10),
    priority         VARCHAR(10)  NOT NULL DEFAULT 'medium'
                         CHECK (priority IN ('fast', 'medium', 'slow')),
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    added_by         VARCHAR(100) NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Trigger to auto-update updated_at on row changes
CREATE OR REPLACE FUNCTION update_subreddit_config_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_subreddit_config_updated_at ON subreddit_config;
CREATE TRIGGER trg_subreddit_config_updated_at
    BEFORE UPDATE ON subreddit_config
    FOR EACH ROW EXECUTE FUNCTION update_subreddit_config_updated_at();

-- Seed with the subreddits that were previously hard-coded in the ingestion service
INSERT INTO subreddit_config (name, interval_seconds, priority, added_by)
VALUES
    ('technology',       60,  'fast',   'phase2-migration'),
    ('worldnews',        60,  'fast',   'phase2-migration'),
    ('science',          120, 'medium', 'phase2-migration'),
    ('programming',      120, 'medium', 'phase2-migration'),
    ('MachineLearning',  180, 'medium', 'phase2-migration'),
    ('datascience',      300, 'slow',   'phase2-migration'),
    ('artificial',       300, 'slow',   'phase2-migration')
ON CONFLICT (name) DO NOTHING;

-- Index for the scheduler's active-rows query
CREATE INDEX IF NOT EXISTS idx_subreddit_config_active
    ON subreddit_config (is_active, priority);

-- ── posts table: expose velocity + trending_score to the Django API ───────────
-- The processing service writes these columns; the Django ORM models already
-- declare them (velocity, trending_score). The columns may already exist from
-- V2; use ADD COLUMN IF NOT EXISTS to be idempotent.

ALTER TABLE posts
    ADD COLUMN IF NOT EXISTS velocity       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trending_score DOUBLE PRECISION;
