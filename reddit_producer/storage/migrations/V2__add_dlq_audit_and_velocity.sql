-- =====================================================================
-- V2__add_dlq_audit_and_velocity.sql
-- Phase 1 additions:
--   1. dlq_events table — persists DLQ messages for audit / replay
--   2. velocity + trending columns on posts — expose computed values
--   3. schema_migrations bookkeeping table (created here, used by runner)
-- =====================================================================

-- ── Migration bookkeeping ─────────────────────────────────────────────────────
-- Tracks which migration files have been applied. The migration runner
-- (migrate.py) reads this table on startup and skips already-applied versions.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    description TEXT,
    applied_at  TIMESTAMP DEFAULT NOW()
);


-- ── DLQ audit table ───────────────────────────────────────────────────────────
-- Every message that failed processing and was routed to reddit.posts.dlq
-- is also persisted here. This gives ops a durable audit trail even after
-- the DLQ consumer's in-memory ring buffer is flushed (restart, etc.).

CREATE TABLE IF NOT EXISTS dlq_events (
    id           BIGSERIAL PRIMARY KEY,
    source_topic TEXT      NOT NULL,
    failed_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    error        TEXT,
    attempt      INT       DEFAULT 1,
    payload      JSONB,
    replayed_at  TIMESTAMP,           -- set when the message is successfully replayed
    resolved     BOOLEAN   DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dlq_source_topic
    ON dlq_events (source_topic, failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
    ON dlq_events (resolved, failed_at DESC)
    WHERE resolved = FALSE;


-- ── Velocity + trending columns on posts ──────────────────────────────────────
-- Previously computed in memory only and not persisted. Now stored on the
-- posts table so the API can filter/sort by trending_score without a
-- separate lookup into the processing layer.

ALTER TABLE posts
    ADD COLUMN IF NOT EXISTS score_velocity   FLOAT   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS comment_velocity FLOAT   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS trending_score   FLOAT   DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_trending      BOOLEAN DEFAULT FALSE;

-- Index for the common dashboard query:
--   WHERE is_trending = TRUE ORDER BY trending_score DESC
CREATE INDEX IF NOT EXISTS idx_posts_trending
    ON posts (trending_score DESC)
    WHERE is_trending = TRUE;


-- ── Record this migration ─────────────────────────────────────────────────────
INSERT INTO schema_migrations (version, description)
VALUES ('V2', 'Add DLQ audit table, velocity columns, and migration bookkeeping')
ON CONFLICT (version) DO NOTHING;
