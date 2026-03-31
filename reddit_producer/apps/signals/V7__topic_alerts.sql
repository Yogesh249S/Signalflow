-- =====================================================================
-- V7__topic_alerts.sql
-- =====================================================================
-- Topic alert webhooks — push notifications for API users.
--
-- watched_topics: one row per (user, topic) subscription.
--   POST /api/v1/alerts/watch/   → insert or enable
--   DELETE /api/v1/alerts/watch/ → set is_active = false
--   The trending pipeline checks this table every cycle and fires
--   webhook_url when the topic's trend_score crosses the threshold.
--
-- alert_deliveries: audit log of every webhook attempt.
--   Lets us: retry failed deliveries, show users their alert history,
--   and deduplicate — one delivery per (topic, user) per cooldown window.
-- =====================================================================

CREATE TABLE IF NOT EXISTS watched_topics (
    id              BIGSERIAL   PRIMARY KEY,
    user_id         INTEGER     NOT NULL REFERENCES auth_user(id) ON DELETE CASCADE,
    topic           TEXT        NOT NULL,

    -- Where to push
    webhook_url     TEXT        NOT NULL,

    -- Trigger conditions
    -- Fire when trend_score >= this value (0 = any trending signal)
    min_trend_score FLOAT       NOT NULL DEFAULT 0.0,
    -- Fire when the topic appears on at least this many platforms
    min_platforms   INTEGER     NOT NULL DEFAULT 1,

    -- Deduplication — don't re-fire within this window
    cooldown_minutes INTEGER    NOT NULL DEFAULT 60,

    -- Lifecycle
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_fired_at   TIMESTAMPTZ,           -- NULL = never fired

    UNIQUE (user_id, topic, webhook_url)
);

CREATE INDEX IF NOT EXISTS idx_watched_topics_active
    ON watched_topics (topic, is_active)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_watched_topics_user
    ON watched_topics (user_id);


-- ── Delivery audit log ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS alert_deliveries (
    id              BIGSERIAL   PRIMARY KEY,
    watched_topic_id BIGINT     NOT NULL REFERENCES watched_topics(id) ON DELETE CASCADE,
    topic           TEXT        NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Snapshot of what triggered the alert
    trend_score     FLOAT,
    platform_count  INTEGER,
    platforms       JSONB,

    -- HTTP outcome
    http_status     INTEGER,               -- NULL = not yet attempted
    success         BOOLEAN     NOT NULL DEFAULT FALSE,
    error_message   TEXT,
    latency_ms      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_topic
    ON alert_deliveries (topic, fired_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_watched
    ON alert_deliveries (watched_topic_id, fired_at DESC);
