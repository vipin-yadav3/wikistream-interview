-- WikiStream Postgres schema
-- Auto-applied on first container startup.

-- Main aggregation table: one row per (5-min window, wiki)
CREATE TABLE IF NOT EXISTS wiki_edit_counts (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    wiki             TEXT        NOT NULL,
    edit_count       BIGINT      NOT NULL DEFAULT 0,
    bot_edit_count   BIGINT      NOT NULL DEFAULT 0,
    net_bytes_added  BIGINT      NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, wiki)
);

-- Staging table used by the foreachBatch upsert pattern
CREATE TABLE IF NOT EXISTS wiki_edit_counts_staging (
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    wiki            TEXT,
    edit_count      BIGINT,
    bot_edit_count  BIGINT,
    net_bytes_added BIGINT
);

-- Curveball: bot storm alerts
CREATE TABLE IF NOT EXISTS bot_alerts (
    id             SERIAL       PRIMARY KEY,
    window_start   TIMESTAMPTZ  NOT NULL,
    window_end     TIMESTAMPTZ  NOT NULL,
    wiki           TEXT         NOT NULL,
    bot_edit_count BIGINT       NOT NULL,
    edit_count     BIGINT       NOT NULL,
    bot_ratio      DOUBLE PRECISION NOT NULL,
    detected_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Handy view for quick inspection during the interview
CREATE OR REPLACE VIEW recent_stats AS
SELECT
    wiki,
    window_start AT TIME ZONE 'UTC' AS window_start_utc,
    edit_count,
    bot_edit_count,
    ROUND((bot_edit_count::numeric / NULLIF(edit_count,0)) * 100, 1) AS bot_pct,
    net_bytes_added,
    last_updated
FROM wiki_edit_counts
ORDER BY window_start DESC, edit_count DESC
LIMIT 50;
