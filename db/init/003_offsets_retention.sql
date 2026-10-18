-- Watchdog: last committed read position per log source. Written in the
-- same transaction as the log_events rows it covers, so a restart resumes
-- exactly where the last commit ended (no lost or duplicated lines).
CREATE TABLE IF NOT EXISTS log_offsets (
    source      TEXT PRIMARY KEY,
    inode       BIGINT NOT NULL,       -- detects rotation: new file at the same path
    byte_offset BIGINT NOT NULL,       -- position just after the last committed line
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Anomaly detection in follow mode only re-examines windows touched by
-- recently ingested rows, instead of rescanning the whole table every run.
CREATE INDEX IF NOT EXISTS idx_log_events_ingested_at ON log_events (ingested_at);

-- Retention deletes old uptime_checks by checked_at alone. The existing
-- (target_name, checked_at) index can't serve a range scan on checked_at.
CREATE INDEX IF NOT EXISTS idx_uptime_checks_checked_at ON uptime_checks (checked_at);
