-- Watchdog: parsed log events
CREATE TABLE IF NOT EXISTS log_events (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,          -- which log file/source this came from
    level       TEXT NOT NULL,          -- INFO, WARN, ERROR, etc.
    message     TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,   -- timestamp parsed from the log line itself
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()  -- when Watchdog processed it
);

CREATE INDEX IF NOT EXISTS idx_log_events_occurred_at ON log_events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_log_events_source_level ON log_events (source, level);

-- Watchdog: anomaly alerts raised
CREATE TABLE IF NOT EXISTS log_alerts (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    reason      TEXT NOT NULL,          -- e.g. "error rate spike: 45/min"
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pulse: uptime check results
CREATE TABLE IF NOT EXISTS uptime_checks (
    id           BIGSERIAL PRIMARY KEY,
    target_name  TEXT NOT NULL,
    target_url   TEXT NOT NULL,
    status_code  INT,                   -- NULL if the request failed entirely (timeout, DNS, etc.)
    latency_ms   INT,
    success      BOOLEAN NOT NULL,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_uptime_checks_target_time ON uptime_checks (target_name, checked_at);

-- Pulse: alert state transitions (only written on state change, not every check)
CREATE TABLE IF NOT EXISTS uptime_alert_state (
    target_name     TEXT PRIMARY KEY,
    current_state    TEXT NOT NULL DEFAULT 'unknown',  -- 'up', 'down', 'unknown'
    consecutive_fails INT NOT NULL DEFAULT 0,
    last_transition_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
