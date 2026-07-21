-- Adds a uniqueness constraint so the same (source, window_start) can
-- never produce two alert rows. Combined with INSERT ... ON CONFLICT DO
-- NOTHING in application code, this makes alert dedup a guaranteed
-- database-level property rather than an app-level race condition
-- (check-then-insert has a TOCTOU gap under concurrent workers; a unique
-- constraint does not).
ALTER TABLE log_alerts
    ADD CONSTRAINT log_alerts_source_window_unique UNIQUE (source, window_start);
