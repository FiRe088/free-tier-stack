package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LogEvent mirrors a row in the log_events table.
type LogEvent struct {
	Source     string
	Level      string
	Message    string
	OccurredAt time.Time
}

// Store wraps a Postgres connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a connection pool and verifies connectivity with a ping.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: creating pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping failed: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases all pooled connections.
func (s *Store) Close() {
	s.pool.Close()
}

// InsertLogEvent writes a single parsed log line to the log_events table.
// Retained from Phase 1 for reference/testing — Phase 2's main.go uses
// InsertLogEventsBatch instead, which is why this is now the slow path.
func (s *Store) InsertLogEvent(ctx context.Context, e LogEvent) error {
	const q = `
		INSERT INTO log_events (source, level, message, occurred_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := s.pool.Exec(ctx, q, e.Source, e.Level, e.Message, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("store: insert log event: %w", err)
	}
	return nil
}

// InsertLogEventsBatch writes multiple events in a single round-trip using
// pgx's pipelined batch protocol. This is the Phase 2 fix for the Phase 1
// bottleneck: 10,007 sequential single-row inserts measured at 945 lines/sec
// with ~9.6s of 10.6s wall time spent on network round-trip wait, not CPU.
// Batching amortizes that round-trip cost across many rows per trip instead
// of paying it once per row.
func (s *Store) InsertLogEventsBatch(ctx context.Context, events []LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	const q = `
		INSERT INTO log_events (source, level, message, occurred_at)
		VALUES ($1, $2, $3, $4)
	`

	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(q, e.Source, e.Level, e.Message, e.OccurredAt)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("store: batch insert: %w", err)
		}
	}

	return nil
}

// AlertsRecorded reports how many new alert rows were inserted.
type AnomalyResult struct {
	AlertsRecorded int
}

// DetectAndRecordErrorSpikes scans log_events for 1-minute windows per
// source where the ERROR count meets or exceeds threshold, and records
// one alert row per such window. Windows that already have an alert
// (from a previous run) are silently skipped via ON CONFLICT DO NOTHING,
// relying on the log_alerts_source_window_unique constraint rather than
// a separate existence check — this avoids a check-then-insert race
// under concurrent access.
//
// This is a fixed 1-minute tumbling window, not a sliding window — a
// known simplification. A burst of errors spanning a window boundary
// (e.g. 30 errors in the last 10s of one minute, 30 more in the first
// 10s of the next) would be split across two windows and might not
// trigger either one individually. A sliding-window implementation is
// a legitimate Phase 4+ improvement, not done here.
func (s *Store) DetectAndRecordErrorSpikes(ctx context.Context, threshold int) (AnomalyResult, error) {
	const selectQ = `
		SELECT source, date_trunc('minute', occurred_at) AS window_start, COUNT(*) AS error_count
		FROM log_events
		WHERE level = 'ERROR'
		GROUP BY source, window_start
		HAVING COUNT(*) >= $1
	`

	rows, err := s.pool.Query(ctx, selectQ, threshold)
	if err != nil {
		return AnomalyResult{}, fmt.Errorf("store: detecting error spikes: %w", err)
	}
	defer rows.Close()

	type window struct {
		source      string
		windowStart time.Time
		errorCount  int
	}
	var windows []window
	for rows.Next() {
		var w window
		if err := rows.Scan(&w.source, &w.windowStart, &w.errorCount); err != nil {
			return AnomalyResult{}, fmt.Errorf("store: scanning error spike row: %w", err)
		}
		windows = append(windows, w)
	}
	if err := rows.Err(); err != nil {
		return AnomalyResult{}, fmt.Errorf("store: iterating error spike rows: %w", err)
	}

	const insertQ = `
		INSERT INTO log_alerts (source, reason, window_start, window_end)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (source, window_start) DO NOTHING
	`

	recorded := 0
	for _, w := range windows {
		reason := fmt.Sprintf("error rate spike: %d errors in 1 minute (threshold: %d)", w.errorCount, threshold)
		windowEnd := w.windowStart.Add(time.Minute)

		tag, err := s.pool.Exec(ctx, insertQ, w.source, reason, w.windowStart, windowEnd)
		if err != nil {
			return AnomalyResult{}, fmt.Errorf("store: inserting alert for %s: %w", w.source, err)
		}
		if tag.RowsAffected() > 0 {
			recorded++
		}
	}

	return AnomalyResult{AlertsRecorded: recorded}, nil
}

// logEventCopySource implements pgx.CopyFromSource, feeding rows to
// CopyFrom one at a time without pre-building a full [][]any slice.
type logEventCopySource struct {
	events []LogEvent
	idx    int
}

func (s *logEventCopySource) Next() bool {
	s.idx++
	return s.idx <= len(s.events)
}

func (s *logEventCopySource) Values() ([]any, error) {
	e := s.events[s.idx-1]
	return []any{e.Source, e.Level, e.Message, e.OccurredAt}, nil
}

func (s *logEventCopySource) Err() error {
	return nil
}

// InsertLogEventsCopy writes multiple events using Postgres's binary COPY
// protocol via pgx.CopyFrom, instead of the extended-query batch protocol
// used by InsertLogEventsBatch.
//
// Phase 4 profiling (mem.prof, alloc_objects) showed InsertLogEventsBatch
// itself was the largest single allocator in the entire program (29.39%
// flat, 86.41% cumulative including the pgx batch machinery it triggers):
// building a pgx.Batch with N individually-Queue'd statements carries real
// per-row bookkeeping overhead (pipelineState tracking, command tag
// allocation, a container/list per in-flight batch). COPY avoids per-row
// query planning and pipeline bookkeeping entirely — rows stream through
// a single binary protocol message rather than N queued statements.
//
// Tradeoff: COPY does not support ON CONFLICT, so this cannot be used for
// InsertLogEventsBatch's use case if conflict handling were ever needed
// (it isn't currently — log_events has no unique constraint). If that
// changes, this method would need reconsidering, not just reuse.
func (s *Store) InsertLogEventsCopy(ctx context.Context, events []LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	src := &logEventCopySource{events: events}

	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"log_events"},
		[]string{"source", "level", "message", "occurred_at"},
		src,
	)
	if err != nil {
		return fmt.Errorf("store: copy insert: %w", err)
	}
	return nil
}
