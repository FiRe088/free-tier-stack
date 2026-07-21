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
