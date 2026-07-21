package store

import (
	"context"
	"fmt"
	"time"

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
// Phase 1: a single shared pool, no batching — every call to InsertLogEvent
// does one round-trip to Postgres. This is intentional: Phase 1 proves
// correctness, Phase 2 introduces batching and measures the improvement.
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
