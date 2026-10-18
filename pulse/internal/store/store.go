package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/FiRe088/free-tier-stack/pulse/internal/alert"
)

// CheckRecord mirrors a row in uptime_checks.
type CheckRecord struct {
	TargetName string
	TargetURL  string
	StatusCode *int // nil if the request failed entirely, matches nullable status_code column
	LatencyMs  *int
	Success    bool
}

type Store struct {
	pool *pgxpool.Pool
}

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

func (s *Store) Close() {
	s.pool.Close()
}

// InsertCheck writes one uptime check result.
func (s *Store) InsertCheck(ctx context.Context, r CheckRecord) error {
	const q = `
		INSERT INTO uptime_checks (target_name, target_url, status_code, latency_ms, success)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := s.pool.Exec(ctx, q, r.TargetName, r.TargetURL, r.StatusCode, r.LatencyMs, r.Success)
	if err != nil {
		return fmt.Errorf("store: insert check: %w", err)
	}
	return nil
}

// AlertTransition describes a state change worth logging/alerting on.
type AlertTransition struct {
	TargetName string
	FromState  string
	ToState    string
}

// UpdateAlertState reads the target's current alert state under a row lock,
// applies one check result via alert.Next, and writes the result back. It
// returns a non-nil transition only when the state actually changed.
//
// The decision itself lives in the alert package as a pure function, so it
// can be tested without a database. This method only does the I/O.
//
// downThreshold is how many consecutive failures are required before a
// target is considered "down" (avoids flapping on a single blip).
func (s *Store) UpdateAlertState(ctx context.Context, targetName string, success bool, downThreshold int) (*AlertTransition, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	var currentState string
	var consecutiveFails int

	const selectQ = `
		SELECT current_state, consecutive_fails
		FROM uptime_alert_state
		WHERE target_name = $1
		FOR UPDATE
	`
	err = tx.QueryRow(ctx, selectQ, targetName).Scan(&currentState, &consecutiveFails)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// First time we've seen this target.
		currentState, consecutiveFails = string(alert.Unknown), 0
	case err != nil:
		// Previously every error was treated as "no row", so a dropped
		// connection or timeout silently reset the target to unknown and
		// could fire a bogus alert. Only ErrNoRows means "new target".
		return nil, fmt.Errorf("store: reading alert state for %s: %w", targetName, err)
	}

	d := alert.Next(alert.State(currentState), consecutiveFails, success, downThreshold)

	const upsertQ = `
		INSERT INTO uptime_alert_state (target_name, current_state, consecutive_fails, last_transition_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (target_name) DO UPDATE
		SET current_state = EXCLUDED.current_state,
		    consecutive_fails = EXCLUDED.consecutive_fails,
		    last_transition_at = CASE
		        WHEN uptime_alert_state.current_state != EXCLUDED.current_state
		        THEN now()
		        ELSE uptime_alert_state.last_transition_at
		    END
	`
	if _, err := tx.Exec(ctx, upsertQ, targetName, string(d.State), d.ConsecutiveFails); err != nil {
		return nil, fmt.Errorf("store: upserting alert state for %s: %w", targetName, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: commit tx: %w", err)
	}

	if !d.Transitioned {
		return nil, nil
	}
	return &AlertTransition{
		TargetName: targetName,
		FromState:  currentState,
		ToState:    string(d.State),
	}, nil
}
