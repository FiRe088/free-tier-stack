package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

// OffsetCommitter writes log events and the file offset they end at in a
// single transaction. It implements ingest.Committer.
//
// Committing both together is what makes ingestion restart-safe: after a
// crash, either the rows and the new offset are both stored or neither is,
// so the next run resumes from exactly the right byte. This is the root-cause
// fix for the restart loop that duplicated log_events 14x (CHALLENGES.md,
// 2026-07-23). Changing the restart policy stopped the loop; this makes
// re-running harmless.
type OffsetCommitter struct {
	*Store
	// UseCopy selects the COPY protocol (faster, see BENCHMARKS.md Phase 4)
	// instead of a pipelined batch of INSERTs. Both run inside the same tx.
	UseCopy bool
}

// LoadPosition returns the last committed position for source, or the zero
// Position if the source has never been committed.
func (c OffsetCommitter) LoadPosition(ctx context.Context, source string) (tailer.Position, error) {
	var inode, offset int64
	err := c.pool.QueryRow(ctx,
		`SELECT inode, byte_offset FROM log_offsets WHERE source = $1`, source,
	).Scan(&inode, &offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return tailer.Position{}, nil
	}
	if err != nil {
		return tailer.Position{}, fmt.Errorf("store: loading offset for %s: %w", source, err)
	}
	return tailer.Position{Inode: uint64(inode), Offset: offset}, nil
}

// Commit inserts events and records end as source's new position, atomically.
// events may be empty (e.g. every line in the batch failed to parse); the
// offset still advances, because those lines have been consumed.
func (c OffsetCommitter) Commit(ctx context.Context, source string, events []LogEvent, end tailer.Position) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after Commit

	if len(events) > 0 {
		if c.UseCopy {
			_, err = tx.CopyFrom(ctx,
				pgx.Identifier{"log_events"},
				[]string{"source", "level", "message", "occurred_at"},
				&logEventCopySource{events: events},
			)
		} else {
			err = sendInsertBatch(ctx, tx, events)
		}
		if err != nil {
			return fmt.Errorf("store: inserting %d events for %s: %w", len(events), source, err)
		}
	}

	const upsert = `
		INSERT INTO log_offsets (source, inode, byte_offset, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (source) DO UPDATE
		SET inode = EXCLUDED.inode, byte_offset = EXCLUDED.byte_offset, updated_at = now()
	`
	if _, err := tx.Exec(ctx, upsert, source, int64(end.Inode), end.Offset); err != nil {
		return fmt.Errorf("store: saving offset for %s: %w", source, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit for %s: %w", source, err)
	}
	return nil
}

func sendInsertBatch(ctx context.Context, tx pgx.Tx, events []LogEvent) error {
	const q = `INSERT INTO log_events (source, level, message, occurred_at) VALUES ($1, $2, $3, $4)`
	b := &pgx.Batch{}
	for _, e := range events {
		b.Queue(q, e.Source, e.Level, e.Message, e.OccurredAt)
	}
	br := tx.SendBatch(ctx, b)
	for range events {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return err
		}
	}
	return br.Close()
}
