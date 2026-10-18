// Package ingest turns one log file into committed database rows.
//
// Each file runs as its own ordered pipeline:
//
//	tailer.Tail ──lines──▶ parse + batch ──batches──▶ commit (rows + offset, one tx)
//
// The three stages run concurrently, so reading and parsing overlap with
// database I/O, but batches are committed strictly in file order. That
// ordering is what makes the saved offset meaningful. With the old design,
// four workers committing interleaved batches from the same file, there was
// no single offset that meant "everything before this is stored".
// Parallelism now comes from running many files at once.
package ingest

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/parser"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

// Committer stores a batch of events and the file position they end at,
// atomically. If Commit returns an error, neither must be stored.
type Committer interface {
	LoadPosition(ctx context.Context, source string) (tailer.Position, error)
	Commit(ctx context.Context, source string, events []store.LogEvent, end tailer.Position) error
}

// Config controls batching and tailing for every file.
type Config struct {
	Follow        bool
	PollInterval  time.Duration
	BatchSize     int
	FlushInterval time.Duration
	// CommitTimeout bounds one commit attempt. Defaults to 10s.
	CommitTimeout time.Duration
	// MaxRetryDelay caps the backoff between failed commits. Defaults to 5s.
	MaxRetryDelay time.Duration
}

// Stats are updated atomically and safe to read from any goroutine.
type Stats struct {
	Lines       atomic.Int64 // lines read
	Inserted    atomic.Int64 // events committed
	ParseErrors atomic.Int64
	CommitFails atomic.Int64 // failed attempts (each was retried)
}

type batch struct {
	events []store.LogEvent
	end    tailer.Position
}

// RunFile ingests one file until EOF (batch mode) or until ctx is cancelled
// (follow mode). On cancellation, lines already read are flushed and
// committed before it returns, so shutdown loses nothing it has read.
func RunFile(ctx context.Context, c Committer, path, source string, cfg Config, stats *Stats) error {
	cfg = withDefaults(cfg)

	start, err := c.LoadPosition(ctx, source)
	if err != nil {
		return fmt.Errorf("ingest: loading position for %s: %w", source, err)
	}

	lines := make(chan tailer.Line, cfg.BatchSize)
	batches := make(chan batch, 2) // small buffer: commit backpressure reaches the reader quickly

	tailErr := make(chan error, 1)
	go func() {
		defer close(lines)
		tailErr <- tailer.Tail(ctx, path, start, tailer.Options{Follow: cfg.Follow, PollInterval: cfg.PollInterval}, lines)
	}()

	// Parse + batch stage. It doesn't watch ctx itself: it runs until the
	// tailer closes lines, so every line already read reaches a batch.
	go func() {
		defer close(batches)
		ticker := time.NewTicker(cfg.FlushInterval)
		defer ticker.Stop()

		cur := batch{events: make([]store.LogEvent, 0, cfg.BatchSize)}
		dirty := false // position advanced since last flush (even if every line failed to parse)
		flush := func() {
			if !dirty {
				return
			}
			batches <- cur
			cur = batch{events: make([]store.LogEvent, 0, cfg.BatchSize)}
			dirty = false
		}

		for {
			select {
			case ln, ok := <-lines:
				if !ok {
					flush()
					return
				}
				stats.Lines.Add(1)
				cur.end, dirty = ln.End, true
				if ln.Text == "" {
					continue
				}
				ev, perr := parser.ParseLine(source, ln.Text)
				if perr != nil {
					stats.ParseErrors.Add(1)
					log.Printf("ingest: %s: %v", source, perr)
					continue
				}
				cur.events = append(cur.events, ev)
				if len(cur.events) >= cfg.BatchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()

	// Commit stage, in order. A failed commit is retried with backoff. If we
	// are shutting down and still can't commit, we stop: the offset was never
	// advanced, so the next run re-reads exactly these lines. Nothing is lost
	// and nothing is duplicated, which is the whole point of committing the
	// offset in the same transaction.
	var commitErr error
	for b := range batches {
		if commitErr != nil {
			continue // drain so upstream goroutines can exit
		}
		if err := commitWithRetry(ctx, c, source, b, cfg, stats); err != nil {
			commitErr = err
			continue
		}
		stats.Inserted.Add(int64(len(b.events)))
	}

	if err := <-tailErr; err != nil {
		return err
	}
	return commitErr
}

func commitWithRetry(ctx context.Context, c Committer, source string, b batch, cfg Config, stats *Stats) error {
	delay := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		// Fresh context per attempt: a shutdown signal must not abort the
		// final flush of lines we have already read.
		cctx, cancel := context.WithTimeout(context.Background(), cfg.CommitTimeout)
		err := c.Commit(cctx, source, b.events, b.end)
		cancel()
		if err == nil {
			return nil
		}
		stats.CommitFails.Add(1)
		log.Printf("ingest: %s: commit attempt %d failed (%d events, will retry): %v", source, attempt, len(b.events), err)

		if ctx.Err() != nil && attempt >= 3 {
			return fmt.Errorf("ingest: %s: giving up on shutdown after %d attempts; offset not advanced, lines will be re-read on restart: %w", source, attempt, err)
		}
		time.Sleep(delay)
		delay = min(delay*2, cfg.MaxRetryDelay)
	}
}

func withDefaults(cfg Config) Config {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if cfg.CommitTimeout <= 0 {
		cfg.CommitTimeout = 10 * time.Second
	}
	if cfg.MaxRetryDelay <= 0 {
		cfg.MaxRetryDelay = 5 * time.Second
	}
	return cfg
}
