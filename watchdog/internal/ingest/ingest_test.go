package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

// memCommitter is an in-memory Committer that can be told to fail, so the
// pipeline's guarantees can be tested without a database. Like the real one,
// a failed Commit stores nothing.
type memCommitter struct {
	mu        sync.Mutex
	events    []store.LogEvent
	pos       map[string]tailer.Position
	positions []int64 // every committed offset, in order
	failNext  int     // fail this many upcoming commits
}

func newMem() *memCommitter { return &memCommitter{pos: map[string]tailer.Position{}} }

func (m *memCommitter) LoadPosition(_ context.Context, source string) (tailer.Position, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pos[source], nil
}

func (m *memCommitter) Commit(_ context.Context, source string, events []store.LogEvent, end tailer.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext > 0 {
		m.failNext--
		return errors.New("simulated database outage")
	}
	m.events = append(m.events, events...)
	m.pos[source] = end
	m.positions = append(m.positions, end.Offset)
	return nil
}

func (m *memCommitter) snapshot() ([]store.LogEvent, []int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]store.LogEvent(nil), m.events...), append([]int64(nil), m.positions...)
}

func logLines(from, to int) string {
	var b strings.Builder
	for i := from; i < to; i++ {
		fmt.Fprintf(&b, "2026-07-21T22:14:15Z INFO message %d\n", i)
	}
	return b.String()
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

var testCfg = Config{BatchSize: 7, FlushInterval: 20 * time.Millisecond, PollInterval: 10 * time.Millisecond, MaxRetryDelay: 20 * time.Millisecond}

func TestBatchModeCommitsEveryLineAndFinalOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, logLines(0, 100))
	m := newMem()
	var stats Stats

	if err := RunFile(context.Background(), m, path, "app.log", testCfg, &stats); err != nil {
		t.Fatal(err)
	}
	events, positions := m.snapshot()
	if len(events) != 100 || stats.Inserted.Load() != 100 {
		t.Fatalf("got %d events (stats %d), want 100", len(events), stats.Inserted.Load())
	}
	for i, e := range events {
		if want := fmt.Sprintf("message %d", i); e.Message != want {
			t.Fatalf("event %d = %q, want %q (out of order?)", i, e.Message, want)
		}
	}
	if last := positions[len(positions)-1]; last != fileSize(t, path) {
		t.Fatalf("final committed offset %d, want file size %d", last, fileSize(t, path))
	}
}

// Commits must be strictly in file order, or the saved offset could claim
// lines are stored when an earlier batch never was.
func TestCommitsAreInFileOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, logLines(0, 500))
	m := newMem()

	if err := RunFile(context.Background(), m, path, "app.log", testCfg, &Stats{}); err != nil {
		t.Fatal(err)
	}
	_, positions := m.snapshot()
	for i := 1; i < len(positions); i++ {
		if positions[i] <= positions[i-1] {
			t.Fatalf("commit %d offset %d not after previous %d", i, positions[i], positions[i-1])
		}
	}
}

// Regression for the 14x duplication incident (CHALLENGES.md 2026-07-23):
// running the job again, even many times, must not re-insert anything.
func TestRerunsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, logLines(0, 50))
	m := newMem()

	for run := 0; run < 14; run++ {
		if err := RunFile(context.Background(), m, path, "app.log", testCfg, &Stats{}); err != nil {
			t.Fatal(err)
		}
	}
	appendTo(t, path, logLines(50, 60))
	if err := RunFile(context.Background(), m, path, "app.log", testCfg, &Stats{}); err != nil {
		t.Fatal(err)
	}

	events, _ := m.snapshot()
	if len(events) != 60 {
		t.Fatalf("got %d events after 15 runs, want exactly 60", len(events))
	}
}

func TestParseErrorsStillAdvanceOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, "garbage\nmore garbage\n")
	m := newMem()
	var stats Stats

	if err := RunFile(context.Background(), m, path, "app.log", testCfg, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.ParseErrors.Load() != 2 {
		t.Fatalf("parse errors = %d, want 2", stats.ParseErrors.Load())
	}
	if pos, _ := m.LoadPosition(context.Background(), "app.log"); pos.Offset != fileSize(t, path) {
		t.Fatalf("offset %d, want %d: bad lines would be re-read forever", pos.Offset, fileSize(t, path))
	}
}

// A failing database must not lose or duplicate lines: the batch is retried
// until it commits.
func TestCommitFailuresAreRetriedWithoutLossOrDuplication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, logLines(0, 40))
	m := newMem()
	m.failNext = 3
	var stats Stats

	if err := RunFile(context.Background(), m, path, "app.log", testCfg, &stats); err != nil {
		t.Fatal(err)
	}
	events, _ := m.snapshot()
	if len(events) != 40 {
		t.Fatalf("got %d events, want 40", len(events))
	}
	if stats.CommitFails.Load() != 3 {
		t.Fatalf("commit failures = %d, want 3", stats.CommitFails.Load())
	}
}

// In follow mode, shutdown flushes everything already read before returning.
func TestFollowShutdownFlushesReadLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, logLines(0, 3))
	m := newMem()

	cfg := testCfg
	cfg.Follow = true
	cfg.FlushInterval = time.Hour // only shutdown or a full batch can flush
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunFile(ctx, m, path, "app.log", cfg, &Stats{}) }()

	time.Sleep(100 * time.Millisecond) // 3 lines read, below batch size, not yet flushed
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if events, _ := m.snapshot(); len(events) != 3 {
		t.Fatalf("got %d events after shutdown, want 3", len(events))
	}
}

func TestFollowIngestsAppendedLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	appendTo(t, path, "")
	m := newMem()

	cfg := testCfg
	cfg.Follow = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunFile(ctx, m, path, "app.log", cfg, &Stats{}) }()

	appendTo(t, path, logLines(0, 25))
	deadline := time.Now().Add(2 * time.Second)
	for {
		if events, _ := m.snapshot(); len(events) == 25 {
			break
		}
		if time.Now().After(deadline) {
			events, _ := m.snapshot()
			t.Fatalf("only %d of 25 appended lines ingested", len(events))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
