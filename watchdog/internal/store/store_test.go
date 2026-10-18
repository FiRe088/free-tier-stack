package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

// newTestStore connects to TEST_DATABASE_URL (schema from db/init applied).
// Skipped when unset, so `go test ./...` still works without a database.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connecting to test database: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// uniqueSource isolates each test's rows and deletes them afterwards.
func uniqueSource(t *testing.T, st *Store) string {
	t.Helper()
	src := fmt.Sprintf("test-%s-%d.log", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		ctx := context.Background()
		st.pool.Exec(ctx, `DELETE FROM log_events WHERE source = $1`, src)
		st.pool.Exec(ctx, `DELETE FROM log_offsets WHERE source = $1`, src)
		st.pool.Exec(ctx, `DELETE FROM log_alerts WHERE source = $1`, src)
	})
	return src
}

func countEvents(t *testing.T, st *Store, src string) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM log_events WHERE source = $1`, src).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func events(src string, n int) []LogEvent {
	out := make([]LogEvent, n)
	for i := range out {
		out[i] = LogEvent{Source: src, Level: "INFO", Message: fmt.Sprintf("m%d", i), OccurredAt: time.Now().UTC()}
	}
	return out
}

func TestCommitStoresRowsAndOffset(t *testing.T) {
	st := newTestStore(t)
	for _, useCopy := range []bool{true, false} {
		t.Run(fmt.Sprintf("copy=%v", useCopy), func(t *testing.T) {
			src := uniqueSource(t, st)
			c := OffsetCommitter{Store: st, UseCopy: useCopy}
			ctx := context.Background()

			if pos, err := c.LoadPosition(ctx, src); err != nil || pos != (tailer.Position{}) {
				t.Fatalf("new source: pos=%+v err=%v, want zero position", pos, err)
			}

			want := tailer.Position{Inode: 4242, Offset: 1234}
			if err := c.Commit(ctx, src, events(src, 25), want); err != nil {
				t.Fatal(err)
			}
			if n := countEvents(t, st, src); n != 25 {
				t.Fatalf("got %d rows, want 25", n)
			}
			if pos, err := c.LoadPosition(ctx, src); err != nil || pos != want {
				t.Fatalf("pos=%+v err=%v, want %+v", pos, err, want)
			}
		})
	}
}

// The core guarantee: if inserting the rows fails, the offset must not move.
// Otherwise a restart would skip lines that were never stored.
func TestCommitIsAtomic(t *testing.T) {
	st := newTestStore(t)
	for _, useCopy := range []bool{true, false} {
		t.Run(fmt.Sprintf("copy=%v", useCopy), func(t *testing.T) {
			src := uniqueSource(t, st)
			c := OffsetCommitter{Store: st, UseCopy: useCopy}
			ctx := context.Background()

			first := tailer.Position{Inode: 1, Offset: 100}
			if err := c.Commit(ctx, src, events(src, 3), first); err != nil {
				t.Fatal(err)
			}

			// Postgres rejects NUL bytes in TEXT, so this batch fails mid-insert.
			bad := events(src, 5)
			bad[3].Message = "contains \x00 a NUL byte"
			if err := c.Commit(ctx, src, bad, tailer.Position{Inode: 1, Offset: 200}); err == nil {
				t.Fatal("expected the bad batch to fail")
			}

			if n := countEvents(t, st, src); n != 3 {
				t.Fatalf("got %d rows, want 3: part of the failed batch was stored", n)
			}
			if pos, _ := c.LoadPosition(ctx, src); pos != first {
				t.Fatalf("offset moved to %+v after a failed commit, want %+v", pos, first)
			}
		})
	}
}

func TestCommitEmptyBatchAdvancesOffset(t *testing.T) {
	st := newTestStore(t)
	src := uniqueSource(t, st)
	c := OffsetCommitter{Store: st, UseCopy: true}
	ctx := context.Background()

	want := tailer.Position{Inode: 7, Offset: 99}
	if err := c.Commit(ctx, src, nil, want); err != nil {
		t.Fatal(err)
	}
	if pos, _ := c.LoadPosition(ctx, src); pos != want {
		t.Fatalf("pos=%+v, want %+v", pos, want)
	}
}

// With a since cutoff, only windows touched by recently ingested rows are
// re-examined, but those windows are counted in full.
func TestDetectErrorSpikesSince(t *testing.T) {
	st := newTestStore(t)
	src := uniqueSource(t, st)
	ctx := context.Background()

	oldWindow := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	newWindow := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	longAgo := time.Now().Add(-24 * time.Hour)

	insert := func(occurred time.Time, ingested time.Time, n int) {
		for i := 0; i < n; i++ {
			if _, err := st.pool.Exec(ctx,
				`INSERT INTO log_events (source, level, message, occurred_at, ingested_at) VALUES ($1, 'ERROR', 'x', $2, $3)`,
				src, occurred.Add(time.Duration(i)*time.Second), ingested); err != nil {
				t.Fatal(err)
			}
		}
	}
	insert(oldWindow, longAgo, 5)    // old spike, ingested a day ago
	insert(newWindow, longAgo, 3)    // 3 old errors in the new window...
	insert(newWindow, time.Now(), 2) // ...plus 2 just ingested: 5 in total

	res, err := st.DetectAndRecordErrorSpikes(ctx, 5, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if res.AlertsRecorded != 1 {
		t.Fatalf("incremental pass recorded %d alerts, want 1 (only the recently touched window)", res.AlertsRecorded)
	}

	res, err = st.DetectAndRecordErrorSpikes(ctx, 5, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if res.AlertsRecorded != 1 {
		t.Fatalf("full pass recorded %d new alerts, want 1 (the old window; the new one is deduplicated)", res.AlertsRecorded)
	}
}
