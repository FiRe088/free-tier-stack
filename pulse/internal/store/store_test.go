package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// newTestStore connects to TEST_DATABASE_URL (schema from db/init applied).
// Tests are skipped when it is unset, so `go test ./...` still works offline.
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

// uniqueTarget returns a target name no other test uses, and removes its
// rows afterwards, so tests can run in parallel against one database.
func uniqueTarget(t *testing.T, st *Store) string {
	t.Helper()
	name := fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		ctx := context.Background()
		st.pool.Exec(ctx, `DELETE FROM uptime_alert_state WHERE target_name = $1`, name)
		st.pool.Exec(ctx, `DELETE FROM uptime_checks WHERE target_name = $1`, name)
	})
	return name
}

// Regression at the database level for CHALLENGES.md 2026-07-22: a target
// that is down from its very first check must produce a transition.
func TestUpdateAlertStateFirstCheckDownAlerts(t *testing.T) {
	st := newTestStore(t)
	name := uniqueTarget(t, st)
	ctx := context.Background()

	tr, err := st.UpdateAlertState(ctx, name, false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tr == nil || tr.FromState != "unknown" || tr.ToState != "down" {
		t.Fatalf("want unknown -> down transition, got %+v", tr)
	}
}

func TestUpdateAlertStateSequence(t *testing.T) {
	st := newTestStore(t)
	name := uniqueTarget(t, st)
	ctx := context.Background()

	steps := []struct {
		success bool
		want    string // "" means no transition expected
	}{
		{true, "unknown->up"},
		{false, ""},
		{false, ""},
		{false, "up->down"}, // third consecutive failure with threshold 3
		{false, ""},         // already down: no repeat alert
		{true, "down->up"},
		{true, ""},
	}

	for i, s := range steps {
		tr, err := st.UpdateAlertState(ctx, name, s.success, 3)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		got := ""
		if tr != nil {
			got = tr.FromState + "->" + tr.ToState
		}
		if got != s.want {
			t.Fatalf("step %d (success=%v): got transition %q, want %q", i, s.success, got, s.want)
		}
	}

	var state string
	var fails int
	if err := st.pool.QueryRow(ctx,
		`SELECT current_state, consecutive_fails FROM uptime_alert_state WHERE target_name = $1`, name,
	).Scan(&state, &fails); err != nil {
		t.Fatal(err)
	}
	if state != "up" || fails != 0 {
		t.Fatalf("final row: state=%s fails=%d, want up/0", state, fails)
	}
}
