package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckStatusCodes(t *testing.T) {
	tests := []struct {
		status  int
		success bool
	}{
		{200, true},
		{204, true},
		{301, false}, // a 3xx is only seen when the client does not follow redirects, as below
		{404, false},
		{500, false},
		{503, false},
	}

	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == 301 {
					w.Header().Set("Location", "/elsewhere")
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			}}
			r := Check(context.Background(), Target{Name: "t", URL: srv.URL}, client)
			if r.Success != tc.success || r.StatusCode != tc.status || r.Err != nil {
				t.Errorf("got success=%v status=%d err=%v, want success=%v status=%d",
					r.Success, r.StatusCode, r.Err, tc.success, tc.status)
			}
		})
	}
}

func TestCheckTimeoutIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := Check(ctx, Target{Name: "slow", URL: srv.URL}, srv.Client())
	if r.Success || r.Err == nil || r.StatusCode != 0 {
		t.Fatalf("expected timeout failure, got success=%v status=%d err=%v", r.Success, r.StatusCode, r.Err)
	}
}

func TestCheckConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listening any more

	r := Check(context.Background(), Target{Name: "gone", URL: url}, http.DefaultClient)
	if r.Success || r.Err == nil {
		t.Fatalf("expected connection error, got success=%v err=%v", r.Success, r.Err)
	}
}

// Regression: CHALLENGES.md 2026-07-22 "Shutdown fabricated false-failure
// records for healthy targets". Cancelling the process context while a round
// is in flight must not turn healthy targets into recorded failures.
func TestRunRoundShutdownDoesNotFabricateFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // slow but healthy
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	targets := make([]Target, 20)
	for i := range targets {
		targets[i] = Target{Name: "t" + strconv.Itoa(i), URL: srv.URL}
	}

	runCtx, shutdown := context.WithCancel(context.Background())
	out := make(chan CheckResult, len(targets))

	done := make(chan struct{})
	go func() {
		RunRound(runCtx, targets, srv.Client(), 2*time.Second, out)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond) // requests are now in flight
	shutdown()                        // simulate SIGTERM mid-round
	<-done
	close(out)

	n := 0
	for r := range out {
		n++
		if !r.Success {
			t.Errorf("%s recorded as failed after shutdown (status=%d err=%v); shutdown fabricated an outage",
				r.Target.Name, r.StatusCode, r.Err)
		}
	}
	if n != len(targets) {
		t.Fatalf("got %d results, want %d", n, len(targets))
	}
}

func TestRunRoundSkippedWhenAlreadyShuttingDown(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := make(chan CheckResult, 1)
	RunRound(ctx, []Target{{Name: "t", URL: srv.URL}}, srv.Client(), time.Second, out)

	if len(out) != 0 || hit.Load() {
		t.Fatal("round ran even though shutdown had already started")
	}
}

// Checks run concurrently: 50 targets that each take 200ms must finish in
// roughly one target's time, not 50 x 200ms.
func TestRunRoundIsConcurrent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	targets := make([]Target, 50)
	for i := range targets {
		targets[i] = Target{Name: "t" + strconv.Itoa(i), URL: srv.URL}
	}
	out := make(chan CheckResult, len(targets))

	start := time.Now()
	RunRound(context.Background(), targets, srv.Client(), 2*time.Second, out)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("round took %v; checks are not running concurrently", elapsed)
	}
	if len(out) != len(targets) {
		t.Fatalf("got %d results, want %d", len(out), len(targets))
	}
}
