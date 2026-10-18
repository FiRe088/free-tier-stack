package checker

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// RunRound checks every target concurrently, one goroutine per target, and
// sends each result to out. It blocks until every check has finished.
//
// ctx is the process lifetime context. If it is already cancelled, the round
// is skipped. Once a round has started, in-flight checks are deliberately
// detached from ctx's cancellation (context.WithoutCancel) and only stop at
// their own timeout.
//
// Why: an uptime monitor's output is a record of truth. When checks shared
// the shutdown context, every SIGTERM aborted in-flight requests and recorded
// them as failures, writing ~100 fake outages per shutdown during load tests
// (CHALLENGES.md, 2026-07-22). Shutting down a few hundred milliseconds
// slower is a better trade than corrupting the data.
func RunRound(ctx context.Context, targets []Target, client *http.Client, timeout time.Duration, out chan<- CheckResult) {
	if ctx.Err() != nil {
		return
	}
	base := context.WithoutCancel(ctx)

	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t Target) {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(base, timeout)
			defer cancel()
			out <- Check(checkCtx, t, client)
		}(target)
	}
	wg.Wait()
}
