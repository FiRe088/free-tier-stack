package checker

import (
	"context"
	"net/http"
	"time"
)

// CheckResult is the outcome of one HTTP health check against a target.
type CheckResult struct {
	Target     Target
	StatusCode int  // 0 if the request failed entirely (timeout, DNS, connection refused)
	LatencyMs  int
	Success    bool
	Err        error // non-nil if the request failed before getting a response
}

// Check performs a single HTTP GET against target.URL, bounded by the
// context's deadline. Success is defined as a 2xx status code — 3xx/4xx/5xx
// all count as failures, since a redirect or error page is not "up" in
// any useful sense for an uptime monitor.
func Check(ctx context.Context, target Target, client *http.Client) CheckResult {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		return CheckResult{
			Target:  target,
			Success: false,
			Err:     err,
		}
	}

	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return CheckResult{
			Target:    target,
			LatencyMs: int(latency.Milliseconds()),
			Success:   false,
			Err:       err,
		}
	}
	defer resp.Body.Close()

	success := resp.StatusCode >= 200 && resp.StatusCode < 300

	return CheckResult{
		Target:     target,
		StatusCode: resp.StatusCode,
		LatencyMs:  int(latency.Milliseconds()),
		Success:    success,
	}
}
