package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// logflood generates synthetic log lines matching Watchdog's expected
// format, at a controllable rate, for load-testing Phase 2's worker pool
// and batching against Phase 1's naive single-insert approach.
//
// Usage:
//   go run . -lines 10000 -out ../../watchdog/sample-logs/flood.log
//   go run . -lines 5000 -rate 1000 -out ../../watchdog/sample-logs/flood.log
//
// -rate controls lines/sec if you want a live-append simulation later
// (Phase 2 tailer will need to handle a growing file, not just a static
// one). For now, -rate 0 means "write all lines immediately, no pacing" —
// used for Phase 1 vs Phase 2 batch-insert throughput comparisons.

var levels = []string{"INFO", "INFO", "INFO", "WARN", "ERROR", "DEBUG"}

var messages = []string{
	"request handled successfully",
	"connection pool nearing capacity",
	"failed to connect to upstream",
	"cache miss, fetching from origin",
	"request timeout after 30s",
	"retrying failed operation",
	"health check passed",
	"slow query detected",
	"rate limit threshold reached",
	"background job completed",
}

func main() {
	lines := flag.Int("lines", 10000, "number of log lines to generate")
	rate := flag.Int("rate", 0, "lines per second; 0 = write all immediately, no pacing")
	outPath := flag.String("out", "flood.log", "output file path")
	errorRate := flag.Float64("error-rate", 0.15, "fraction of lines that are ERROR level (0.0-1.0)")
	flag.Parse()

	f, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logflood: failed to create output file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()

	var interval time.Duration
	if *rate > 0 {
		interval = time.Second / time.Duration(*rate)
	}

	for i := 0; i < *lines; i++ {
		ts := time.Now().UTC().Format(time.RFC3339)

		var level string
		if rng.Float64() < *errorRate {
			level = "ERROR"
		} else {
			level = levels[rng.Intn(len(levels))]
		}

		msg := messages[rng.Intn(len(messages))]

		line := fmt.Sprintf("%s %s %s (line=%d)\n", ts, level, msg, i)
		if _, err := f.WriteString(line); err != nil {
			fmt.Fprintf(os.Stderr, "logflood: write failed at line %d: %v\n", i, err)
			os.Exit(1)
		}

		if *rate > 0 {
			time.Sleep(interval)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("logflood: wrote %d lines to %s in %s (%.0f lines/sec)\n",
		*lines, *outPath, elapsed, float64(*lines)/elapsed.Seconds())
}
