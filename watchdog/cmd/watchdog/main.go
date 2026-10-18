package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/ingest"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
)

func main() {
	databaseURL := requireEnv("DATABASE_URL")
	logSourceDir := requireEnv("LOG_SOURCE_DIR")
	follow := envBoolDefault("FOLLOW", false)
	maxConcurrentFiles := envIntDefault("WORKER_POOL_SIZE", 4) // batch mode only; follow mode runs every file
	batchSize := envIntDefault("BATCH_SIZE", 2000)             // measured: 2000 matches the old 4-worker throughput with one ordered pipeline
	batchIntervalMs := envIntDefault("BATCH_INTERVAL_MS", 500)
	pollIntervalMs := envIntDefault("POLL_INTERVAL_MS", 250)
	discoverIntervalSec := envIntDefault("DISCOVER_INTERVAL_SEC", 5)
	anomalyIntervalSec := envIntDefault("ANOMALY_INTERVAL_SEC", 60)
	errorThreshold := envIntDefault("ERROR_SPIKE_THRESHOLD", 5)

	// copy (default): Postgres COPY protocol, ~1.75x faster (BENCHMARKS.md Phase 4)
	// batch:          pipelined INSERTs, kept for A/B comparison
	insertStrategy := os.Getenv("INSERT_STRATEGY")
	if insertStrategy == "" {
		insertStrategy = "copy"
	}
	if insertStrategy != "batch" && insertStrategy != "copy" {
		log.Fatalf("watchdog: invalid INSERT_STRATEGY %q, must be 'batch' or 'copy'", insertStrategy)
	}

	if profilePath := os.Getenv("CPU_PROFILE"); profilePath != "" {
		f, err := os.Create(profilePath)
		if err != nil {
			log.Fatalf("watchdog: could not create CPU profile file: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("watchdog: could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		log.Printf("watchdog: CPU profiling enabled, writing to %s", profilePath)
	}

	memProfilePath := os.Getenv("MEM_PROFILE")
	if memProfilePath != "" {
		defer func() {
			f, err := os.Create(memProfilePath)
			if err != nil {
				log.Printf("watchdog: could not create memory profile file: %v", err)
				return
			}
			defer f.Close()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("watchdog: could not write memory profile: %v", err)
				return
			}
			log.Printf("watchdog: memory profile written to %s", memProfilePath)
		}()
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	st, err := store.New(connCtx, databaseURL)
	connCancel()
	if err != nil {
		log.Fatalf("watchdog: failed to connect to store: %v", err)
	}
	defer st.Close()

	committer := store.OffsetCommitter{Store: st, UseCopy: insertStrategy == "copy"}
	cfg := ingest.Config{
		Follow:        follow,
		PollInterval:  time.Duration(pollIntervalMs) * time.Millisecond,
		BatchSize:     batchSize,
		FlushInterval: time.Duration(batchIntervalMs) * time.Millisecond,
	}
	var stats ingest.Stats

	mode := "batch"
	if follow {
		mode = "follow"
	}
	log.Printf("watchdog: connected (mode=%s batch_size=%d batch_interval=%dms error_threshold=%d insert_strategy=%s)",
		mode, batchSize, batchIntervalMs, errorThreshold, insertStrategy)

	runFile := func(path string) {
		source := filepath.Base(path)
		log.Printf("watchdog: ingesting %s", path)
		if err := ingest.RunFile(runCtx, committer, path, source, cfg, &stats); err != nil {
			log.Printf("watchdog: %s: %v", source, err)
		}
	}

	detect := func(since time.Time) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		res, err := st.DetectAndRecordErrorSpikes(ctx, errorThreshold, since)
		if err != nil {
			log.Printf("watchdog: anomaly detection failed: %v", err)
			return
		}
		if res.AlertsRecorded > 0 {
			log.Printf("watchdog: anomaly detection: new_alerts=%d", res.AlertsRecorded)
		}
	}

	var wg sync.WaitGroup
	start := time.Now()

	if !follow {
		// Batch mode: ingest what's there, run detection once, exit. Safe to
		// re-run: offsets mean already-ingested lines are skipped.
		files, err := listLogFiles(logSourceDir)
		if err != nil {
			log.Fatalf("watchdog: %v", err)
		}
		sem := make(chan struct{}, max(1, maxConcurrentFiles))
		for _, path := range files {
			wg.Add(1)
			sem <- struct{}{}
			go func(p string) {
				defer wg.Done()
				defer func() { <-sem }()
				runFile(p)
			}(path)
		}
		wg.Wait()
		logStats(&stats, time.Since(start))
		detect(time.Time{})
		return
	}

	// Follow mode: a long-running daemon. Pick up new files as they appear,
	// run anomaly detection periodically on newly ingested data.
	running := map[string]bool{}
	discover := func() {
		files, err := listLogFiles(logSourceDir)
		if err != nil {
			log.Printf("watchdog: %v", err)
			return
		}
		for _, path := range files {
			if running[path] {
				continue
			}
			running[path] = true
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				runFile(p)
			}(path)
		}
	}
	discover()

	discoverTicker := time.NewTicker(time.Duration(discoverIntervalSec) * time.Second)
	defer discoverTicker.Stop()
	anomalyTicker := time.NewTicker(time.Duration(anomalyIntervalSec) * time.Second)
	defer anomalyTicker.Stop()
	lastDetect := time.Time{} // first pass covers everything already stored

loop:
	for {
		select {
		case <-runCtx.Done():
			log.Printf("watchdog: shutdown signal received, flushing")
			break loop
		case <-discoverTicker.C:
			discover()
		case <-anomalyTicker.C:
			// Small overlap so rows committed just before the previous pass
			// started are never skipped. Re-checking a window is harmless:
			// alerts are deduplicated by a unique constraint.
			now := time.Now()
			detect(lastDetect.Add(-time.Minute))
			lastDetect = now
		}
	}

	wg.Wait() // every file pipeline flushes and commits what it has read
	detect(lastDetect.Add(-time.Minute))
	logStats(&stats, time.Since(start))
}

func listLogFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files, nil
}

func logStats(s *ingest.Stats, elapsed time.Duration) {
	inserted := s.Inserted.Load()
	rate := 0.0
	if secs := elapsed.Seconds(); secs > 0 {
		rate = float64(inserted) / secs
	}
	log.Printf("watchdog: ingestion done. lines=%d inserted=%d parse_errors=%d commit_retries=%d elapsed=%s (%.0f lines/sec)",
		s.Lines.Load(), inserted, s.ParseErrors.Load(), s.CommitFails.Load(), elapsed.Round(time.Millisecond), rate)
}

func envBoolDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Fatalf("watchdog: invalid boolean for %s: %q", key, v)
	}
	return b
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("watchdog: required environment variable %s is not set", key)
	}
	return v
}

func envIntDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("watchdog: invalid integer for %s: %q", key, v)
	}
	return n
}
