package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/FiRe088/free-tier-stack/pulse/internal/checker"
	"github.com/FiRe088/free-tier-stack/pulse/internal/store"
)

func main() {
	databaseURL := requireEnv("DATABASE_URL")
	targetsFile := requireEnv("TARGETS_FILE")
	checkTimeoutMs := envIntDefault("CHECK_TIMEOUT_MS", 3000)
	checkIntervalSec := envIntDefault("CHECK_INTERVAL_SEC", 30)
	downThreshold := envIntDefault("DOWN_THRESHOLD", 3)

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Phase 4: optional profiling. Unlike Watchdog (a batch job), Pulse is
	// long-running — CPU profiling runs for the whole process lifetime and
	// stops on shutdown; memory profiling snapshots right before exit.
	if profilePath := os.Getenv("CPU_PROFILE"); profilePath != "" {
		f, err := os.Create(profilePath)
		if err != nil {
			log.Fatalf("pulse: could not create CPU profile file: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("pulse: could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		log.Printf("pulse: CPU profiling enabled, writing to %s", profilePath)
	}

	if memProfilePath := os.Getenv("MEM_PROFILE"); memProfilePath != "" {
		defer func() {
			f, err := os.Create(memProfilePath)
			if err != nil {
				log.Printf("pulse: could not create memory profile file: %v", err)
				return
			}
			defer f.Close()
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("pulse: could not write memory profile: %v", err)
				return
			}
			log.Printf("pulse: memory profile written to %s", memProfilePath)
		}()
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	st, err := store.New(connCtx, databaseURL)
	connCancel()
	if err != nil {
		log.Fatalf("pulse: failed to connect to store: %v", err)
	}
	defer st.Close()

	cfg, err := checker.LoadConfig(targetsFile)
	if err != nil {
		log.Fatalf("pulse: failed to load config: %v", err)
	}

	log.Printf("pulse: connected. monitoring %d targets, interval=%ds timeout=%dms down_threshold=%d",
		len(cfg.Targets), checkIntervalSec, checkTimeoutMs, downThreshold)

	client := &http.Client{
		// No client-level Timeout set deliberately — each request already
		// carries its own context deadline via checker.Check. A second,
		// client-level timeout here would be redundant and could produce
		// confusing error messages (two different timeout mechanisms
		// racing each other).
	}

	results := make(chan checker.CheckResult, len(cfg.Targets))

	runOneRound := func() {
		var wg sync.WaitGroup
		for _, target := range cfg.Targets {
			wg.Add(1)
			go func(t checker.Target) {
				defer wg.Done()
				// Deliberately NOT derived from runCtx: an in-flight check should
				// be allowed to finish on its own timeout even after a shutdown
				// signal arrives. Tying this to runCtx caused every shutdown to
				// record a burst of false "context canceled" failures for targets
				// that were actually healthy — confirmed via uptime_checks rows
				// showing NULL status_code for ok-N/slow-N targets exactly at
				// shutdown time. Accuracy over shutdown speed for an uptime monitor.
				checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Duration(checkTimeoutMs)*time.Millisecond)
				defer checkCancel()
				results <- checker.Check(checkCtx, t, client)
			}(target)
		}
		wg.Wait()
	}

	// Single writer goroutine consumes results, so Postgres writes for
	// this round never run concurrently with each other — avoids lock
	// contention on uptime_alert_state row-level locks between targets
	// that happen to finish their HTTP check at the same instant.
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for r := range results {
			writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)

			var statusCode, latencyMs *int
			if r.StatusCode != 0 {
				sc := r.StatusCode
				statusCode = &sc
			}
			if r.LatencyMs != 0 {
				lm := r.LatencyMs
				latencyMs = &lm
			}

			if err := st.InsertCheck(writeCtx, store.CheckRecord{
				TargetName: r.Target.Name,
				TargetURL:  r.Target.URL,
				StatusCode: statusCode,
				LatencyMs:  latencyMs,
				Success:    r.Success,
			}); err != nil {
				log.Printf("pulse: failed to record check for %s: %v", r.Target.Name, err)
			}

			transition, err := st.UpdateAlertState(writeCtx, r.Target.Name, r.Success, downThreshold)
			if err != nil {
				log.Printf("pulse: failed to update alert state for %s: %v", r.Target.Name, err)
			} else if transition != nil {
				log.Printf("pulse: ALERT: %s transitioned %s -> %s",
					transition.TargetName, transition.FromState, transition.ToState)
			}

			if !r.Success {
				reason := "non-2xx or timeout"
				if r.Err != nil {
					reason = r.Err.Error()
				}
				log.Printf("pulse: check failed for %s (%s): status=%d latency=%dms reason=%s",
					r.Target.Name, r.Target.URL, r.StatusCode, r.LatencyMs, reason)
			} else {
				log.Printf("pulse: check ok for %s (%s): status=%d latency=%dms",
					r.Target.Name, r.Target.URL, r.StatusCode, r.LatencyMs)
			}

			writeCancel()
		}
	}()

	ticker := time.NewTicker(time.Duration(checkIntervalSec) * time.Second)
	defer ticker.Stop()

	log.Printf("pulse: running initial check round")
	runOneRound()

loop:
	for {
		select {
		case <-runCtx.Done():
			log.Printf("pulse: shutdown signal received")
			break loop
		case <-ticker.C:
			log.Printf("pulse: running scheduled check round")
			runOneRound()
		}
	}

	close(results)
	writerWg.Wait()
	log.Printf("pulse: shutdown complete")
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("pulse: required environment variable %s is not set", key)
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
		log.Fatalf("pulse: invalid integer for %s: %q", key, v)
	}
	return n
}
