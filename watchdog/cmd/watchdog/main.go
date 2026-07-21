package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/parser"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

func main() {
	databaseURL := requireEnv("DATABASE_URL")
	logSourceDir := requireEnv("LOG_SOURCE_DIR")
	workerPoolSize := envIntDefault("WORKER_POOL_SIZE", 4)
	batchSize := envIntDefault("BATCH_SIZE", 100)
	batchIntervalMs := envIntDefault("BATCH_INTERVAL_MS", 500)
	errorThreshold := envIntDefault("ERROR_SPIKE_THRESHOLD", 5)

	// runCtx is cancelled on SIGTERM/SIGINT. This is what makes shutdown
	// "graceful": the tailer stops reading new lines, workers finish
	// flushing whatever they're currently holding, and main exits cleanly
	// instead of being killed mid-write. connCtx is separate and NOT tied
	// to the signal, because the final anomaly-detection query and the
	// last batch flush need to complete even after shutdown begins.
	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connCancel()

	st, err := store.New(connCtx, databaseURL)
	if err != nil {
		log.Fatalf("watchdog: failed to connect to store: %v", err)
	}
	defer st.Close()

	log.Printf("watchdog: connected to database (workers=%d batch_size=%d batch_interval=%dms error_threshold=%d)",
		workerPoolSize, batchSize, batchIntervalMs, errorThreshold)

	type rawLine struct {
		source string
		text   string
	}

	lines := make(chan rawLine, workerPoolSize*batchSize)

	var totalProcessed int64
	var totalErrors int64

	var workerWg sync.WaitGroup
	for w := 0; w < workerPoolSize; w++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()

			batch := make([]store.LogEvent, 0, batchSize)
			ticker := time.NewTicker(time.Duration(batchIntervalMs) * time.Millisecond)
			defer ticker.Stop()

			flush := func() {
				if len(batch) == 0 {
					return
				}
				// Deliberately NOT tied to runCtx — a flush in progress
				// when SIGTERM arrives should still be allowed to finish
				// (up to 5s), otherwise graceful shutdown would drop
				// exactly the data it's supposed to protect.
				bctx, bcancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := st.InsertLogEventsBatch(bctx, batch); err != nil {
					log.Printf("watchdog: worker %d: batch insert failed (%d events lost): %v",
						workerID, len(batch), err)
					atomic.AddInt64(&totalErrors, int64(len(batch)))
				} else {
					atomic.AddInt64(&totalProcessed, int64(len(batch)))
				}
				bcancel()
				batch = batch[:0]
			}

			for {
				select {
				case rl, ok := <-lines:
					if !ok {
						flush()
						return
					}
					event, perr := parser.ParseLine(rl.source, rl.text)
					if perr != nil {
						log.Printf("watchdog: worker %d: parse error: %v", workerID, perr)
						atomic.AddInt64(&totalErrors, 1)
						continue
					}
					batch = append(batch, event)
					if len(batch) >= batchSize {
						flush()
					}
				case <-ticker.C:
					flush()
				}
			}
		}(w)
	}

	entries, err := os.ReadDir(logSourceDir)
	if err != nil {
		log.Fatalf("watchdog: failed to read log source dir %s: %v", logSourceDir, err)
	}

readLoop:
	for _, entry := range entries {
		select {
		case <-runCtx.Done():
			log.Printf("watchdog: shutdown signal received, stopping before %s", entry.Name())
			break readLoop
		default:
		}

		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		path := filepath.Join(logSourceDir, entry.Name())
		source := entry.Name()
		log.Printf("watchdog: processing %s", path)

		errs := tailer.ReadLines(runCtx, path, func(line string) error {
			lines <- rawLine{source: source, text: line}
			return nil
		})
		for _, e := range errs {
			log.Printf("watchdog: reader error: %v", e)
		}
	}

	close(lines)
	workerWg.Wait()

	log.Printf("watchdog: ingestion done. processed=%d errors=%d",
		atomic.LoadInt64(&totalProcessed), atomic.LoadInt64(&totalErrors))

	// Anomaly detection runs after ingestion completes, using its own
	// context independent of runCtx, so a shutdown signal during ingestion
	// doesn't also cancel this — we still want to know about error spikes
	// in whatever data did make it in before shutdown.
	anomalyCtx, anomalyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer anomalyCancel()

	result, err := st.DetectAndRecordErrorSpikes(anomalyCtx, errorThreshold)
	if err != nil {
		log.Printf("watchdog: anomaly detection failed: %v", err)
	} else {
		log.Printf("watchdog: anomaly detection done. new_alerts=%d", result.AlertsRecorded)
	}
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
