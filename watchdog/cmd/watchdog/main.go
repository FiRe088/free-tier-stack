package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	st, err := store.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("watchdog: failed to connect to store: %v", err)
	}
	defer st.Close()

	log.Printf("watchdog: connected to database (workers=%d batch_size=%d batch_interval=%dms)",
		workerPoolSize, batchSize, batchIntervalMs)

	// rawLine carries a line plus which file it came from, so parsing can
	// happen inside the worker instead of the single reader goroutine —
	// this keeps the reader cheap and parallelizes the CPU-bound parsing
	// step across all workers.
	type rawLine struct {
		source string
		text   string
	}

	// Bounded channel: this is the backpressure mechanism. If all workers
	// are busy flushing batches to Postgres, this channel fills up and the
	// reader goroutine blocks on the send — it does NOT buffer unboundedly.
	// On a 128MB-capped container, unbounded buffering is how you get OOM
	// killed under a real log flood; a bounded channel converts that into
	// graceful slowdown instead.
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

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		path := filepath.Join(logSourceDir, entry.Name())
		source := entry.Name()
		log.Printf("watchdog: processing %s", path)

		errs := tailer.ReadLines(path, func(line string) error {
			lines <- rawLine{source: source, text: line}
			return nil
		})
		for _, e := range errs {
			log.Printf("watchdog: reader error: %v", e)
		}
	}

	close(lines)
	workerWg.Wait()

	log.Printf("watchdog: done. processed=%d errors=%d",
		atomic.LoadInt64(&totalProcessed), atomic.LoadInt64(&totalErrors))
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
