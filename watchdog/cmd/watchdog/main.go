package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/parser"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
	"github.com/FiRe088/free-tier-stack/watchdog/internal/tailer"
)

func main() {
	databaseURL := requireEnv("DATABASE_URL")
	logSourceDir := requireEnv("LOG_SOURCE_DIR")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	st, err := store.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("watchdog: failed to connect to store: %v", err)
	}
	defer st.Close()

	log.Printf("watchdog: connected to database")

	entries, err := os.ReadDir(logSourceDir)
	if err != nil {
		log.Fatalf("watchdog: failed to read log source dir %s: %v", logSourceDir, err)
	}

	totalProcessed := 0
	totalErrors := 0

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		path := filepath.Join(logSourceDir, entry.Name())
		source := entry.Name()

		log.Printf("watchdog: processing %s", path)

		errs := tailer.ReadLines(path, func(line string) error {
			// Use a fresh short-lived context per insert. Phase 1 does not
			// batch — every line is one round-trip to Postgres. Phase 2
			// replaces this with a worker pool and batched inserts.
			ictx, icancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer icancel()

			event, perr := parser.ParseLine(source, line)
			if perr != nil {
				return perr
			}

			if serr := st.InsertLogEvent(ictx, event); serr != nil {
				return serr
			}

			totalProcessed++
			return nil
		})

		for _, e := range errs {
			totalErrors++
			log.Printf("watchdog: error: %v", e)
		}
	}

	log.Printf("watchdog: done. processed=%d errors=%d", totalProcessed, totalErrors)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("watchdog: required environment variable %s is not set", key)
	}
	return v
}
