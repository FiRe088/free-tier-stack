package tailer

import (
	"bufio"
	"context"
	"fmt"
	"os"
)

// ReadLines reads a file line by line and calls handleLine for each one.
// Phase 1/2: reads the whole file once and returns — no live-follow.
//
// Phase 3 adds ctx cancellation: if ctx is cancelled mid-file (e.g. the
// process received SIGTERM), ReadLines stops sending new lines and
// returns immediately rather than finishing the file. This is what makes
// graceful shutdown meaningful — without it, a SIGTERM during a large
// file would either be ignored until the file finishes, or kill the
// process mid-write with no chance to flush pending batches.
func ReadLines(ctx context.Context, path string, handleLine func(line string) error) []error {
	var errs []error

	f, err := os.Open(path)
	if err != nil {
		return []error{fmt.Errorf("tailer: opening %s: %w", path, err)}
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("tailer: %s: cancelled after %d lines: %w", path, lineNum, ctx.Err()))
			return errs
		default:
		}

		lineNum++
		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := handleLine(line); err != nil {
			errs = append(errs, fmt.Errorf("tailer: line %d: %w", lineNum, err))
		}
	}

	if err := scanner.Err(); err != nil {
		errs = append(errs, fmt.Errorf("tailer: scanning %s: %w", path, err))
	}

	return errs
}
