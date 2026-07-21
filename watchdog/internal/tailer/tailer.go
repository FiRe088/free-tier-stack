package tailer

import (
	"bufio"
	"fmt"
	"os"
)

// ReadLines reads a file line by line and calls handleLine for each one.
// Phase 1: reads the whole file once and returns — no live-follow, no
// tracking of read position. Phase 2 will replace this with a real
// tail -f style follower that watches for new lines being appended.
//
// handleLine errors are not fatal — a single malformed line should not
// stop processing of the rest of the file. Callers are expected to log
// the error and continue, which is why this function returns a slice
// of errors rather than stopping on the first one.
func ReadLines(path string, handleLine func(line string) error) []error {
	var errs []error

	f, err := os.Open(path)
	if err != nil {
		return []error{fmt.Errorf("tailer: opening %s: %w", path, err)}
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
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
