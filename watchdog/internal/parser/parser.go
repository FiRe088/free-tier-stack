package parser

import (
	"fmt"
	"strings"
	"time"

	"github.com/FiRe088/free-tier-stack/watchdog/internal/store"
)

// Expected line format (Phase 1, fixed):
//   2026-07-21T22:14:15Z LEVEL message text here
//
// Example:
//   2026-07-21T22:14:15Z ERROR failed to connect to upstream
//
// Phase 1 intentionally supports exactly one format. Real-world log
// parsers (e.g. grok patterns) support many formats via config — that's
// a legitimate Phase 3+ extension, not a Phase 1 requirement.

const timeLayout = time.RFC3339

// ParseLine converts one raw log line into a store.LogEvent.
// source identifies which log file/stream this line came from.
func ParseLine(source, rawLine string) (store.LogEvent, error) {
	rawLine = strings.TrimSpace(rawLine)
	if rawLine == "" {
		return store.LogEvent{}, fmt.Errorf("parser: empty line")
	}

	parts := strings.SplitN(rawLine, " ", 3)
	if len(parts) != 3 {
		return store.LogEvent{}, fmt.Errorf("parser: malformed line, expected 3 fields, got %d: %q", len(parts), rawLine)
	}

	ts, level, message := parts[0], parts[1], parts[2]

	occurredAt, err := time.Parse(timeLayout, ts)
	if err != nil {
		return store.LogEvent{}, fmt.Errorf("parser: bad timestamp %q: %w", ts, err)
	}

	level = strings.ToUpper(level)
	switch level {
	case "INFO", "WARN", "ERROR", "DEBUG":
		// valid
	default:
		return store.LogEvent{}, fmt.Errorf("parser: unknown level %q", level)
	}

	return store.LogEvent{
		Source:     source,
		Level:      level,
		Message:    message,
		OccurredAt: occurredAt,
	}, nil
}
