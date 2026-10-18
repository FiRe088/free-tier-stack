package parser

import (
	"testing"
	"time"
)

func TestParseLine(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantErr   bool
		wantLevel string
		wantMsg   string
	}{
		{"valid error line", "2026-07-21T22:14:15Z ERROR failed to connect", false, "ERROR", "failed to connect"},
		{"lowercase level normalised", "2026-07-21T22:14:15Z warn disk 90% full", false, "WARN", "disk 90% full"},
		{"message keeps inner spaces", "2026-07-21T22:14:15Z INFO a  b   c", false, "INFO", "a  b   c"},
		{"surrounding whitespace trimmed", "   2026-07-21T22:14:15Z DEBUG x   ", false, "DEBUG", "x"},
		{"empty line", "", true, "", ""},
		{"too few fields", "2026-07-21T22:14:15Z ERROR", true, "", ""},
		{"bad timestamp", "21/07/2026 ERROR boom", true, "", ""},
		{"unknown level", "2026-07-21T22:14:15Z FATAL boom", true, "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := ParseLine("app.log", tc.line)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got event %+v", ev)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ev.Level != tc.wantLevel || ev.Message != tc.wantMsg || ev.Source != "app.log" {
				t.Errorf("got level=%q msg=%q source=%q", ev.Level, ev.Message, ev.Source)
			}
			if !ev.OccurredAt.Equal(time.Date(2026, 7, 21, 22, 14, 15, 0, time.UTC)) {
				t.Errorf("wrong timestamp: %v", ev.OccurredAt)
			}
		})
	}
}
