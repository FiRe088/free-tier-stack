package checker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "targets.yml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string // substring; empty means success
		wantN   int
	}{
		{"valid", "targets:\n  - name: a\n    url: http://a\n  - name: b\n    url: http://b\n", "", 2},
		{"no targets", "targets: []\n", "no targets", 0},
		{"missing name", "targets:\n  - url: http://a\n", "empty name", 0},
		{"missing url", "targets:\n  - name: a\n", "empty url", 0},
		{"not yaml", "targets: [unclosed\n", "parsing config", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeConfig(t, tc.body))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(cfg.Targets) != tc.wantN {
				t.Fatalf("got %d targets, want %d", len(cfg.Targets), tc.wantN)
			}
		})
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "nope.yml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
