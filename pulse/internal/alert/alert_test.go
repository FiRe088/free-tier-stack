package alert

import "testing"

func TestNext(t *testing.T) {
	tests := []struct {
		name      string
		current   State
		fails     int
		success   bool
		threshold int
		want      Decision
	}{
		// Regression: CHALLENGES.md 2026-07-22. A target that fails its very
		// first check must alert, not be silently treated as "nothing changed".
		{"first check fails, threshold 1", Unknown, 0, false, 1, Decision{Down, 1, true}},
		{"first check succeeds", Unknown, 0, true, 3, Decision{Up, 0, true}},
		{"empty state treated as unknown", "", 0, false, 1, Decision{Down, 1, true}},

		{"below threshold stays up", Up, 0, false, 3, Decision{Up, 1, false}},
		{"one below threshold stays up", Up, 1, false, 3, Decision{Up, 2, false}},
		{"reaching threshold goes down", Up, 2, false, 3, Decision{Down, 3, true}},
		{"already down keeps counting, no new alert", Down, 3, false, 3, Decision{Down, 4, false}},
		{"recovery alerts once", Down, 7, true, 3, Decision{Up, 0, true}},
		{"steady up is silent", Up, 0, true, 3, Decision{Up, 0, false}},
		{"success resets a partial failure streak", Up, 2, true, 3, Decision{Up, 0, false}},
		{"unknown below threshold stays unknown", Unknown, 0, false, 3, Decision{Unknown, 1, false}},
		{"zero threshold treated as one", Up, 0, false, 0, Decision{Down, 1, true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Next(tc.current, tc.fails, tc.success, tc.threshold)
			if got != tc.want {
				t.Errorf("Next(%q, %d, %v, %d) = %+v, want %+v",
					tc.current, tc.fails, tc.success, tc.threshold, got, tc.want)
			}
		})
	}
}

// A flapping target (fail, ok, fail, ok...) with threshold > 1 must never
// alert, because no failure streak ever reaches the threshold.
func TestNextFlappingNeverAlerts(t *testing.T) {
	state, fails := Up, 0
	for i := 0; i < 20; i++ {
		d := Next(state, fails, i%2 == 1, 2)
		if d.Transitioned {
			t.Fatalf("round %d: unexpected transition to %s", i, d.State)
		}
		state, fails = d.State, d.ConsecutiveFails
	}
}
