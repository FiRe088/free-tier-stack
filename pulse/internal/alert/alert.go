// Package alert holds Pulse's up/down state machine as a pure function.
//
// It used to live inside store.UpdateAlertState, tangled up with SQL. That
// it impossible to test without a database, which is how the suppressed
// first-check alert bug in CHALLENGES.md went unnoticed. With the decision
// pulled out, the state machine is tested exhaustively in microseconds and
// the store only does I/O.
package alert

// State is a target's alerting state as stored in uptime_alert_state.
type State string

const (
	Unknown State = "unknown" // never checked yet
	Up      State = "up"
	Down    State = "down"
)

// Decision is the outcome of applying one check result to the current state.
type Decision struct {
	State            State
	ConsecutiveFails int
	// Transitioned is true when State differs from the previous state.
	// Leaving Unknown counts: a target that is broken from its very first
	// check must alert. Suppressing that case was a real bug (see
	// CHALLENGES.md, 2026-07-22 "Alert suppressed on the exact case it
	// mattered most").
	Transitioned bool
}

// Next applies a single check result to the current state.
//
// downThreshold is how many consecutive failures are needed before a target
// is marked Down, so a single blip doesn't page anyone. Values below 1 are
// treated as 1. A single success always marks the target Up and resets the
// failure counter.
func Next(current State, consecutiveFails int, success bool, downThreshold int) Decision {
	if downThreshold < 1 {
		downThreshold = 1
	}
	if current == "" {
		current = Unknown
	}

	if success {
		return Decision{State: Up, ConsecutiveFails: 0, Transitioned: current != Up}
	}

	fails := consecutiveFails + 1
	next := current
	if fails >= downThreshold {
		next = Down
	}
	return Decision{State: next, ConsecutiveFails: fails, Transitioned: next != current}
}
