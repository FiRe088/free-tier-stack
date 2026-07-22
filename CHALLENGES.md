
## 2026-07-21 Missing file caused false "add dependency" error
**Problem:** `go build` suggested `go get` for an internal package
(`internal/tailer`), which should never happen for same-module imports.
**Root cause:** The `tailer.go` file was never actually created — a
heredoc command from an earlier step was skipped or didn't execute before
moving on to the next file.
**Fix:** Verified with `ls -la` that the file didn't exist, then re-ran
the heredoc to create it.
**Lesson:** Always verify a file was written (`cat` or `ls`) immediately
after creating it, before building on top of it — don't trust that a
command ran just because the terminal moved to the next prompt.

## 2026-07-22 Alert suppressed on the exact case it mattered most
**Problem:** Pulse's uptime_alert_state table showed correct down/up
states after testing against real failing/succeeding targets, but zero
ALERT log lines were ever printed despite DOWN_THRESHOLD=1 and immediate,
reproducible failures.
**Root cause:** UpdateAlertState's transition-detection logic explicitly
suppressed alerting when a target had no prior row (`isNewTarget == true`),
on the theory that a target's first-ever check shouldn't count as an
"alert-worthy transition." This meant the single most important case for
an uptime monitor — a target that is broken from the very first check —
was silently never alerted on, while later transitions (which never
happened in testing, since state was already 'down') would have worked.
**Fix:** Removed the isNewTarget suppression entirely. Any state change,
including the first-ever observation, now generates an alert. unknown
is not a neutral state — transitioning out of it is real information.
**Lesson:** Verify claimed behavior against the database directly, not
just log output. The state machine was working correctly the whole time
(uptime_alert_state had the right data); only the alerting/logging layer
on top of it was silently wrong. Checking one layer (logs) and assuming
the layer underneath matches is how this kind of bug hides.
