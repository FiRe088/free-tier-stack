
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

## 2026-07-22 Shutdown fabricated false-failure records for healthy targets
**Problem:** Load-testing Pulse against 100 mock targets and killing it
mid-run (via timeout) revealed that every shutdown produced a burst of
"context canceled" failures in uptime_checks for targets that were
actually healthy — confirmed via query showing NULL status_code entries
for ok-N and slow-N targets (categories that never legitimately fail)
landing in a single-second window matching shutdown time exactly.
**Root cause:** Each check's context was derived from runCtx
(context.WithTimeout(runCtx, ...)), the same context cancelled by
SIGTERM/SIGINT. The moment shutdown fired, every in-flight HTTP request
was aborted mid-request and its result recorded as a real failure,
indistinguishable in the database from a genuine outage.
**Fix:** Changed the per-check context to derive from
context.Background() instead of runCtx, so in-flight checks run to
their own natural timeout regardless of shutdown signals. Verified by
re-running the same load test and confirming zero false-failure rows
where previously there were ~100.
**Lesson:** For a service whose entire job is producing a record of
truth (uptime status), a shutdown mechanism that corrupts that record
is worse than one that shuts down slowly. Correctness under shutdown
needs the same scrutiny as correctness under load — "does it stop
cleanly" is not the same question as "does it stop truthfully."

## 2026-07-23 Watchdog restart loop caused 14x data duplication
**Problem:** After a laptop restart, `docker compose ps` showed Watchdog's
container uptime resetting every ~15-20 seconds while Postgres and Pulse
stayed stable for a full minute. RestartCount was 10.
**Root cause:** docker-compose.yml gave every service, including
Watchdog, `restart: unless-stopped`. Watchdog is architected as a batch
job — it processes files once and exits by design (this was true from
Phase 1 onward). Under `unless-stopped`, Docker interpreted every clean
exit as a crash to recover from, restarting it in an infinite loop. Each
cycle re-ingested the same 500,007-line file with no deduplication on
log_events (unlike log_alerts, which has a unique constraint from Phase
3), inflating the table to 7.28 million rows — about 14.6x the correct
count — before the loop was caught and stopped.
**Fix:** Changed Watchdog's restart policy to "no", matching its actual
execution model (run-once, on-demand or scheduled — not a daemon).
Truncated the corrupted tables and re-ran once to confirm a clean
500,007-row result.
**Lesson:** A restart policy is a claim about a service's execution
model, and it needs to match that model exactly — not just be copied
from a neighboring service in the same compose file. This bug was
invisible in every previous manual test (`docker run watchdog-bin`
standalone, or foreground `docker compose up watchdog`) because those
never exercised the `unless-stopped` policy under detached (`-d`) mode
long enough to observe a restart cycle. It only surfaced by accident,
via an unrelated laptop restart forcing Docker to re-evaluate container
state — a reminder that policy-level config (restart, resource limits,
healthchecks) needs deliberate testing under realistic conditions
(detached, left running), not just a one-off foreground run.

## 2026-07-23 Recurring pattern: files built and tested but never committed
**Problem:** Third occurrence in this project of files existing on disk,
working correctly, even tested — but never actually added to git. This
time: both Dockerfiles, both .dockerignore files, and the entire
mockserver load-testing tool, all from Step 14, sat uncommitted through
an entire subsequent session (Grafana setup) before being caught via a
routine `git status` check.
**Root cause:** Creating a file and testing that it works is not the
same action as committing it, and nothing in the normal workflow forces
a check between them. Each individual step "worked" in isolation —
Dockerfiles built successfully, containers ran — creating false
confidence that the work was fully captured, when only the runtime
artifacts were verified, not the version-control state.
**Fix:** Same as before — `git status` before considering any step
complete, not just after commands that are expected to change tracked
files.
**Lesson:** This is the third time this exact gap has occurred (see the
"previously-untracked schema and challenges log" entry earlier). A
one-off mistake is a mistake; three occurrences is a process gap. The
actual fix going forward: treat `git status` showing untracked files as
equally important to a build succeeding — a Dockerfile that builds a
working image but was never committed provides zero value to anyone
who clones the repo, which is the entire point of version control.

## 2026-07-24 CPU credit exhaustion stalled the EC2 instance mid-deploy
**Problem:** Running `docker compose up -d --build` directly on the
t3.micro instance appeared to hang indefinitely — the build produced no
output, and shortly after, even a brand-new SSH connection attempt
stalled before completing the handshake.
**Root cause:** t3.micro is a burstable instance type: it earns CPU
credits at a fixed baseline rate and can spend them faster during bursts,
but once the credit balance hits zero, the instance is throttled down to
its baseline (~10% of a vCPU). The cumulative CPU cost of `apt upgrade`,
installing Docker, and compiling two Go binaries back-to-back exhausted
the credit pool entirely — confirmed via CloudWatch's CPUCreditBalance
metric showing 0.0. At that throttle level, even SSH's handshake could
take minutes to complete, which looked identical to a genuinely hung
process from the outside.
**Fix:** Rebooted the instance (preserves the public IP, unlike stop/
start) to get a clean, responsive state, then abandoned the
build-on-target approach entirely. Built both Go binaries into Docker
images on WSL (unthrottled, unlimited CPU), exported them with
`docker save | gzip`, transferred via `scp`, and loaded them on the
instance with `docker load` — no compilation ever happens on the
constrained target again.
**Lesson:** Never build/compile on the actual deployment target when
that target is a burstable, resource-constrained instance — build on
capable hardware and ship the artifact. This is standard CI/CD practice
for exactly this reason, not just a workaround for a slow instance:
compiling is a bursty, unpredictable CPU cost, and a burstable instance's
whole selling point (cheap baseline performance) is incompatible with
unpredictable heavy compute. The distinction between "the instance is
broken" and "the instance is correctly throttled per its pricing model"
matters — CloudWatch's CPUCreditBalance metric is the direct way to
tell them apart rather than guessing from symptoms alone.
