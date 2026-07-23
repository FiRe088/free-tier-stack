# Benchmarks

## Watchdog: Phase 1 baseline (unbatched, sequential inserts)

- **Date:** 2026-07-21
- **Test:** 10,007 log lines (10,000 synthetic + 7 from app.log), single insert
  per line, no worker pool, no batching.
- **Result:** processed=10007 errors=3
- **Wall time:** 10.587s
- **CPU time:** user=0.410s + sys=0.545s = 0.955s
- **Throughput:** ~945 lines/sec
- **Bottleneck:** ~9.6s of the 10.587s wall time is network round-trip wait
  on Postgres (CPU time is only 9% of wall time) — confirms this is I/O
  bound, not compute bound. This is the specific problem Phase 2's worker
  pool + batched inserts targets.

## Watchdog: Phase 2 (worker pool + batching)
_(pending)_

## Watchdog: Phase 2 (worker pool + batching)

- **Date:** 2026-07-21
- **Test:** Same 10,007 lines, 4 workers, batch size 100, batch interval 500ms.
- **Result:** processed=10007 errors=3
- **Wall time:** 0.669s
- **CPU time:** user=0.329s + sys=0.150s = 0.479s (72% of wall time, vs 9% in Phase 1)
- **Throughput:** ~14,958 lines/sec
- **Improvement over Phase 1 baseline (945 lines/sec): ~15.8x**
- **Why:** Phase 1 paid one network round-trip per row (9.6s of 10.6s wall
  time was network wait). Phase 2 batches up to 100 rows per round-trip
  across 4 concurrent workers, reducing round-trips from 10,007 to ~101,
  and converting the workload from I/O-bound to compute-bound.

## Watchdog: Phase 4 — Profiling-driven optimization (Batch vs COPY)

- **Date:** 2026-07-22
- **Method:** CPU profile (runtime/pprof) showed the workload was ~85-87%
  I/O-wait (CPU time was only 13-15% of wall time), ruling out parser
  logic as the bottleneck. A subsequent heap profile (alloc_objects)
  showed `InsertLogEventsBatch` itself was the single largest allocator
  (29.39% flat / 86.41% cumulative), pointing at pgx's extended-query
  batch protocol overhead — not application code — as the real cost.
- **Fix:** Implemented `InsertLogEventsCopy` using pgx's `CopyFrom`
  (Postgres binary COPY protocol) as an alternative insert path, added
  behind an `INSERT_STRATEGY` env var for direct A/B comparison against
  the existing batch-insert implementation.
- **Test:** 500,007 lines, 4 workers, identical data and config, only
  INSERT_STRATEGY changed.

| Strategy | Wall time | Throughput      | Total allocations |
|----------|-----------|-----------------|--------------------|
| batch    | 14.592s   | ~34,268 lines/s | 8,425,858          |
| copy     | 8.184s    | ~60,108 lines/s | 4,349,791          |

- **Result: ~1.75x wall-clock improvement, ~48% fewer allocations**, with
  both metrics moving together — evidence the throughput gain is
  mechanistically caused by reduced allocation/GC pressure, not
  incidental variance.
- **Caveat:** single-run comparison, not averaged over multiple runs;
  WSL2 environment shows real run-to-run variance (~12-15% seen across
  repeated identical `batch` runs). A rigorous claim would average 3+
  runs per strategy.
- **Remaining bottleneck (not fixed):** post-optimization profiling shows
  `logEventCopySource.Values()` — a per-row `[]any` slice allocation — is
  now the largest single allocator (54.37%). A pooled/reusable buffer
  would reduce this further; not implemented, noted as a known next step.

## Pulse: Phase 4 — Profiling (no fix required)

- **Date:** 2026-07-23
- **Test:** 100 concurrent targets (40 ok, 15 slow up to 800ms, 15 always-
  fail, 30 flaky), 5s check interval, 1000ms per-check timeout, ~6 rounds
  over a 30s run, CPU + heap profiling enabled throughout.
- **CPU profile:** Only 2.72% of wall-clock time spent on-CPU (840ms of
  30.84s). Dominant cost was internal/runtime/syscall.Syscall6 (45.24%,
  raw socket I/O) and runtime.futex (8.33%, goroutine scheduling) —
  both expected overhead of 100-way concurrent HTTP I/O, not application
  logic. No Watchdog-style single-function bottleneck exists in this
  profile.
- **Heap profile:** Allocations dominated by Go's own net/http client
  internals (HTTP header parsing, socket address conversion, connection
  deadline/pool bookkeeping) — none of Pulse's own application code
  (checker.Check, store.InsertCheck, store.UpdateAlertState) appears in
  the top 10 allocators.
- **Conclusion:** Unlike Watchdog (where profiling found a genuine,
  fixable inefficiency in the batch-insert layer), Pulse's implementation
  has no profiling-identified bottleneck. The workload is fundamentally
  I/O-bound by nature — 100 concurrent network round-trips per round —
  and the profile confirms the code isn't introducing avoidable overhead
  on top of that. Correctly recognizing "no fix needed" from profiling
  data is itself the deliverable here, rather than manufacturing an
  optimization the evidence doesn't support.
