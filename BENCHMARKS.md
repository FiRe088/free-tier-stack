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
