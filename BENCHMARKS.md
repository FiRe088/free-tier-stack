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
