# Free-Tier Stack: Watchdog + Pulse

![CI](https://github.com/FiRe088/free-tier-stack/actions/workflows/ci.yml/badge.svg)

Two backend services written from scratch in Go, running together on a single
AWS free-tier `t3.micro` instance:

- **Watchdog** is a concurrent log ingestion and anomaly-detection service. It parses
  log files with a worker pool, bulk-loads them into PostgreSQL and raises
  deduplicated alerts on error-rate spikes.
- **Pulse** is a concurrent HTTP uptime monitor. It checks each target in its own
  goroutine, records results, and tracks up/down state transitions.

I built this to learn what sits underneath the frameworks I normally use: worker
pools, backpressure, profiling, and what actually happens when a service is
killed mid-request. Every claim below is backed by a measurement in
[BENCHMARKS.md](BENCHMARKS.md) or an incident write-up in
[CHALLENGES.md](CHALLENGES.md).

## Architecture

```mermaid
flowchart LR
    L[Log files] --> W[Watchdog<br/>worker pool + COPY]
    T[HTTP targets] <--> P[Pulse<br/>goroutine per target]
    W --> DB[(PostgreSQL 16)]
    P --> DB
    DB --> G[Grafana dashboard]
    subgraph EC2 t3.micro, Docker Compose
      W
      P
      DB
      G
    end
```

- **Go 1.25** using the standard library and `pgx/v5`, with no web framework.
- **PostgreSQL 16** is self-hosted in a container instead of RDS, so the stack
  can stay inside the free tier indefinitely rather than for RDS's 12-month window.
- Each container has hard memory and CPU limits. The resource budget was set
  before any code was written.

## Results

| What | Result |
|---|---|
| Worker-pool batching vs sequential inserts | **945 → 14,958 lines/sec (15.8×)** |
| `COPY` protocol vs batched `INSERT` (500k lines) | **1.75× faster, 48% fewer allocations** |
| Graceful shutdown mid-file (SIGINT) | 14,401 rows processed = 14,401 rows in DB; zero lost or duplicated |
| Pulse under load | 100 concurrent targets, sub-second rounds against 800 ms targets |
| Live on t3.micro (Postgres + Pulse + Grafana) | 538 MB / 911 MB used; Pulse uses 5.9 MB |

The COPY optimisation came out of profiling. A CPU profile ruled out the
parser, and a heap profile then showed the batch-insert layer was the largest
allocator. Profiling Pulse turned up no fixable bottleneck, and I didn't
invent one. Full method and caveats are in [BENCHMARKS.md](BENCHMARKS.md).

## Bugs worth reading about

- **Shutdown fabricated outages.** Killing Pulse mid-round recorded about 100 healthy
  targets as failed, because each check's context inherited the shutdown signal.
  I found it by querying the database, not the logs.
- **Alert suppressed on the case that mattered most.** A target that was down from
  its very first check never alerted.
- **Restart loop caused 14× data duplication.** A copied `restart: unless-stopped`
  policy turned a run-once job into an infinite re-ingestion loop.

Root causes, fixes and lessons for each are in [CHALLENGES.md](CHALLENGES.md).

## Run it locally

```bash
cp .env.example .env              # set real passwords
docker compose up -d --build      # Postgres + Pulse
docker compose run --rm watchdog  # one ingestion run
docker compose --profile dashboard up -d grafana   # http://localhost:3000
```

Tests: `cd watchdog && go test ./...` (CI runs `go vet`, `go test -race` and
image builds against a real Postgres on every push).

## Known limitations and next steps

- Watchdog currently processes files in batch mode and exits. The next step is
  live tailing with byte offsets checkpointed in the same transaction as each
  COPY batch, which makes restarts idempotent.
- There is no retention policy yet on `log_events` and `uptime_checks`.
- Pulse records alerts in Postgres but doesn't send notifications yet.
- After the COPY change, the largest remaining allocator is the per-row `[]any`
  slice in the COPY source. A pooled buffer is the planned fix.
