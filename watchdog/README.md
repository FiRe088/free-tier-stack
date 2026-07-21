# Free-Tier Stack: Watchdog + Pulse

Two backend services sharing one AWS free-tier EC2 instance:

- **Watchdog** — concurrent log tailer, parser, and anomaly alerter (Go)
- **Pulse** — concurrent HTTP uptime monitor with alerting (Go)

Both write to a single self-hosted PostgreSQL instance (not RDS, to stay
inside the AWS free tier indefinitely rather than the 12-month RDS window).

## Status
🚧 In progress — build log and setup steps below.

## Architecture
_(diagram + design notes go here once Step 2 blueprint is finalized)_

## Local Development Setup
_(step-by-step instructions go here — OS prerequisites, docker compose usage)_

## Deployment (AWS EC2 Free Tier)
_(EC2 setup, security group config, deployment steps)_

## Performance & Profiling Results
_(pprof flame graphs, before/after benchmarks — filled in during Phase 4)_
