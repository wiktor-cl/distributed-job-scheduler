# Distributed Job Scheduler

Distributed job scheduler with a from-scratch Raft implementation, deterministic
simulation testing, and fencing-token based mutual exclusion.

This repository is intentionally scoped as a systems portfolio project. It
prioritizes precise, testable terminology over marketing claims. Until a
guarantee is covered by tests and documented in the relevant phase, it is
treated as planned work rather than completed behavior.

## Current Phase

Phase 0 scaffold is in progress/completed:

- Go module and directory layout
- CI skeleton for build, vet, unit tests, deterministic simulation smoke tests,
  and nightly chaos test configuration
- Documentation structure and initial ADRs

Phase 1 Raft implementation has not started in this scaffold commit.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go build ./...
```

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go vet ./...
```

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./...
```

## Project Layout

```text
cmd/                    entrypoints
internal/raft           Raft implementation
internal/wal            write-ahead log
internal/scheduler      scheduler state machine
internal/sim            deterministic simulation framework
internal/verify         invariant and history verification
internal/gateway        storage gateway validating fencing tokens
internal/observability  metrics and tracing
docs/                   architecture, consistency, failures, ADRs
deploy/                 local observability stack
benchmarks/             benchmark methodology and results
```

## Guarantees Targeted By The Project

- At-least-once delivery of jobs to workers.
- Idempotent and serialized application of state-machine transitions.
- Mutual exclusion for external resource writes through fencing tokens validated
  by the Storage Gateway.
- Crash, partition, delay, reorder, duplicate-message, and pause coverage through
  deterministic simulation testing.

These are project goals until the matching code, tests, and documentation are
implemented in later phases.

