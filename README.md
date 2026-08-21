# Distributed Job Scheduler

Distributed job scheduler with a from-scratch Raft implementation, deterministic
simulation testing, and fencing-token based mutual exclusion.

This repository is intentionally scoped as a systems portfolio project. It
prioritizes precise, testable terminology over marketing claims. Until a
guarantee is covered by tests and documented in the relevant phase, it is
treated as planned work rather than completed behavior.

## Current Status

The repository contains an autonomous deterministic implementation slice:

- Raft core: autonomous election timeouts, `RequestVote`, `AppendEntries`,
  heartbeats, `nextIndex`/`matchIndex`, majority commit for current-term
  entries, conflicting suffix repair, crash/restart over persistent state, and
  snapshot installation through the normal replication flow.
- WAL: append-only JSONL persistence with fsync on every safety-critical write.
- Raft-replicated scheduler state machine: submit, claim, start, complete,
  fail, retry, lease expiry, DLQ, and idempotent completion are serialized as
  log commands and applied only after commit.
- Storage Gateway: external resource writes guarded by fencing tokens.
- Deterministic simulation primitives: virtual clock, virtual network, and
  failure injector.
- Verification: Raft invariants, job invariants, invariant-based history
  verifier, deterministic chaos seed tests, and benchmarks.

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

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/verify -run TestChaosSeed -count=1 -args -seed=48213
```

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go run ./cmd/schedulerctl raft-demo
go run ./cmd/schedulerctl snapshot-demo
go run ./cmd/schedulerctl fencing-demo
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

## Verified Scope

Implemented and verified:

- Automatic leader election and failover under deterministic simulation.
- Majority-only commit behavior.
- Lagging follower catch-up with backtracking.
- Snapshot plus remaining log catch-up.
- Raft-replicated scheduler state convergence for equal `lastApplied`.
- Scheduler terminal-state and idempotency behavior.
- Fencing-token enforcement at the Storage Gateway.
- WAL replay for term, vote, log entries, and snapshots.
- 1,000 deterministic randomized seeds in normal tests.

Implemented but limited:

- Observability is an in-process metrics package plus Prometheus/Grafana
  scaffolding, not a full production telemetry deployment.
- Benchmarks are in-process deterministic benchmarks.

Planned / stretch:

- Real HTTP/gRPC/TCP multi-process cluster runtime.
- Full linearizability checker for a narrow lease/lock layer.
- Storage corruption fault injection.
- Dynamic Raft membership changes.

Explicitly out of scope:

- Exactly-once job execution.
- Byzantine fault tolerance.
- Production-ready deployment claims.

