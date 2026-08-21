# Distributed Job Scheduler

Distributed job scheduler with a from-scratch Raft implementation, deterministic
simulation testing, and fencing-token based mutual exclusion.

This repository is intentionally scoped as a systems portfolio project. It
prioritizes precise, testable terminology over marketing claims. Until a
guarantee is covered by tests and documented in the relevant phase, it is
treated as planned work rather than completed behavior.

## Current Status

The repository contains a compact deterministic implementation slice:

- Raft core: `RequestVote`, `AppendEntries`, majority commit for current-term
  entries, conflicting suffix truncation, crash/restart over persistent state,
  and snapshot installation.
- WAL: append-only JSONL persistence with fsync on every safety-critical write.
- Scheduler state machine: submit, claim, start, complete, fail, retry, lease
  expiry, DLQ, and idempotent completion.
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
go run ./cmd/schedulerctl demo
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

The test suite covers the named invariants at the library/simulation layer. It
does not claim a production network service, a full linearizability checker,
Byzantine fault tolerance, storage corruption handling, or membership changes.
