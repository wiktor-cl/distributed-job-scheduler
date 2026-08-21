# ADR 0003: Initial fsync Policy

## Context

Raft safety depends on persisting `currentTerm`, `votedFor`, and log entries
before responding to RPCs whose correctness depends on those writes. The project
must choose an explicit durability policy.

## Decision

Start with fsync on every safety-critical metadata or log write.

## Consequences

This favors crash-recovery clarity over write latency. It makes Phase 1 easier
to reason about and gives tests a stricter durability model. Later batching can
be evaluated as an optimization, but it must be documented with the exact window
of risk it introduces.

## Alternatives Considered

- Batched fsync: better throughput, but introduces a risk window where recent
  writes can be lost on crash.
- OS-buffered writes only: simpler and faster, but too weak for the project's
  stated crash-recovery goals.


