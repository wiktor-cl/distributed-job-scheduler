# ADR 0005: Event-Driven Raft Core

## Context

The initial cluster harness drove elections and replication externally. That was
useful for a first invariant model, but it was not the same execution path a
real runtime would use.

## Decision

Move election timeout handling, leader transitions, heartbeats, `nextIndex`,
`matchIndex`, replication retry, commit advancement, and snapshot sending into
`raft.Node`. The core is event-driven and deterministic: callers deliver ticks
and RPC messages, but do not decide consensus outcomes.

## Consequences

The same node implementation runs under deterministic simulation and can be
wrapped by a future real network runtime. Tests no longer need to call
`Elect("n1")` to force progress.

## Alternatives Considered

- Goroutine-per-node core: closer to a process runtime, but harder to test
  deterministically.
- Keep model-only cluster logic: simpler, but weaker as an interview artifact.

