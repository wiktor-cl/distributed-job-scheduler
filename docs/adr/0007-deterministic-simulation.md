# ADR 0007: Deterministic Simulation

## Context

Sleep-based distributed tests are slow and flaky. The project needs reproducible
failures with seeds.

## Decision

Use `VirtualClock`, `VirtualNetwork`, and a simulation cluster that delivers
ticks and RPCs to the same `raft.Node` implementation used by other runtimes.

## Consequences

`same seed -> same event order -> same result`. Failing randomized tests report
the seed and step so a scenario can be replayed with `go test`.

## Alternatives Considered

- Wall-clock integration tests: realistic timing, but poor reproducibility.
- Separate model checker implementation: useful for proofs, but risks diverging
  from production code.

