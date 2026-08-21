# Chaos Testing

Deterministic simulation testing is the main verification strategy for this
project.

## Planned Components

- `VirtualClock`: simulation-controlled time. Simulation tests must not depend
  on `time.Sleep` or wall-clock time.
- `VirtualNetwork`: deterministic message delay, drop, reorder, duplication,
  and partition behavior.
- `FailureInjector`: deterministic crash, pause, restart, and partition events.
- Invariant-based history verifier: checks recorded histories against Raft and
  scheduler invariants.

The must-have verifier is an invariant-based history verifier. It must not be
called a linearizability checker unless a real linearizability check is
implemented for a specific narrow layer.

## Seed Reproduction

The target command shape for reproducing failures is:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./... -run TestChaos -count=1 -args -seed=48213
```

## Bug Catalogue

This section will be filled during Phase 3 with at least three real bugs found
and fixed by deterministic simulation:

| Seed | Sequence | Broken invariant | Root cause | Fix |
| ---- | -------- | ---------------- | ---------- | --- |
| TBD  | TBD      | TBD              | TBD        | TBD |

## Current Limitation

Phase 0 contains only the documentation shell. The deterministic simulator and
verifier are planned for later phases.

