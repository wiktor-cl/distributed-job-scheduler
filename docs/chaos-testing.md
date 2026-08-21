# Chaos Testing

Deterministic simulation testing is the main verification strategy for this
project.

## Implemented Components

- `VirtualClock`: simulation-controlled time. Simulation tests must not depend
  on `time.Sleep` or wall-clock time.
- `VirtualNetwork`: deterministic message delay, drop, reorder, duplication,
  and partition behavior.
- `FailureInjector`: deterministic crash, pause, restart, and partition events.
- `sim.Cluster`: runs the same autonomous `raft.Node` core through the
  `raft.Transport` interface.
- Replicated scheduler commands: chaos operations that mutate jobs are proposed
  through Raft and applied only after commit.
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

Current single-seed command:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/verify -run TestChaosSeed -count=1 -args -seed=48213
```

## Bug Catalogue

| Seed/test | Sequence | Broken invariant | Root cause | Fix |
| --------- | -------- | ---------------- | ---------- | --- |
| `TestFollowerTruncatesConflictingSuffix` | leader sends replacement entry after same prefix | Log Matching | follower handled term conflict but initially missed the missing-suffix append path | `AppendEntries` now appends `Entries[i:]` after either conflict truncation or first missing entry |
| `TestWorkerCrashBeforeCompletionRedeliversAfterLeaseExpiry` | claim, start, lease expiry, second claim | Job liveness under at-least-once delivery | expired leases need owner release before redelivery | `ExpireLeasesCommand` clears owner/deadline and next claim issues a higher fencing token |
| `TestRejectsStaleFencingToken` | token 7 write followed by token 6 write | Gateway fencing | stale external writers cannot be blocked by scheduler state alone | gateway tracks highest accepted token per resource and rejects lower tokens |

## Current Limitation

The chaos suite is invariant-based. It is not a full Knossos-style
linearizability checker.

