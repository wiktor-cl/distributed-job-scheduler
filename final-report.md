# Final Report

## Implemented

- Refactored Raft into an autonomous event-driven `raft.Node`.
- Added `raft.Transport` so core Raft does not know whether messages use a real
  or simulated network.
- Added `nextIndex` and `matchIndex` per follower.
- Added automatic election timeout handling, RequestVote flow, heartbeats,
  leader step-down on higher terms, replication retry/backtracking, and
  current-term majority commit advancement.
- Integrated `InstallSnapshot` into normal replication flow.
- Reworked deterministic simulation so it runs the same Raft node code.
- Added 1,000-seed randomized deterministic test coverage in normal tests and a
  10,000-seed nightly CI command.
- Added scheduler/gateway/WAL tests for terminal states, fencing, stale token
  rejection, and crash-point replay.

## Architecture Changes

Raft is now event-driven. The simulator owns time and delivery, but consensus
decisions live in `raft.Node`. `internal/sim.Cluster` implements `raft.Transport`
with `VirtualNetwork`.

## Guarantees Verified

- Election safety for live simulated nodes.
- Majority requirement for commits.
- Stale candidate rejection.
- Step-down on higher term.
- Automatic leader failover after killing a leader.
- Log catch-up through `nextIndex` backtracking.
- Conflicting follower suffix repair.
- Snapshot installation plus remaining log replication.
- Idempotent job completion.
- DLQ/completed terminal behavior.
- Monotonic fencing tokens.
- Stale fencing token rejection at Storage Gateway.
- WAL replay for term, vote, entries, and snapshot.

## Limitations

- No production HTTP/gRPC/TCP cluster runtime is implemented yet.
- No full Jepsen or Knossos-style linearizability checker.
- No Byzantine fault tolerance.
- No storage corruption or torn-write handling.
- No dynamic membership changes.
- No exactly-once job execution claim.

## Tests

- `internal/sim`: autonomous leader election, failover, quorum loss, lagging
  follower catch-up, snapshot catch-up.
- `internal/raft`: RequestVote restrictions, conflict repair, WAL-backed
  restart compatibility, snapshot unit behavior.
- `internal/verify`: randomized deterministic scenario generation with seeds.
- `internal/scheduler`: delivery/transition semantics, retries, DLQ.
- `internal/gateway`: fencing-token enforcement.
- `internal/wal`: replay and crash-point coverage.

## Reproduction

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/verify -run TestChaosSeed -count=1 -args -seed=48213
```

For the normal deterministic seed suite:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=1000
```

For a longer soak:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=10000
```

## Benchmarks

Benchmarks are reproducible with:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./benchmarks -bench . -benchmem
```

Only locally measured results are listed in `benchmarks/results.md`.

## Interview Demo

1. Automatic leader failover:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/sim -run TestAutomaticLeaderElectionAndFailover -count=1 -v
```

2. Lagging follower catches up through snapshot:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/sim -run TestSnapshotIsInstalledThroughReplicationFlow -count=1 -v
```

3. Stale fencing token rejected after ownership change:

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/gateway -run TestOldOwnerTokenRejectedAfterOwnershipChange -count=1 -v
```

