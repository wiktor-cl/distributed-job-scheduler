# Final Report

## 1. Critical Issues Fixed

- Scheduler mutations now flow through Raft: scheduler commands are JSON encoded,
  proposed to Raft, replicated, committed, then applied to each node's own
  scheduler state machine.
- `raft.Node` now owns a generic `StateMachine` interface; it does not import
  the scheduler package.
- Logical-time reset bugs were fixed by passing `now` into time-dependent RPC
  handlers and step-down paths.
- `Crash` no longer performs graceful cleanup or persists extra state.
- Scheduler retry jitter is deterministic: any jitter value is part of the
  replicated command, not locally generated per node.
- Snapshots now contain serialized scheduler state via `StateMachine.Snapshot`.
- `InstallSnapshot` restores scheduler state through `StateMachine.Restore`.
- The old synchronous Raft `Cluster` helper was removed.
- Chaos tests now mutate scheduler state through replicated scheduler commands.
- `CommittedEntryDurability` fails when a node's `commitIndex` covers an entry
  missing from both log and snapshot.

## 2. Replicated Scheduler Architecture

```text
client scheduler.Command
    -> scheduler.EncodeCommand(JSON)
    -> raft.Node.Propose
    -> AppendEntries replication
    -> current-term entry reaches majority
    -> leader commitIndex advances
    -> committed entry applied
    -> scheduler.StateMachine.Apply(command bytes)
    -> replicated scheduler state
```

Each simulated Raft node owns its own scheduler state machine. Equal
`lastApplied` indexes are checked for equal scheduler snapshots.

## 3. Failure Semantics

- `Crash(node)`: stops ticks and message processing immediately; no step-down or
  extra persistence is performed.
- `GracefulStop(node)`: intentionally steps down before stopping.
- `Pause(node)`: stops execution without reconstructing the node.
- `Restart(node)`: rebuilds volatile Raft state from persisted term, vote, log,
  and snapshot; scheduler state is restored from snapshot and then catches up
  through committed log entries.

## 4. Invariants

- Election Safety.
- Log Matching.
- Leader Completeness.
- Committed Entry Durability.
- State Machine Safety.
- Replicated Scheduler State Equality.
- Scheduler terminal-state invariants.
- Monotonic fencing tokens.
- Stale fencing token rejection at the Storage Gateway.

## 5. Determinism

The replicated scheduler state machine does not read wall-clock time or generate
local random jitter. Time (`Now`) and jitter/backoff values are command fields,
therefore every node applies the same deterministic input from the log.

## 6. Snapshot Semantics

Raft compaction calls `StateMachine.Snapshot()` at the compacted index.
`InstallSnapshot` persists the Raft snapshot, restores scheduler state from the
snapshot bytes, sets `commitIndex`/`lastApplied`, and resumes normal log
replication after the snapshot boundary.

## 7. Tests

- `TestSchedulerStateReplicatesThroughCommittedRaftLog`
- `TestFencingTokenMonotonicAcrossFailoverAndRestart`
- `TestSnapshotIsInstalledThroughReplicationFlow`
- `TestElectionDeadlineResetsAgainstLogicalNow`
- `TestCommittedEntryDurabilityRejectsMissingCommittedEntry`
- `TestRandomizedDeterministicSeeds`
- Existing Raft election, conflict repair, WAL replay, gateway, scheduler, and
  deterministic simulation tests.

## 8. Known Limitations

- No production HTTP/gRPC/TCP runtime yet.
- No full Jepsen/Knossos linearizability checker.
- No Byzantine fault tolerance.
- No storage corruption or torn-write model.
- No dynamic membership changes.
- No exactly-once execution guarantee.

## 9. Commands Executed

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=1000
go test ./benchmarks -bench . -benchmem
go run ./cmd/schedulerctl raft-demo
go run ./cmd/schedulerctl snapshot-demo
go run ./cmd/schedulerctl fencing-demo
```

## 10. Senior-Review Checklist

HIGH severity findings: none known after this pass.

MEDIUM severity findings:

- Real multi-process transport remains unimplemented.
- Crash atomicity is limited to the current WAL/snapshot abstraction; storage
  corruption is out of scope.
- Race-mode randomized testing defaults to fewer seeds so full `go test -race
  ./...` remains practical; normal tests and CI deterministic seed jobs run
  1,000+ seeds.

LOW severity findings:

- Benchmark coverage is still focused on proposal throughput.
- Demo commands are deterministic simulation demos, not production processes.
