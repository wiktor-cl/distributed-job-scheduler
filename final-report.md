# Final Report

## Result

RESULT: SENIOR-LEVEL PORTFOLIO READY

This result is limited to the current deterministic, in-process architecture.
There are no known remaining HIGH severity Raft/scheduler safety,
determinism, snapshot, or crash/restart findings after the adversarial review
pass. The project still does not claim production networking, linearizable
reads, exactly-once job execution, dynamic membership, Byzantine tolerance, or
storage corruption recovery.

## Current Architecture

```text
client scheduler.Command
    -> scheduler.EncodeCommand(JSON)
    -> raft.Node.Propose
    -> AppendEntries replication
    -> majority replication
    -> current-term commit rule
    -> apply committed entry
    -> scheduler.StateMachine.Apply(command bytes)
    -> replicated scheduler state
```

Every simulated Raft node owns an independent scheduler state machine. Normal
distributed mutations go through Raft commit/apply; direct scheduler calls are
kept to unit tests and the single-node fencing demo.

## Adversarial Senior Review

### HIGH

Finding: state-machine snapshot could be taken at the wrong log index.
Severity: HIGH.
Affected files/functions: `internal/raft/node.go`, `Node.Compact`; previous
snapshot demo/test flow.
Failure scenario: a leader applied through index 100, called `Compact(80)`,
serialized scheduler state through 100, and stored it as
`LastIncludedIndex=80`. After restart or InstallSnapshot, entries 81-100 could
be replayed over a state that already contained them, advancing fencing tokens
or duplicating other non-idempotent transitions.
Root cause: the generic FSM interface can snapshot only the current state, not
historical state at an arbitrary index.
Fix: `Compact(index)` now rejects FSM compaction unless `index == lastApplied`
and `index <= commitIndex`; snapshot tests now compact exactly at the applied
boundary and append suffix entries afterward.
Regression test: `TestCompactRejectsStateMachineSnapshotAtDifferentAppliedIndex`,
`TestFollowerRestoresSchedulerFromSnapshotThenReplaysSuffix`,
`TestSnapshotIsInstalledThroughReplicationFlow`.

Finding: failed FSM apply advanced `lastApplied`.
Severity: HIGH.
Affected files/functions: `internal/raft/node.go`, `applyCommitted`.
Failure scenario: a malformed replicated command could return an FSM error
while Raft still recorded the entry as applied. A restarted or lagging node
could then skip the failed command forever.
Root cause: `lastApplied` and `applied` were updated before `fsm.Apply`.
Fix: apply now runs first; `lastApplied` and `applied` advance only after a
successful FSM application.
Regression test: `TestApplyErrorDoesNotAdvanceLastApplied`,
`TestMalformedReplicatedCommandIsRejectedBeforeMutation`.

Finding: restart leaked old recurring tick callbacks.
Severity: HIGH.
Affected files/functions: `internal/sim/raft_cluster.go`, `Restart`,
`scheduleTick`.
Failure scenario: after `Restart`, the old scheduled tick loop still referenced
the node ID and began ticking the new node in parallel with the new loop. That
made restart behavior depend on stale callbacks and could accelerate elections
or heartbeats unrealistically.
Root cause: scheduled closures were keyed only by node ID, not by node
generation.
Fix: each restart increments a per-node tick generation; stale tick callbacks
stop without rescheduling.
Regression test: `TestRestartDoesNotReuseVolatileRaftState`,
`TestSameSeedProducesIdenticalEventHistory`.

Finding: stale InstallSnapshot could roll back an already-applied FSM.
Severity: HIGH.
Affected files/functions: `internal/raft/node.go`, `handleInstallSnapshot`.
Failure scenario: a follower that had already applied beyond the incoming
snapshot index could receive an older snapshot and restore the FSM backward
while keeping `lastApplied` high.
Root cause: snapshot installation restored the FSM whenever the snapshot
boundary exceeded the previous snapshot boundary, without checking
`lastApplied`.
Fix: if the incoming snapshot is already covered by `lastApplied`, Raft may
persist/compact metadata but does not restore the FSM backward.
Regression test: covered by snapshot catch-up and full restart tests.

Finding: semantic no-op scheduler commands could poison Raft apply.
Severity: HIGH.
Affected files/functions: `internal/scheduler/scheduler.go`, `claim`, `start`,
`complete`.
Failure scenario: a committed claim/start/complete for a missing job returned
an error, causing FSM apply to stop even though the command is a deterministic
no-op from the scheduler's perspective.
Root cause: business-level rejection and malformed command rejection used the
same error channel.
Fix: missing-job claim/start/complete now return deterministic no-op results;
malformed commands such as unknown type, negative time, negative lease, or
invalid required fields still return errors before mutation.
Regression test: randomized chaos seeds with replicated scheduler commands,
`TestMalformedReplicatedCommandIsRejectedBeforeMutation`.

### MEDIUM

Finding: read consistency is local, not linearizable.
Severity: MEDIUM.
Affected files/functions: docs and public simulation helpers.
Failure scenario: reading scheduler state from a follower or lagging node can
return stale data.
Root cause: no ReadIndex, leader lease, or read quorum protocol exists.
Fix: documentation now separates write consistency from read consistency and
states that linearizable reads are not implemented.
Regression test: not applicable; this is a documented architectural boundary.

Finding: verifier had snapshot-boundary blind spots and one false positive.
Severity: MEDIUM.
Affected files/functions: `internal/verify/verify.go`.
Failure scenario: verifier previously treated snapshot coverage too broadly
for committed boundary terms, and after tightening it initially compared
compacted-away log prefixes as if they were still visible.
Root cause: Log Matching and durability checks did not model the shared
visible range above snapshot boundaries precisely enough.
Fix: verifier now checks snapshot boundary term compatibility and compares
only the common visible suffix above the max snapshot boundary.
Regression test: `TestLogMatchingRejectsSnapshotBoundaryTermMismatch`,
`TestVerifierRejectsMissingCommittedEntry`,
`TestVerifierRejectsDivergentSchedulerStateAtEqualLastApplied`.

Finding: chaos generator coverage was under-evidenced.
Severity: MEDIUM.
Affected files/functions: `internal/verify/chaos_test.go`,
`internal/sim/network.go`.
Failure scenario: passing many seeds was less meaningful without counters for
whether the suite actually reached asymmetric partition, snapshot, crash,
restart, drops, or duplicates.
Root cause: operations were randomized, but coverage was not measured.
Fix: chaos now injects controlled drop/duplicate faults, includes asymmetric
partition and snapshot operations, and has aggregate coverage counters.
Regression test: `TestChaosScenarioCoverageCounters`.

Finding: performance scales poorly as cluster size grows.
Severity: MEDIUM.
Affected files/functions: `internal/raft/node.go`, `sendAppendEntries`,
`entriesFrom`; benchmarks.
Failure scenario: `AppendEntries` sends the full remaining suffix from
`nextIndex`, and `entriesFrom` clones matching entries on every send. With 7
nodes, proposal cost rises notably because each proposal fans out more cloned
payloads and event callbacks.
Root cause: simple correctness-first replication path; no batching limit,
conflict-term optimization, or zero-copy payload management.
Fix: documented as a real bottleneck; no premature optimization added in this
review.
Regression test: `go test ./benchmarks -bench . -benchmem`.

Finding: real crash atomicity and storage corruption are not modeled.
Severity: MEDIUM.
Affected files/functions: `internal/raft.MemoryStorage`, `internal/wal`.
Failure scenario: torn writes, corrupted snapshots, or partial fsync failures
are outside the current deterministic simulator.
Root cause: the simulation uses in-memory storage and the WAL tests cover
replay behavior, not arbitrary corruption.
Fix: documented limitation.
Regression test: WAL replay tests remain in scope; corruption injection is
future work.

### LOW

Finding: verifier cannot prove the exact command history inside older snapshot
state.
Severity: LOW.
Affected files/functions: `internal/verify/verify.go`.
Failure scenario: for committed entries strictly below a snapshot boundary, the
verifier can check snapshot boundary term and scheduler fingerprint equality,
but it cannot reconstruct every historical command from opaque snapshot bytes.
Root cause: snapshots intentionally store compacted state, not full log
history.
Fix: documented limitation; boundary and live suffix checks were strengthened.
Regression test: snapshot boundary and scheduler fingerprint tests.

Finding: demo commands are deterministic simulation demos.
Severity: LOW.
Affected files/functions: `cmd/schedulerctl`.
Failure scenario: a reader could mistake demo output for a production
multi-process cluster.
Root cause: CLI demos run the in-process simulator.
Fix: README/final report clarify current scope.
Regression test: demo commands are run in final validation.

## Regression Test Additions

- `TestOldLeaderCannotCommitInMinorityPartition`
- `TestCommittedEntrySurvivesLeaderCrashBeforeCommitBroadcast`
- `TestUncommittedEntryIsOverwrittenAfterLeaderCrash`
- `TestFollowerRestoresSchedulerFromSnapshotThenReplaysSuffix`
- `TestCrashDoesNotPerformPersistenceSideEffects`
- `TestRestartDoesNotReuseVolatileRaftState`
- `TestSchedulerStateConvergesAfterMultipleLeaderChanges`
- `TestFencingTokenRemainsMonotonicAfterSnapshotAndFullRestart`
- `TestSameSeedProducesIdenticalEventHistory`
- `TestVerifierRejectsMissingCommittedEntry`
- `TestVerifierRejectsDivergentSchedulerStateAtEqualLastApplied`
- `TestFullClusterRestartAfterSnapshotContinuesScheduling`
- `TestChaosScenarioCoverageCounters`
- `TestMalformedReplicatedCommandIsRejectedBeforeMutation`
- `TestSnapshotRestoreRejectsLostFencingTokenMetadata`

## Write And Read Guarantees

Write path: scheduler mutations in the distributed flow are serialized through
Raft and applied only after commit. The tested write path covers submit, claim,
start, complete, fail, retry, lease expiry, DLQ, fencing token advancement,
snapshot catch-up, crash/restart, and full-cluster restart.

Read path: there is no public linearizable read API. Local scheduler state can
be stale if inspected on a follower or lagging node. The verified read-like
property is that nodes at the same `lastApplied` index have equal canonical
scheduler fingerprints.

## Resource And Performance Review

- `AppendEntries` sends the complete suffix from `nextIndex`; this is simple
  and correct for the simulator, but can produce large payloads for lagging
  followers.
- `entriesFrom()` clones entries per peer send, increasing allocations with
  cluster size.
- Snapshot state is copied when saved and transmitted; acceptable for small
  tests, not tuned for large production snapshots.
- Verifier checks are small-cluster oriented. They are acceptable for 3-7 node
  deterministic tests but are not intended as an unbounded production checker.

## CI

The GitHub workflow runs:

- `gofmt` check.
- `go build ./...`.
- `go vet ./...`.
- `go test ./...`.
- explicit `go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=1000`.
- `go test -race ./...`.
- nightly 10,000 deterministic seeds.

Failing chaos seeds are reproducible with:

```powershell
go test ./internal/verify -run TestChaosSeed -count=1 -args -seed=<seed>
```

## Final Validation Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
gofmt -w .
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=1000
go test ./internal/verify -run TestRandomizedDeterministicSeeds -count=1 -args -seeds=10000
go test ./benchmarks -bench . -benchmem
go run ./cmd/schedulerctl raft-demo
go run ./cmd/schedulerctl snapshot-demo
go run ./cmd/schedulerctl fencing-demo
```
