# Consistency Model

The project implements a Raft-replicated state machine for scheduler metadata.
Scheduler transitions are serialized as Raft log commands and applied only after
the corresponding entry is committed.

The project does not claim exactly-once execution. The intended guarantee is:

- Delivery layer: at-least-once delivery.
- State-machine layer: idempotent and serialized transition application.
- Handler layer: idempotency is a handler contract, not a scheduler guarantee.

External resource writes require fencing-token validation at the Storage Gateway.
The scheduler assigns fencing tokens as part of lease state, but the gateway is
the component that rejects stale writes.

## Write Consistency Guarantees

Scheduler mutations that are part of the distributed flow are encoded as
`scheduler.Command` payloads, appended to the Raft leader's log, replicated to a
majority, committed under the current-term Raft rule, and then applied to each
node's scheduler state machine. The simulation tests cover submit, claim, start,
complete, fail, retry, lease expiry, DLQ, snapshots, crash/restart, and
failover through that path.

Direct calls to `scheduler.StateMachine.ApplyCommand` are used only by
scheduler unit tests and the local `fencing-demo`; they are not an authoritative
distributed write path.

## Read Consistency Guarantees

The repository does not currently implement a public linearizable scheduler
read API. Inspecting a node's scheduler state returns that node's local applied
state. Reading from a follower, a paused node, or a node behind the current
leader can be stale.

No ReadIndex, leader lease, or read quorum protocol is implemented yet, so the
project must not be interpreted as providing linearizable reads. The verified
read-like property in tests is narrower: nodes with the same `lastApplied`
index must have the same canonical scheduler snapshot/fingerprint.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/scheduler ./internal/gateway ./internal/verify
```

## Verified Scope

The write-path guarantees are verified at the deterministic library/simulation
layer using the same autonomous Raft node core that a future real transport
would wrap. The project does not claim exactly-once execution, linearizable
reads, full linearizability checking, or safety for direct writes that bypass
the Storage Gateway.

