# Failure Model

## In Scope

- Crash-recovery faults.
- Node pause and restart.
- Network partitions, including asymmetric partitions.
- Message loss, delay, reordering, and duplication.
- Clock drift in deterministic simulation.

## Out Of Scope For Must-Have

- Byzantine behavior.
- Storage corruption and torn writes.
- Membership changes.

Storage corruption and membership changes are stretch goals. They must not be
documented as verified behavior until implemented and tested.

## Verified Scope

Crash/restart, partitions, message delay/drop/reorder/duplication primitives,
and pause/kill controls exist in the deterministic harness. The current
must-have tests exercise a compact subset of those faults through seed-driven
chaos tests.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./...
```
