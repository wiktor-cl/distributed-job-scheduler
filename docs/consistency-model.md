# Consistency Model

The project targets a Raft-replicated state machine for scheduler metadata.
State-machine transitions are serialized through committed Raft log entries.

The project does not claim exactly-once execution. The intended guarantee is:

- Delivery layer: at-least-once delivery.
- State-machine layer: idempotent and serialized transition application.
- Handler layer: idempotency is a handler contract, not a scheduler guarantee.

External resource writes require fencing-token validation at the Storage Gateway.
The scheduler assigns fencing tokens as part of lease state, but the gateway is
the component that rejects stale writes.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/scheduler ./internal/gateway ./internal/verify
```

## Verified Scope

The guarantees are verified at the deterministic library/simulation layer. The
project does not claim exactly-once execution, full linearizability checking,
or safety for direct writes that bypass the Storage Gateway.
