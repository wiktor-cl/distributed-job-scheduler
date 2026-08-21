# Architecture

The target system has four major layers:

```text
clients/workers
    |
scheduler API
    |
Raft replicated state machine
    |
Storage Gateway -> external resource
```

The scheduler state is replicated through Raft. External side effects are not
made safe by Raft alone. Writes to external resources must pass through the
Storage Gateway, which validates fencing tokens and rejects stale writers.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./...
```

## Implemented Status

The current code implements this architecture as a deterministic library and
test harness. The production RPC boundary and worker fleet are intentionally
left out of the verified scope.
