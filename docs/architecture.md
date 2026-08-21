# Architecture

The implemented system has four major layers:

```text
clients/workers
    |
scheduler API
    |
autonomous Raft node core
    |
Storage Gateway -> external resource
```

Raft nodes are event-driven. They receive ticks and RPC messages, but elections,
heartbeats, replication retry, `nextIndex`, `matchIndex`, and commit advancement
are owned by `raft.Node`.

Transport is abstracted by `raft.Transport`:

```text
              +--------------------+
raft.Node --> | raft.Transport     |
              +---------+----------+
                        |
          +-------------+--------------+
          |                            |
   VirtualNetwork              future real transport
```

External side effects are not made safe by Raft alone. Writes to external
resources must pass through the Storage Gateway, which validates fencing tokens
and rejects stale writers.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./...
```

## Implemented Status

The current code implements this architecture as a deterministic library and
simulation harness. The production RPC boundary and worker fleet are
intentionally left out of the verified scope.

