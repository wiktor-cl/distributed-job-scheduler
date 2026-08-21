# ADR 0006: Transport Abstraction

## Context

Raft safety logic should not depend on whether messages cross a real socket or a
virtual deterministic network.

## Decision

Define `raft.Transport` with methods for `RequestVote`, `AppendEntries`, and
`InstallSnapshot` requests and responses. `raft.Node` emits RPCs only through
this interface.

## Consequences

`internal/sim.Cluster` implements the transport with `VirtualNetwork`. A future
HTTP/gRPC/TCP runtime can implement the same interface without changing Raft
core logic.

## Alternatives Considered

- Direct function calls between nodes: fast, but hides network behavior.
- Transport inside the simulator only: would keep Raft coupled to tests.

