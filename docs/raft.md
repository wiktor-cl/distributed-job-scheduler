# Raft

This project will implement Raft according to "In Search of an Understandable
Consensus Algorithm" by Ongaro and Ousterhout.

## Implemented Scope

- Leader election through deterministic election requests in the cluster model.
- `RequestVote` with the log up-to-date restriction from section 5.4.1.
- `AppendEntries` log replication and conflict handling.
- Commit advancement following section 5.4.2: a leader commits an entry when it
  is stored on a majority of servers and the entry is from the leader's current
  term. Entries from previous terms are committed indirectly after a current-term
  entry is committed.
- Persistent `currentTerm`, `votedFor`, and log entries before responding to
  RPCs whose safety depends on those writes.
- Snapshot installation and log compaction in the deterministic model, following
  section 7.

## Required Invariants

- Election Safety: at most one leader per term.
- Leader Append-Only: a leader never overwrites or deletes entries in its own log.
- Log Matching: if two logs contain an entry with the same index and term, all
  preceding entries are identical.
- Leader Completeness: if an entry is committed in a term, it is present in the
  logs of all future leaders.
- State Machine Safety: if a server has applied a log entry at a given index,
  no other server applies a different command at that index.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/raft ./internal/verify
```

## Current Limitation

The implementation is a deterministic library core, not a production RPC
daemon. Election timeouts are represented by tests/simulation rather than a
long-running network process.
