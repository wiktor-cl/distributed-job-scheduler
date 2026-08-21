# ADR 0008: Snapshot And Compaction Strategy

## Context

Followers can fall behind the retained log prefix. Raft section 7 uses
`InstallSnapshot` for this case.

## Decision

Leaders send `InstallSnapshot` through normal replication when
`nextIndex[follower] <= snapshot.LastIncludedIndex`. After installation, normal
AppendEntries resumes from the next index.

## Consequences

Lagging followers can recover from compaction without a manual
`InstallSnapshots()` test hook. The deterministic tests cover snapshot plus
remaining log catch-up.

## Alternatives Considered

- Manual snapshot installation from the harness: easy, but not representative.
- No compaction: simpler, but does not answer long-running log growth concerns.

