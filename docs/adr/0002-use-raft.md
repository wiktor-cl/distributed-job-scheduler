# ADR 0002: Use Raft

## Context

The scheduler needs replicated metadata with a leader, durable log, and clear
safety properties that can be tested under crashes and partitions.

## Decision

Implement Raft from scratch for the scheduler's replicated state machine.

## Consequences

The project can demonstrate election safety, log matching, leader completeness,
and state-machine safety directly. The implementation cost is higher than using
a database primitive, but the result is better aligned with the project's
distributed systems goals.

## Alternatives Considered

- PostgreSQL advisory locks: useful operational primitive, but it would not
  demonstrate consensus implementation.
- etcd or HashiCorp Raft: production-grade options, but they hide the core
  mechanics this project is meant to expose.


