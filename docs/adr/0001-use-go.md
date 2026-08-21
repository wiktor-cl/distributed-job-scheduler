# ADR 0001: Use Go

## Context

The project needs concurrent nodes, deterministic tests, and a codebase that is
clear enough to discuss in backend and distributed systems interviews.

## Decision

Use Go for the implementation.

## Consequences

Goroutines and channels map naturally to actor-like nodes in simulation. Go's
standard test tooling is sufficient for deterministic and property-style tests.
The language is also common in backend infrastructure work.

## Alternatives Considered

- Java: strong ecosystem, but heavier scaffolding for this portfolio scope.
- Python: fast to prototype, but weaker static guarantees for this systems code.
- Rust: excellent safety, but higher implementation cost for the intended pace.


