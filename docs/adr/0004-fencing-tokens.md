# ADR 0004: Fencing Tokens For External Resource Writes

## Context

Lease timeouts alone do not prevent an old owner from writing to an external
resource after it has paused or lost leadership. The scheduler cannot guarantee
mutual exclusion over resources it does not control.

## Decision

Use monotonically increasing fencing tokens on leases, and require all external
writes to pass through a Storage Gateway that validates those tokens.

## Consequences

The scheduler can assign lease ownership, but stale-write rejection is enforced
at the gateway boundary. Tests must demonstrate that a write with a lower token
than the highest accepted token for a resource is rejected.

## Alternatives Considered

- Lease timeout only: insufficient under pauses and ambiguous failure.
- Handler-side best effort checks: inconsistent unless every handler implements
  the same fencing contract correctly.

