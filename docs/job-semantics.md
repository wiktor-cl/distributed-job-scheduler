# Job Semantics

This project separates job semantics into three layers.

## Delivery Layer

The target delivery guarantee is at-least-once. A worker may receive the same
job more than once after an ambiguous failure, such as a crash after performing
work but before the scheduler observes completion.

## State-Machine Layer

The target state-machine guarantee is idempotent and serialized transition
application. For example, a second `complete(job_id)` command must not produce a
second logical completion effect when the job is already completed.

This is not an exactly-once execution claim.

## Handler Layer

Handler idempotency is a contract outside the scheduler. The scheduler can pass
an idempotency token to the handler. Whether an external side effect is
idempotent depends on the handler and the external system.

## Fencing

Leases carry monotonically increasing fencing tokens. Writes to external
resources must pass through the Storage Gateway:

```text
Scheduler -> Storage Gateway -> External Resource
                  ^
          validates fencing token
```

The gateway rejects writes whose token is lower than the highest accepted token
for that resource.

## Local Commands

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./internal/scheduler ./internal/gateway
```

## Verified Tests

- `TestWorkerCrashBeforeCompletionRedeliversAfterLeaseExpiry`
- `TestCompletionRetryIsIdempotentAfterAckLoss`
- `TestRetriesMoveToDeadLetterQueue`
- `TestFencingTokensAreStrictlyMonotonic`
- `TestRejectsStaleFencingToken`
