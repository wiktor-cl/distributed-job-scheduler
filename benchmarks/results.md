# Benchmark Results

Benchmarks are planned for Phase 4.

## Planned Measurements

- Throughput in jobs/second for 3, 5, and 7 node clusters.
- p50, p95, and p99 job latency under normal operation.
- p50, p95, and p99 job latency under active deterministic chaos.
- Leader failover time from leader crash to accepting new writes.
- Backlog catch-up behavior after a long partition heals.

## Reproduction

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./benchmarks/...
```

## Current Limitation

No benchmark implementation exists in Phase 0.

