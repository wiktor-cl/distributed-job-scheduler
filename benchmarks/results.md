# Benchmark Results

Benchmarks are implemented as Go benchmark tests. Results should be regenerated
on the target machine before citing numbers in a README or interview.

## Planned Measurements

- Throughput in jobs/second for 3, 5, and 7 node clusters.
- p50, p95, and p99 job latency under normal operation.
- p50, p95, and p99 job latency under active deterministic chaos.
- Leader failover time from leader crash to accepting new writes.
- Backlog catch-up behavior after a long partition heals.

## Local Run: 2026-08-21

Environment:

- OS/arch: Windows amd64
- CPU: 12th Gen Intel(R) Core(TM) i7-12650H
- Benchmark: deterministic in-process Raft proposal throughput

```text
BenchmarkRaftProposeThroughput/3_nodes-16    18780    121997 ns/op    2333 B/op    21 allocs/op
BenchmarkRaftProposeThroughput/5_nodes-16    10000    127575 ns/op    3223 B/op    35 allocs/op
BenchmarkRaftProposeThroughput/7_nodes-16    10000    184743 ns/op    4439 B/op    49 allocs/op
```

## Reproduction

```powershell
cd C:\Users\jhinr\Downloads\projekty\distributed-job-scheduler
go test ./benchmarks -bench . -benchmem
```

## Current Limitation

The benchmark currently measures deterministic in-process Raft proposal
throughput. It does not include kernel networking, TLS, disk WAL latency, or a
production worker fleet.
