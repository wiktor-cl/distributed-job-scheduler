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
- Go version: see `go version` on the test host
- Benchmark: deterministic in-process autonomous Raft proposal throughput
- Warm-up: automatic leader election plus 1,000 simulated events

```text
BenchmarkRaftProposeThroughput/3_nodes-16    10000     123957 ns/op    17373 B/op    103 allocs/op
BenchmarkRaftProposeThroughput/5_nodes-16    10000     521292 ns/op    32265 B/op    188 allocs/op
BenchmarkRaftProposeThroughput/7_nodes-16     7046    1119906 ns/op    48544 B/op    272 allocs/op
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

