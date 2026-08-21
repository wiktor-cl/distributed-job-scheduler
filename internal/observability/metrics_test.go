package observability

import (
	"testing"
	"time"
)

func TestMetricsCountersGaugesAndPercentiles(t *testing.T) {
	metrics := NewMetrics()
	metrics.Inc("leader_changes_total")
	metrics.Inc("leader_changes_total")
	metrics.Gauge("queue_depth", 7)
	metrics.Observe("job_latency", 10*time.Millisecond)
	metrics.Observe("job_latency", 20*time.Millisecond)
	metrics.Observe("job_latency", 30*time.Millisecond)

	if metrics.Counter("leader_changes_total") != 2 {
		t.Fatal("counter mismatch")
	}
	if metrics.GaugeValue("queue_depth") != 7 {
		t.Fatal("gauge mismatch")
	}
	if metrics.Percentile("job_latency", 95) != 20*time.Millisecond {
		t.Fatalf("p95 = %s", metrics.Percentile("job_latency", 95))
	}
}
