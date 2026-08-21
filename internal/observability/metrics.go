package observability

import (
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	mu         sync.Mutex
	counters   map[string]uint64
	histograms map[string][]time.Duration
	gauges     map[string]float64
}

func NewMetrics() *Metrics {
	return &Metrics{
		counters:   map[string]uint64{},
		histograms: map[string][]time.Duration{},
		gauges:     map[string]float64{},
	}
}

func (m *Metrics) Inc(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[name]++
}

func (m *Metrics) Observe(name string, value time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.histograms[name] = append(m.histograms[name], value)
}

func (m *Metrics) Gauge(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = value
}

func (m *Metrics) Counter(name string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *Metrics) Percentile(name string, percentile float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := append([]time.Duration(nil), m.histograms[name]...)
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if percentile <= 0 {
		return values[0]
	}
	if percentile >= 100 {
		return values[len(values)-1]
	}
	idx := int((percentile / 100) * float64(len(values)-1))
	return values[idx]
}

func (m *Metrics) GaugeValue(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[name]
}
