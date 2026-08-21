package sim

import (
	"container/heap"
	"fmt"
)

type Clock interface {
	Now() int64
	Schedule(delay int64, name string, fn func()) uint64
	RunUntilIdle(limit int) error
}

type event struct {
	id   uint64
	at   int64
	name string
	fn   func()
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].id < h[j].id
	}
	return h[i].at < h[j].at
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

type VirtualClock struct {
	now    int64
	nextID uint64
	queue  eventHeap
	trace  []string
}

func NewVirtualClock() *VirtualClock {
	clock := &VirtualClock{}
	heap.Init(&clock.queue)
	return clock
}

func (c *VirtualClock) Now() int64 {
	return c.now
}

func (c *VirtualClock) Schedule(delay int64, name string, fn func()) uint64 {
	if delay < 0 {
		delay = 0
	}
	c.nextID++
	heap.Push(&c.queue, event{id: c.nextID, at: c.now + delay, name: name, fn: fn})
	return c.nextID
}

func (c *VirtualClock) RunUntilIdle(limit int) error {
	steps := 0
	for c.queue.Len() > 0 {
		if limit > 0 && steps >= limit {
			return fmt.Errorf("virtual clock step limit reached after %d events", steps)
		}
		item := heap.Pop(&c.queue).(event)
		c.now = item.at
		c.trace = append(c.trace, item.name)
		item.fn()
		steps++
	}
	return nil
}

func (c *VirtualClock) Trace() []string {
	return append([]string(nil), c.trace...)
}
