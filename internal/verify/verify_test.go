package verify

import (
	"testing"

	"github.com/jhinr/distributed-job-scheduler/internal/scheduler"
)

func TestHistoryRejectsNonMonotonicFencingToken(t *testing.T) {
	err := History([]HistoryEvent{
		{Operation: "claim", JobID: "j1", Owner: "w1", Token: 2},
		{Operation: "expire", JobID: "j1"},
		{Operation: "claim", JobID: "j1", Owner: "w2", Token: 1},
	})
	if err == nil {
		t.Fatal("expected invariant violation")
	}
}

func TestJobInvariantsRejectTerminalOwner(t *testing.T) {
	err := JobInvariants(map[string]scheduler.Job{
		"j1": {ID: "j1", Status: scheduler.Completed, Owner: "w1"},
	})
	if err == nil {
		t.Fatal("expected invariant violation")
	}
}
