package verify

import (
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
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

func TestCommittedEntryDurabilityRejectsDifferentEntryAtCommittedIndex(t *testing.T) {
	node := raft.NewNode("n1", []raft.NodeID{"n1"}, raft.NewMemoryStorage(raft.PersistentState{
		Entries: []raft.Entry{{Index: 1, Term: 2, Command: []byte("different")}},
	}))
	node.CommitThrough(1)
	err := CommittedEntryDurability(map[raft.NodeID]*raft.Node{
		"n1": node,
	}, map[uint64]raft.Entry{
		1: {Index: 1, Term: 1, Command: []byte("committed")},
	})
	if err == nil {
		t.Fatal("expected committed entry durability violation")
	}
}
