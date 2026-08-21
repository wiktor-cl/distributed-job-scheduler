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

func TestCommittedEntryDurabilityRejectsMissingCommittedEntry(t *testing.T) {
	node := raft.NewNode("n1", []raft.NodeID{"n1"}, raft.NewMemoryStorage(raft.PersistentState{
		Snapshot: raft.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1},
		Entries:  []raft.Entry{{Index: 3, Term: 1, Command: []byte("three")}},
	}))
	node.CommitThrough(3)
	err := CommittedEntryDurability(map[raft.NodeID]*raft.Node{
		"n1": node,
	}, map[uint64]raft.Entry{
		2: {Index: 2, Term: 1, Command: []byte("two")},
	})
	if err == nil {
		t.Fatal("expected missing committed entry durability violation")
	}
}

func TestVerifierRejectsMissingCommittedEntry(t *testing.T) {
	node := raft.NewNode("n1", []raft.NodeID{"n1"}, raft.NewMemoryStorage(raft.PersistentState{
		Entries: []raft.Entry{
			{Index: 1, Term: 1, Command: []byte("one")},
			{Index: 3, Term: 1, Command: []byte("three")},
		},
	}))
	node.CommitThrough(3)
	err := RaftCluster(map[raft.NodeID]*raft.Node{
		"n1": node,
	}, map[uint64]raft.Entry{
		2: {Index: 2, Term: 1, Command: []byte("missing")},
	})
	if err == nil {
		t.Fatal("expected verifier to reject missing committed entry")
	}
}

func TestVerifierRejectsDivergentSchedulerStateAtEqualLastApplied(t *testing.T) {
	entry := raft.Entry{Index: 1, Term: 1, Command: []byte("same")}
	n1 := raft.NewNode("n1", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{Entries: []raft.Entry{entry}}))
	n2 := raft.NewNode("n2", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{Entries: []raft.Entry{entry}}))
	n1.CommitThrough(1)
	n2.CommitThrough(1)
	s1 := scheduler.NewStateMachine(1)
	s2 := scheduler.NewStateMachine(1)
	if _, err := s1.ApplyCommand(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "same-index-different-state"}); err != nil {
		t.Fatal(err)
	}
	err := ReplicatedSchedulerStateEquality(map[raft.NodeID]*raft.Node{
		"n1": n1,
		"n2": n2,
	}, map[raft.NodeID]*scheduler.StateMachine{
		"n1": s1,
		"n2": s2,
	})
	if err == nil {
		t.Fatal("expected divergent scheduler state violation")
	}
}

func TestLogMatchingRejectsSnapshotBoundaryTermMismatch(t *testing.T) {
	n1 := raft.NewNode("n1", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{
		Snapshot: raft.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 1},
	}))
	n2 := raft.NewNode("n2", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{
		Snapshot: raft.Snapshot{LastIncludedIndex: 3, LastIncludedTerm: 2},
	}))
	if err := LogMatching(map[raft.NodeID]*raft.Node{"n1": n1, "n2": n2}); err == nil {
		t.Fatal("expected snapshot boundary log matching violation")
	}
}

func TestLogMatchingAllowsUncommittedConflictBeyondCommitIndex(t *testing.T) {
	n1 := raft.NewNode("n1", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{
		Snapshot: raft.Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 6},
	}))
	n2 := raft.NewNode("n2", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{
		Entries: []raft.Entry{{Index: 10, Term: 4, Command: []byte("uncommitted")}},
	}))
	if err := LogMatching(map[raft.NodeID]*raft.Node{"n1": n1, "n2": n2}); err != nil {
		t.Fatalf("uncommitted conflicting suffix should be repairable, not a log matching violation: %v", err)
	}
}
