package raft_test

import (
	"bytes"
	"testing"

	"github.com/jhinr/distributed-job-scheduler/internal/raft"
	"github.com/jhinr/distributed-job-scheduler/internal/verify"
)

func TestFiveNodeClusterElectsSingleLeader(t *testing.T) {
	cluster := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"})
	ok, err := cluster.Elect("n1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected election to win")
	}
	if err := verify.ElectionSafety(cluster.Nodes()); err != nil {
		t.Fatal(err)
	}
}

func TestRequestVoteRejectsCandidateWithStaleLog(t *testing.T) {
	voter := raft.NewNode("voter", []raft.NodeID{"voter", "candidate"}, raft.NewMemoryStorage(raft.PersistentState{
		CurrentTerm: 4,
		Entries:     []raft.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 4}},
	}))
	resp, err := voter.RequestVote(raft.RequestVoteRequest{
		Term:         5,
		CandidateID:  "candidate",
		LastLogIndex: 2,
		LastLogTerm:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.VoteGranted {
		t.Fatal("stale candidate received vote")
	}
}

func TestLogReplicationCommitsCurrentTermEntryOnMajority(t *testing.T) {
	cluster := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"})
	if ok, err := cluster.Elect("n1"); err != nil || !ok {
		t.Fatalf("election ok=%v err=%v", ok, err)
	}
	entry, committed, err := cluster.Propose([]byte("job:submit:1"))
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("entry was not committed")
	}
	if entry.Term == 0 || entry.Index != 1 {
		t.Fatalf("entry = %+v", entry)
	}
	if err := verify.RaftCluster(cluster.Nodes(), cluster.Committed()); err != nil {
		t.Fatal(err)
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	follower := raft.NewNode("f", []raft.NodeID{"l", "f"}, raft.NewMemoryStorage(raft.PersistentState{
		CurrentTerm: 2,
		Entries: []raft.Entry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 2, Command: []byte("old")},
		},
	}))
	resp, err := follower.AppendEntries(raft.AppendEntriesRequest{
		Term:         3,
		LeaderID:     "l",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []raft.Entry{{Index: 2, Term: 3, Command: []byte("new")}},
		LeaderCommit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatal("append failed")
	}
	entries := follower.Entries()
	if len(entries) != 2 || entries[1].Term != 3 || !bytes.Equal(entries[1].Command, []byte("new")) {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestCrashRestartReplaysPersistentState(t *testing.T) {
	cluster := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3"})
	if ok, err := cluster.Elect("n1"); err != nil || !ok {
		t.Fatalf("election ok=%v err=%v", ok, err)
	}
	if _, committed, err := cluster.Propose([]byte("job:submit:crash")); err != nil || !committed {
		t.Fatalf("propose committed=%v err=%v", committed, err)
	}
	cluster.Kill("n2")
	cluster.Restart("n2")
	if got := cluster.Nodes()["n2"].PersistentState().CurrentTerm; got != 1 {
		t.Fatalf("term after restart = %d", got)
	}
	if err := cluster.ReplicateAll(); err != nil {
		t.Fatal(err)
	}
	if err := verify.RaftCluster(cluster.Nodes(), cluster.Committed()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotInstallCatchesUpLaggingFollower(t *testing.T) {
	cluster := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3"})
	if ok, err := cluster.Elect("n1"); err != nil || !ok {
		t.Fatalf("election ok=%v err=%v", ok, err)
	}
	cluster.Partition("n1", "n3")
	for i := 0; i < 3; i++ {
		if _, committed, err := cluster.Propose([]byte{byte('a' + i)}); err != nil || !committed {
			t.Fatalf("propose %d committed=%v err=%v", i, committed, err)
		}
	}
	if err := cluster.CompactLeader(2, []byte("snapshot-at-2")); err != nil {
		t.Fatal(err)
	}
	cluster.Heal()
	if err := cluster.InstallSnapshots(); err != nil {
		t.Fatal(err)
	}
	if got := cluster.Nodes()["n3"].Snapshot().LastIncludedIndex; got != 2 {
		t.Fatalf("snapshot index on lagging follower = %d", got)
	}
}
