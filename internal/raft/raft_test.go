package raft_test

import (
	"bytes"
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/verify"
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

func TestLeaderBacktracksNextIndexAndRepairsConflictingFollower(t *testing.T) {
	ids := []raft.NodeID{"leader", "follower"}
	leaderStore := raft.NewMemoryStorage(raft.PersistentState{
		CurrentTerm: 3,
		Entries: []raft.Entry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 3, Command: []byte("c")},
		},
	})
	followerStore := raft.NewMemoryStorage(raft.PersistentState{
		CurrentTerm: 3,
		Entries: []raft.Entry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 2, Command: []byte("wrong-b")},
			{Index: 3, Term: 2, Command: []byte("wrong-c")},
			{Index: 4, Term: 2, Command: []byte("wrong-d")},
		},
	})
	transport := &syncTransport{nodes: map[raft.NodeID]*raft.Node{}}
	leader := raft.NewNodeWithConfig("leader", ids, leaderStore, transport, raft.NodeConfig{Seed: 1})
	follower := raft.NewNodeWithConfig("follower", ids, followerStore, transport, raft.NodeConfig{Seed: 2})
	transport.nodes["leader"] = leader
	transport.nodes["follower"] = follower

	leader.BecomeLeader()
	if err := transport.Drain(50); err != nil {
		t.Fatal(err)
	}

	got := follower.Entries()
	want := leader.Entries()
	if len(got) != len(want) {
		t.Fatalf("follower log length = %d want %d; got=%+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Command, want[i].Command) {
			t.Fatalf("entry %d got=%+v want=%+v", i, got[i], want[i])
		}
	}
	if leader.NextIndex()["follower"] != 4 || leader.MatchIndex()["follower"] != 3 {
		t.Fatalf("leader replication state next=%v match=%v", leader.NextIndex(), leader.MatchIndex())
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

type syncTransport struct {
	nodes map[raft.NodeID]*raft.Node
	queue []func() error
}

func (t *syncTransport) Drain(limit int) error {
	for i := 0; len(t.queue) > 0; i++ {
		if i >= limit {
			return testingError("transport drain limit reached")
		}
		fn := t.queue[0]
		t.queue = t.queue[1:]
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (t *syncTransport) SendRequestVote(from, to raft.NodeID, req raft.RequestVoteRequest) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleRequestVote(from, req) })
}

func (t *syncTransport) SendRequestVoteResponse(from, to raft.NodeID, resp raft.RequestVoteResponse) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleRequestVoteResponse(from, resp, 0) })
}

func (t *syncTransport) SendAppendEntries(from, to raft.NodeID, req raft.AppendEntriesRequest) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleAppendEntries(from, req, 0) })
}

func (t *syncTransport) SendAppendEntriesResponse(from, to raft.NodeID, resp raft.AppendEntriesResponse) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleAppendEntriesResponse(from, resp) })
}

func (t *syncTransport) SendInstallSnapshot(from, to raft.NodeID, req raft.InstallSnapshotRequest) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleInstallSnapshot(from, req, 0) })
}

func (t *syncTransport) SendInstallSnapshotResponse(from, to raft.NodeID, resp raft.InstallSnapshotResponse) {
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleInstallSnapshotResponse(from, resp) })
}

type testingError string

func (e testingError) Error() string { return string(e) }
