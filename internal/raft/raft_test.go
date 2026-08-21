package raft_test

import (
	"bytes"
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/sim"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/verify"
)

func TestFiveNodeClusterElectsSingleLeader(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, 901)
	_, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	if err := verify.ElectionSafety(cluster.LiveNodes()); err != nil {
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

func TestElectionDeadlineResetsAgainstLogicalNow(t *testing.T) {
	node := raft.NewNodeWithConfig("n1", []raft.NodeID{"n1", "n2"}, raft.NewMemoryStorage(raft.PersistentState{}), nil, raft.NodeConfig{
		ElectionTimeoutMin: 100,
		ElectionTimeoutMax: 100,
		Seed:               1,
	})
	if err := node.HandleAppendEntries("n2", raft.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}, 1000); err != nil {
		t.Fatal(err)
	}
	if node.ElectionDeadline() <= 1000 {
		t.Fatalf("deadline reset against zero: got %d", node.ElectionDeadline())
	}
	if err := node.Tick(1001); err != nil {
		t.Fatal(err)
	}
	if node.Role() != raft.Follower {
		t.Fatalf("node started false election at time 1001; role=%s", node.Role())
	}
	if err := node.HandleRequestVote("n2", raft.RequestVoteRequest{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, 1000); err != nil {
		t.Fatal(err)
	}
	if node.ElectionDeadline() <= 1000 {
		t.Fatalf("vote request deadline reset against zero: got %d", node.ElectionDeadline())
	}
}

func TestLogReplicationCommitsCurrentTermEntryOnMajority(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, 902)
	if _, err := cluster.RunUntilLeader(5000); err != nil {
		t.Fatal(err)
	}
	entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "job-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
		t.Fatal(err)
	}
	if entry.Term == 0 || entry.Index != 1 {
		t.Fatalf("entry = %+v", entry)
	}
	if err := verify.RaftCluster(cluster.LiveNodes(), cluster.CommittedEntries()); err != nil {
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
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 903)
	if _, err := cluster.RunUntilLeader(5000); err != nil {
		t.Fatal(err)
	}
	entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "job-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
		t.Fatal(err)
	}
	cluster.Crash("n2")
	cluster.Restart("n2")
	if got := cluster.Nodes()["n2"].PersistentState().CurrentTerm; got == 0 {
		t.Fatalf("term after restart = %d", got)
	}
	if err := cluster.RunUntilLogIndex("n2", entry.Index, 10000); err != nil {
		t.Fatal(err)
	}
	if err := verify.RaftCluster(cluster.LiveNodes(), cluster.CommittedEntries()); err != nil {
		t.Fatal(err)
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
	t.queue = append(t.queue, func() error { return t.nodes[to].HandleRequestVote(from, req, 0) })
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
