package sim_test

import (
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/sim"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/verify"
)

func TestAutomaticLeaderElectionAndFailover(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, 48213)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	firstLeader := leader.ID()
	cluster.Kill(firstLeader)
	newLeader, err := cluster.RunUntilLeader(10000)
	if err != nil {
		t.Fatal(err)
	}
	if newLeader.ID() == firstLeader {
		t.Fatalf("expected failover away from killed leader %s", firstLeader)
	}
	if err := verify.ElectionSafety(cluster.LiveNodes()); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderCannotCommitWithoutQuorum(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 11)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		if id != leader.ID() {
			cluster.Partition(leader.ID(), id)
		}
	}
	entry, err := cluster.Propose([]byte("isolated-write"))
	if err != nil {
		t.Fatal(err)
	}
	_ = cluster.RunEvents(500)
	if leader.CommitIndex() >= entry.Index {
		t.Fatalf("leader committed index %d without quorum", entry.Index)
	}
}

func TestLaggingFollowerCatchesUpWithNextIndexBacktracking(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 21)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	lagging := raft.NodeID("n1")
	if lagging == leader.ID() {
		lagging = "n2"
	}
	cluster.Pause(lagging)
	isolate(cluster, lagging)
	for i := 0; i < 10; i++ {
		entry, err := cluster.Propose([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
	}
	cluster.Heal()
	cluster.Resume(lagging)
	if err := cluster.RunUntilLogIndex(lagging, 10, 20000); err != nil {
		t.Fatal(err)
	}
	if got := cluster.Nodes()[lagging].LastLogIndex(); got != 10 {
		t.Fatalf("lagging follower index = %d", got)
	}
}

func TestSnapshotIsInstalledThroughReplicationFlow(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 31)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	lagging := raft.NodeID("n1")
	if lagging == leader.ID() {
		lagging = "n2"
	}
	cluster.Pause(lagging)
	isolate(cluster, lagging)
	for i := 0; i < 100; i++ {
		entry, err := cluster.Propose([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.CompactLeader(80, []byte("snapshot-80")); err != nil {
		t.Fatal(err)
	}
	cluster.Heal()
	cluster.Resume(lagging)
	if _, err := cluster.RunUntilLeader(10000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilApplied(lagging, 100, 50000); err != nil {
		node := cluster.Nodes()[lagging]
		leader, _ := cluster.Leader()
		t.Fatalf("%v; leader=%s role=%s term=%d snapshot=%d log=%d commit=%d next=%v match=%v; lagging role=%s term=%d snapshot=%d log=%d commit=%d applied=%d", err, leader.ID(), leader.Role(), leader.CurrentTerm(), leader.Snapshot().LastIncludedIndex, leader.LastLogIndex(), leader.CommitIndex(), leader.NextIndex(), leader.MatchIndex(), node.Role(), node.CurrentTerm(), node.Snapshot().LastIncludedIndex, node.LastLogIndex(), node.CommitIndex(), node.LastApplied())
	}
	if got := cluster.Nodes()[lagging].Snapshot().LastIncludedIndex; got != 80 {
		t.Fatalf("snapshot index = %d", got)
	}
}

func isolate(cluster *sim.Cluster, id raft.NodeID) {
	for _, other := range []raft.NodeID{"n1", "n2", "n3", "n4", "n5"} {
		if other != id {
			cluster.Partition(id, other)
		}
	}
}
