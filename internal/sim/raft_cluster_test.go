package sim_test

import (
	"fmt"
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/gateway"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
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
	entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "isolated-write"})
	if err != nil {
		t.Fatal(err)
	}
	_ = cluster.RunEvents(500)
	if leader.CommitIndex() >= entry.Index {
		t.Fatalf("leader committed index %d without quorum", entry.Index)
	}
}

func TestSchedulerStateReplicatesThroughCommittedRaftLog(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 121)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	submit, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "job-repl", Payload: "payload"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(submit.Index, 5000); err != nil {
		t.Fatal(err)
	}
	for id := range cluster.Nodes() {
		if err := cluster.RunUntilApplied(id, submit.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	assertSchedulerConverged(t, cluster)

	cluster.Crash(leader.ID())
	newLeader, err := cluster.RunUntilLeader(10000)
	if err != nil {
		t.Fatal(err)
	}
	commands := []scheduler.Command{
		{Type: scheduler.ClaimCommand, JobID: "job-repl", WorkerID: "worker-1", Now: 10, LeaseDuration: 10},
		{Type: scheduler.StartCommand, JobID: "job-repl", WorkerID: "worker-1", Now: 11},
		{Type: scheduler.CompleteCommand, JobID: "job-repl", WorkerID: "worker-1", Now: 12, CompletionToken: "complete-1"},
	}
	var last raft.Entry
	for _, cmd := range commands {
		last, err = cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatalf("leader %s propose %s: %v", newLeader.ID(), cmd.Type, err)
		}
		if err := cluster.RunUntilCommitted(last.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	cluster.Restart(leader.ID())
	cluster.Heal()
	for id := range cluster.Nodes() {
		if err := cluster.RunUntilApplied(id, last.Index, 30000); err != nil {
			t.Fatal(err)
		}
	}
	assertSchedulerConverged(t, cluster)
	for id, sm := range cluster.LiveSchedulers() {
		job, ok := sm.Job("job-repl")
		if !ok || job.Status != scheduler.Completed {
			t.Fatalf("%s job state = %+v exists=%v", id, job, ok)
		}
	}
}

func TestFencingTokenMonotonicAcrossFailoverAndRestart(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 122)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.SubmitCommand, JobID: "fence-job", MaxAttempts: 3},
		{Type: scheduler.ClaimCommand, JobID: "fence-job", WorkerID: "old-worker", Now: 1, LeaseDuration: 2},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
	}
	oldJob, _ := cluster.Scheduler(leader.ID()).Job("fence-job")
	oldToken := oldJob.FencingToken
	cluster.Crash(leader.ID())
	newLeader, err := cluster.RunUntilLeader(10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.ExpireLeasesCommand, Now: 4},
		{Type: scheduler.ClaimCommand, JobID: "fence-job", WorkerID: "new-worker", Now: 4, LeaseDuration: 10},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
	}
	newJob, _ := cluster.Scheduler(newLeader.ID()).Job("fence-job")
	if newJob.FencingToken <= oldToken {
		t.Fatalf("token did not increase across failover: old=%d new=%d", oldToken, newJob.FencingToken)
	}
	gw := gateway.New()
	if err := gw.Write(gateway.Write{Resource: "fence-resource", Value: "new", Token: newJob.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if err := gw.Write(gateway.Write{Resource: "fence-resource", Value: "old", Token: oldToken}); err == nil {
		t.Fatal("old token write accepted")
	}
	cluster.Restart(leader.ID())
	cluster.Heal()
	entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.ExpireLeasesCommand, Now: 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
		t.Fatal(err)
	}
	entry, err = cluster.ProposeScheduler(scheduler.Command{Type: scheduler.ClaimCommand, JobID: "fence-job", WorkerID: "third-worker", Now: 20, LeaseDuration: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilApplied(leader.ID(), entry.Index, 30000); err != nil {
		t.Fatal(err)
	}
	restartedJob, _ := cluster.Scheduler(leader.ID()).Job("fence-job")
	if restartedJob.FencingToken <= newJob.FencingToken {
		t.Fatalf("token did not advance after restart: restarted=%d previous=%d", restartedJob.FencingToken, newJob.FencingToken)
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
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("catch-up-job-%d", i)})
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
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("snapshot-job-%03d", i), Payload: "payload"})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.CompactLeader(80); err != nil {
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
	assertSchedulerConverged(t, cluster)
}

func isolate(cluster *sim.Cluster, id raft.NodeID) {
	for _, other := range []raft.NodeID{"n1", "n2", "n3", "n4", "n5"} {
		if other != id {
			cluster.Partition(id, other)
		}
	}
}

func assertSchedulerConverged(t *testing.T, cluster *sim.Cluster) {
	t.Helper()
	fingerprints := cluster.SchedulerFingerprints()
	var want string
	for id, got := range fingerprints {
		if want == "" {
			want = got
			continue
		}
		if got != want {
			t.Fatalf("scheduler state diverged at %s: got %s want %s all=%v", id, got, want, fingerprints)
		}
	}
}
