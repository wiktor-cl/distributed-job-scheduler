package sim_test

import (
	"fmt"
	"reflect"
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

func TestOldLeaderCannotCommitInMinorityPartition(t *testing.T) {
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

func TestCommittedEntrySurvivesLeaderCrashBeforeCommitBroadcast(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 112)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "survives-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
		t.Fatal(err)
	}
	cluster.Crash(leader.ID())
	newLeader, err := cluster.RunUntilLeader(10000)
	if err != nil {
		t.Fatal(err)
	}
	noOp, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "post-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(noOp.Index, 10000); err != nil {
		t.Fatal(err)
	}
	for id := range cluster.LiveNodes() {
		if err := cluster.RunUntilApplied(id, noOp.Index, 30000); err != nil {
			t.Fatal(err)
		}
		if _, ok := cluster.Scheduler(id).Job("survives-crash"); !ok {
			t.Fatalf("%s under leader %s lost committed pre-crash entry", id, newLeader.ID())
		}
	}
}

func TestUncommittedEntryIsOverwrittenAfterLeaderCrash(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 113)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		if id != leader.ID() {
			cluster.Partition(leader.ID(), id)
		}
	}
	uncommitted, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "uncommitted"})
	if err != nil {
		t.Fatal(err)
	}
	_ = cluster.RunEvents(500)
	if leader.CommitIndex() >= uncommitted.Index {
		t.Fatalf("test setup failed: isolated leader committed %d", uncommitted.Index)
	}
	cluster.Crash(leader.ID())
	cluster.Heal()
	replacementLeader, err := cluster.RunUntilLeader(10000)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "replacement"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(replacement.Index, 10000); err != nil {
		t.Fatal(err)
	}
	cluster.Restart(leader.ID())
	for id := range cluster.Nodes() {
		if err := cluster.RunUntilApplied(id, replacement.Index, 30000); err != nil {
			t.Fatal(err)
		}
		if _, ok := cluster.Scheduler(id).Job("uncommitted"); ok {
			t.Fatalf("%s kept uncommitted entry after leader %s overwrite", id, replacementLeader.ID())
		}
		if _, ok := cluster.Scheduler(id).Job("replacement"); !ok {
			t.Fatalf("%s missing replacement entry", id)
		}
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

func TestCrashDoesNotPerformPersistenceSideEffects(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 123)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	before := leader.PersistentState()
	cluster.Crash(leader.ID())
	after := cluster.Nodes()[leader.ID()].PersistentState()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("crash changed persistent state: before=%+v after=%+v", before, after)
	}
}

func TestRestartDoesNotReuseVolatileRaftState(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 124)
	leader, err := cluster.RunUntilLeader(5000)
	if err != nil {
		t.Fatal(err)
	}
	id := leader.ID()
	old := cluster.Nodes()[id]
	cluster.Crash(id)
	cluster.Restart(id)
	restarted := cluster.Nodes()[id]
	if restarted == old {
		t.Fatal("restart reused old node pointer")
	}
	if restarted.Role() != raft.Follower {
		t.Fatalf("restart reused volatile role: %s", restarted.Role())
	}
	if len(restarted.NextIndex()) != 0 || len(restarted.MatchIndex()) != 0 {
		t.Fatalf("restart reused leader replication state: next=%v match=%v", restarted.NextIndex(), restarted.MatchIndex())
	}
}

func TestSchedulerStateConvergesAfterMultipleLeaderChanges(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, 125)
	for i := 0; i < 4; i++ {
		leader, err := cluster.RunUntilLeader(20000)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("multi-leader-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 20000); err != nil {
			t.Fatal(err)
		}
		if i < 3 {
			cluster.Crash(leader.ID())
			cluster.Heal()
			if _, err := cluster.RunUntilLeader(30000); err != nil {
				t.Fatal(err)
			}
			cluster.Restart(leader.ID())
		}
	}
	cluster.Heal()
	leader, err := cluster.RunUntilLeader(30000)
	if err != nil {
		t.Fatal(err)
	}
	final, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "after-restarts"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(final.Index, 30000); err != nil {
		t.Fatal(err)
	}
	for id := range cluster.LiveNodes() {
		if err := cluster.RunUntilApplied(id, final.Index, 50000); err != nil {
			t.Fatalf("leader=%s: %v", leader.ID(), err)
		}
	}
	assertSchedulerConverged(t, cluster)
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

func TestFencingTokenRemainsMonotonicAfterSnapshotAndFullRestart(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 126)
	if _, err := cluster.RunUntilLeader(5000); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.SubmitCommand, JobID: "snapshot-fence", MaxAttempts: 3},
		{Type: scheduler.ClaimCommand, JobID: "snapshot-fence", WorkerID: "w1", Now: 1, LeaseDuration: 2},
		{Type: scheduler.ExpireLeasesCommand, Now: 4},
		{Type: scheduler.ClaimCommand, JobID: "snapshot-fence", WorkerID: "w2", Now: 4, LeaseDuration: 2},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	leader, _ := cluster.Leader()
	second, _ := cluster.Scheduler(leader.ID()).Job("snapshot-fence")
	if err := cluster.CompactLeader(leader.LastApplied()); err != nil {
		t.Fatal(err)
	}
	for id := range cluster.Nodes() {
		cluster.Crash(id)
	}
	for id := range cluster.Nodes() {
		cluster.Restart(id)
	}
	if _, err := cluster.RunUntilLeader(30000); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.ExpireLeasesCommand, Now: 10},
		{Type: scheduler.ClaimCommand, JobID: "snapshot-fence", WorkerID: "w3", Now: 10, LeaseDuration: 5},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 30000); err != nil {
			t.Fatal(err)
		}
	}
	leader, _ = cluster.Leader()
	third, _ := cluster.Scheduler(leader.ID()).Job("snapshot-fence")
	if third.FencingToken <= second.FencingToken {
		t.Fatalf("fencing token regressed after snapshot/full restart: before=%d after=%d", second.FencingToken, third.FencingToken)
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
	for i := 0; i < 80; i++ {
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
	for i := 80; i < 100; i++ {
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("snapshot-job-%03d", i), Payload: "payload"})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 5000); err != nil {
			t.Fatal(err)
		}
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

func TestFollowerRestoresSchedulerFromSnapshotThenReplaysSuffix(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 127)
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
	for i := 0; i < 5; i++ {
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("snap-prefix-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	leader, _ = cluster.Leader()
	if err := cluster.CompactLeader(leader.LastApplied()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("snap-suffix-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	cluster.Heal()
	cluster.Resume(lagging)
	if err := cluster.RunUntilApplied(lagging, 8, 50000); err != nil {
		t.Fatal(err)
	}
	if got := cluster.Nodes()[lagging].Snapshot().LastIncludedIndex; got != 5 {
		t.Fatalf("lagging snapshot index=%d want 5", got)
	}
	for _, id := range []string{"snap-prefix-0", "snap-prefix-4", "snap-suffix-0", "snap-suffix-2"} {
		if _, ok := cluster.Scheduler(lagging).Job(id); !ok {
			t.Fatalf("lagging follower missing %s after snapshot+suffix replay", id)
		}
	}
	assertSchedulerConverged(t, cluster)
}

func TestFullClusterRestartAfterSnapshotContinuesScheduling(t *testing.T) {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, 128)
	if _, err := cluster.RunUntilLeader(10000); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("restart-job-%02d", i), MaxAttempts: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	leader, _ := cluster.Leader()
	if err := cluster.CompactLeader(leader.LastApplied()); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.ClaimCommand, JobID: "restart-job-00", WorkerID: "worker", Now: 1, LeaseDuration: 10},
		{Type: scheduler.StartCommand, JobID: "restart-job-00", WorkerID: "worker", Now: 2},
		{Type: scheduler.CompleteCommand, JobID: "restart-job-00", WorkerID: "worker", Now: 3},
		{Type: scheduler.FailCommand, JobID: "restart-job-01", Now: 4, Error: "first", BackoffBase: 1},
		{Type: scheduler.RetryDueCommand, Now: 6},
		{Type: scheduler.FailCommand, JobID: "restart-job-01", Now: 7, Error: "second", BackoffBase: 1},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
			t.Fatal(err)
		}
	}
	for id := range cluster.Nodes() {
		cluster.Crash(id)
	}
	for id := range cluster.Nodes() {
		cluster.Restart(id)
	}
	if _, err := cluster.RunUntilLeader(50000); err != nil {
		t.Fatal(err)
	}
	final, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "post-full-restart"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.RunUntilCommitted(final.Index, 50000); err != nil {
		t.Fatal(err)
	}
	for id := range cluster.LiveNodes() {
		if err := cluster.RunUntilApplied(id, final.Index, 80000); err != nil {
			t.Fatal(err)
		}
		sm := cluster.Scheduler(id)
		completed, _ := sm.Job("restart-job-00")
		if completed.Status != scheduler.Completed {
			t.Fatalf("%s completed job state=%+v", id, completed)
		}
		dlq, _ := sm.Job("restart-job-01")
		if dlq.Status != scheduler.DLQ {
			t.Fatalf("%s dlq job state=%+v", id, dlq)
		}
		if _, ok := sm.Job("post-full-restart"); !ok {
			t.Fatalf("%s missing post-restart job", id)
		}
	}
	assertSchedulerConverged(t, cluster)
}

func TestSameSeedProducesIdenticalEventHistory(t *testing.T) {
	run := func() ([]string, map[raft.NodeID]string) {
		cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 129)
		leader, err := cluster.RunUntilLeader(5000)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("det-%d", i)})
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.RunUntilCommitted(entry.Index, 10000); err != nil {
				t.Fatal(err)
			}
		}
		cluster.Crash(leader.ID())
		cluster.Heal()
		if _, err := cluster.RunUntilLeader(10000); err != nil {
			t.Fatal(err)
		}
		return cluster.EventHistory(), cluster.SchedulerFingerprints()
	}
	historyA, statesA := run()
	historyB, statesB := run()
	if !reflect.DeepEqual(historyA, historyB) {
		t.Fatalf("same seed produced different history:\nA=%v\nB=%v", historyA, historyB)
	}
	if !reflect.DeepEqual(statesA, statesB) {
		t.Fatalf("same seed produced different states:\nA=%v\nB=%v", statesA, statesB)
	}
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
