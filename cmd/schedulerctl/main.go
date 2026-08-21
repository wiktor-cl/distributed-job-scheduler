package main

import (
	"fmt"
	"os"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/gateway"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/sim"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: schedulerctl raft-demo|snapshot-demo|fencing-demo")
		return
	}
	switch os.Args[1] {
	case "raft-demo":
		raftDemo()
	case "snapshot-demo":
		snapshotDemo()
	case "fencing-demo", "demo":
		fencingDemo()
	default:
		fmt.Println("usage: schedulerctl raft-demo|snapshot-demo|fencing-demo")
	}
}

func raftDemo() {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 77)
	leader, err := cluster.RunUntilLeader(5000)
	must(err)
	fmt.Printf("leader=%s term=%d\n", leader.ID(), leader.CurrentTerm())
	submit, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "demo-job", Payload: "payload"})
	must(err)
	must(cluster.RunUntilCommitted(submit.Index, 5000))
	cluster.Crash(leader.ID())
	newLeader, err := cluster.RunUntilLeader(10000)
	must(err)
	fmt.Printf("after crash new_leader=%s term=%d\n", newLeader.ID(), newLeader.CurrentTerm())
	for _, cmd := range []scheduler.Command{
		{Type: scheduler.ClaimCommand, JobID: "demo-job", WorkerID: "worker-1", Now: 10, LeaseDuration: 10},
		{Type: scheduler.StartCommand, JobID: "demo-job", WorkerID: "worker-1", Now: 11},
		{Type: scheduler.CompleteCommand, JobID: "demo-job", WorkerID: "worker-1", Now: 12, CompletionToken: "done"},
	} {
		entry, err := cluster.ProposeScheduler(cmd)
		must(err)
		must(cluster.RunUntilCommitted(entry.Index, 5000))
	}
	cluster.Restart(leader.ID())
	must(cluster.RunEvents(5000))
	fmt.Printf("scheduler_states=%v\n", cluster.SchedulerFingerprints())
}

func snapshotDemo() {
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3"}, 88)
	leader, err := cluster.RunUntilLeader(5000)
	must(err)
	lagging := raft.NodeID("n2")
	if leader.ID() == lagging {
		lagging = "n3"
	}
	cluster.Pause(lagging)
	isolate(cluster, lagging)
	for i := 0; i < 25; i++ {
		entry, err := cluster.ProposeScheduler(scheduler.Command{Type: scheduler.SubmitCommand, JobID: fmt.Sprintf("snapshot-demo-%02d", i)})
		must(err)
		must(cluster.RunUntilCommitted(entry.Index, 5000))
	}
	must(cluster.CompactLeader(20))
	cluster.Heal()
	cluster.Resume(lagging)
	_, err = cluster.RunUntilLeader(10000)
	must(err)
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		must(cluster.RunUntilApplied(id, 25, 30000))
	}
	fmt.Printf("lagging=%s snapshot_index=%d converged=%t\n", lagging, cluster.Nodes()[lagging].Snapshot().LastIncludedIndex, converged(cluster.SchedulerFingerprints()))
}

func fencingDemo() {
	sm := scheduler.NewStateMachine(1)
	gw := gateway.New()
	_, _ = sm.ApplyCommand(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "job-1", Payload: "write external resource"})
	first, _ := sm.ApplyCommand(scheduler.Command{Type: scheduler.ClaimCommand, JobID: "job-1", WorkerID: "old-worker", Now: 1, LeaseDuration: 1})
	_, _ = sm.ApplyCommand(scheduler.Command{Type: scheduler.ExpireLeasesCommand, Now: 2})
	second, _ := sm.ApplyCommand(scheduler.Command{Type: scheduler.ClaimCommand, JobID: "job-1", WorkerID: "new-worker", Now: 2, LeaseDuration: 10})

	if err := gw.Write(gateway.Write{Resource: "resource-1", Value: "new-worker-write", Token: second.Job.FencingToken}); err != nil {
		fmt.Printf("new write rejected unexpectedly: %v\n", err)
		os.Exit(1)
	}
	err := gw.Write(gateway.Write{Resource: "resource-1", Value: "old-worker-write", Token: first.Job.FencingToken})
	fmt.Printf("first token=%d second token=%d stale-write-error=%v\n", first.Job.FencingToken, second.Job.FencingToken, err)
}

func must(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func isolate(cluster *sim.Cluster, id raft.NodeID) {
	for _, other := range []raft.NodeID{"n1", "n2", "n3"} {
		if other != id {
			cluster.Partition(id, other)
		}
	}
}

func converged(states map[raft.NodeID]string) bool {
	var want string
	for _, state := range states {
		if want == "" {
			want = state
			continue
		}
		if state != want {
			return false
		}
	}
	return true
}
