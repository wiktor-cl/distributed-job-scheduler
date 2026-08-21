package verify

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"

	"github.com/jhinr/distributed-job-scheduler/internal/gateway"
	"github.com/jhinr/distributed-job-scheduler/internal/raft"
	"github.com/jhinr/distributed-job-scheduler/internal/scheduler"
)

var (
	seedFlag  = flag.Int64("seed", 1, "single deterministic chaos seed")
	seedsFlag = flag.Int("seeds", 25, "number of deterministic chaos seeds")
)

func TestChaosSeed(t *testing.T) {
	runChaosSeed(t, *seedFlag)
}

func TestChaosManySeeds(t *testing.T) {
	for i := 0; i < *seedsFlag; i++ {
		seed := int64(1000 + i)
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runChaosSeed(t, seed)
		})
	}
}

func runChaosSeed(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	cluster := raft.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"})
	sm := scheduler.NewStateMachine(seed)
	gw := gateway.New()
	var history []HistoryEvent

	for step := 0; step < 40; step++ {
		switch rng.Intn(9) {
		case 0: // submit
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.SubmitCommand, JobID: jobID, Payload: "payload", MaxAttempts: 3})
			_ = proposeIfLeader(cluster, []byte("submit:"+jobID))
		case 1: // partition
			a := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			b := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			if a != b {
				cluster.Partition(a, b)
			}
		case 2: // elect
			id := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			_, _ = cluster.Elect(id)
		case 3: // kill
			cluster.Kill(raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5))))
		case 4: // restart
			cluster.Restart(raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5))))
		case 5: // claim
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			worker := fmt.Sprintf("w%d", rng.Intn(3))
			result, _ := sm.Apply(scheduler.Command{Type: scheduler.ClaimCommand, JobID: jobID, WorkerID: worker, Now: int64(step), LeaseDuration: 3})
			if result.Changed {
				history = append(history, HistoryEvent{Time: int64(step), Operation: "claim", JobID: jobID, Owner: worker, Token: result.Job.FencingToken})
			}
		case 6: // timeout
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.ExpireLeasesCommand, Now: int64(step + 10)})
		case 7: // heal
			cluster.Heal()
			_ = cluster.ReplicateAll()
		case 8: // retry/fail
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.FailCommand, JobID: jobID, Now: int64(step), Error: "simulated", BackoffBase: 1})
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.RetryDueCommand, Now: int64(step + 10)})
		}
		if err := RaftCluster(cluster.Nodes(), cluster.Committed()); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, step, err)
		}
		if err := JobInvariants(sm.Jobs()); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, step, err)
		}
		if err := History(history); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, step, err)
		}
	}

	if err := gw.Write(gateway.Write{Resource: "r1", Value: "new", Token: 2}); err != nil {
		t.Fatalf("seed %d gateway setup: %v", seed, err)
	}
	if err := gw.Write(gateway.Write{Resource: "r1", Value: "old", Token: 1}); err == nil {
		t.Fatalf("seed %d accepted stale gateway write", seed)
	}
}

func proposeIfLeader(cluster *raft.Cluster, command []byte) error {
	if _, ok := cluster.Leader(); !ok {
		if ok, err := cluster.Elect("n1"); err != nil || !ok {
			return err
		}
	}
	_, _, err := cluster.Propose(command)
	return err
}
