package verify

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/gateway"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/sim"
)

var (
	seedFlag  = flag.Int64("seed", 1, "single deterministic chaos seed")
	seedsFlag = flag.Int("seeds", 1000, "number of deterministic chaos seeds")
)

func TestChaosSeed(t *testing.T) {
	runChaosSeed(t, *seedFlag)
}

func TestRandomizedDeterministicSeeds(t *testing.T) {
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
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, seed)
	if _, err := cluster.RunUntilLeader(5000); err != nil {
		t.Fatalf("seed %d initial election: %v", seed, err)
	}
	sm := scheduler.NewStateMachine(seed)
	gw := gateway.New()
	var history []HistoryEvent
	committed := map[uint64]raft.Entry{}

	for step := 0; step < 40; step++ {
		switch rng.Intn(9) {
		case 0: // submit
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.SubmitCommand, JobID: jobID, Payload: "payload", MaxAttempts: 3})
			entry, err := proposeIfLeader(cluster, []byte("submit:"+jobID))
			if err == nil {
				if waitEntryCommitted(cluster, entry, 2000) {
					committed[entry.Index] = entry
				}
			}
		case 1: // partition
			a := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			b := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			if a != b {
				cluster.Partition(a, b)
			}
		case 2: // elect
			_ = cluster.RunEvents(100)
		case 3: // kill
			if leader, ok := cluster.Leader(); ok && rng.Intn(2) == 0 {
				cluster.Kill(leader.ID())
			} else {
				cluster.Kill(raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5))))
			}
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
			_ = cluster.RunEvents(100)
		case 8: // retry/fail
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.FailCommand, JobID: jobID, Now: int64(step), Error: "simulated", BackoffBase: 1})
			_, _ = sm.Apply(scheduler.Command{Type: scheduler.RetryDueCommand, Now: int64(step + 10)})
		}
		_ = cluster.RunEvents(20)
		if err := RaftCluster(cluster.LiveNodes(), committed); err != nil {
			t.Fatalf("seed %d step %d: %v; committed=%v; nodes=%s", seed, step, err, committed, describeNodes(cluster.LiveNodes()))
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

func describeNodes(nodes map[raft.NodeID]*raft.Node) string {
	out := ""
	for id, node := range nodes {
		out += fmt.Sprintf("%s role=%s term=%d commit=%d log=%v; ", id, node.Role(), node.CurrentTerm(), node.CommitIndex(), node.Entries())
	}
	return out
}

func proposeIfLeader(cluster *sim.Cluster, command []byte) (raft.Entry, error) {
	if _, ok := cluster.Leader(); !ok {
		if _, err := cluster.RunUntilLeader(5000); err != nil {
			return raft.Entry{}, err
		}
	}
	entry, err := cluster.Propose(command)
	return entry, err
}

func waitEntryCommitted(cluster *sim.Cluster, entry raft.Entry, maxEvents int) bool {
	for step := 0; step < maxEvents; step++ {
		if leader, ok := cluster.Leader(); ok && leader.CommitIndex() >= entry.Index {
			for _, candidate := range leader.Entries() {
				if candidate.Index == entry.Index && candidate.Term == entry.Term && string(candidate.Command) == string(entry.Command) {
					return true
				}
			}
			return false
		}
		if err := cluster.RunEvents(1); err != nil {
			return false
		}
	}
	return false
}
