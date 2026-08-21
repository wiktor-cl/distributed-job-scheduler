package verify

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
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
	runChaosSeedWithCoverage(t, *seedFlag)
}

func TestRandomizedDeterministicSeeds(t *testing.T) {
	seeds := *seedsFlag
	if raceEnabled && seeds == 1000 {
		seeds = 100
	}
	for i := 0; i < seeds; i++ {
		seed := int64(1000 + i)
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runChaosSeedWithCoverage(t, seed)
		})
	}
}

func TestChaosScenarioCoverageCounters(t *testing.T) {
	var total chaosCoverage
	for i := 0; i < 100; i++ {
		total.add(runChaosSeedWithCoverage(t, int64(20000+i)))
	}
	if total.QuorumLoss == 0 || total.AsymmetricPartition == 0 || total.Crash == 0 || total.Restart == 0 || total.Snapshot == 0 || total.DuplicatedMessages == 0 || total.DroppedMessages == 0 {
		t.Fatalf("chaos coverage too weak: %+v", total)
	}
}

type chaosCoverage struct {
	QuorumLoss          int
	AsymmetricPartition int
	Crash               int
	Restart             int
	Snapshot            int
	DuplicatedMessages  int
	DroppedMessages     int
}

func (c *chaosCoverage) add(other chaosCoverage) {
	c.QuorumLoss += other.QuorumLoss
	c.AsymmetricPartition += other.AsymmetricPartition
	c.Crash += other.Crash
	c.Restart += other.Restart
	c.Snapshot += other.Snapshot
	c.DuplicatedMessages += other.DuplicatedMessages
	c.DroppedMessages += other.DroppedMessages
}

func runChaosSeedWithCoverage(t *testing.T, seed int64) chaosCoverage {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	cluster := sim.NewCluster([]raft.NodeID{"n1", "n2", "n3", "n4", "n5"}, seed)
	if _, err := cluster.RunUntilLeader(5000); err != nil {
		t.Fatalf("seed %d initial election: %v", seed, err)
	}
	cluster.SetNetworkFaults(15, 25)
	gw := gateway.New()
	var history []HistoryEvent
	lastToken := map[string]uint64{}
	var coverage chaosCoverage

	for step := 0; step < 40; step++ {
		switch rng.Intn(12) {
		case 0: // submit
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = proposeSchedulerIfLeader(cluster, scheduler.Command{Type: scheduler.SubmitCommand, JobID: jobID, Payload: "payload", MaxAttempts: 3})
		case 1: // partition
			a := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			b := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			if a != b {
				cluster.Partition(a, b)
				coverage.QuorumLoss++
			}
		case 2: // elect
			_ = cluster.RunEvents(100)
		case 3: // kill
			if leader, ok := cluster.Leader(); ok && rng.Intn(2) == 0 {
				cluster.Kill(leader.ID())
			} else {
				cluster.Kill(raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5))))
			}
			coverage.Crash++
		case 4: // restart
			cluster.Restart(raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5))))
			coverage.Restart++
		case 5: // claim
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			worker := fmt.Sprintf("w%d", rng.Intn(3))
			entry, err := proposeSchedulerIfLeader(cluster, scheduler.Command{Type: scheduler.ClaimCommand, JobID: jobID, WorkerID: worker, Now: int64(step), LeaseDuration: 3})
			if err == nil && waitEntryCommitted(cluster, entry, 2000) {
				if leader, ok := cluster.Leader(); ok {
					if job, exists := cluster.Scheduler(leader.ID()).Job(jobID); exists && job.Owner == worker {
						if job.FencingToken > lastToken[jobID] {
							lastToken[jobID] = job.FencingToken
							history = append(history, HistoryEvent{Time: int64(step), Operation: "claim", JobID: jobID, Owner: worker, Token: job.FencingToken})
						}
					}
				}
			}
		case 6: // timeout
			_, _ = proposeSchedulerIfLeader(cluster, scheduler.Command{Type: scheduler.ExpireLeasesCommand, Now: int64(step + 10)})
		case 7: // heal
			cluster.Heal()
			_ = cluster.RunEvents(100)
		case 8: // retry/fail
			jobID := fmt.Sprintf("job-%d", rng.Intn(6))
			_, _ = proposeSchedulerIfLeader(cluster, scheduler.Command{Type: scheduler.FailCommand, JobID: jobID, Now: int64(step), Error: "simulated", BackoffBase: 1})
			_, _ = proposeSchedulerIfLeader(cluster, scheduler.Command{Type: scheduler.RetryDueCommand, Now: int64(step + 10)})
		case 9: // compact/snapshot on current state machine boundary
			if leader, ok := cluster.Leader(); ok && leader.LastApplied() > leader.Snapshot().LastIncludedIndex {
				if err := cluster.CompactLeader(leader.LastApplied()); err == nil {
					coverage.Snapshot++
				}
			}
		case 10: // asymmetric partition
			from := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			to := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			if from != to {
				cluster.AsymmetricPartition(from, to)
				coverage.AsymmetricPartition++
			}
		case 11: // pause/resume far-behind candidates
			id := raft.NodeID(fmt.Sprintf("n%d", 1+rng.Intn(5)))
			cluster.Pause(id)
			_ = cluster.RunEvents(10)
			cluster.Resume(id)
		}
		_ = cluster.RunEvents(20)
		committed := cluster.CommittedEntries()
		if err := RaftCluster(cluster.LiveNodes(), committed); err != nil {
			t.Fatalf("seed %d step %d time=%d: %v; committed=%v; nodes=%s", seed, step, cluster.Clock().Now(), err, committed, describeNodes(cluster.LiveNodes()))
		}
		for id, sm := range liveSchedulers(cluster) {
			if err := JobInvariants(sm.Jobs()); err != nil {
				t.Fatalf("seed %d step %d node=%s: %v", seed, step, id, err)
			}
		}
		if err := ReplicatedSchedulerStateEquality(cluster.LiveNodes(), cluster.LiveSchedulers()); err != nil {
			t.Fatalf("seed %d step %d time=%d: %v", seed, step, cluster.Clock().Now(), err)
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
	if len(cluster.EventHistory()) == 0 {
		t.Fatalf("seed %d produced no deterministic event history", seed)
	}
	coverage.DuplicatedMessages = countDuplicateDeliveries(cluster)
	coverage.DroppedMessages = countDroppedMessages(cluster)
	return coverage
}

func describeNodes(nodes map[raft.NodeID]*raft.Node) string {
	out := ""
	for id, node := range nodes {
		out += fmt.Sprintf("%s role=%s term=%d commit=%d log=%v; ", id, node.Role(), node.CurrentTerm(), node.CommitIndex(), node.Entries())
	}
	return out
}

func proposeSchedulerIfLeader(cluster *sim.Cluster, command scheduler.Command) (raft.Entry, error) {
	if _, ok := cluster.Leader(); !ok {
		if _, err := cluster.RunUntilLeader(5000); err != nil {
			return raft.Entry{}, err
		}
	}
	entry, err := cluster.ProposeScheduler(command)
	if err != nil {
		return raft.Entry{}, err
	}
	_ = cluster.RunUntilCommitted(entry.Index, 2000)
	return entry, nil
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

func liveSchedulers(cluster *sim.Cluster) map[raft.NodeID]*scheduler.StateMachine {
	out := map[raft.NodeID]*scheduler.StateMachine{}
	for id := range cluster.LiveNodes() {
		out[id] = cluster.Scheduler(id)
	}
	return out
}

func countDroppedMessages(cluster *sim.Cluster) int {
	return len(cluster.DroppedMessages())
}

func countDuplicateDeliveries(cluster *sim.Cluster) int {
	seen := map[string]int{}
	for _, msg := range cluster.DeliveredMessages() {
		key := fmt.Sprintf("%s|%s|%s|%#v", msg.From, msg.To, msg.Type, msg.Payload)
		seen[key]++
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	duplicates := 0
	for _, key := range keys {
		if seen[key] > 1 {
			duplicates += seen[key] - 1
		}
	}
	return duplicates
}
