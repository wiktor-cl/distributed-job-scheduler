package sim

import (
	"fmt"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
)

const defaultTickInterval int64 = 10

type raftMessage struct {
	kind string
	rv   raft.RequestVoteRequest
	rvr  raft.RequestVoteResponse
	ae   raft.AppendEntriesRequest
	aer  raft.AppendEntriesResponse
	snap raft.InstallSnapshotRequest
	sr   raft.InstallSnapshotResponse
}

type Cluster struct {
	seed         int64
	clock        *VirtualClock
	network      *VirtualNetwork
	ids          []raft.NodeID
	nodes        map[raft.NodeID]*raft.Node
	schedulers   map[raft.NodeID]*scheduler.StateMachine
	stores       map[raft.NodeID]*raft.MemoryStorage
	alive        map[raft.NodeID]bool
	paused       map[raft.NodeID]bool
	tickGen      map[raft.NodeID]uint64
	tickInterval int64
	events       []string
}

func NewCluster(ids []raft.NodeID, seed int64) *Cluster {
	clock := NewVirtualClock()
	cluster := &Cluster{
		seed:         seed,
		clock:        clock,
		ids:          append([]raft.NodeID(nil), ids...),
		nodes:        map[raft.NodeID]*raft.Node{},
		schedulers:   map[raft.NodeID]*scheduler.StateMachine{},
		stores:       map[raft.NodeID]*raft.MemoryStorage{},
		alive:        map[raft.NodeID]bool{},
		paused:       map[raft.NodeID]bool{},
		tickGen:      map[raft.NodeID]uint64{},
		tickInterval: defaultTickInterval,
	}
	cluster.network = NewVirtualNetwork(clock, NetworkConfig{MinDelay: 1, MaxDelay: 5, Seed: seed})
	for i, id := range cluster.ids {
		store := raft.NewMemoryStorage(raft.PersistentState{})
		fsm := scheduler.NewStateMachine(seed + int64(i+1)*101)
		cluster.stores[id] = store
		cluster.schedulers[id] = fsm
		cluster.alive[id] = true
		cfg := raft.NodeConfig{
			ElectionTimeoutMin: 150 + int64(i*20),
			ElectionTimeoutMax: 300 + int64(i*20),
			HeartbeatInterval:  50,
			Seed:               seed + int64(i+1)*997,
			StateMachine:       fsm,
		}
		cluster.nodes[id] = raft.NewNodeWithConfig(id, cluster.ids, store, cluster, cfg)
		cluster.register(id)
		cluster.scheduleTick(id, int64(i), cluster.tickGen[id])
	}
	return cluster
}

func (c *Cluster) Scheduler(id raft.NodeID) *scheduler.StateMachine {
	return c.schedulers[id]
}

func (c *Cluster) SchedulerFingerprints() map[raft.NodeID]string {
	out := make(map[raft.NodeID]string, len(c.schedulers))
	for id, sm := range c.schedulers {
		out[id] = sm.Fingerprint()
	}
	return out
}

func (c *Cluster) LiveSchedulers() map[raft.NodeID]*scheduler.StateMachine {
	out := map[raft.NodeID]*scheduler.StateMachine{}
	for id := range c.LiveNodes() {
		out[id] = c.schedulers[id]
	}
	return out
}

func (c *Cluster) CommittedEntries() map[uint64]raft.Entry {
	out := map[uint64]raft.Entry{}
	for _, node := range c.LiveNodes() {
		for index, entry := range node.AppliedEntries() {
			out[index] = entry
		}
	}
	return out
}

func (c *Cluster) Clock() *VirtualClock { return c.clock }

func (c *Cluster) SetNetworkFaults(dropPermille, dupePermille int) {
	c.network.SetFaults(dropPermille, dupePermille)
}

func (c *Cluster) EventHistory() []string {
	out := append([]string(nil), c.events...)
	for _, item := range c.clock.Trace() {
		out = append(out, "clock:"+item)
	}
	return out
}

func (c *Cluster) DeliveredMessages() []Message {
	return c.network.Delivered()
}

func (c *Cluster) DroppedMessages() []Message {
	return c.network.Dropped()
}

func (c *Cluster) Nodes() map[raft.NodeID]*raft.Node {
	out := make(map[raft.NodeID]*raft.Node, len(c.nodes))
	for id, node := range c.nodes {
		out[id] = node
	}
	return out
}

func (c *Cluster) LiveNodes() map[raft.NodeID]*raft.Node {
	out := make(map[raft.NodeID]*raft.Node, len(c.nodes))
	for id, node := range c.nodes {
		if c.alive[id] && !c.paused[id] {
			out[id] = node
		}
	}
	return out
}

func (c *Cluster) Leader() (*raft.Node, bool) {
	var leader *raft.Node
	for _, id := range c.ids {
		node := c.nodes[id]
		if c.alive[id] && !c.paused[id] && node.Role() == raft.Leader {
			if leader != nil && leader.CurrentTerm() == node.CurrentTerm() {
				return nil, false
			}
			if leader == nil || node.CurrentTerm() > leader.CurrentTerm() {
				leader = node
			}
		}
	}
	if leader == nil {
		return nil, false
	}
	return leader, true
}

func (c *Cluster) RunUntilLeader(maxEvents int) (*raft.Node, error) {
	for step := 0; step < maxEvents; step++ {
		if leader, ok := c.Leader(); ok {
			return leader, nil
		}
		if !c.clock.RunNext() {
			return nil, fmt.Errorf("seed=%d no events while waiting for leader", c.seed)
		}
	}
	return nil, fmt.Errorf("seed=%d no leader after %d events", c.seed, maxEvents)
}

func (c *Cluster) RunEvents(maxEvents int) error {
	for step := 0; step < maxEvents; step++ {
		if !c.clock.RunNext() {
			return nil
		}
	}
	return nil
}

func (c *Cluster) Propose(command []byte) (raft.Entry, error) {
	leader, ok := c.Leader()
	if !ok {
		return raft.Entry{}, fmt.Errorf("seed=%d no leader for proposal", c.seed)
	}
	entry, err := leader.Propose(command)
	if err != nil {
		return raft.Entry{}, err
	}
	c.events = append(c.events, fmt.Sprintf("proposal leader=%s index=%d", leader.ID(), entry.Index))
	return entry, nil
}

func (c *Cluster) ProposeScheduler(cmd scheduler.Command) (raft.Entry, error) {
	payload, err := scheduler.EncodeCommand(cmd)
	if err != nil {
		return raft.Entry{}, err
	}
	return c.Propose(payload)
}

func (c *Cluster) RunUntilCommitted(index uint64, maxEvents int) error {
	for step := 0; step < maxEvents; step++ {
		if leader, ok := c.Leader(); ok && leader.CommitIndex() >= index {
			return nil
		}
		if !c.clock.RunNext() {
			return fmt.Errorf("seed=%d event queue drained before commit index=%d", c.seed, index)
		}
	}
	return fmt.Errorf("seed=%d commit index=%d not reached after %d events", c.seed, index, maxEvents)
}

func (c *Cluster) RunUntilLogIndex(id raft.NodeID, index uint64, maxEvents int) error {
	for step := 0; step < maxEvents; step++ {
		if c.nodes[id].LastLogIndex() >= index {
			return nil
		}
		if !c.clock.RunNext() {
			return fmt.Errorf("seed=%d queue drained before %s reached log index=%d", c.seed, id, index)
		}
	}
	return fmt.Errorf("seed=%d %s did not reach log index=%d", c.seed, id, index)
}

func (c *Cluster) RunUntilApplied(id raft.NodeID, index uint64, maxEvents int) error {
	for step := 0; step < maxEvents; step++ {
		if c.nodes[id].LastApplied() >= index {
			return nil
		}
		if !c.clock.RunNext() {
			return fmt.Errorf("seed=%d queue drained before %s applied index=%d", c.seed, id, index)
		}
	}
	return fmt.Errorf("seed=%d %s did not apply index=%d", c.seed, id, index)
}

func (c *Cluster) CompactLeader(index uint64) error {
	leader, ok := c.Leader()
	if !ok {
		return fmt.Errorf("seed=%d no leader for compaction", c.seed)
	}
	return leader.Compact(index)
}

func (c *Cluster) Crash(id raft.NodeID) {
	c.alive[id] = false
	c.events = append(c.events, fmt.Sprintf("crash %s", id))
}

func (c *Cluster) GracefulStop(id raft.NodeID) {
	if c.nodes[id] != nil && c.nodes[id].Role() == raft.Leader {
		_ = c.nodes[id].StepDown(c.nodes[id].CurrentTerm(), c.clock.Now())
	}
	c.alive[id] = false
	c.events = append(c.events, fmt.Sprintf("graceful-stop %s", id))
}

func (c *Cluster) Kill(id raft.NodeID) { c.Crash(id) }

func (c *Cluster) Restart(id raft.NodeID) {
	c.alive[id] = true
	c.paused[id] = false
	c.tickGen[id]++
	generation := c.tickGen[id]
	old := c.nodes[id]
	fsm := scheduler.NewStateMachine(c.seed + int64(len(c.events)+1)*313)
	c.schedulers[id] = fsm
	cfg := raft.NodeConfig{ElectionTimeoutMin: 150, ElectionTimeoutMax: 300, HeartbeatInterval: 50, Seed: c.seed + int64(len(c.events)+1)*991}
	cfg.StateMachine = fsm
	c.nodes[id] = raft.NewNodeWithConfig(id, c.ids, c.stores[id], c, cfg)
	if old != nil && old.Role() == raft.Leader {
		c.events = append(c.events, fmt.Sprintf("restart former-leader %s", id))
	}
	c.register(id)
	c.scheduleTick(id, 0, generation)
}

func (c *Cluster) Pause(id raft.NodeID)  { c.paused[id] = true }
func (c *Cluster) Resume(id raft.NodeID) { c.paused[id] = false }

func (c *Cluster) Partition(a, b raft.NodeID) {
	c.network.Partition(string(a), string(b))
}

func (c *Cluster) AsymmetricPartition(from, to raft.NodeID) {
	c.network.AsymmetricPartition(string(from), string(to))
}

func (c *Cluster) Heal() {
	c.network.Heal()
}

func (c *Cluster) SendRequestVote(from, to raft.NodeID, req raft.RequestVoteRequest) {
	c.send(from, to, raftMessage{kind: "request_vote", rv: req})
}

func (c *Cluster) SendRequestVoteResponse(from, to raft.NodeID, resp raft.RequestVoteResponse) {
	c.send(from, to, raftMessage{kind: "request_vote_response", rvr: resp})
}

func (c *Cluster) SendAppendEntries(from, to raft.NodeID, req raft.AppendEntriesRequest) {
	c.send(from, to, raftMessage{kind: "append_entries", ae: req})
}

func (c *Cluster) SendAppendEntriesResponse(from, to raft.NodeID, resp raft.AppendEntriesResponse) {
	c.send(from, to, raftMessage{kind: "append_entries_response", aer: resp})
}

func (c *Cluster) SendInstallSnapshot(from, to raft.NodeID, req raft.InstallSnapshotRequest) {
	c.send(from, to, raftMessage{kind: "install_snapshot", snap: req})
}

func (c *Cluster) SendInstallSnapshotResponse(from, to raft.NodeID, resp raft.InstallSnapshotResponse) {
	c.send(from, to, raftMessage{kind: "install_snapshot_response", sr: resp})
}

func (c *Cluster) send(from, to raft.NodeID, payload raftMessage) {
	c.network.Send(Message{From: string(from), To: string(to), Type: payload.kind, Payload: payload})
}

func (c *Cluster) register(id raft.NodeID) {
	c.network.Register(string(id), func(msg Message) {
		nodeID := raft.NodeID(msg.To)
		if !c.alive[nodeID] || c.paused[nodeID] {
			return
		}
		payload := msg.Payload.(raftMessage)
		from := raft.NodeID(msg.From)
		node := c.nodes[nodeID]
		switch payload.kind {
		case "request_vote":
			_ = node.HandleRequestVote(from, payload.rv, c.clock.Now())
		case "request_vote_response":
			_ = node.HandleRequestVoteResponse(from, payload.rvr, c.clock.Now())
		case "append_entries":
			_ = node.HandleAppendEntries(from, payload.ae, c.clock.Now())
		case "append_entries_response":
			_ = node.HandleAppendEntriesResponse(from, payload.aer, c.clock.Now())
		case "install_snapshot":
			_ = node.HandleInstallSnapshot(from, payload.snap, c.clock.Now())
		case "install_snapshot_response":
			_ = node.HandleInstallSnapshotResponse(from, payload.sr, c.clock.Now())
		}
	})
}

func (c *Cluster) scheduleTick(id raft.NodeID, delay int64, generation uint64) {
	c.clock.Schedule(delay, "tick:"+string(id), func() {
		if c.tickGen[id] != generation {
			return
		}
		if c.alive[id] && !c.paused[id] {
			_ = c.nodes[id].Tick(c.clock.Now())
		}
		c.scheduleTick(id, c.tickInterval, generation)
	})
}
