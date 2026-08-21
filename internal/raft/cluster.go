package raft

import "fmt"

type Cluster struct {
	ids       []NodeID
	nodes     map[NodeID]*Node
	stores    map[NodeID]*MemoryStorage
	alive     map[NodeID]bool
	blocked   map[[2]NodeID]bool
	leaders   map[uint64]NodeID
	committed map[uint64]Entry
}

func NewCluster(ids []NodeID) *Cluster {
	ids = sortedNodeIDs(ids)
	cluster := &Cluster{
		ids:       ids,
		nodes:     map[NodeID]*Node{},
		stores:    map[NodeID]*MemoryStorage{},
		alive:     map[NodeID]bool{},
		blocked:   map[[2]NodeID]bool{},
		leaders:   map[uint64]NodeID{},
		committed: map[uint64]Entry{},
	}
	for _, id := range ids {
		store := NewMemoryStorage(PersistentState{})
		cluster.stores[id] = store
		cluster.nodes[id] = NewNode(id, ids, store)
		cluster.alive[id] = true
	}
	return cluster
}

func (c *Cluster) Nodes() map[NodeID]*Node {
	out := make(map[NodeID]*Node, len(c.nodes))
	for id, node := range c.nodes {
		out[id] = node
	}
	return out
}

func (c *Cluster) Committed() map[uint64]Entry {
	out := make(map[uint64]Entry, len(c.committed))
	for index, entry := range c.committed {
		out[index] = cloneEntry(entry)
	}
	return out
}

func (c *Cluster) Elect(candidateID NodeID) (bool, error) {
	candidate, ok := c.nodes[candidateID]
	if !ok || !c.alive[candidateID] {
		return false, fmt.Errorf("candidate %s unavailable", candidateID)
	}
	req, err := candidate.StartElection()
	if err != nil {
		return false, err
	}
	votes := 1
	for _, id := range c.ids {
		if id == candidateID || !c.alive[id] || !c.canSend(candidateID, id) {
			continue
		}
		resp, err := c.nodes[id].RequestVote(req)
		if err != nil {
			return false, err
		}
		if resp.Term > candidate.CurrentTerm() {
			return false, candidate.StepDown(resp.Term)
		}
		if resp.VoteGranted {
			votes++
		}
	}
	if votes >= majority(len(c.ids)) {
		candidate.BecomeLeader()
		if existing, ok := c.leaders[candidate.CurrentTerm()]; ok && existing != candidateID {
			return false, fmt.Errorf("election safety violated: leaders %s and %s in term %d", existing, candidateID, candidate.CurrentTerm())
		}
		c.leaders[candidate.CurrentTerm()] = candidateID
		return true, nil
	}
	return false, nil
}

func (c *Cluster) Leader() (*Node, bool) {
	for _, id := range c.ids {
		if c.alive[id] && c.nodes[id].Role() == Leader {
			return c.nodes[id], true
		}
	}
	return nil, false
}

func (c *Cluster) Propose(command []byte) (Entry, bool, error) {
	leader, ok := c.Leader()
	if !ok {
		return Entry{}, false, fmt.Errorf("no leader")
	}
	entry, err := leader.Propose(command)
	if err != nil {
		return Entry{}, false, err
	}
	replicas := 1
	for _, id := range c.ids {
		if id == leader.ID() || !c.alive[id] || !c.canSend(leader.ID(), id) {
			continue
		}
		prevIndex := entry.Index - 1
		prevTerm := uint64(0)
		if prevIndex > 0 {
			prev, ok := leader.entryAt(prevIndex)
			if !ok {
				return Entry{}, false, fmt.Errorf("leader missing previous entry %d", prevIndex)
			}
			prevTerm = prev.Term
		}
		resp, err := c.nodes[id].AppendEntries(AppendEntriesRequest{
			Term:         leader.CurrentTerm(),
			LeaderID:     leader.ID(),
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      []Entry{entry},
			LeaderCommit: leader.CommitIndex(),
		})
		if err != nil {
			return Entry{}, false, err
		}
		if resp.Term > leader.CurrentTerm() {
			return Entry{}, false, leader.StepDown(resp.Term)
		}
		if resp.Success {
			replicas++
		}
	}
	if replicas >= majority(len(c.ids)) && entry.Term == leader.CurrentTerm() {
		leader.CommitThrough(entry.Index)
		c.committed[entry.Index] = cloneEntry(entry)
		c.replicateCommit(leader)
		return entry, true, nil
	}
	return entry, false, nil
}

func (c *Cluster) ReplicateAll() error {
	leader, ok := c.Leader()
	if !ok {
		return fmt.Errorf("no leader")
	}
	for _, id := range c.ids {
		if id == leader.ID() || !c.alive[id] || !c.canSend(leader.ID(), id) {
			continue
		}
		if err := c.syncFollower(leader, c.nodes[id]); err != nil {
			return err
		}
	}
	c.replicateCommit(leader)
	return nil
}

func (c *Cluster) CompactLeader(index uint64, state []byte) error {
	leader, ok := c.Leader()
	if !ok {
		return fmt.Errorf("no leader")
	}
	return leader.Compact(index, state)
}

func (c *Cluster) InstallSnapshots() error {
	leader, ok := c.Leader()
	if !ok {
		return fmt.Errorf("no leader")
	}
	for _, id := range c.ids {
		if id == leader.ID() || !c.alive[id] || !c.canSend(leader.ID(), id) {
			continue
		}
		if _, err := c.nodes[id].InstallSnapshot(InstallSnapshotRequest{
			Term:     leader.CurrentTerm(),
			LeaderID: leader.ID(),
			Snapshot: leader.Snapshot(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) Kill(id NodeID) {
	c.alive[id] = false
	if node := c.nodes[id]; node != nil && node.Role() == Leader {
		node.role = Follower
	}
}

func (c *Cluster) Restart(id NodeID) {
	c.alive[id] = true
	c.nodes[id] = NewNode(id, c.ids, c.stores[id])
}

func (c *Cluster) Partition(a, b NodeID) {
	c.blocked[[2]NodeID{a, b}] = true
	c.blocked[[2]NodeID{b, a}] = true
}

func (c *Cluster) AsymmetricPartition(from, to NodeID) {
	c.blocked[[2]NodeID{from, to}] = true
}

func (c *Cluster) Heal() {
	c.blocked = map[[2]NodeID]bool{}
}

func (c *Cluster) canSend(from, to NodeID) bool {
	return !c.blocked[[2]NodeID{from, to}]
}

func (c *Cluster) syncFollower(leader, follower *Node) error {
	if leader.Snapshot().LastIncludedIndex > 0 &&
		follower.LastLogIndex() < leader.Snapshot().LastIncludedIndex {
		_, err := follower.InstallSnapshot(InstallSnapshotRequest{
			Term:     leader.CurrentTerm(),
			LeaderID: leader.ID(),
			Snapshot: leader.Snapshot(),
		})
		return err
	}
	prevIndex := leader.Snapshot().LastIncludedIndex
	prevTerm := leader.Snapshot().LastIncludedTerm
	entries := leader.Entries()
	if len(entries) > 0 {
		prevIndex = entries[0].Index - 1
		if prevIndex > 0 {
			prev, ok := leader.entryAt(prevIndex)
			if !ok {
				return fmt.Errorf("missing leader prev entry %d", prevIndex)
			}
			prevTerm = prev.Term
		}
	}
	resp, err := follower.AppendEntries(AppendEntriesRequest{
		Term:         leader.CurrentTerm(),
		LeaderID:     leader.ID(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: leader.CommitIndex(),
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		// The deterministic cluster retries with an empty prev index for tests that
		// intentionally create conflicting prefixes.
		_, err = follower.AppendEntries(AppendEntriesRequest{
			Term:         leader.CurrentTerm(),
			LeaderID:     leader.ID(),
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      entries,
			LeaderCommit: leader.CommitIndex(),
		})
	}
	return err
}

func (c *Cluster) replicateCommit(leader *Node) {
	for _, id := range c.ids {
		if id == leader.ID() || !c.alive[id] || !c.canSend(leader.ID(), id) {
			continue
		}
		node := c.nodes[id]
		prevIndex := node.LastLogIndex()
		prevTerm := uint64(0)
		if prevIndex > 0 {
			if prev, ok := node.entryAt(prevIndex); ok {
				prevTerm = prev.Term
			}
		}
		_, _ = node.AppendEntries(AppendEntriesRequest{
			Term:         leader.CurrentTerm(),
			LeaderID:     leader.ID(),
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: leader.CommitIndex(),
		})
	}
}
