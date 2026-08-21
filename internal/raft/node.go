package raft

import (
	"fmt"
	"math/rand"
)

type Node struct {
	id        NodeID
	peers     []NodeID
	storage   Storage
	transport Transport
	fsm       StateMachine
	rng       *rand.Rand
	cfg       NodeConfig

	role        Role
	currentTerm uint64
	votedFor    NodeID
	log         []Entry
	snapshot    Snapshot

	commitIndex uint64
	lastApplied uint64
	applied     map[uint64]Entry
	applyError  error

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	votes      map[NodeID]bool

	electionDeadline int64
	heartbeatDue     int64
	knownLeader      NodeID
	leaderChanges    uint64
	elections        uint64
}

func NewNode(id NodeID, peers []NodeID, storage Storage) *Node {
	return NewNodeWithConfig(id, peers, storage, nil, NodeConfig{})
}

func NewNodeWithConfig(id NodeID, peers []NodeID, storage Storage, transport Transport, cfg NodeConfig) *Node {
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 150
	}
	if cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 150
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50
	}
	state := storage.State()
	if cfg.StateMachine != nil && len(state.Snapshot.State) > 0 {
		_ = cfg.StateMachine.Restore(state.Snapshot.State)
	}
	n := &Node{
		id:          id,
		peers:       sortedNodeIDs(peers),
		storage:     storage,
		transport:   transport,
		fsm:         cfg.StateMachine,
		rng:         rand.New(rand.NewSource(cfg.Seed)),
		cfg:         cfg,
		role:        Follower,
		currentTerm: state.CurrentTerm,
		votedFor:    state.VotedFor,
		log:         cloneEntries(state.Entries),
		snapshot:    state.Snapshot,
		commitIndex: state.Snapshot.LastIncludedIndex,
		lastApplied: state.Snapshot.LastIncludedIndex,
		applied:     map[uint64]Entry{},
		nextIndex:   map[NodeID]uint64{},
		matchIndex:  map[NodeID]uint64{},
		votes:       map[NodeID]bool{},
	}
	n.resetElectionDeadline(0)
	return n
}

func (n *Node) SetTransport(transport Transport) { n.transport = transport }
func (n *Node) StateMachine() StateMachine       { return n.fsm }
func (n *Node) ApplyError() error                { return n.applyError }

func (n *Node) ID() NodeID                       { return n.id }
func (n *Node) Role() Role                       { return n.role }
func (n *Node) CurrentTerm() uint64              { return n.currentTerm }
func (n *Node) VotedFor() NodeID                 { return n.votedFor }
func (n *Node) CommitIndex() uint64              { return n.commitIndex }
func (n *Node) LastApplied() uint64              { return n.lastApplied }
func (n *Node) Entries() []Entry                 { return cloneEntries(n.log) }
func (n *Node) Snapshot() Snapshot               { return n.snapshot }
func (n *Node) ElectionDeadline() int64          { return n.electionDeadline }
func (n *Node) KnownLeader() NodeID              { return n.knownLeader }
func (n *Node) LeaderChanges() uint64            { return n.leaderChanges }
func (n *Node) Elections() uint64                { return n.elections }
func (n *Node) PersistentState() PersistentState { return n.storage.State() }

func (n *Node) NextIndex() map[NodeID]uint64 {
	out := make(map[NodeID]uint64, len(n.nextIndex))
	for id, index := range n.nextIndex {
		out[id] = index
	}
	return out
}

func (n *Node) MatchIndex() map[NodeID]uint64 {
	out := make(map[NodeID]uint64, len(n.matchIndex))
	for id, index := range n.matchIndex {
		out[id] = index
	}
	return out
}

func (n *Node) LastLogIndex() uint64 {
	if len(n.log) == 0 {
		return n.snapshot.LastIncludedIndex
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) LastLogTerm() uint64 {
	if len(n.log) == 0 {
		return n.snapshot.LastIncludedTerm
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) Tick(now int64) error {
	switch n.role {
	case Leader:
		if now >= n.heartbeatDue {
			n.broadcastAppendEntries()
			n.heartbeatDue = now + n.cfg.HeartbeatInterval
		}
	default:
		if now >= n.electionDeadline {
			return n.startElection(now)
		}
	}
	return nil
}

func (n *Node) StartElection() (RequestVoteRequest, error) {
	if err := n.startElection(0); err != nil {
		return RequestVoteRequest{}, err
	}
	return RequestVoteRequest{Term: n.currentTerm, CandidateID: n.id, LastLogIndex: n.LastLogIndex(), LastLogTerm: n.LastLogTerm()}, nil
}

func (n *Node) BecomeLeader() {
	n.becomeLeader(0)
}

func (n *Node) StepDown(term uint64, at ...int64) error {
	now := int64(0)
	if len(at) > 0 {
		now = at[0]
	}
	if term < n.currentTerm {
		return nil
	}
	if n.role == Leader && term > n.currentTerm {
		n.leaderChanges++
	}
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.knownLeader = ""
	n.votes = map[NodeID]bool{}
	n.resetElectionDeadline(now)
	return n.storage.SaveTermVote(n.currentTerm, n.votedFor)
}

func (n *Node) RequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	return n.handleRequestVote(req, 0)
}

func (n *Node) HandleRequestVote(from NodeID, req RequestVoteRequest, now int64) error {
	resp, err := n.handleRequestVote(req, now)
	if err != nil {
		return err
	}
	if n.transport != nil {
		n.transport.SendRequestVoteResponse(n.id, from, resp)
	}
	return nil
}

func (n *Node) HandleRequestVoteResponse(from NodeID, resp RequestVoteResponse, now int64) error {
	if resp.Term > n.currentTerm {
		return n.StepDown(resp.Term, now)
	}
	if n.role != Candidate || resp.Term != n.currentTerm || !resp.VoteGranted {
		return nil
	}
	n.votes[from] = true
	if len(n.votes) >= majority(len(n.peers)) {
		n.becomeLeader(now)
	}
	return nil
}

func (n *Node) AppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return n.handleAppendEntries(req, 0)
}

func (n *Node) HandleAppendEntries(from NodeID, req AppendEntriesRequest, now int64) error {
	resp, err := n.handleAppendEntries(req, now)
	if err != nil {
		return err
	}
	if n.transport != nil {
		n.transport.SendAppendEntriesResponse(n.id, from, resp)
	}
	return nil
}

func (n *Node) HandleAppendEntriesResponse(from NodeID, resp AppendEntriesResponse, at ...int64) error {
	now := int64(0)
	if len(at) > 0 {
		now = at[0]
	}
	if resp.Term > n.currentTerm {
		return n.StepDown(resp.Term, now)
	}
	if n.role != Leader || resp.Term != n.currentTerm {
		return nil
	}
	if resp.Success {
		n.matchIndex[from] = max(n.matchIndex[from], resp.MatchIndex)
		n.nextIndex[from] = n.matchIndex[from] + 1
		n.advanceCommitIndex()
		return nil
	}
	if n.nextIndex[from] > n.snapshot.LastIncludedIndex+1 {
		n.nextIndex[from]--
	} else if n.snapshot.LastIncludedIndex > 0 {
		n.nextIndex[from] = n.snapshot.LastIncludedIndex
	}
	n.sendAppendEntries(from)
	return nil
}

func (n *Node) InstallSnapshot(req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return n.handleInstallSnapshot(req, 0)
}

func (n *Node) HandleInstallSnapshot(from NodeID, req InstallSnapshotRequest, now int64) error {
	resp, err := n.handleInstallSnapshot(req, now)
	if err != nil {
		return err
	}
	if n.transport != nil {
		n.transport.SendInstallSnapshotResponse(n.id, from, resp)
	}
	return nil
}

func (n *Node) HandleInstallSnapshotResponse(from NodeID, resp InstallSnapshotResponse, at ...int64) error {
	now := int64(0)
	if len(at) > 0 {
		now = at[0]
	}
	if resp.Term > n.currentTerm {
		return n.StepDown(resp.Term, now)
	}
	if n.role != Leader || resp.Term != n.currentTerm {
		return nil
	}
	n.matchIndex[from] = max(n.matchIndex[from], resp.LastIncludedIndex)
	n.nextIndex[from] = n.matchIndex[from] + 1
	n.sendAppendEntries(from)
	return nil
}

func (n *Node) Propose(command []byte) (Entry, error) {
	if n.role != Leader {
		return Entry{}, fmt.Errorf("node %s is %s, not leader", n.id, n.role)
	}
	entry := Entry{Index: n.LastLogIndex() + 1, Term: n.currentTerm, Command: append([]byte(nil), command...)}
	if err := n.storage.AppendEntry(entry); err != nil {
		return Entry{}, err
	}
	n.log = append(n.log, cloneEntry(entry))
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1
	n.broadcastAppendEntries()
	return entry, nil
}

func (n *Node) CommitThrough(index uint64) {
	n.commitIndex = min(index, n.LastLogIndex())
	n.applyCommitted()
}

func (n *Node) Compact(index uint64) error {
	if index <= n.snapshot.LastIncludedIndex {
		return nil
	}
	entry, ok := n.entryAt(index)
	if !ok {
		return fmt.Errorf("cannot compact missing index %d", index)
	}
	var state []byte
	var err error
	if n.fsm != nil {
		state, err = n.fsm.Snapshot()
		if err != nil {
			return err
		}
	}
	snapshot := Snapshot{LastIncludedIndex: index, LastIncludedTerm: entry.Term, State: append([]byte(nil), state...)}
	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		return err
	}
	n.snapshot = snapshot
	n.log = n.truncatePrefix(index)
	return nil
}

func (n *Node) AppliedEntries() map[uint64]Entry {
	out := make(map[uint64]Entry, len(n.applied))
	for index, entry := range n.applied {
		out[index] = cloneEntry(entry)
	}
	return out
}

func (n *Node) handleRequestVote(req RequestVoteRequest, now int64) (RequestVoteResponse, error) {
	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm {
		if err := n.StepDown(req.Term, now); err != nil {
			return RequestVoteResponse{}, err
		}
	}
	upToDate := req.LastLogTerm > n.LastLogTerm() ||
		(req.LastLogTerm == n.LastLogTerm() && req.LastLogIndex >= n.LastLogIndex())
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	if canVote && upToDate {
		n.votedFor = req.CandidateID
		n.resetElectionDeadline(now)
		if err := n.storage.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			return RequestVoteResponse{}, err
		}
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}
	return RequestVoteResponse{Term: n.currentTerm}, nil
}

func (n *Node) handleAppendEntries(req AppendEntriesRequest, now int64) (AppendEntriesResponse, error) {
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.StepDown(req.Term, now); err != nil {
			return AppendEntriesResponse{}, err
		}
	}
	n.knownLeader = req.LeaderID
	n.resetElectionDeadline(now)
	if !n.hasEntryAt(req.PrevLogIndex, req.PrevLogTerm) {
		return AppendEntriesResponse{Term: n.currentTerm, MatchIndex: n.LastLogIndex()}, nil
	}
	matchIndex := req.PrevLogIndex
	for i, entry := range req.Entries {
		if existing, ok := n.entryAt(entry.Index); ok {
			if existing.Term == entry.Term {
				matchIndex = entry.Index
				continue
			}
			if err := n.storage.TruncateSuffix(entry.Index); err != nil {
				return AppendEntriesResponse{}, err
			}
			n.log = n.truncateSuffix(entry.Index)
		}
		for _, appendEntry := range req.Entries[i:] {
			if appendEntry.Index <= n.snapshot.LastIncludedIndex {
				continue
			}
			if err := n.storage.AppendEntry(appendEntry); err != nil {
				return AppendEntriesResponse{}, err
			}
			n.log = append(n.log, cloneEntry(appendEntry))
			matchIndex = appendEntry.Index
		}
		break
	}
	if len(req.Entries) > 0 {
		matchIndex = req.Entries[len(req.Entries)-1].Index
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.LastLogIndex())
		n.applyCommitted()
	}
	return AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: matchIndex}, nil
}

func (n *Node) handleInstallSnapshot(req InstallSnapshotRequest, now int64) (InstallSnapshotResponse, error) {
	if req.Term < n.currentTerm {
		return InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.StepDown(req.Term, now); err != nil {
			return InstallSnapshotResponse{}, err
		}
	}
	n.knownLeader = req.LeaderID
	n.resetElectionDeadline(now)
	if req.Snapshot.LastIncludedIndex <= n.snapshot.LastIncludedIndex {
		return InstallSnapshotResponse{Term: n.currentTerm, LastIncludedIndex: n.snapshot.LastIncludedIndex}, nil
	}
	if err := n.storage.SaveSnapshot(req.Snapshot); err != nil {
		return InstallSnapshotResponse{}, err
	}
	if n.fsm != nil {
		if err := n.fsm.Restore(req.Snapshot.State); err != nil {
			return InstallSnapshotResponse{}, err
		}
	}
	n.snapshot = req.Snapshot
	n.log = n.truncatePrefix(req.Snapshot.LastIncludedIndex)
	n.commitIndex = max(n.commitIndex, req.Snapshot.LastIncludedIndex)
	n.lastApplied = max(n.lastApplied, req.Snapshot.LastIncludedIndex)
	return InstallSnapshotResponse{Term: n.currentTerm, LastIncludedIndex: req.Snapshot.LastIncludedIndex}, nil
}

func (n *Node) startElection(now int64) error {
	n.role = Candidate
	n.currentTerm++
	n.elections++
	n.votedFor = n.id
	n.knownLeader = ""
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionDeadline(now)
	if err := n.storage.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		return err
	}
	req := RequestVoteRequest{Term: n.currentTerm, CandidateID: n.id, LastLogIndex: n.LastLogIndex(), LastLogTerm: n.LastLogTerm()}
	for _, peer := range n.peers {
		if peer != n.id && n.transport != nil {
			n.transport.SendRequestVote(n.id, peer, req)
		}
	}
	if len(n.votes) >= majority(len(n.peers)) {
		n.becomeLeader(now)
	}
	return nil
}

func (n *Node) becomeLeader(now int64) {
	if n.role == Leader {
		return
	}
	n.role = Leader
	n.knownLeader = n.id
	n.leaderChanges++
	lastNext := n.LastLogIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastNext
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = n.LastLogIndex()
	n.nextIndex[n.id] = lastNext
	n.heartbeatDue = now
	n.broadcastAppendEntries()
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		if peer != n.id {
			n.sendAppendEntries(peer)
		}
	}
}

func (n *Node) sendAppendEntries(peer NodeID) {
	if n.transport == nil {
		return
	}
	next := n.nextIndex[peer]
	if next == 0 {
		next = n.LastLogIndex() + 1
		n.nextIndex[peer] = next
	}
	if n.snapshot.LastIncludedIndex > 0 && next <= n.snapshot.LastIncludedIndex {
		n.transport.SendInstallSnapshot(n.id, peer, InstallSnapshotRequest{Term: n.currentTerm, LeaderID: n.id, Snapshot: n.snapshot})
		return
	}
	prevIndex := next - 1
	prevTerm := uint64(0)
	if prevIndex > 0 {
		prev, ok := n.entryAt(prevIndex)
		if !ok {
			n.nextIndex[peer] = max(n.snapshot.LastIncludedIndex+1, next-1)
			return
		}
		prevTerm = prev.Term
	}
	n.transport.SendAppendEntries(n.id, peer, AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      n.entriesFrom(next),
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) advanceCommitIndex() {
	for index := n.LastLogIndex(); index > n.commitIndex; index-- {
		entry, ok := n.entryAt(index)
		if !ok || entry.Term != n.currentTerm {
			continue
		}
		replicas := 0
		for _, peer := range n.peers {
			if peer == n.id {
				if n.LastLogIndex() >= index {
					replicas++
				}
				continue
			}
			if n.matchIndex[peer] >= index {
				replicas++
			}
		}
		if replicas >= majority(len(n.peers)) {
			n.commitIndex = index
			n.applyCommitted()
			n.broadcastAppendEntries()
			return
		}
	}
}

func (n *Node) entriesFrom(index uint64) []Entry {
	var entries []Entry
	for _, entry := range n.log {
		if entry.Index >= index {
			entries = append(entries, cloneEntry(entry))
		}
	}
	return entries
}

func (n *Node) resetElectionDeadline(now int64) {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin + 1
	if span <= 0 {
		span = 1
	}
	n.electionDeadline = now + n.cfg.ElectionTimeoutMin + n.rng.Int63n(span)
}

func (n *Node) entryAt(index uint64) (Entry, bool) {
	if index == n.snapshot.LastIncludedIndex {
		return Entry{Index: index, Term: n.snapshot.LastIncludedTerm}, true
	}
	for _, entry := range n.log {
		if entry.Index == index {
			return cloneEntry(entry), true
		}
	}
	return Entry{}, false
}

func (n *Node) hasEntryAt(index, term uint64) bool {
	if index == 0 && term == 0 {
		return true
	}
	entry, ok := n.entryAt(index)
	return ok && entry.Term == term
}

func (n *Node) truncateSuffix(fromIndex uint64) []Entry {
	out := n.log[:0]
	for _, entry := range n.log {
		if entry.Index < fromIndex {
			out = append(out, entry)
		}
	}
	return append([]Entry(nil), out...)
}

func (n *Node) truncatePrefix(throughIndex uint64) []Entry {
	out := n.log[:0]
	for _, entry := range n.log {
		if entry.Index > throughIndex {
			out = append(out, entry)
		}
	}
	return append([]Entry(nil), out...)
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, ok := n.entryAt(n.lastApplied)
		if ok {
			n.applied[n.lastApplied] = entry
			if n.fsm != nil {
				if err := n.fsm.Apply(entry.Command); err != nil {
					n.applyError = err
					return
				}
			}
		}
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
