package raft

import "fmt"

type Node struct {
	id      NodeID
	peers   []NodeID
	storage Storage

	role        Role
	currentTerm uint64
	votedFor    NodeID
	log         []Entry
	snapshot    Snapshot

	commitIndex uint64
	lastApplied uint64
	applied     map[uint64]Entry
}

func NewNode(id NodeID, peers []NodeID, storage Storage) *Node {
	state := storage.State()
	return &Node{
		id:          id,
		peers:       sortedNodeIDs(peers),
		storage:     storage,
		role:        Follower,
		currentTerm: state.CurrentTerm,
		votedFor:    state.VotedFor,
		log:         cloneEntries(state.Entries),
		snapshot:    state.Snapshot,
		commitIndex: state.Snapshot.LastIncludedIndex,
		lastApplied: state.Snapshot.LastIncludedIndex,
		applied:     map[uint64]Entry{},
	}
}

func (n *Node) ID() NodeID                       { return n.id }
func (n *Node) Role() Role                       { return n.role }
func (n *Node) CurrentTerm() uint64              { return n.currentTerm }
func (n *Node) VotedFor() NodeID                 { return n.votedFor }
func (n *Node) CommitIndex() uint64              { return n.commitIndex }
func (n *Node) LastApplied() uint64              { return n.lastApplied }
func (n *Node) Entries() []Entry                 { return cloneEntries(n.log) }
func (n *Node) Snapshot() Snapshot               { return n.snapshot }
func (n *Node) PersistentState() PersistentState { return n.storage.State() }

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

func (n *Node) StartElection() (RequestVoteRequest, error) {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	if err := n.storage.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
		return RequestVoteRequest{}, err
	}
	return RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.LastLogIndex(),
		LastLogTerm:  n.LastLogTerm(),
	}, nil
}

func (n *Node) BecomeLeader() {
	n.role = Leader
}

func (n *Node) StepDown(term uint64) error {
	if term < n.currentTerm {
		return nil
	}
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	return n.storage.SaveTermVote(n.currentTerm, n.votedFor)
}

func (n *Node) RequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm {
		if err := n.StepDown(req.Term); err != nil {
			return RequestVoteResponse{}, err
		}
	}
	upToDate := req.LastLogTerm > n.LastLogTerm() ||
		(req.LastLogTerm == n.LastLogTerm() && req.LastLogIndex >= n.LastLogIndex())
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	if canVote && upToDate {
		n.votedFor = req.CandidateID
		if err := n.storage.SaveTermVote(n.currentTerm, n.votedFor); err != nil {
			return RequestVoteResponse{}, err
		}
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
	}
	return RequestVoteResponse{Term: n.currentTerm}, nil
}

func (n *Node) AppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.StepDown(req.Term); err != nil {
			return AppendEntriesResponse{}, err
		}
	}
	if !n.hasEntryAt(req.PrevLogIndex, req.PrevLogTerm) {
		return AppendEntriesResponse{Term: n.currentTerm}, nil
	}
	for i, entry := range req.Entries {
		if existing, ok := n.entryAt(entry.Index); ok {
			if existing.Term == entry.Term {
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
		}
		break
	}
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.LastLogIndex())
		n.applyCommitted()
	}
	return AppendEntriesResponse{Term: n.currentTerm, Success: true}, nil
}

func (n *Node) InstallSnapshot(req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	if req.Term < n.currentTerm {
		return InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if req.Term > n.currentTerm || n.role != Follower {
		if err := n.StepDown(req.Term); err != nil {
			return InstallSnapshotResponse{}, err
		}
	}
	if req.Snapshot.LastIncludedIndex <= n.snapshot.LastIncludedIndex {
		return InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if err := n.storage.SaveSnapshot(req.Snapshot); err != nil {
		return InstallSnapshotResponse{}, err
	}
	n.snapshot = req.Snapshot
	n.log = n.truncatePrefix(req.Snapshot.LastIncludedIndex)
	n.commitIndex = max(n.commitIndex, req.Snapshot.LastIncludedIndex)
	n.lastApplied = max(n.lastApplied, req.Snapshot.LastIncludedIndex)
	return InstallSnapshotResponse{Term: n.currentTerm}, nil
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
	return entry, nil
}

func (n *Node) CommitThrough(index uint64) {
	n.commitIndex = min(index, n.LastLogIndex())
	n.applyCommitted()
}

func (n *Node) Compact(index uint64, state []byte) error {
	if index <= n.snapshot.LastIncludedIndex {
		return nil
	}
	entry, ok := n.entryAt(index)
	if !ok {
		return fmt.Errorf("cannot compact missing index %d", index)
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
