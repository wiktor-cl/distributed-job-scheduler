package raft

import (
	"bytes"
	"fmt"
	"sort"
)

type NodeID string

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// Entry is a Raft log entry. Indexes are one-based and monotonically increasing.
type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	State             []byte
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	Entries     []Entry
	Snapshot    Snapshot
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

type InstallSnapshotRequest struct {
	Term     uint64
	LeaderID NodeID
	Snapshot Snapshot
}

type InstallSnapshotResponse struct {
	Term              uint64
	LastIncludedIndex uint64
}

type Transport interface {
	SendRequestVote(from, to NodeID, req RequestVoteRequest)
	SendRequestVoteResponse(from, to NodeID, resp RequestVoteResponse)
	SendAppendEntries(from, to NodeID, req AppendEntriesRequest)
	SendAppendEntriesResponse(from, to NodeID, resp AppendEntriesResponse)
	SendInstallSnapshot(from, to NodeID, req InstallSnapshotRequest)
	SendInstallSnapshotResponse(from, to NodeID, resp InstallSnapshotResponse)
}

type NodeConfig struct {
	ElectionTimeoutMin int64
	ElectionTimeoutMax int64
	HeartbeatInterval  int64
	Seed               int64
}

type Storage interface {
	SaveTermVote(term uint64, votedFor NodeID) error
	AppendEntry(entry Entry) error
	TruncateSuffix(fromIndex uint64) error
	SaveSnapshot(snapshot Snapshot) error
	State() PersistentState
}

type MemoryStorage struct {
	state PersistentState
}

func NewMemoryStorage(state PersistentState) *MemoryStorage {
	state.Entries = append([]Entry(nil), state.Entries...)
	state.Snapshot.State = append([]byte(nil), state.Snapshot.State...)
	return &MemoryStorage{state: state}
}

func (s *MemoryStorage) SaveTermVote(term uint64, votedFor NodeID) error {
	s.state.CurrentTerm = term
	s.state.VotedFor = votedFor
	return nil
}

func (s *MemoryStorage) AppendEntry(entry Entry) error {
	s.state.Entries = append(s.state.Entries, cloneEntry(entry))
	return nil
}

func (s *MemoryStorage) TruncateSuffix(fromIndex uint64) error {
	out := s.state.Entries[:0]
	for _, entry := range s.state.Entries {
		if entry.Index < fromIndex {
			out = append(out, entry)
		}
	}
	s.state.Entries = append([]Entry(nil), out...)
	return nil
}

func (s *MemoryStorage) SaveSnapshot(snapshot Snapshot) error {
	s.state.Snapshot = Snapshot{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		State:             append([]byte(nil), snapshot.State...),
	}
	out := s.state.Entries[:0]
	for _, entry := range s.state.Entries {
		if entry.Index > snapshot.LastIncludedIndex {
			out = append(out, entry)
		}
	}
	s.state.Entries = append([]Entry(nil), out...)
	return nil
}

func (s *MemoryStorage) State() PersistentState {
	return clonePersistentState(s.state)
}

func clonePersistentState(state PersistentState) PersistentState {
	state.Entries = cloneEntries(state.Entries)
	state.Snapshot.State = append([]byte(nil), state.Snapshot.State...)
	return state
}

func cloneEntries(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	for i, entry := range entries {
		out[i] = cloneEntry(entry)
	}
	return out
}

func cloneEntry(entry Entry) Entry {
	entry.Command = append([]byte(nil), entry.Command...)
	return entry
}

func sameCommand(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func sortedNodeIDs(ids []NodeID) []NodeID {
	out := append([]NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func majority(n int) int {
	return n/2 + 1
}

func errf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
