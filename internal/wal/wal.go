package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LogEntry is the durable Raft log payload persisted by Store.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// Snapshot records the latest compacted state-machine image.
type Snapshot struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	State             []byte `json:"state,omitempty"`
}

// State is the complete durable state reconstructed from a WAL.
type State struct {
	CurrentTerm uint64
	VotedFor    string
	Entries     []LogEntry
	Snapshot    Snapshot
}

type record struct {
	Type     string    `json:"type"`
	Term     uint64    `json:"term,omitempty"`
	VotedFor string    `json:"voted_for,omitempty"`
	Entry    *LogEntry `json:"entry,omitempty"`
	Index    uint64    `json:"index,omitempty"`
	Snapshot *Snapshot `json:"snapshot,omitempty"`
}

// Store is an append-only JSONL WAL. It fsyncs every write to keep the crash
// recovery model intentionally strict and easy to defend.
type Store struct {
	mu    sync.Mutex
	path  string
	file  *os.File
	state State
}

// Open loads or creates a WAL at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	state, err := Replay(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Store{path: path, file: file, state: state}, nil
}

// Replay reconstructs durable state from path.
func Replay(path string) (State, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	defer file.Close()

	var state State
	scanner := bufio.NewScanner(file)
	for line := uint64(1); scanner.Scan(); line++ {
		var rec record
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return State{}, fmt.Errorf("replay line %d: %w", line, err)
		}
		switch rec.Type {
		case "term_vote":
			state.CurrentTerm = rec.Term
			state.VotedFor = rec.VotedFor
		case "entry":
			if rec.Entry == nil {
				return State{}, fmt.Errorf("replay line %d: missing entry", line)
			}
			state.Entries = append(state.Entries, *rec.Entry)
		case "truncate_suffix":
			state.Entries = truncateSuffix(state.Entries, rec.Index)
		case "snapshot":
			if rec.Snapshot == nil {
				return State{}, fmt.Errorf("replay line %d: missing snapshot", line)
			}
			state.Snapshot = *rec.Snapshot
			state.Entries = truncatePrefix(state.Entries, rec.Snapshot.LastIncludedIndex)
		default:
			return State{}, fmt.Errorf("replay line %d: unknown record type %q", line, rec.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		return State{}, err
	}
	return state, nil
}

// SetTermVote persists currentTerm and votedFor before callers respond to RPCs.
func (s *Store) SetTermVote(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.write(record{Type: "term_vote", Term: term, VotedFor: votedFor}); err != nil {
		return err
	}
	s.state.CurrentTerm = term
	s.state.VotedFor = votedFor
	return nil
}

// Append persists one log entry.
func (s *Store) Append(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.write(record{Type: "entry", Entry: &entry}); err != nil {
		return err
	}
	s.state.Entries = append(s.state.Entries, entry)
	return nil
}

// TruncateSuffix removes entries with index >= fromIndex after a log conflict.
func (s *Store) TruncateSuffix(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.write(record{Type: "truncate_suffix", Index: fromIndex}); err != nil {
		return err
	}
	s.state.Entries = truncateSuffix(s.state.Entries, fromIndex)
	return nil
}

// SaveSnapshot persists a state-machine snapshot and drops compacted entries.
func (s *Store) SaveSnapshot(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.write(record{Type: "snapshot", Snapshot: &snapshot}); err != nil {
		return err
	}
	s.state.Snapshot = snapshot
	s.state.Entries = truncatePrefix(s.state.Entries, snapshot.LastIncludedIndex)
	return nil
}

// State returns a copy of the reconstructed durable state.
func (s *Store) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneState(s.state)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *Store) write(rec record) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if _, err := s.file.Write(payload); err != nil {
		return err
	}
	return s.file.Sync()
}

func truncateSuffix(entries []LogEntry, fromIndex uint64) []LogEntry {
	out := entries[:0]
	for _, entry := range entries {
		if entry.Index < fromIndex {
			out = append(out, entry)
		}
	}
	return append([]LogEntry(nil), out...)
}

func truncatePrefix(entries []LogEntry, throughIndex uint64) []LogEntry {
	out := entries[:0]
	for _, entry := range entries {
		if entry.Index > throughIndex {
			out = append(out, entry)
		}
	}
	return append([]LogEntry(nil), out...)
}

func cloneState(state State) State {
	state.Entries = append([]LogEntry(nil), state.Entries...)
	state.Snapshot.State = append([]byte(nil), state.Snapshot.State...)
	return state
}
