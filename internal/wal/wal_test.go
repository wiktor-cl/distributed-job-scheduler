package wal

import (
	"path/filepath"
	"testing"
)

func TestReplayRestoresTermVoteEntriesAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetTermVote(3, "n2"); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(LogEntry{Index: 1, Term: 1, Command: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(LogEntry{Index: 2, Term: 2, Command: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := store.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(LogEntry{Index: 2, Term: 3, Command: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, State: []byte("snapshot")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	state, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentTerm != 3 || state.VotedFor != "n2" {
		t.Fatalf("term/vote = %d/%q", state.CurrentTerm, state.VotedFor)
	}
	if len(state.Entries) != 1 || state.Entries[0].Index != 2 || state.Entries[0].Term != 3 {
		t.Fatalf("entries = %+v", state.Entries)
	}
	if state.Snapshot.LastIncludedIndex != 1 || string(state.Snapshot.State) != "snapshot" {
		t.Fatalf("snapshot = %+v", state.Snapshot)
	}
}
