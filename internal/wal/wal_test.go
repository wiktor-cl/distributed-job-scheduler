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

func TestCrashPointReplayAfterEachDurableWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetTermVote(4, "n1"); err != nil {
		t.Fatal(err)
	}
	assertReplay := func(term uint64, votedFor string, entries int, snapshotIndex uint64) {
		t.Helper()
		state, err := Replay(path)
		if err != nil {
			t.Fatal(err)
		}
		if state.CurrentTerm != term || state.VotedFor != votedFor || len(state.Entries) != entries || state.Snapshot.LastIncludedIndex != snapshotIndex {
			t.Fatalf("replay = %+v, entries=%d", state, len(state.Entries))
		}
	}
	assertReplay(4, "n1", 0, 0)
	if err := store.Append(LogEntry{Index: 1, Term: 4, Command: []byte("cmd")}); err != nil {
		t.Fatal(err)
	}
	assertReplay(4, "n1", 1, 0)
	if err := store.SaveSnapshot(Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 4, State: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	assertReplay(4, "n1", 0, 1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
