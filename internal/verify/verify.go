package verify

import (
	"bytes"
	"fmt"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
)

type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) Error() string {
	return fmt.Sprintf("%s: %s", v.Invariant, v.Detail)
}

func ElectionSafety(nodes map[raft.NodeID]*raft.Node) error {
	leaders := map[uint64]raft.NodeID{}
	for id, node := range nodes {
		if node.Role() != raft.Leader {
			continue
		}
		term := node.CurrentTerm()
		if existing, ok := leaders[term]; ok && existing != id {
			return Violation{Invariant: "Election Safety", Detail: fmt.Sprintf("leaders %s and %s in term %d", existing, id, term)}
		}
		leaders[term] = id
	}
	return nil
}

func LogMatching(nodes map[raft.NodeID]*raft.Node) error {
	for aID, a := range nodes {
		for bID, b := range nodes {
			if aID >= bID {
				continue
			}
			aEntries := a.Entries()
			bEntries := b.Entries()
			if err := snapshotBoundaryCompatible(aID, a, bID, b); err != nil {
				return err
			}
			for _, ae := range aEntries {
				for _, be := range bEntries {
					if ae.Index != be.Index || ae.Term != be.Term {
						continue
					}
					floor := maxUint64(a.Snapshot().LastIncludedIndex, b.Snapshot().LastIncludedIndex)
					if err := prefixesEqual(aEntries, bEntries, floor, ae.Index); err != nil {
						return Violation{Invariant: "Log Matching", Detail: fmt.Sprintf("%s/%s index %d: %v", aID, bID, ae.Index, err)}
					}
				}
			}
		}
	}
	return nil
}

func StateMachineSafety(nodes map[raft.NodeID]*raft.Node) error {
	applied := map[uint64]raft.Entry{}
	for id, node := range nodes {
		for index, entry := range node.AppliedEntries() {
			if existing, ok := applied[index]; ok {
				if existing.Term != entry.Term || !bytes.Equal(existing.Command, entry.Command) {
					return Violation{Invariant: "State Machine Safety", Detail: fmt.Sprintf("node %s applied different entry at index %d", id, index)}
				}
				continue
			}
			applied[index] = entry
		}
	}
	return nil
}

func LeaderCompleteness(nodes map[raft.NodeID]*raft.Node, committed map[uint64]raft.Entry) error {
	for _, committedEntry := range committed {
		for id, node := range nodes {
			if node.Role() != raft.Leader || node.CurrentTerm() < committedEntry.Term {
				continue
			}
			found := false
			for _, entry := range node.Entries() {
				if entry.Index == committedEntry.Index && entry.Term == committedEntry.Term && bytes.Equal(entry.Command, committedEntry.Command) {
					found = true
					break
				}
			}
			if !found && committedEntry.Index > node.Snapshot().LastIncludedIndex {
				return Violation{Invariant: "Leader Completeness", Detail: fmt.Sprintf("leader %s lacks committed entry %d", id, committedEntry.Index)}
			}
			if !found && committedEntry.Index == node.Snapshot().LastIncludedIndex && committedEntry.Term != node.Snapshot().LastIncludedTerm {
				return Violation{Invariant: "Leader Completeness", Detail: fmt.Sprintf("leader %s snapshot boundary term %d differs from committed entry %d term %d", id, node.Snapshot().LastIncludedTerm, committedEntry.Index, committedEntry.Term)}
			}
		}
	}
	return nil
}

func CommittedEntryDurability(nodes map[raft.NodeID]*raft.Node, committed map[uint64]raft.Entry) error {
	for _, committedEntry := range committed {
		for id, node := range nodes {
			if node.CommitIndex() < committedEntry.Index {
				continue
			}
			if committedEntry.Index < node.Snapshot().LastIncludedIndex {
				continue
			}
			if committedEntry.Index == node.Snapshot().LastIncludedIndex {
				if committedEntry.Term != node.Snapshot().LastIncludedTerm {
					return Violation{Invariant: "Committed Entry Durability", Detail: fmt.Sprintf("node %s snapshot boundary term %d differs from committed entry %d term %d", id, node.Snapshot().LastIncludedTerm, committedEntry.Index, committedEntry.Term)}
				}
				continue
			}
			found := false
			for _, entry := range node.Entries() {
				if entry.Index != committedEntry.Index {
					continue
				}
				found = true
				if entry.Term != committedEntry.Term || !bytes.Equal(entry.Command, committedEntry.Command) {
					return Violation{Invariant: "Committed Entry Durability", Detail: fmt.Sprintf("node %s has different entry at committed index %d", id, committedEntry.Index)}
				}
				break
			}
			if !found {
				return Violation{Invariant: "Committed Entry Durability", Detail: fmt.Sprintf("node %s is committed through %d but lacks entry %d", id, node.CommitIndex(), committedEntry.Index)}
			}
		}
	}
	return nil
}

func ReplicatedSchedulerStateEquality(nodes map[raft.NodeID]*raft.Node, schedulers map[raft.NodeID]*scheduler.StateMachine) error {
	byApplied := map[uint64]string{}
	for id, node := range nodes {
		sm := schedulers[id]
		if sm == nil {
			continue
		}
		fingerprint := sm.Fingerprint()
		if existing, ok := byApplied[node.LastApplied()]; ok && existing != fingerprint {
			return Violation{Invariant: "Replicated Scheduler State Equality", Detail: fmt.Sprintf("node %s differs at lastApplied %d", id, node.LastApplied())}
		}
		byApplied[node.LastApplied()] = fingerprint
	}
	return nil
}

func RaftCluster(nodes map[raft.NodeID]*raft.Node, committed map[uint64]raft.Entry) error {
	for _, check := range []func() error{
		func() error { return ElectionSafety(nodes) },
		func() error { return LogMatching(nodes) },
		func() error { return StateMachineSafety(nodes) },
		func() error { return LeaderCompleteness(nodes, committed) },
		func() error { return CommittedEntryDurability(nodes, committed) },
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func JobInvariants(jobs map[string]scheduler.Job) error {
	for id, job := range jobs {
		if job.FencingToken == 0 && job.Owner != "" {
			return Violation{Invariant: "Job Fencing Token", Detail: fmt.Sprintf("%s has owner without token", id)}
		}
		if (job.Status == scheduler.Completed || job.Status == scheduler.DLQ) && job.Owner != "" {
			return Violation{Invariant: "Terminal Ownership", Detail: fmt.Sprintf("%s is terminal but owned by %s", id, job.Owner)}
		}
		if job.Owner == "" && job.LeaseUntil != 0 {
			return Violation{Invariant: "Lease Ownership", Detail: fmt.Sprintf("%s has lease deadline without owner", id)}
		}
	}
	return nil
}

type HistoryEvent struct {
	Time      int64
	JobID     string
	Owner     string
	Token     uint64
	Operation string
}

func History(events []HistoryEvent) error {
	highestToken := map[string]uint64{}
	activeOwner := map[string]string{}
	for _, event := range events {
		switch event.Operation {
		case "claim":
			if event.Token <= highestToken[event.JobID] {
				return Violation{Invariant: "Monotonic Fencing Token", Detail: fmt.Sprintf("%s token %d after %d", event.JobID, event.Token, highestToken[event.JobID])}
			}
			highestToken[event.JobID] = event.Token
			activeOwner[event.JobID] = event.Owner
		case "expire", "complete", "fail":
			delete(activeOwner, event.JobID)
		}
		if owner := activeOwner[event.JobID]; owner != "" && event.Owner != "" && owner != event.Owner {
			return Violation{Invariant: "Mutual Exclusion", Detail: fmt.Sprintf("%s active owner %s conflicts with %s", event.JobID, owner, event.Owner)}
		}
	}
	return nil
}

func prefixesEqual(a, b []raft.Entry, after, through uint64) error {
	byIndex := map[uint64]raft.Entry{}
	for _, entry := range a {
		if entry.Index > after && entry.Index <= through {
			byIndex[entry.Index] = entry
		}
	}
	for _, entry := range b {
		if entry.Index <= after || entry.Index > through {
			continue
		}
		other, ok := byIndex[entry.Index]
		if !ok {
			return fmt.Errorf("missing index %d", entry.Index)
		}
		if other.Term != entry.Term || !bytes.Equal(other.Command, entry.Command) {
			return fmt.Errorf("different entry at index %d", entry.Index)
		}
	}
	return nil
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func snapshotBoundaryCompatible(aID raft.NodeID, a *raft.Node, bID raft.NodeID, b *raft.Node) error {
	aSnap := a.Snapshot()
	bSnap := b.Snapshot()
	if aSnap.LastIncludedIndex > 0 && aSnap.LastIncludedIndex == bSnap.LastIncludedIndex && aSnap.LastIncludedTerm != bSnap.LastIncludedTerm {
		return Violation{Invariant: "Log Matching", Detail: fmt.Sprintf("%s/%s snapshot index %d has terms %d/%d", aID, bID, aSnap.LastIncludedIndex, aSnap.LastIncludedTerm, bSnap.LastIncludedTerm)}
	}
	if err := snapshotMatchesVisibleEntries(aID, aSnap, bID, b); err != nil {
		return err
	}
	if err := snapshotMatchesVisibleEntries(bID, bSnap, aID, a); err != nil {
		return err
	}
	return nil
}

func snapshotMatchesVisibleEntries(snapshotNode raft.NodeID, snapshot raft.Snapshot, logNode raft.NodeID, node *raft.Node) error {
	if snapshot.LastIncludedIndex == 0 {
		return nil
	}
	if node.CommitIndex() < snapshot.LastIncludedIndex {
		return nil
	}
	for _, entry := range node.Entries() {
		if entry.Index == snapshot.LastIncludedIndex && entry.Term != snapshot.LastIncludedTerm {
			return Violation{Invariant: "Log Matching", Detail: fmt.Sprintf("%s snapshot index %d term %d conflicts with %s log term %d", snapshotNode, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, logNode, entry.Term)}
		}
	}
	return nil
}
