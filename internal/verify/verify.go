package verify

import (
	"bytes"
	"fmt"

	"github.com/jhinr/distributed-job-scheduler/internal/raft"
	"github.com/jhinr/distributed-job-scheduler/internal/scheduler"
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
			for _, ae := range aEntries {
				for _, be := range bEntries {
					if ae.Index != be.Index || ae.Term != be.Term {
						continue
					}
					if err := prefixesEqual(aEntries, bEntries, ae.Index); err != nil {
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
		}
	}
	return nil
}

func RaftCluster(nodes map[raft.NodeID]*raft.Node, committed map[uint64]raft.Entry) error {
	for _, check := range []func() error{
		func() error { return ElectionSafety(nodes) },
		func() error { return LogMatching(nodes) },
		func() error { return StateMachineSafety(nodes) },
		func() error { return LeaderCompleteness(nodes, committed) },
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

func prefixesEqual(a, b []raft.Entry, through uint64) error {
	byIndex := map[uint64]raft.Entry{}
	for _, entry := range a {
		if entry.Index <= through {
			byIndex[entry.Index] = entry
		}
	}
	for _, entry := range b {
		if entry.Index > through {
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
