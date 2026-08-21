package scheduler

import (
	"encoding/json"
	"testing"
)

func TestWorkerCrashBeforeCompletionRedeliversAfterLeaseExpiry(t *testing.T) {
	sm := NewStateMachine(1)
	if _, err := sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-1", Payload: "send-email"}); err != nil {
		t.Fatal(err)
	}
	first, err := sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-1", WorkerID: "w1", Now: 10, LeaseDuration: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || first.Job.FencingToken != 1 {
		t.Fatalf("first claim = %+v", first)
	}
	if _, err := sm.ApplyCommand(Command{Type: StartCommand, JobID: "job-1", WorkerID: "w1", Now: 11}); err != nil {
		t.Fatal(err)
	}
	if _, err := sm.ApplyCommand(Command{Type: ExpireLeasesCommand, Now: 16}); err != nil {
		t.Fatal(err)
	}
	second, err := sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-1", WorkerID: "w2", Now: 16, LeaseDuration: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Changed || second.Job.Owner != "w2" || second.Job.FencingToken != 2 {
		t.Fatalf("second claim = %+v", second)
	}
}

func TestCompletionRetryIsIdempotentAfterAckLoss(t *testing.T) {
	sm := NewStateMachine(1)
	_, _ = sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-2", Payload: "charge"})
	_, _ = sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-2", WorkerID: "w1", Now: 1, LeaseDuration: 10})
	_, _ = sm.ApplyCommand(Command{Type: StartCommand, JobID: "job-2", WorkerID: "w1", Now: 2})
	first, err := sm.ApplyCommand(Command{Type: CompleteCommand, JobID: "job-2", WorkerID: "w1", Now: 3, CompletionToken: "token-abc"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := sm.ApplyCommand(Command{Type: CompleteCommand, JobID: "job-2", WorkerID: "w1", Now: 4, CompletionToken: "token-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || second.Changed {
		t.Fatalf("completion changed flags first=%v second=%v", first.Changed, second.Changed)
	}
	if first.Job.CompletedAt != second.Job.CompletedAt || second.Job.CompletionToken != "token-abc" {
		t.Fatalf("non-idempotent completion: first=%+v second=%+v", first.Job, second.Job)
	}
}

func TestRetriesMoveToDeadLetterQueue(t *testing.T) {
	sm := NewStateMachine(1)
	_, _ = sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-3", MaxAttempts: 2})
	_, _ = sm.ApplyCommand(Command{Type: FailCommand, JobID: "job-3", Now: 1, Error: "timeout", BackoffBase: 10})
	_, _ = sm.ApplyCommand(Command{Type: RetryDueCommand, Now: 11})
	_, _ = sm.ApplyCommand(Command{Type: FailCommand, JobID: "job-3", Now: 12, Error: "timeout again", BackoffBase: 10})
	dlq := sm.DeadLetters()
	if len(dlq) != 1 || dlq[0].ID != "job-3" || dlq[0].Error != "timeout again" {
		t.Fatalf("dlq = %+v", dlq)
	}
}

func TestDLQAndCompletedJobsAreTerminal(t *testing.T) {
	sm := NewStateMachine(1)
	_, _ = sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-terminal", MaxAttempts: 1})
	_, _ = sm.ApplyCommand(Command{Type: FailCommand, JobID: "job-terminal", Now: 1, Error: "boom"})
	claim, err := sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-terminal", WorkerID: "w1", Now: 2, LeaseDuration: 10})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Changed || claim.Job.Status != DLQ {
		t.Fatalf("DLQ job changed on claim: %+v", claim)
	}

	_, _ = sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-completed"})
	_, _ = sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-completed", WorkerID: "w1", Now: 1, LeaseDuration: 10})
	_, _ = sm.ApplyCommand(Command{Type: StartCommand, JobID: "job-completed", WorkerID: "w1", Now: 2})
	_, _ = sm.ApplyCommand(Command{Type: CompleteCommand, JobID: "job-completed", WorkerID: "w1", Now: 3})
	start, err := sm.ApplyCommand(Command{Type: StartCommand, JobID: "job-completed", WorkerID: "w1", Now: 4})
	if err != nil {
		t.Fatal(err)
	}
	if start.Changed || start.Job.Status != Completed {
		t.Fatalf("completed job returned to running: %+v", start)
	}
}

func TestFencingTokensAreStrictlyMonotonic(t *testing.T) {
	sm := NewStateMachine(1)
	_, _ = sm.ApplyCommand(Command{Type: SubmitCommand, JobID: "job-4"})
	a, _ := sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-4", WorkerID: "w1", Now: 1, LeaseDuration: 1})
	_, _ = sm.ApplyCommand(Command{Type: ExpireLeasesCommand, Now: 2})
	b, _ := sm.ApplyCommand(Command{Type: ClaimCommand, JobID: "job-4", WorkerID: "w2", Now: 2, LeaseDuration: 1})
	if b.Job.FencingToken <= a.Job.FencingToken {
		t.Fatalf("tokens not monotonic: %d then %d", a.Job.FencingToken, b.Job.FencingToken)
	}
}

func TestMalformedReplicatedCommandIsRejectedBeforeMutation(t *testing.T) {
	sm := NewStateMachine(1)
	if err := sm.Apply([]byte(`{"Type":"claim","JobID":"bad","WorkerID":"w","Now":1,"LeaseDuration":-1}`)); err == nil {
		t.Fatal("expected malformed lease duration to be rejected")
	}
	if _, ok := sm.Job("bad"); ok {
		t.Fatal("malformed command mutated scheduler state")
	}
	if err := sm.Apply([]byte(`{"Type":"unknown","JobID":"bad"}`)); err == nil {
		t.Fatal("expected unknown command to be rejected")
	}
}

func TestSnapshotRestoreRejectsLostFencingTokenMetadata(t *testing.T) {
	payload, err := json.Marshal(snapshot{
		Jobs: map[string]Job{
			"j1": {ID: "j1", Status: Claimed, Owner: "w1", FencingToken: 4, LeaseUntil: 10},
		},
		NextToken: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	sm := NewStateMachine(1)
	if err := sm.Restore(payload); err == nil {
		t.Fatal("expected snapshot with stale next token to be rejected")
	}
}
