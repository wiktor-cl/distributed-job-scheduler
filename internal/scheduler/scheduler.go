package scheduler

import (
	"encoding/json"
	"fmt"
	"time"
)

type JobStatus string

const (
	Pending   JobStatus = "pending"
	Claimed   JobStatus = "claimed"
	Running   JobStatus = "running"
	Completed JobStatus = "completed"
	Failed    JobStatus = "failed"
	DLQ       JobStatus = "dead_letter"
)

type Job struct {
	ID              string
	Payload         string
	Status          JobStatus
	Owner           string
	FencingToken    uint64
	LeaseUntil      int64
	Attempts        int
	MaxAttempts     int
	NextRetryAt     int64
	IdempotencyKey  string
	CompletionToken string
	Error           string
	CompletedAt     int64
}

type CommandType string

const (
	SubmitCommand       CommandType = "submit"
	ClaimCommand        CommandType = "claim"
	StartCommand        CommandType = "start"
	CompleteCommand     CommandType = "complete"
	FailCommand         CommandType = "fail"
	ExpireLeasesCommand CommandType = "expire_leases"
	RetryDueCommand     CommandType = "retry_due"
)

type Command struct {
	Type            CommandType
	JobID           string
	Payload         string
	WorkerID        string
	Now             int64
	LeaseDuration   int64
	MaxAttempts     int
	IdempotencyKey  string
	CompletionToken string
	Error           string
	BackoffBase     int64
	BackoffJitter   int64
}

type Result struct {
	Job     Job
	Changed bool
	Message string
}

type StateMachine struct {
	jobs      map[string]Job
	nextToken uint64
}

func NewStateMachine(seed int64) *StateMachine {
	_ = seed
	return &StateMachine{
		jobs:      map[string]Job{},
		nextToken: 1,
	}
}

type snapshot struct {
	Jobs      map[string]Job `json:"jobs"`
	NextToken uint64         `json:"next_token"`
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(payload []byte) (Command, error) {
	var cmd Command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *StateMachine) ApplyBytes(payload []byte) error {
	cmd, err := DecodeCommand(payload)
	if err != nil {
		return err
	}
	_, err = s.ApplyCommand(cmd)
	return err
}

func (s *StateMachine) Apply(payload []byte) error {
	return s.ApplyBytes(payload)
}

func (s *StateMachine) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{Jobs: s.Jobs(), NextToken: s.nextToken})
}

func (s *StateMachine) Restore(payload []byte) error {
	if len(payload) == 0 {
		s.jobs = map[string]Job{}
		s.nextToken = 1
		return nil
	}
	var snap snapshot
	if err := json.Unmarshal(payload, &snap); err != nil {
		return err
	}
	s.jobs = map[string]Job{}
	for id, job := range snap.Jobs {
		s.jobs[id] = job
	}
	s.nextToken = snap.NextToken
	if s.nextToken == 0 {
		s.nextToken = 1
	}
	return nil
}

func (s *StateMachine) Fingerprint() string {
	payload, err := s.Snapshot()
	if err != nil {
		return "error:" + err.Error()
	}
	return string(payload)
}

func (s *StateMachine) NextToken() uint64 {
	return s.nextToken
}

func (s *StateMachine) ApplyCommand(cmd Command) (Result, error) {
	switch cmd.Type {
	case SubmitCommand:
		return s.submit(cmd), nil
	case ClaimCommand:
		return s.claim(cmd)
	case StartCommand:
		return s.start(cmd)
	case CompleteCommand:
		return s.complete(cmd)
	case FailCommand:
		return s.fail(cmd), nil
	case ExpireLeasesCommand:
		return s.expire(cmd), nil
	case RetryDueCommand:
		return s.retryDue(cmd), nil
	default:
		return Result{}, fmt.Errorf("unknown command type %q", cmd.Type)
	}
}

func (s *StateMachine) Job(id string) (Job, bool) {
	job, ok := s.jobs[id]
	return job, ok
}

func (s *StateMachine) Jobs() map[string]Job {
	out := make(map[string]Job, len(s.jobs))
	for id, job := range s.jobs {
		out[id] = job
	}
	return out
}

func (s *StateMachine) DeadLetters() []Job {
	var out []Job
	for _, job := range s.jobs {
		if job.Status == DLQ {
			out = append(out, job)
		}
	}
	return out
}

func (s *StateMachine) submit(cmd Command) Result {
	if existing, ok := s.jobs[cmd.JobID]; ok {
		return Result{Job: existing, Message: "idempotent submit"}
	}
	maxAttempts := cmd.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	job := Job{
		ID:             cmd.JobID,
		Payload:        cmd.Payload,
		Status:         Pending,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: cmd.IdempotencyKey,
	}
	s.jobs[job.ID] = job
	return Result{Job: job, Changed: true, Message: "submitted"}
}

func (s *StateMachine) claim(cmd Command) (Result, error) {
	job, ok := s.jobs[cmd.JobID]
	if !ok {
		return Result{}, fmt.Errorf("missing job %s", cmd.JobID)
	}
	if job.Status == Completed || job.Status == DLQ {
		return Result{Job: job, Message: "terminal job not claimable"}, nil
	}
	if job.Status == Failed && job.NextRetryAt > cmd.Now {
		return Result{Job: job, Message: "retry backoff not elapsed"}, nil
	}
	if job.Owner != "" && job.LeaseUntil > cmd.Now {
		return Result{Job: job, Message: "active lease held"}, nil
	}
	job.Status = Claimed
	job.Owner = cmd.WorkerID
	job.FencingToken = s.nextFence()
	job.LeaseUntil = cmd.Now + cmd.LeaseDuration
	s.jobs[job.ID] = job
	return Result{Job: job, Changed: true, Message: "claimed"}, nil
}

func (s *StateMachine) start(cmd Command) (Result, error) {
	job, ok := s.jobs[cmd.JobID]
	if !ok {
		return Result{}, fmt.Errorf("missing job %s", cmd.JobID)
	}
	if job.Status == Completed || job.Status == DLQ {
		return Result{Job: job, Message: "terminal job not startable"}, nil
	}
	if job.Owner != cmd.WorkerID || job.LeaseUntil <= cmd.Now {
		return Result{Job: job, Message: "worker does not hold active lease"}, nil
	}
	job.Status = Running
	s.jobs[job.ID] = job
	return Result{Job: job, Changed: true, Message: "running"}, nil
}

func (s *StateMachine) complete(cmd Command) (Result, error) {
	job, ok := s.jobs[cmd.JobID]
	if !ok {
		return Result{}, fmt.Errorf("missing job %s", cmd.JobID)
	}
	if job.Status == Completed {
		return Result{Job: job, Message: "idempotent complete"}, nil
	}
	if job.Status == DLQ {
		return Result{Job: job, Message: "dead-lettered job not completable"}, nil
	}
	if job.Owner != cmd.WorkerID || job.LeaseUntil <= cmd.Now {
		return Result{Job: job, Message: "completion ignored without active lease"}, nil
	}
	job.Status = Completed
	job.CompletedAt = cmd.Now
	job.CompletionToken = cmd.CompletionToken
	job.Owner = ""
	job.LeaseUntil = 0
	s.jobs[job.ID] = job
	return Result{Job: job, Changed: true, Message: "completed"}, nil
}

func (s *StateMachine) fail(cmd Command) Result {
	job, ok := s.jobs[cmd.JobID]
	if !ok {
		return Result{Message: "missing job"}
	}
	if job.Status == Completed || job.Status == DLQ {
		return Result{Job: job, Message: "terminal job unchanged"}
	}
	job.Attempts++
	job.Owner = ""
	job.LeaseUntil = 0
	job.Error = cmd.Error
	if job.Attempts >= job.MaxAttempts {
		job.Status = DLQ
	} else {
		job.Status = Failed
		delay := cmd.BackoffBase
		if delay == 0 {
			delay = int64(time.Second)
		}
		job.NextRetryAt = cmd.Now + delay*int64(1<<max(job.Attempts-1, 0)) + cmd.BackoffJitter
	}
	s.jobs[job.ID] = job
	return Result{Job: job, Changed: true, Message: "failed attempt recorded"}
}

func (s *StateMachine) expire(cmd Command) Result {
	changed := false
	var last Job
	for id, job := range s.jobs {
		if (job.Status == Claimed || job.Status == Running) && job.LeaseUntil <= cmd.Now {
			job.Owner = ""
			job.LeaseUntil = 0
			s.jobs[id] = job
			changed = true
			last = job
		}
	}
	return Result{Job: last, Changed: changed, Message: "expired leases"}
}

func (s *StateMachine) retryDue(cmd Command) Result {
	changed := false
	var last Job
	for id, job := range s.jobs {
		if job.Status == Failed && job.NextRetryAt <= cmd.Now {
			job.Status = Pending
			job.NextRetryAt = 0
			s.jobs[id] = job
			changed = true
			last = job
		}
	}
	return Result{Job: last, Changed: changed, Message: "retry due"}
}

func (s *StateMachine) nextFence() uint64 {
	token := s.nextToken
	s.nextToken++
	return token
}
