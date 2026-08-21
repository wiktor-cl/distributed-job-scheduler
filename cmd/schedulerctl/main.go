package main

import (
	"fmt"
	"os"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/gateway"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/scheduler"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "demo" {
		fmt.Println("usage: schedulerctl demo")
		return
	}
	sm := scheduler.NewStateMachine(1)
	gw := gateway.New()
	_, _ = sm.Apply(scheduler.Command{Type: scheduler.SubmitCommand, JobID: "job-1", Payload: "write external resource"})
	first, _ := sm.Apply(scheduler.Command{Type: scheduler.ClaimCommand, JobID: "job-1", WorkerID: "old-worker", Now: 1, LeaseDuration: 1})
	_, _ = sm.Apply(scheduler.Command{Type: scheduler.ExpireLeasesCommand, Now: 2})
	second, _ := sm.Apply(scheduler.Command{Type: scheduler.ClaimCommand, JobID: "job-1", WorkerID: "new-worker", Now: 2, LeaseDuration: 10})

	if err := gw.Write(gateway.Write{Resource: "resource-1", Value: "new-worker-write", Token: second.Job.FencingToken}); err != nil {
		fmt.Printf("new write rejected unexpectedly: %v\n", err)
		os.Exit(1)
	}
	err := gw.Write(gateway.Write{Resource: "resource-1", Value: "old-worker-write", Token: first.Job.FencingToken})
	fmt.Printf("first token=%d second token=%d stale-write-error=%v\n", first.Job.FencingToken, second.Job.FencingToken, err)
}
