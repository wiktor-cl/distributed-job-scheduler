package benchmarks

import (
	"fmt"
	"testing"

	"github.com/wiktor-cl/distributed-job-scheduler/internal/raft"
	"github.com/wiktor-cl/distributed-job-scheduler/internal/sim"
)

func BenchmarkRaftProposeThroughput(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("%d_nodes", size), func(b *testing.B) {
			ids := make([]raft.NodeID, size)
			for i := range ids {
				ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
			}
			cluster := sim.NewCluster(ids, int64(size))
			if _, err := cluster.RunUntilLeader(5000); err != nil {
				b.Fatal(err)
			}
			if err := cluster.RunEvents(1000); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := cluster.Propose([]byte(fmt.Sprintf("job-%d", i)))
				if err != nil {
					b.Fatal(err)
				}
				if err := cluster.RunUntilCommitted(entry.Index, 50000); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
