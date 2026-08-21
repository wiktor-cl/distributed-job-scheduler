package benchmarks

import (
	"fmt"
	"testing"

	"github.com/jhinr/distributed-job-scheduler/internal/raft"
)

func BenchmarkRaftProposeThroughput(b *testing.B) {
	for _, size := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("%d_nodes", size), func(b *testing.B) {
			ids := make([]raft.NodeID, size)
			for i := range ids {
				ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
			}
			cluster := raft.NewCluster(ids)
			if ok, err := cluster.Elect("n1"); err != nil || !ok {
				b.Fatalf("election ok=%v err=%v", ok, err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, committed, err := cluster.Propose([]byte(fmt.Sprintf("job-%d", i))); err != nil || !committed {
					b.Fatalf("propose committed=%v err=%v", committed, err)
				}
			}
		})
	}
}
