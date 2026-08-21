package gateway

import (
	"fmt"
	"sync"
)

type Write struct {
	Resource string
	Value    string
	Token    uint64
}

type Resource struct {
	Value                string
	HighestAcceptedToken uint64
}

type Gateway struct {
	mu        sync.Mutex
	resources map[string]Resource
}

func New() *Gateway {
	return &Gateway{resources: map[string]Resource{}}
}

// Write validates a fencing token before mutating the external resource.
func (g *Gateway) Write(write Write) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	current := g.resources[write.Resource]
	if write.Token < current.HighestAcceptedToken {
		return fmt.Errorf("stale fencing token for %s: got %d, highest accepted %d", write.Resource, write.Token, current.HighestAcceptedToken)
	}
	g.resources[write.Resource] = Resource{Value: write.Value, HighestAcceptedToken: write.Token}
	return nil
}

func (g *Gateway) Read(resource string) Resource {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.resources[resource]
}
