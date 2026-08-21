package sim

import (
	"math/rand"
)

type Message struct {
	From    string
	To      string
	Type    string
	Payload any
}

type NetworkConfig struct {
	MinDelay     int64
	MaxDelay     int64
	DropPermille int
	DupePermille int
	Seed         int64
}

type VirtualNetwork struct {
	clock     Clock
	rng       *rand.Rand
	cfg       NetworkConfig
	blocked   map[[2]string]bool
	handlers  map[string]func(Message)
	delivered []Message
	dropped   []Message
}

func NewVirtualNetwork(clock Clock, cfg NetworkConfig) *VirtualNetwork {
	return &VirtualNetwork{
		clock:    clock,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		cfg:      cfg,
		blocked:  map[[2]string]bool{},
		handlers: map[string]func(Message){},
	}
}

func (n *VirtualNetwork) Register(node string, handler func(Message)) {
	n.handlers[node] = handler
}

func (n *VirtualNetwork) Send(msg Message) {
	if n.blocked[[2]string{msg.From, msg.To}] || n.randPermille(n.cfg.DropPermille) {
		n.dropped = append(n.dropped, msg)
		return
	}
	copies := 1
	if n.randPermille(n.cfg.DupePermille) {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		copyMsg := msg
		delay := n.delay()
		n.clock.Schedule(delay, msg.Type, func() {
			n.delivered = append(n.delivered, copyMsg)
			if handler := n.handlers[copyMsg.To]; handler != nil {
				handler(copyMsg)
			}
		})
	}
}

func (n *VirtualNetwork) Partition(a, b string) {
	n.blocked[[2]string{a, b}] = true
	n.blocked[[2]string{b, a}] = true
}

func (n *VirtualNetwork) AsymmetricPartition(from, to string) {
	n.blocked[[2]string{from, to}] = true
}

func (n *VirtualNetwork) Heal() {
	n.blocked = map[[2]string]bool{}
}

func (n *VirtualNetwork) Delivered() []Message {
	return append([]Message(nil), n.delivered...)
}

func (n *VirtualNetwork) Dropped() []Message {
	return append([]Message(nil), n.dropped...)
}

func (n *VirtualNetwork) randPermille(threshold int) bool {
	if threshold <= 0 {
		return false
	}
	return n.rng.Intn(1000) < threshold
}

func (n *VirtualNetwork) delay() int64 {
	if n.cfg.MaxDelay <= n.cfg.MinDelay {
		return n.cfg.MinDelay
	}
	return n.cfg.MinDelay + n.rng.Int63n(n.cfg.MaxDelay-n.cfg.MinDelay+1)
}
