package sim

import "testing"

func TestVirtualClockIsDeterministic(t *testing.T) {
	clock := NewVirtualClock()
	var got []string
	clock.Schedule(10, "third", func() { got = append(got, "third") })
	clock.Schedule(0, "first", func() { got = append(got, "first") })
	clock.Schedule(5, "second", func() { got = append(got, "second") })
	if err := clock.RunUntilIdle(10); err != nil {
		t.Fatal(err)
	}
	want := []string{"first", "second", "third"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestVirtualNetworkReproducesBySeed(t *testing.T) {
	run := func() []Message {
		clock := NewVirtualClock()
		net := NewVirtualNetwork(clock, NetworkConfig{MinDelay: 1, MaxDelay: 5, DupePermille: 500, Seed: 42})
		net.Register("b", func(Message) {})
		for i := 0; i < 10; i++ {
			net.Send(Message{From: "a", To: "b", Type: "append"})
		}
		if err := clock.RunUntilIdle(100); err != nil {
			t.Fatal(err)
		}
		return net.Delivered()
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].From != b[i].From || a[i].To != b[i].To {
			t.Fatalf("message %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}
