package gateway

import "testing"

func TestRejectsStaleFencingToken(t *testing.T) {
	gw := New()
	if err := gw.Write(Write{Resource: "invoice:42", Value: "new-owner", Token: 7}); err != nil {
		t.Fatal(err)
	}
	if err := gw.Write(Write{Resource: "invoice:42", Value: "old-owner", Token: 6}); err == nil {
		t.Fatal("expected stale write to be rejected")
	}
	got := gw.Read("invoice:42")
	if got.Value != "new-owner" || got.HighestAcceptedToken != 7 {
		t.Fatalf("resource changed after stale write: %+v", got)
	}
}
