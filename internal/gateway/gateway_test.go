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

func TestOldOwnerTokenRejectedAfterOwnershipChange(t *testing.T) {
	gw := New()
	oldToken := uint64(10)
	newToken := uint64(11)
	if err := gw.Write(Write{Resource: "resource:lease-demo", Value: "new-owner", Token: newToken}); err != nil {
		t.Fatal(err)
	}
	if err := gw.Write(Write{Resource: "resource:lease-demo", Value: "old-owner", Token: oldToken}); err == nil {
		t.Fatal("expected old owner token to be rejected after ownership change")
	}
	got := gw.Read("resource:lease-demo")
	if got.Value != "new-owner" || got.HighestAcceptedToken != newToken {
		t.Fatalf("stale owner mutated protected resource: %+v", got)
	}
}
