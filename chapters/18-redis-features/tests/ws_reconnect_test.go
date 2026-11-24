package tests

import (
	"context"
	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
)

func TestChapter12ReconnectAndStaleDisconnect(t *testing.T) {
	engine, _, store := newChapter09Engine(t, nil)
	first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c1"})
	if err != nil || first.ResumeCredential == "" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil {
		t.Fatal(err)
	}
	second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c2", SessionID: first.SessionID, ResumeCredential: first.ResumeCredential})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Reconnected || second.SessionID != first.SessionID || second.Generation <= first.Generation {
		t.Fatalf("second=%#v first=%#v", second, first)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetConnection(context.Background(), "c2"); err != nil {
		t.Fatalf("stale disconnect removed c2: %v", err)
	}
	if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c3", SessionID: first.SessionID, ResumeCredential: "wrong"}); err == nil {
		t.Fatal("wrong credential accepted")
	}
	_ = wsstore.ConnectionStateActive
}
