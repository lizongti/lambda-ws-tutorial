package tests

import (
	"context"
	"github.com/aura-studio/lambda/ws"
	"testing"
)

func TestChapter13OfflineReplay(t *testing.T) {
	engine, pusher, store := newChapter09Engine(t, func(o *ws.Options) { o.Offline.AckPolicy = ws.AckPolicyPostSuccess })
	connected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "offline-c1", SessionID: connected.SessionID, Generation: connected.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := engine.PushToSession(context.Background(), connected.SessionID, ws.OutboundMessage{Type: ws.TypeEvent, Path: "/notice", Payload: []byte(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(context.Background(), connected.SessionID, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	reconnected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c2", SessionID: connected.SessionID, ResumeCredential: connected.ResumeCredential})
	if err != nil {
		t.Fatal(err)
	}
	if reconnected.Replay.Sent != 1 || pusher.CallCount() != 1 {
		t.Fatalf("replay=%#v calls=%d", reconnected.Replay, pusher.CallCount())
	}
	pending, err = store.ListPending(context.Background(), connected.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("postSuccess queue remains=%#v err=%v", pending, err)
	}
}
