package tests

import (
	"context"
	"github.com/aura-studio/lambda/ws"
	"testing"
)

func TestChapter14RoomBroadcast(t *testing.T) {
	engine, pusher, _ := newChapter09Engine(t, nil)
	first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []ws.ConnectResult{first, second} {
		ctx := &ws.Context{Context: context.Background(), ConnectionID: item.ConnectionID, SessionID: item.SessionID, Principal: ws.Principal{Subject: "anonymous"}}
		if err := engine.JoinRoom(ctx, "lobby"); err != nil {
			t.Fatal(err)
		}
	}
	result := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message", Payload: []byte(`{"text":"hello"}`)}, ws.BroadcastOptions{})
	if result.Err != nil || result.Succeeded != 2 || pusher.CallCount() != 2 {
		t.Fatalf("broadcast=%#v calls=%d", result, pusher.CallCount())
	}
	excluded := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message"}, ws.BroadcastOptions{ExcludeSender: true, SenderSessionID: first.SessionID})
	if excluded.Err != nil || excluded.Succeeded != 1 {
		t.Fatalf("excluded=%#v", excluded)
	}
}
