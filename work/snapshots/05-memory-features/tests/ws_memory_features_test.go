package tests

import (
	"context"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
	"time"
)

func TestChapter05RoomOfflineAndIdempotency(t *testing.T) {
	ctx := context.Background()
	store, now := chapter04Store()
	session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
	if err := store.CreateSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := store.Join(ctx, wsstore.RoomMember{RoomID: "lobby", SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	members, cursor, err := store.Members(ctx, "lobby", 10, "")
	if err != nil || len(members) != 1 || cursor != "" {
		t.Fatalf("members=%v cursor=%q err=%v", members, cursor, err)
	}
	if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1, Envelope: []byte(`{"ok":true}`), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if err := store.MarkSent(ctx, "s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, "s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	record, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "incoming-1", ExpiresAt: now.Add(time.Minute)})
	if err != nil || !acquired || record.MessageID != "incoming-1" {
		t.Fatalf("begin=%#v acquired=%t err=%v", record, acquired, err)
	}
	if _, acquired, err = store.BeginIdempotency(ctx, record); err != nil || acquired {
		t.Fatalf("duplicate acquired=%t err=%v", acquired, err)
	}
}
