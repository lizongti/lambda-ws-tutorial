package tests

import (
	"context"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
	"time"
)

func TestChapter16DynamoDBRoomOfflineIdempotency(t *testing.T) {
	ctx := context.Background()
	store, now := chapter15Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-feature", CurrentConnectionID: "ddb-feature-c", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Join(ctx, wsstore.RoomMember{RoomID: "r", SessionID: "ddb-feature", ConnectionID: "ddb-feature-c", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "ddb-feature", MessageID: "m", Generation: 1, Envelope: []byte(`{"ok":true}`), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, "ddb-feature", 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	_, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "ddb-feature", MessageID: "i", ExpiresAt: now.Add(time.Minute)})
	if err != nil || !acquired {
		t.Fatalf("acquired=%t err=%v", acquired, err)
	}
	members, _, err := store.Members(ctx, "r", 10, "")
	if err != nil || len(members) != 1 {
		t.Fatalf("members=%#v err=%v", members, err)
	}
}
