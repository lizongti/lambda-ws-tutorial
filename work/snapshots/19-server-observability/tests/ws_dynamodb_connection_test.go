package tests

import (
	"context"
	wsstore "github.com/aura-studio/lambda/ws/store"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
	"testing"
	"time"
)

func chapter15Store(t *testing.T) (*dynamodbstore.Store, time.Time) {
	t.Helper()
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return dynamodbstore.New(dynamodbtest.New(), "tutorial", dynamodbstore.WithClock(func() time.Time { return now })), now
}

func TestChapter15DynamoDBConnectionSession(t *testing.T) {
	ctx := context.Background()
	store, now := chapter15Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-s1", CurrentConnectionID: "ddb-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "ddb-c1", SessionID: "ddb-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "ddb-s1", "ddb-c2", 1)
	if err != nil || bound.Generation != 2 {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "ddb-s1", "ddb-c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind=%v", err)
	}
}
