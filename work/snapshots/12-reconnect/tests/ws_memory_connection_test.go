package tests

import (
	"context"
	wsstore "github.com/aura-studio/lambda/ws/store"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
	"sync"
	"testing"
	"time"
)

func chapter04Store() (*memorystore.Store, time.Time) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return memorystore.New(memorystore.WithClock(func() time.Time { return now })), now
}

func TestChapter04ConnectionSessionAndRace(t *testing.T) {
	ctx := context.Background()
	store, now := chapter04Store()
	session := wsstore.Session{SessionID: "s1", PrincipalSubject: "alice", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
	if err := store.CreateSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	connection := wsstore.Connection{ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "alice", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}
	if err := store.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "s1", "c2", 1)
	if err != nil || bound.Generation != 2 || bound.CurrentConnectionID != "c2" {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "s1", "c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind err=%v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = store.TouchConnection(ctx, "c1", 1, now.Add(time.Second)) }()
	}
	wg.Wait()
	got, err := store.GetConnection(ctx, "c1")
	if err != nil || !got.LastSeenAt.Equal(now.Add(time.Second)) {
		t.Fatalf("touch=%#v err=%v", got, err)
	}
}
