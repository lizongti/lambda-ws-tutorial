package tests

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	wsstore "github.com/aura-studio/lambda/ws/store"
	redisstore "github.com/aura-studio/lambda/ws/store/redis"
	goredis "github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func chapter17Store(t *testing.T) (*redisstore.Store, time.Time) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	now := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	return redisstore.New(client, redisstore.WithClock(func() time.Time { return now })), now
}

func TestChapter17RedisConnectionSession(t *testing.T) {
	ctx := context.Background()
	store, now := chapter17Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "redis-s1", CurrentConnectionID: "redis-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "redis-c1", SessionID: "redis-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "redis-s1", "redis-c2", 1)
	if err != nil || bound.Generation != 2 {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "redis-s1", "redis-c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind=%v", err)
	}
}
