package tests

import (
	"context"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
	"time"
)

func TestChapter11HeartbeatAndMaintenance(t *testing.T) {
	engine, _, store := newChapter09Engine(t, nil)
	now := engine.Clock.Now()
	chapter09Seed(t, engine, store, "stale", "stale-session")
	stale, _ := store.GetConnection(context.Background(), "stale")
	stale.LastSeenAt = now.Add(-2 * engine.HeartbeatTimeout())
	if err := store.PutConnection(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunMaintenanceWithResult(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if result.StaleCleaned != 1 {
		t.Fatalf("maintenance=%#v", result)
	}
	if _, err := store.GetConnection(context.Background(), "stale"); !wsstore.IsNotFound(err) {
		t.Fatalf("stale connection remains: %v", err)
	}
	if engine.HeartbeatInterval() <= 0 || engine.HeartbeatTimeout() <= engine.HeartbeatInterval() {
		t.Fatal("invalid heartbeat configuration")
	}
	_ = time.Second
}
