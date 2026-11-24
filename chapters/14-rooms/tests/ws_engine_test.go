package tests

import (
	"context"
	"encoding/json"
	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
	"sync/atomic"
	"testing"
	"time"
)

type chapter09Clock struct{ now time.Time }

func (c *chapter09Clock) Now() time.Time { return c.now }

type chapter09IDs struct{ n atomic.Int64 }

func (i *chapter09IDs) ID(prefix string) string {
	n := i.n.Add(1)
	return prefix + "-" + time.Unix(n, 0).Format("150405")
}

func newChapter09Engine(t *testing.T, configure func(*ws.Options)) (*ws.Engine, *ws.FakePusher, wsstore.StateStore) {
	t.Helper()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	store := memorystore.New(memorystore.WithClock(func() time.Time { return now }))
	pusher := ws.NewFakePusher()
	options := ws.NewOptions(ws.WithDevelopment(true), ws.WithAuthenticator(ws.NewAnonymousAuthenticator()), ws.WithStateStore(store), ws.WithPusher(pusher), ws.WithClock(&chapter09Clock{now: now}), ws.WithIDGenerator(&chapter09IDs{}), ws.WithRetrySleeper(func(context.Context, time.Duration) error { return nil }), ws.OptionFunc(func(o *ws.Options) { o.Auth.Anonymous = true; o.Push.MaxRetries = 0 }))
	if configure != nil {
		configure(options)
	}
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	return engine, pusher, store
}

func chapter09Seed(t *testing.T, engine *ws.Engine, store wsstore.StateStore, connectionID, sessionID string) {
	t.Helper()
	now := engine.Clock.Now()
	if err := store.CreateSession(context.Background(), wsstore.Session{SessionID: sessionID, PrincipalSubject: "anonymous", CurrentConnectionID: connectionID, Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(context.Background(), wsstore.Connection{ConnectionID: connectionID, SessionID: sessionID, PrincipalSubject: "anonymous", Generation: 1, LastSeenAt: now, ExpiresAt: now.Add(time.Hour), State: wsstore.ConnectionStateActive}); err != nil {
		t.Fatal(err)
	}
}

func TestChapter09SameConnectionReply(t *testing.T) {
	engine, pusher, store := newChapter09Engine(t, nil)
	chapter09Seed(t, engine, store, "connection-A", "session-A")
	engine.Handle("/echo", func(ctx *ws.Context) {
		if err := engine.Reply(ctx, map[string]string{"text": "hello"}); err != nil {
			t.Fatal(err)
		}
	})
	body, _ := json.Marshal(ws.Envelope{Version: 1, Type: ws.TypeRequest, Path: "/echo", RequestID: "r1", MessageID: "m1"})
	if err := engine.HandleMessage(context.Background(), ws.MessageEvent{ConnectionID: "connection-A", RouteKey: ws.DefaultRouteKey, Body: string(body)}); err != nil {
		t.Fatal(err)
	}
	calls := pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != "connection-A" {
		t.Fatalf("calls=%#v", calls)
	}
}
