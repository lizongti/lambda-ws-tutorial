package ws

import (
	"context"
	"errors"
	"testing"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
)

type flakyGetConnectionStore struct {
	wsstore.StateStore
	calls    int
	failures int
	err      error
}

func (s *flakyGetConnectionStore) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	s.calls++
	if s.calls <= s.failures {
		return wsstore.Connection{}, s.err
	}
	return s.StateStore.GetConnection(ctx, id)
}

func newRetryTestEngine(t *testing.T, store wsstore.StateStore, sleeps *[]time.Duration) *Engine {
	t.Helper()
	options := NewOptions(
		WithDevelopment(true),
		WithStateStore(store),
		WithPusher(NewFakePusher()),
		WithRetrySleeper(func(ctx context.Context, delay time.Duration) error {
			*sleeps = append(*sleeps, delay)
			return nil
		}),
	)
	options.Push.MaxRetries = 2
	options.Push.InitialBackoff = time.Millisecond
	options.Push.MaxBackoff = 4 * time.Millisecond
	engine, err := NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestRetryingStateStoreRetriesOnlyUnavailableWithBounds(t *testing.T) {
	base := memorystore.New()
	if err := base.PutConnection(context.Background(), wsstore.Connection{ConnectionID: "c1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	unavailable := wsstore.NewError(wsstore.ErrorCodeUnavailable, "GetConnection", errors.New("temporary"))
	store := &flakyGetConnectionStore{StateStore: base, failures: 1, err: unavailable}
	var sleeps []time.Duration
	engine := newRetryTestEngine(t, store, &sleeps)
	if engine.StateStore() != store {
		t.Fatal("StateStore must retain the injected backend")
	}

	connection, err := engine.Options.StateStore.GetConnection(context.Background(), "c1")
	if err != nil || connection.ConnectionID != "c1" {
		t.Fatalf("GetConnection() = %+v, %v", connection, err)
	}
	if store.calls != 2 || len(sleeps) != 1 || sleeps[0] != time.Millisecond {
		t.Fatalf("calls/sleeps = %d/%v", store.calls, sleeps)
	}

	store.calls = 0
	store.failures = 3
	sleeps = nil
	_, err = engine.Options.StateStore.GetConnection(context.Background(), "c1")
	if !wsstore.IsUnavailable(err) || store.calls != 3 || len(sleeps) != 2 {
		t.Fatalf("bounded retry = %v, calls=%d sleeps=%v", err, store.calls, sleeps)
	}

	store.calls = 0
	store.failures = 1
	store.err = wsstore.NewError(wsstore.ErrorCodeInvalid, "GetConnection", nil)
	sleeps = nil
	_, err = engine.Options.StateStore.GetConnection(context.Background(), "c1")
	if !wsstore.IsInvalid(err) || store.calls != 1 || len(sleeps) != 0 {
		t.Fatalf("permanent error retry = %v, calls=%d sleeps=%v", err, store.calls, sleeps)
	}
}
