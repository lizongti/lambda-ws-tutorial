package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

func TestWSMaintenanceStrictStaleBoundaryAndGoneCleanup(t *testing.T) {
	t.Run("stale boundary and live connection", func(t *testing.T) {
		f := newWSSessionTestFixture(t, func(options *ws.Options) {
			options.Heartbeat.Timeout = time.Minute
			options.Heartbeat.MaintenanceBatchSize = 10
		})
		ctx := context.Background()
		stale := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-stale", "alice", "", ""))
		live := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-live", "alice", "", ""))

		f.clock.Advance(time.Minute)
		result, err := f.engine.RunMaintenanceWithResult(ctx, time.Time{})
		if err != nil || result.StaleScanned != 0 || result.StaleCleaned != 0 {
			t.Fatalf("maintenance at exact stale boundary = %+v, %v; want no strict-before match", result, err)
		}
		if _, err := f.store.GetConnection(ctx, stale.ConnectionID); err != nil {
			t.Fatalf("connection at exact timeout boundary was cleaned: %v", err)
		}

		f.clock.Advance(30 * time.Second)
		if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, live, ws.NewPing("request-live-heartbeat"))); err != nil {
			t.Fatalf("live heartbeat error = %v", err)
		}
		f.clock.Advance(30 * time.Second)
		result, err = f.engine.RunMaintenanceWithResult(ctx, time.Time{})
		if err != nil || result.StaleScanned != 1 || result.StaleCleaned != 1 || result.Failed != 0 {
			t.Fatalf("stale maintenance = %+v, %v", result, err)
		}
		if _, err := f.store.GetConnection(ctx, stale.ConnectionID); !wsstore.IsNotFound(err) {
			t.Fatalf("stale connection was not removed: %v", err)
		}
		staleSession, err := f.store.GetSession(ctx, stale.SessionID)
		if err != nil || staleSession.CurrentConnectionID != "" || staleSession.Generation != stale.Generation {
			t.Fatalf("stale session was not retained and unbound: %+v, %v", staleSession, err)
		}
		liveConnection, err := f.store.GetConnection(ctx, live.ConnectionID)
		if err != nil || liveConnection.Generation != live.Generation || !liveConnection.LastSeenAt.Equal(f.base.Add(90*time.Second)) {
			t.Fatalf("live connection was damaged by stale cleanup: %+v, %v", liveConnection, err)
		}
		liveSession, err := f.store.GetSession(ctx, live.SessionID)
		if err != nil || liveSession.CurrentConnectionID != live.ConnectionID {
			t.Fatalf("live binding changed during stale cleanup: %+v, %v", liveSession, err)
		}
	})

	t.Run("pusher gone", func(t *testing.T) {
		f := newWSSessionTestFixture(t, nil)
		ctx := context.Background()
		connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-gone-maintenance", "alice", "", ""))
		f.pusher.SetErrors(ws.NewPushError(ws.PushErrorGone, errors.New("private API Gateway detail")))
		err := f.engine.Push(ctx, connected.ConnectionID, ws.NewEvent("/gone", "request-gone", json.RawMessage(`{"value":1}`)))
		if !ws.IsPushGone(err) {
			t.Fatalf("Push(gone) error = %v, want classified gone", err)
		}
		if _, err := f.store.GetConnection(ctx, connected.ConnectionID); !wsstore.IsNotFound(err) {
			t.Fatalf("gone connection remains stored: %v", err)
		}
		session, err := f.store.GetSession(ctx, connected.SessionID)
		if err != nil || session.CurrentConnectionID != "" || session.Generation != connected.Generation {
			t.Fatalf("gone session was not retained and unbound: %+v, %v", session, err)
		}
		result, err := f.engine.RunMaintenanceWithResult(ctx, f.clock.Now())
		if err != nil || result.StaleScanned != 0 || result.StaleCleaned != 0 {
			t.Fatalf("maintenance after gone cleanup = %+v, %v", result, err)
		}
	})
}

type wsMaintenanceReplacementStore struct {
	*memory.Store
	once        sync.Once
	injectErr   error
	replacedAt  time.Time
	replacement wsstore.Connection
}

func (s *wsMaintenanceReplacementStore) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	connections, err := s.Store.ListStaleConnections(ctx, before, limit)
	if err != nil || len(connections) == 0 {
		return connections, err
	}
	s.once.Do(func() {
		old := connections[0]
		bound, err := s.Store.BindConnection(ctx, old.SessionID, old.ConnectionID, old.Generation)
		if err != nil {
			s.injectErr = err
			return
		}
		replacement := old
		replacement.Generation = bound.Generation
		replacement.LastSeenAt = s.replacedAt
		replacement.ExpiresAt = s.replacedAt.Add(time.Hour)
		if err := s.Store.PutConnection(ctx, replacement); err != nil {
			s.injectErr = err
			return
		}
		s.replacement = replacement
	})
	return connections, nil
}

func TestWSMaintenanceStaleSnapshotCannotDeleteReplacement(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := &wsSessionTestClock{now: base}
	ids := &wsSessionTestIDGenerator{}
	store := &wsMaintenanceReplacementStore{
		Store:      memory.New(memory.WithClock(clock.Now)),
		replacedAt: base.Add(2 * time.Minute),
	}
	ctx := context.Background()
	if err := store.CreateSession(ctx, wsstore.Session{
		SessionID: "session-maintenance-race", PrincipalSubject: "alice", ExpiresAt: base.Add(4 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "session-maintenance-race", "connection-maintenance-race", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{
		ConnectionID: "connection-maintenance-race", SessionID: bound.SessionID, PrincipalSubject: "alice",
		Generation: bound.Generation, ConnectedAt: base, LastSeenAt: base, ExpiresAt: base.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}); err != nil {
		t.Fatal(err)
	}
	options := wsSessionTestOptions(clock, ids)
	options.StateStore = store
	options.Pusher = ws.NewFakePusher()
	options.Heartbeat.Timeout = time.Minute
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	result, err := engine.RunMaintenanceWithResult(ctx, base.Add(2*time.Minute))
	if store.injectErr != nil {
		t.Fatalf("replacement injection error = %v", store.injectErr)
	}
	if err != nil || result.StaleScanned != 1 || result.StaleCleaned != 0 || result.Conflicts != 1 || result.Failed != 0 {
		t.Fatalf("maintenance with replacement conflict = %+v, %v", result, err)
	}
	connection, err := store.GetConnection(ctx, "connection-maintenance-race")
	if err != nil || connection.Generation != store.replacement.Generation || connection.Generation != 2 {
		t.Fatalf("replacement connection after stale cleanup = %+v, %v", connection, err)
	}
	session, err := store.GetSession(ctx, "session-maintenance-race")
	if err != nil || session.CurrentConnectionID != connection.ConnectionID || session.Generation != connection.Generation {
		t.Fatalf("replacement binding after stale cleanup = %+v, %v", session, err)
	}
}

type wsMaintenanceTouchStore struct {
	*memory.Store
	once      sync.Once
	touchAt   time.Time
	injectErr error
}

func (s *wsMaintenanceTouchStore) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	connections, err := s.Store.ListStaleConnections(ctx, before, limit)
	if err != nil || len(connections) == 0 {
		return connections, err
	}
	s.once.Do(func() {
		snapshot := connections[0]
		s.injectErr = s.Store.TouchConnection(ctx, snapshot.ConnectionID, snapshot.Generation, s.touchAt)
	})
	return connections, nil
}

func TestWSMaintenanceStaleSnapshotCannotDeleteTouchedConnection(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := &wsSessionTestClock{now: base}
	store := &wsMaintenanceTouchStore{
		Store:   memory.New(memory.WithClock(clock.Now)),
		touchAt: base.Add(90 * time.Second),
	}
	options := wsSessionTestOptions(clock, &wsSessionTestIDGenerator{})
	options.StateStore = store
	options.Pusher = ws.NewFakePusher()
	options.Heartbeat.Timeout = time.Minute
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	connected := mustWSSessionTestConnect(t, engine, wsSessionTestConnectEvent("connection-maintenance-touch", "alice", "", ""))
	clock.Advance(2 * time.Minute)

	result, err := engine.RunMaintenanceWithResult(context.Background(), clock.Now())
	if store.injectErr != nil {
		t.Fatalf("Touch injection error = %v", store.injectErr)
	}
	if err != nil || result.StaleScanned != 1 || result.StaleCleaned != 0 || result.Conflicts != 1 || result.Failed != 0 {
		t.Fatalf("maintenance after Touch = %+v, %v", result, err)
	}
	connection, err := store.GetConnection(context.Background(), connected.ConnectionID)
	if err != nil || !connection.LastSeenAt.Equal(store.touchAt) {
		t.Fatalf("touched connection after stale cleanup = %+v, %v", connection, err)
	}
	session, err := store.GetSession(context.Background(), connected.SessionID)
	if err != nil || session.CurrentConnectionID != connected.ConnectionID || session.Generation != connected.Generation {
		t.Fatalf("touched session binding after stale cleanup = %+v, %v", session, err)
	}
}

func TestWSMaintenanceExpiryBatchesAcrossBackends(t *testing.T) {
	for _, backend := range []wsIntegrationBackend{wsIntegrationMemory, wsIntegrationDynamoDB, wsIntegrationRedis} {
		t.Run(string(backend), func(t *testing.T) {
			f := newWSIntegrationFixture(t, backend, func(options *ws.Options) {
				options.Heartbeat.MaintenanceBatchSize = 1
			})
			ctx := context.Background()
			for id, lifetime := range map[string]time.Duration{
				"session-expired-a": time.Hour,
				"session-expired-b": 2 * time.Hour,
				"session-live":      4 * time.Hour,
				"session-owner":     24 * time.Hour,
			} {
				if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: id, ExpiresAt: f.base.Add(lifetime)}); err != nil {
					t.Fatalf("CreateSession(%s) error = %v", id, err)
				}
			}
			for i, id := range []string{"offline-expired-a", "offline-expired-b", "offline-live"} {
				if err := f.store.Append(ctx, wsstore.OfflineMessage{
					SessionID: "session-owner", MessageID: id, Generation: 0,
					Envelope: json.RawMessage(`{"value":1}`), ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour),
				}); err != nil {
					t.Fatalf("Append(%s) error = %v", id, err)
				}
				if _, acquired, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{
					SessionID: "session-owner", MessageID: "idempotency-" + id,
					ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour),
				}); err != nil || !acquired {
					t.Fatalf("BeginIdempotency(%s) = acquired %t, error %v", id, acquired, err)
				}
			}

			// The store clock intentionally remains at base. The explicit maintenance
			// timestamp deletes physical rows, while subsequent reads can distinguish
			// the second expired row from rows removed by the first bounded batch.
			maintenanceTime := f.base.Add(3 * time.Hour)
			result, err := f.engine.RunMaintenanceWithResult(ctx, maintenanceTime)
			if err != nil || result.Failed != 0 || result.ExpiredSessionBatches != 1 ||
				result.ExpiredOfflineBatches != 1 || result.ExpiredIdempotencyBatches != 1 {
				t.Fatalf("first expiration maintenance = %+v, %v", result, err)
			}
			if _, err := f.store.GetSession(ctx, "session-expired-a"); !wsstore.IsNotFound(err) {
				t.Fatalf("first expired session remains after first batch: %v", err)
			}
			if _, err := f.store.GetSession(ctx, "session-expired-b"); err != nil {
				t.Fatalf("first session batch removed more than one row: %v", err)
			}
			pending, err := f.store.ListPending(ctx, "session-owner", 10)
			if err != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"offline-expired-b", "offline-live"}) {
				t.Fatalf("offline rows after first batch = %v, %v", wsOfflineTestMessageIDs(pending), err)
			}
			if _, err := f.store.GetIdempotency(ctx, "session-owner", "idempotency-offline-expired-a"); !wsstore.IsNotFound(err) {
				t.Fatalf("first expired idempotency row remains: %v", err)
			}
			if _, err := f.store.GetIdempotency(ctx, "session-owner", "idempotency-offline-expired-b"); err != nil {
				t.Fatalf("first idempotency batch removed more than one row: %v", err)
			}

			result, err = f.engine.RunMaintenanceWithResult(ctx, maintenanceTime)
			if err != nil || result.Failed != 0 {
				t.Fatalf("second expiration maintenance = %+v, %v", result, err)
			}
			for _, id := range []string{"session-expired-a", "session-expired-b"} {
				if _, err := f.store.GetSession(ctx, id); !wsstore.IsNotFound(err) {
					t.Fatalf("expired session %s remains after second batch: %v", id, err)
				}
			}
			if _, err := f.store.GetSession(ctx, "session-live"); err != nil {
				t.Fatalf("live session was removed: %v", err)
			}
			pending, err = f.store.ListPending(ctx, "session-owner", 10)
			if err != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"offline-live"}) {
				t.Fatalf("offline rows after second batch = %v, %v", wsOfflineTestMessageIDs(pending), err)
			}
			if _, err := f.store.GetIdempotency(ctx, "session-owner", "idempotency-offline-expired-b"); !wsstore.IsNotFound(err) {
				t.Fatalf("second expired idempotency row remains: %v", err)
			}
			if _, err := f.store.GetIdempotency(ctx, "session-owner", "idempotency-offline-live"); err != nil {
				t.Fatalf("live idempotency row was removed: %v", err)
			}
		})
	}
}

type wsMaintenanceFailureStore struct {
	wsstore.StateStore
	mu            sync.Mutex
	failOperation string
	failure       error
	calls         map[string]int
	limits        map[string]int
}

func (s *wsMaintenanceFailureStore) invoke(operation string, limit int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls == nil {
		s.calls = make(map[string]int)
		s.limits = make(map[string]int)
	}
	s.calls[operation]++
	s.limits[operation] = limit
	if s.failOperation == operation {
		return s.failure
	}
	return nil
}

func (s *wsMaintenanceFailureStore) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	if err := s.invoke("stale_scan", limit); err != nil {
		return nil, err
	}
	return s.StateStore.ListStaleConnections(ctx, before, limit)
}

func (s *wsMaintenanceFailureStore) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	if err := s.invoke("stale_cleanup", 0); err != nil {
		return err
	}
	return s.StateStore.DeleteConnectionIfMatch(ctx, id, generation)
}

func (s *wsMaintenanceFailureStore) ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	if err := s.invoke("expired_connections", limit); err != nil {
		return nil, err
	}
	return s.StateStore.(wsstore.ConnectionExpirationStore).ListExpiredConnections(ctx, before, limit)
}

func (s *wsMaintenanceFailureStore) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	if err := s.invoke("expired_sessions", limit); err != nil {
		return err
	}
	return s.StateStore.DeleteExpiredSessions(ctx, before, limit)
}

func (s *wsMaintenanceFailureStore) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	if err := s.invoke("expired_offline", limit); err != nil {
		return err
	}
	return s.StateStore.DeleteExpired(ctx, before, limit)
}

func (s *wsMaintenanceFailureStore) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	if err := s.invoke("expired_idempotency", limit); err != nil {
		return err
	}
	return s.StateStore.DeleteExpiredIdempotency(ctx, before, limit)
}

func (s *wsMaintenanceFailureStore) callCount(operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[operation]
}

func (s *wsMaintenanceFailureStore) batchLimit(operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limits[operation]
}

func newWSMaintenanceFailureEngine(t *testing.T, store *wsMaintenanceFailureStore, clock *wsSessionTestClock) *ws.Engine {
	t.Helper()
	options := wsSessionTestOptions(clock, &wsSessionTestIDGenerator{})
	options.StateStore = store
	options.Pusher = ws.NewFakePusher()
	options.Heartbeat.MaintenanceBatchSize = 7
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return engine
}

func TestWSMaintenanceStorageFailuresAreReportedPerBatch(t *testing.T) {
	for _, test := range []struct {
		name               string
		operation          string
		sessionBatches     int
		offlineBatches     int
		idempotencyBatches int
	}{
		{name: "expired connections", operation: "expired_connections", sessionBatches: 1, offlineBatches: 1, idempotencyBatches: 1},
		{name: "expired sessions", operation: "expired_sessions", sessionBatches: 0, offlineBatches: 1, idempotencyBatches: 1},
		{name: "expired offline", operation: "expired_offline", sessionBatches: 1, offlineBatches: 0, idempotencyBatches: 1},
		{name: "expired idempotency", operation: "expired_idempotency", sessionBatches: 1, offlineBatches: 1, idempotencyBatches: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			injected := errors.New("injected maintenance batch failure")
			clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
			store := &wsMaintenanceFailureStore{
				StateStore: memory.New(memory.WithClock(clock.Now)), failOperation: test.operation, failure: injected,
			}
			engine := newWSMaintenanceFailureEngine(t, store, clock)
			result, err := engine.RunMaintenanceWithResult(context.Background(), clock.Now())
			if !errors.Is(err, injected) {
				t.Fatalf("RunMaintenanceWithResult() error = %v, want injected failure", err)
			}
			if result.Failed != 1 || result.ExpiredSessionBatches != test.sessionBatches ||
				result.ExpiredOfflineBatches != test.offlineBatches || result.ExpiredIdempotencyBatches != test.idempotencyBatches {
				t.Fatalf("maintenance result = %+v", result)
			}
			for _, operation := range []string{"expired_connections", "expired_sessions", "expired_offline", "expired_idempotency"} {
				if store.callCount(operation) != 1 {
					t.Errorf("%s calls = %d, want 1; one failed batch must not suppress later batches", operation, store.callCount(operation))
				}
				if store.batchLimit(operation) != 7 {
					t.Errorf("%s limit = %d, want configured batch size 7", operation, store.batchLimit(operation))
				}
			}
		})
	}

	t.Run("stale scan failure does not suppress later batches", func(t *testing.T) {
		injected := errors.New("injected stale scan failure")
		clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
		store := &wsMaintenanceFailureStore{
			StateStore: memory.New(memory.WithClock(clock.Now)), failOperation: "stale_scan", failure: injected,
		}
		engine := newWSMaintenanceFailureEngine(t, store, clock)
		result, err := engine.RunMaintenanceWithResult(context.Background(), clock.Now())
		if !errors.Is(err, injected) || result.StaleScanned != 0 || result.Failed != 1 || store.callCount("stale_scan") != 1 {
			t.Fatalf("stale scan result = %+v, error = %v, calls = %d", result, err, store.callCount("stale_scan"))
		}
		for _, operation := range []string{"expired_connections", "expired_sessions", "expired_offline", "expired_idempotency"} {
			if store.callCount(operation) != 1 {
				t.Errorf("%s calls = %d, want 1 after stale scan failure", operation, store.callCount(operation))
			}
		}
	})

	t.Run("stale cleanup failure does not suppress expiration batches", func(t *testing.T) {
		injected := errors.New("injected stale cleanup failure")
		clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
		store := &wsMaintenanceFailureStore{
			StateStore: memory.New(memory.WithClock(clock.Now)), failOperation: "stale_cleanup", failure: injected,
		}
		engine := newWSMaintenanceFailureEngine(t, store, clock)
		connected := mustWSSessionTestConnect(t, engine, wsSessionTestConnectEvent("connection-maintenance-failure", "alice", "", ""))
		clock.Advance(2 * time.Minute)
		result, err := engine.RunMaintenanceWithResult(context.Background(), clock.Now())
		if !errors.Is(err, injected) || result.StaleScanned != 1 || result.StaleCleaned != 0 || result.Failed != 1 ||
			result.ExpiredSessionBatches != 1 || result.ExpiredOfflineBatches != 1 || result.ExpiredIdempotencyBatches != 1 {
			t.Fatalf("stale cleanup result = %+v, error = %v", result, err)
		}
		if _, getErr := store.GetConnection(context.Background(), connected.ConnectionID); getErr != nil {
			t.Fatalf("failed stale cleanup removed its connection: %v", getErr)
		}
		for _, operation := range []string{"expired_connections", "expired_sessions", "expired_offline", "expired_idempotency"} {
			if store.callCount(operation) != 1 {
				t.Errorf("%s calls = %d, want 1 after stale cleanup failure", operation, store.callCount(operation))
			}
		}
	})
}

func TestWSMaintenanceAPIGatewayReconnectAfterHardDisconnect(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-hard-old", "alice", "", ""))
	disconnect := wsLifecycleGatewayRequest("connection-hard-old", "request-hard-disconnect", ws.RouteDisconnect, "")
	response, err := f.engine.HandleEvent(ctx, disconnect)
	requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusOK)
	if _, err := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("hard disconnect left physical connection: %v", err)
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != "" || stored.Generation != first.Generation {
		t.Fatalf("session after hard disconnect = %+v, %v", stored, err)
	}

	reconnect := wsLifecycleGatewayRequest("connection-hard-new", "request-hard-reconnect", ws.RouteConnect, "")
	reconnect.QueryStringParameters = map[string]string{
		"sessionId":        first.SessionID,
		"resumeCredential": first.ResumeCredential,
	}
	response, err = f.engine.HandleEvent(ctx, reconnect)
	requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusOK, first.ResumeCredential)
	stored, err = f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != "connection-hard-new" || stored.Generation != first.Generation+1 {
		t.Fatalf("session after hard reconnect = %+v, %v", stored, err)
	}
	connection, err := f.store.GetConnection(ctx, "connection-hard-new")
	if err != nil || connection.SessionID != first.SessionID || connection.Generation != first.Generation+1 || connection.State != wsstore.ConnectionStateActive {
		t.Fatalf("new physical connection = %+v, %v", connection, err)
	}

	// A duplicate hard disconnect for the dead API Gateway connection cannot
	// tear down the replacement established from the retained logical session.
	response, err = f.engine.HandleEvent(ctx, disconnect)
	requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusOK)
	stored, err = f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != "connection-hard-new" || stored.Generation != first.Generation+1 {
		t.Fatalf("late hard disconnect changed replacement = %+v, %v", stored, err)
	}
}
