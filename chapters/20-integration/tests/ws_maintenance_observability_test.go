package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

func TestWSMaintenanceExpiredConnectionStatsAndPhysicalCleanup(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-expired-only", "alice", "", ""))
	connection, err := f.store.GetConnection(ctx, connected.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	// The connection is recently seen at maintenance time, so only the explicit
	// expiration index can find it after its earlier ExpiresAt has passed.
	connection.LastSeenAt = f.base.Add(59*time.Minute + 45*time.Second)
	if err := f.store.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(61 * time.Minute)
	if _, err := f.store.GetConnection(ctx, connected.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("test setup did not make connection logically expired: %v", err)
	}
	recorder := &wsEngineObservabilityRecorder{}
	f.engine.SetLogger(recorder)
	f.engine.SetMetrics(recorder)
	result, err := f.engine.RunMaintenanceWithResult(ctx, time.Time{})
	if err != nil || result.StaleScanned != 0 || result.ExpiredScanned != 1 || result.ExpiredCleaned != 1 ||
		result.Skipped != 0 || result.Conflicts != 0 || result.Failed != 0 {
		t.Fatalf("expired connection maintenance = %+v, %v", result, err)
	}
	remaining, err := f.store.ListExpiredConnections(ctx, f.clock.Now(), 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expired connection was not physically removed: %+v, %v", remaining, err)
	}

	logs, metrics := recorder.snapshot()
	foundLog := false
	for _, event := range logs {
		if event.Name == ws.LogEventMaintenance {
			foundLog = true
			if event.Outcome != "success" || event.Maintenance == nil || *event.Maintenance != result {
				t.Fatalf("maintenance log = %+v, result = %+v", event, result)
			}
			if strings.Contains(event.String(), connected.ResumeCredential) {
				t.Fatalf("maintenance log leaked resume credential: %s", event.String())
			}
		}
	}
	if !foundLog {
		t.Fatal("maintenance summary log was not emitted")
	}
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceSucceeded, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceExpiredConnections, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceConflicts, 0)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceFailures, 0)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceSkipped, 0)
}

type wsMaintenanceObservabilityStore struct {
	*memory.Store
	failure error
}

func (s *wsMaintenanceObservabilityStore) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	switch id {
	case "connection-conflict":
		return wsstore.NewError(wsstore.ErrorCodeConditionConflict, "DeleteConnectionIfMatch", errors.New("replacement generation"))
	case "connection-failure":
		return s.failure
	default:
		return s.Store.DeleteConnectionIfMatch(ctx, id, generation)
	}
}

func (s *wsMaintenanceObservabilityStore) DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error {
	switch id {
	case "connection-conflict":
		return wsstore.NewError(wsstore.ErrorCodeConditionConflict, "DeleteConnectionIfMatchAndLastSeen", errors.New("replacement generation"))
	case "connection-failure":
		return s.failure
	default:
		return s.Store.DeleteConnectionIfMatchAndLastSeen(ctx, id, generation, lastSeen)
	}
}

func TestWSMaintenanceOutcomeStatisticsAndSafeHooks(t *testing.T) {
	const privateFailure = "private-maintenance-backend-detail"
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := &wsSessionTestClock{now: base}
	ids := &wsSessionTestIDGenerator{}
	store := &wsMaintenanceObservabilityStore{
		Store:   memory.New(memory.WithClock(clock.Now)),
		failure: errors.New(privateFailure),
	}
	ctx := context.Background()
	for _, connection := range []wsstore.Connection{
		{ConnectionID: "connection-conflict", Generation: 1, LastSeenAt: base},
		{ConnectionID: "connection-failure", Generation: 1, LastSeenAt: base, ExpiresAt: base.Add(time.Hour)},
		{ConnectionID: "connection-duplicate", Generation: 1, LastSeenAt: base},
	} {
		if err := store.PutConnection(ctx, connection); err != nil {
			t.Fatal(err)
		}
	}
	options := wsSessionTestOptions(clock, ids)
	options.StateStore = store
	options.Pusher = ws.NewFakePusher()
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &wsEngineObservabilityRecorder{}
	engine.SetLogger(recorder)
	engine.SetMetrics(recorder)
	clock.Advance(2 * time.Hour)
	result, err := engine.RunMaintenanceWithResult(ctx, time.Time{})
	if err == nil || result.StaleScanned != 3 || result.StaleCleaned != 1 || result.ExpiredScanned != 1 ||
		result.ExpiredCleaned != 0 || result.Skipped != 1 || result.Conflicts != 1 || result.Failed != 1 {
		t.Fatalf("maintenance outcome statistics = %+v, %v", result, err)
	}
	logs, metrics := recorder.snapshot()
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceSucceeded, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceConflicts, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceFailures, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceSkipped, 1)
	assertMaintenanceMetric(t, metrics, ws.MetricMaintenanceExpiredConnections, 0)
	foundSummary := false
	var diagnostic strings.Builder
	for _, event := range logs {
		fmt.Fprintf(&diagnostic, "%s|%#v\n", event.String(), event)
		if event.Name == ws.LogEventMaintenance {
			foundSummary = true
			if event.Outcome != "partial_failure" || event.ErrorCode != "internal_error" || event.Maintenance == nil || *event.Maintenance != result {
				t.Fatalf("partial failure log = %+v, result = %+v", event, result)
			}
		}
	}
	if !foundSummary {
		t.Fatal("partial failure summary log was not emitted")
	}
	if strings.Contains(diagnostic.String(), privateFailure) {
		t.Fatalf("maintenance logs leaked backend error detail: %s", diagnostic.String())
	}
}

func TestWSMaintenancePanickingObservabilityHooksAreContained(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	f.engine.SetLogger(ws.LoggerFunc(func(context.Context, ws.LogEvent) { panic("private logger panic") }))
	f.engine.SetMetrics(ws.MetricsFunc(func(context.Context, ws.Metric) { panic("private metrics panic") }))
	result, err := f.engine.RunMaintenanceWithResult(context.Background(), time.Time{})
	if err != nil || result.Failed != 0 || result.ExpiredSessionBatches != 1 || result.ExpiredOfflineBatches != 1 || result.ExpiredIdempotencyBatches != 1 {
		t.Fatalf("maintenance with panicking hooks = %+v, %v", result, err)
	}
}

func assertMaintenanceMetric(t *testing.T, metrics []ws.Metric, name string, want int64) {
	t.Helper()
	for _, metric := range metrics {
		if metric.Name == name {
			if metric.Kind != ws.MetricCounter || metric.Value != want {
				t.Fatalf("metric %s = %+v, want counter %d", name, metric, want)
			}
			return
		}
	}
	t.Fatalf("metric %s was not emitted; metrics = %+v", name, metrics)
}
