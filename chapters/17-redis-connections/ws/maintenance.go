package ws

import (
	"context"
	"errors"
	"fmt"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

// MaintenanceResult summarizes one bounded maintenance invocation. Session,
// offline and idempotency stores deliberately expose batch deletion rather than
// row counts, so their expiration fields count completed batches. Connection
// expiration scans return records and therefore have exact scan/cleanup counts.
// Skipped counts duplicate candidates and unsupported optional cleanup batches.
type MaintenanceResult struct {
	StaleScanned              int
	StaleCleaned              int
	ExpiredScanned            int
	ExpiredCleaned            int
	Skipped                   int
	Conflicts                 int
	Failed                    int
	ExpiredSessionBatches     int
	ExpiredOfflineBatches     int
	ExpiredIdempotencyBatches int
}

// HandleDisconnect conditionally removes exactly the physical binding observed
// in the store. Late events for an old generation are successful no-ops.
func (e *Engine) HandleDisconnect(ctx context.Context, event DisconnectEvent) (err error) {
	if e == nil || e.Options == nil {
		return ErrInternal
	}
	defer func() {
		if recover() != nil {
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return ErrInvalidEvent
	}
	if err := ValidateConnectionID(event.ConnectionID); err != nil {
		return fmt.Errorf("%w: connectionId", ErrInvalidEvent)
	}
	connection, err := e.Options.StateStore.GetConnection(ctx, event.ConnectionID)
	if wsstore.IsNotFound(err) {
		// A previous attempt may have deleted the physical connection before one
		// of the secondary writes failed. Retry those writes with the fencing
		// metadata carried by the event instead of treating NotFound as proof
		// that the whole disconnect completed.
		if err := e.cleanupMissingConnection(ctx, event); err != nil {
			e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "failure", RequestID: event.RequestID, ConnectionID: event.ConnectionID, SessionID: event.SessionID, Generation: event.Generation, ErrorCode: safeErrorCode(err)})
			return err
		}
		e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "duplicate", RequestID: event.RequestID, ConnectionID: event.ConnectionID})
		return nil
	}
	if err != nil {
		return err
	}
	if event.Generation != 0 && event.Generation != connection.Generation {
		return nil
	}
	if event.SessionID != "" && event.SessionID != connection.SessionID {
		return nil
	}
	if err := e.cleanupConnection(ctx, connection, "disconnect"); err != nil {
		if isFencingError(err) {
			return nil
		}
		e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "failure", RequestID: event.RequestID, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, ErrorCode: safeErrorCode(err)})
		return err
	}
	e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "success", RequestID: event.RequestID, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, Generation: connection.Generation})
	return nil
}

// cleanupMissingConnection finishes secondary cleanup after the physical
// connection has already disappeared. Both operations are attempted so one
// storage failure cannot suppress the other, and a later disconnect can retry
// either operation independently. The session CAS protects a replacement
// generation; room cleanup is scoped to the old physical connection ID.
func (e *Engine) cleanupMissingConnection(ctx context.Context, event DisconnectEvent) error {
	var errs []error
	if event.SessionID != "" {
		if err := e.Options.StateStore.UnbindConnectionIfMatch(ctx, event.SessionID, event.ConnectionID, event.Generation); err != nil && !wsstore.IsConditionConflict(err) {
			errs = append(errs, err)
		}
	}
	if err := e.Options.StateStore.RemoveConnection(ctx, event.ConnectionID); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// cleanupConnection is shared by disconnect, stale maintenance and Pusher gone.
// Conditional deletion happens before secondary cleanup so a replacement stored
// under the same connection ID cannot be damaged by an old snapshot.
func (e *Engine) cleanupConnection(ctx context.Context, connection wsstore.Connection, reason string) error {
	var err error
	if reason == "stale" {
		if _, supported := e.stateStore.(wsstore.ConnectionFreshnessStore); supported {
			freshnessStore := e.Options.StateStore.(wsstore.ConnectionFreshnessStore)
			err = freshnessStore.DeleteConnectionIfMatchAndLastSeen(ctx, connection.ConnectionID, connection.Generation, connection.LastSeenAt)
		} else {
			err = e.Options.StateStore.DeleteConnectionIfMatch(ctx, connection.ConnectionID, connection.Generation)
		}
	} else {
		err = e.Options.StateStore.DeleteConnectionIfMatch(ctx, connection.ConnectionID, connection.Generation)
	}
	if err != nil {
		return err
	}
	var errs []error
	if err := e.Options.StateStore.RemoveConnection(ctx, connection.ConnectionID); err != nil {
		errs = append(errs, err)
	}
	if connection.SessionID != "" {
		if err := e.Options.StateStore.UnbindConnectionIfMatch(ctx, connection.SessionID, connection.ConnectionID, connection.Generation); err != nil && !wsstore.IsConditionConflict(err) {
			errs = append(errs, err)
		}
	}
	e.observability.record(ctx, Metric{Name: MetricActiveConnections, Kind: MetricCounter, Value: -1, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, Reason: reason})
	if reason == "stale" || reason == "gone" {
		e.observability.record(ctx, Metric{Name: MetricStaleCleanups, Kind: MetricCounter, Value: 1, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, Reason: reason})
	}
	return errors.Join(errs...)
}

// RunMaintenance executes one bounded set of cleanup batches. now may be zero,
// in which case the injected Clock is used.
func (e *Engine) RunMaintenance(ctx context.Context, now time.Time) error {
	_, err := e.RunMaintenanceWithResult(ctx, now)
	return err
}

// RunMaintenanceWithResult is the inspectable form of RunMaintenance.
func (e *Engine) RunMaintenanceWithResult(ctx context.Context, now time.Time) (result MaintenanceResult, err error) {
	if e == nil || e.Options == nil {
		return result, ErrInternal
	}
	defer func() {
		if recover() != nil {
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return result, ErrInvalidEvent
	}
	if now.IsZero() {
		now = e.Options.Clock.Now()
	}
	var errs []error
	type candidateKey struct {
		connectionID string
		generation   uint64
	}
	seen := make(map[candidateKey]struct{})
	connections, listErr := e.Options.StateStore.ListStaleConnections(ctx, e.heartbeatCutoff(now), e.Options.Heartbeat.MaintenanceBatchSize)
	if listErr != nil {
		result.Failed++
		errs = append(errs, listErr)
	} else {
		result.StaleScanned = len(connections)
		for _, connection := range connections {
			seen[candidateKey{connectionID: connection.ConnectionID, generation: connection.Generation}] = struct{}{}
			if cleanupErr := e.cleanupConnection(ctx, connection, "stale"); cleanupErr != nil {
				if isFencingError(cleanupErr) {
					result.Conflicts++
					continue
				}
				result.Failed++
				errs = append(errs, cleanupErr)
				continue
			}
			result.StaleCleaned++
		}
	}

	if _, supported := e.stateStore.(wsstore.ConnectionExpirationStore); !supported {
		result.Skipped++
	} else {
		expirationStore := e.Options.StateStore.(wsstore.ConnectionExpirationStore)
		expiredConnections, listErr := expirationStore.ListExpiredConnections(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize)
		if listErr != nil {
			result.Failed++
			errs = append(errs, listErr)
		} else {
			result.ExpiredScanned = len(expiredConnections)
			for _, connection := range expiredConnections {
				key := candidateKey{connectionID: connection.ConnectionID, generation: connection.Generation}
				if _, duplicate := seen[key]; duplicate {
					result.Skipped++
					continue
				}
				seen[key] = struct{}{}
				if cleanupErr := e.cleanupConnection(ctx, connection, "expired"); cleanupErr != nil {
					if isFencingError(cleanupErr) {
						result.Conflicts++
						continue
					}
					result.Failed++
					errs = append(errs, cleanupErr)
					continue
				}
				result.ExpiredCleaned++
			}
		}
	}

	if err := e.Options.StateStore.DeleteExpiredSessions(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredSessionBatches = 1
	}
	if err := e.Options.StateStore.DeleteExpired(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredOfflineBatches = 1
	}
	if err := e.Options.StateStore.DeleteExpiredIdempotency(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredIdempotencyBatches = 1
	}

	joined := errors.Join(errs...)
	outcome := "success"
	if joined != nil {
		outcome = "partial_failure"
	}
	e.recordMaintenanceObservability(ctx, result, outcome, joined)
	return result, joined
}

func (e *Engine) recordMaintenanceObservability(ctx context.Context, result MaintenanceResult, outcome string, err error) {
	stats := result
	e.observability.log(ctx, LogEvent{
		Name: LogEventMaintenance, Outcome: outcome, Count: result.StaleCleaned + result.ExpiredCleaned,
		ErrorCode: safeErrorCode(err), Maintenance: &stats,
	})
	for _, metric := range []Metric{
		{Name: MetricMaintenanceSucceeded, Kind: MetricCounter, Value: int64(result.StaleCleaned + result.ExpiredCleaned)},
		{Name: MetricMaintenanceConflicts, Kind: MetricCounter, Value: int64(result.Conflicts)},
		{Name: MetricMaintenanceFailures, Kind: MetricCounter, Value: int64(result.Failed)},
		{Name: MetricMaintenanceSkipped, Kind: MetricCounter, Value: int64(result.Skipped)},
		{Name: MetricMaintenanceExpiredConnections, Kind: MetricCounter, Value: int64(result.ExpiredCleaned)},
		{Name: MetricMaintenanceExpiredSessions, Kind: MetricCounter, Value: int64(result.ExpiredSessionBatches)},
		{Name: MetricMaintenanceExpiredOffline, Kind: MetricCounter, Value: int64(result.ExpiredOfflineBatches)},
		{Name: MetricMaintenanceExpiredIdempotency, Kind: MetricCounter, Value: int64(result.ExpiredIdempotencyBatches)},
	} {
		e.observability.record(ctx, metric)
	}
}
