package ws

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

const (
	LogEventConnect     = "connect"
	LogEventDisconnect  = "disconnect"
	LogEventReconnect   = "reconnect"
	LogEventPush        = "push"
	LogEventReplay      = "replay"
	LogEventBroadcast   = "broadcast"
	LogEventMaintenance = "maintenance"
)

const (
	MetricActiveConnections = "ws.active_connections"
	MetricPushFailures      = "ws.push_failures"
	MetricStaleCleanups     = "ws.stale_cleanups"
	MetricOfflineQueueDepth = "ws.offline_queue_depth"
	MetricReplay            = "ws.replay"
	MetricBroadcastFailures = "ws.broadcast_failures"

	MetricMaintenanceSucceeded          = "ws.maintenance_succeeded"
	MetricMaintenanceConflicts          = "ws.maintenance_conflicts"
	MetricMaintenanceFailures           = "ws.maintenance_failures"
	MetricMaintenanceSkipped            = "ws.maintenance_skipped"
	MetricMaintenanceExpiredConnections = "ws.maintenance_expired_connections"
	MetricMaintenanceExpiredSessions    = "ws.maintenance_expired_session_batches"
	MetricMaintenanceExpiredOffline     = "ws.maintenance_expired_offline_batches"
	MetricMaintenanceExpiredIdempotency = "ws.maintenance_expired_idempotency_batches"
)

// LogEvent is a structured, allow-listed diagnostic event. It intentionally has
// no payload, credential, token, header, claim, or arbitrary error-text field.
type LogEvent struct {
	Name         string
	Outcome      string
	RequestID    string
	ConnectionID string
	SessionID    string
	MessageID    string
	RoomID       string
	ErrorCode    string
	Reason       string
	Count        int
	Generation   uint64
	// Maintenance is non-nil only for maintenance summaries and contains counts,
	// never record identifiers, payloads, credentials, or backend error text.
	Maintenance *MaintenanceResult
}

// String provides a safe representation for loggers which format the event.
func (e LogEvent) String() string {
	formatted := fmt.Sprintf("event=%q outcome=%q requestId=%q connectionId=%q sessionId=%q messageId=%q roomId=%q errorCode=%q reason=%q count=%d generation=%d",
		e.Name, e.Outcome, e.RequestID, e.ConnectionID, e.SessionID, e.MessageID, e.RoomID, e.ErrorCode, e.Reason, e.Count, e.Generation)
	if e.Maintenance != nil {
		formatted += fmt.Sprintf(" maintenance=%+v", *e.Maintenance)
	}
	return formatted
}

// Logger receives structured events. Implementations must not add request
// bodies or ConnectEvent values to their output.
type Logger interface {
	Log(context.Context, LogEvent)
}

// LoggerFunc adapts a function to Logger.
type LoggerFunc func(context.Context, LogEvent)

func (f LoggerFunc) Log(ctx context.Context, event LogEvent) {
	if f != nil {
		f(ctx, event)
	}
}

// MetricKind distinguishes cumulative counters from point-in-time gauges.
type MetricKind string

const (
	MetricCounter MetricKind = "counter"
	MetricGauge   MetricKind = "gauge"
)

// Metric contains only fixed metric names and safe dimensional fields.
type Metric struct {
	Name         string
	Kind         MetricKind
	Value        int64
	RequestID    string
	ConnectionID string
	SessionID    string
	RoomID       string
	Reason       string
}

// Metrics receives counters and gauges emitted by Engine.
type Metrics interface {
	Record(context.Context, Metric)
}

// MetricsFunc adapts a function to Metrics.
type MetricsFunc func(context.Context, Metric)

func (f MetricsFunc) Record(ctx context.Context, metric Metric) {
	if f != nil {
		f(ctx, metric)
	}
}

type observability struct {
	mu      sync.RWMutex
	logger  Logger
	metrics Metrics
}

func (o *observability) setLogger(logger Logger) {
	o.mu.Lock()
	o.logger = logger
	o.mu.Unlock()
}

func (o *observability) setMetrics(metrics Metrics) {
	o.mu.Lock()
	o.metrics = metrics
	o.mu.Unlock()
}

func (o *observability) log(ctx context.Context, event LogEvent) {
	if o == nil {
		return
	}
	o.mu.RLock()
	logger := o.logger
	o.mu.RUnlock()
	if nilDependency(logger) {
		return
	}
	event = sanitizeLogEvent(event)
	// Diagnostics must not be able to take down event processing.
	defer func() { _ = recover() }()
	logger.Log(ctx, event)
}

func (o *observability) record(ctx context.Context, metric Metric) {
	if o == nil {
		return
	}
	o.mu.RLock()
	metrics := o.metrics
	o.mu.RUnlock()
	if nilDependency(metrics) {
		return
	}
	metric.RequestID = safeLogID(metric.RequestID, MaxRequestIDBytes)
	metric.ConnectionID = safeLogID(metric.ConnectionID, MaxConnectionIDBytes)
	metric.SessionID = safeLogID(metric.SessionID, MaxSessionIDBytes)
	metric.RoomID = safeLogID(metric.RoomID, MaxRoomIDBytes)
	metric.Reason = safeLogToken(metric.Reason, 64)
	defer func() { _ = recover() }()
	metrics.Record(ctx, metric)
}

// SetLogger replaces the structured log hook. A nil logger disables logging.
func (e *Engine) SetLogger(logger Logger) {
	if e != nil {
		e.observability.setLogger(logger)
	}
}

// SetMetrics replaces the metrics hook. A nil hook disables metrics.
func (e *Engine) SetMetrics(metrics Metrics) {
	if e != nil {
		e.observability.setMetrics(metrics)
	}
}

func sanitizeLogEvent(event LogEvent) LogEvent {
	event.Name = safeLogToken(event.Name, 64)
	event.Outcome = safeLogToken(event.Outcome, 64)
	event.RequestID = safeLogID(event.RequestID, MaxRequestIDBytes)
	event.ConnectionID = safeLogID(event.ConnectionID, MaxConnectionIDBytes)
	event.SessionID = safeLogID(event.SessionID, MaxSessionIDBytes)
	event.MessageID = safeLogID(event.MessageID, MaxMessageIDBytes)
	event.RoomID = safeLogID(event.RoomID, MaxRoomIDBytes)
	event.ErrorCode = safeLogToken(event.ErrorCode, MaxErrorCodeBytes)
	event.Reason = safeLogToken(event.Reason, 64)
	if event.Count < 0 {
		event.Count = 0
	}
	return event
}

func safeLogID(value string, max int) string {
	if value == "" {
		return ""
	}
	if len(value) > max || strings.TrimSpace(value) != value {
		return "[redacted]"
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return "[redacted]"
		}
	}
	return value
}

func safeLogToken(value string, max int) string {
	if value == "" {
		return ""
	}
	if len(value) > max {
		return "[redacted]"
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune("_.-:", r) {
			return "[redacted]"
		}
	}
	return value
}
