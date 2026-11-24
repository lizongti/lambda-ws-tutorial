package ws

import "time"

// heartbeatCutoff is strict: ListStaleConnections selects LastSeenAt values
// before this timestamp, not a connection observed exactly at the boundary.
func (e *Engine) heartbeatCutoff(now time.Time) time.Time {
	return now.Add(-e.Options.Heartbeat.Timeout)
}

// HeartbeatInterval reports the configured application heartbeat cadence. It
// does not alter API Gateway's own idle or maximum connection lifetime limits.
func (e *Engine) HeartbeatInterval() time.Duration {
	if e == nil || e.Options == nil {
		return 0
	}
	return e.Options.Heartbeat.Interval
}

// HeartbeatTimeout reports the inactivity window used by RunMaintenance.
func (e *Engine) HeartbeatTimeout() time.Duration {
	if e == nil || e.Options == nil {
		return 0
	}
	return e.Options.Heartbeat.Timeout
}
