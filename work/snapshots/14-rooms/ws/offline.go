package ws

import (
	"context"
	"encoding/json"
	"errors"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

// DeliveryStatus distinguishes a live PostToConnection success from durable
// offline acceptance. Neither status implies client acknowledgment.
type DeliveryStatus string

type replaySentMarker interface {
	MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error
}

const (
	DeliverySent    DeliveryStatus = "sent"
	DeliveryOffline DeliveryStatus = "offline"
)

// DeliveryResult describes one session delivery without retaining its payload.
type DeliveryResult struct {
	Status       DeliveryStatus
	SessionID    string
	ConnectionID string
	MessageID    string
	Gone         bool
	Offline      bool
}

// ReplayResult summarizes a bounded, ordered replay attempt.
type ReplayResult struct {
	Attempted    int
	Sent         int
	Acknowledged int
	Retained     int
	Failed       int
	Batches      int
	Truncated    bool
}

// PushToSession preserves the original Engine API. A nil error means either a
// live push succeeded or the message was durably accepted for offline replay;
// use PushToSessionWithResult when the distinction matters.
func (e *Engine) PushToSession(ctx context.Context, sessionID string, msg OutboundMessage) error {
	_, err := e.PushToSessionWithResult(ctx, sessionID, msg)
	return err
}

// PushToSessionWithResult resolves the current binding, fences stale records,
// and applies the configured offline policy without depending on instance
// affinity. Binding changes are re-resolved once before falling back to queue.
func (e *Engine) PushToSessionWithResult(ctx context.Context, sessionID string, msg OutboundMessage) (DeliveryResult, error) {
	result := DeliveryResult{SessionID: sessionID}
	if e == nil || ctx == nil {
		return result, ErrInvalidEvent
	}
	if err := ValidateSessionID(sessionID); err != nil {
		return result, err
	}
	session, err := e.Options.StateStore.GetSession(ctx, sessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return result, errors.Join(ErrSessionNotFound, err)
		}
		return result, err
	}
	prepared, body, err := e.prepareOutbound(msg, sessionID)
	if err != nil {
		return result, err
	}
	result.MessageID = prepared.MessageID

	for attempt := 0; attempt < 2; attempt++ {
		result.ConnectionID = session.CurrentConnectionID
		if session.CurrentConnectionID == "" {
			return e.queueSessionDelivery(ctx, result, prepared, body)
		}
		connection, getErr := e.Options.StateStore.GetConnection(ctx, session.CurrentConnectionID)
		if getErr != nil {
			if !wsstore.IsNotFound(getErr) {
				return result, getErr
			}
			_ = e.Options.StateStore.RemoveConnection(ctx, session.CurrentConnectionID)
			if unbindErr := e.Options.StateStore.UnbindConnectionIfMatch(ctx, session.SessionID, session.CurrentConnectionID, session.Generation); unbindErr != nil && !wsstore.IsConditionConflict(unbindErr) {
				return result, unbindErr
			}
			return e.queueSessionDelivery(ctx, result, prepared, body)
		}
		if connection.State != wsstore.ConnectionStateActive || connection.SessionID != session.SessionID || connection.Generation != session.Generation {
			latest, latestErr := e.Options.StateStore.GetSession(ctx, sessionID)
			if latestErr != nil {
				return result, latestErr
			}
			session = latest
			continue
		}

		err = e.postEncoded(ctx, &connection, prepared.MessageID, body)
		if err == nil {
			result.Status = DeliverySent
			return result, nil
		}
		if !IsPushGone(err) {
			return result, err
		}
		result.Gone = true
		if !e.Options.Offline.Enabled {
			return result, err
		}
		queued, queueErr := e.queueSessionDelivery(ctx, result, prepared, body)
		if queueErr != nil {
			return queued, errors.Join(err, queueErr)
		}
		return queued, nil
	}
	return e.queueSessionDelivery(ctx, result, prepared, body)
}

func (e *Engine) queueSessionDelivery(ctx context.Context, result DeliveryResult, msg OutboundMessage, body []byte) (DeliveryResult, error) {
	if !e.Options.Offline.Enabled {
		return result, ErrOfflineDisabled
	}
	if err := e.enqueueOffline(ctx, result.SessionID, msg, body); err != nil {
		return result, err
	}
	result.Status = DeliveryOffline
	result.Offline = true
	return result, nil
}

func (e *Engine) enqueueOffline(ctx context.Context, sessionID string, msg OutboundMessage, body []byte) error {
	if !e.Options.Offline.Enabled {
		return ErrOfflineDisabled
	}
	session, err := e.Options.StateStore.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	now := e.Options.Clock.Now()
	if !session.ExpiresAt.IsZero() && !session.ExpiresAt.After(now) {
		return ErrSessionNotFound
	}
	offline := wsstore.OfflineMessage{
		SessionID:  sessionID,
		MessageID:  msg.MessageID,
		Generation: session.Generation,
		Envelope:   json.RawMessage(append([]byte(nil), body...)),
		CreatedAt:  now,
		ExpiresAt:  now.Add(e.Options.Offline.Retention),
		State:      wsstore.OfflineMessageStatePending,
	}
	if err := e.Options.StateStore.Append(ctx, offline); err != nil {
		e.observability.log(ctx, LogEvent{Name: LogEventPush, Outcome: "offline_rejected", SessionID: sessionID, MessageID: msg.MessageID, ErrorCode: safeErrorCode(err)})
		return err
	}
	depth, depthErr := e.offlineQueueDepth(ctx, sessionID)
	if depthErr == nil {
		e.observability.record(ctx, Metric{Name: MetricOfflineQueueDepth, Kind: MetricGauge, Value: int64(depth), SessionID: sessionID})
	}
	e.observability.log(ctx, LogEvent{Name: LogEventPush, Outcome: "offline", SessionID: sessionID, MessageID: msg.MessageID, Generation: session.Generation})
	return nil
}

// ReplayOffline sends a bounded snapshot in sequence order. Unsent messages are
// delivered immediately; clientAck messages already sent are redelivered only
// after Offline.AckTimeout. Retained messages remain in the snapshot after
// MarkSent, so batching never skips entries behind them; a queue larger than
// MaxBatchSize is continued by a later explicit replay/reconnect invocation.
func (e *Engine) ReplayOffline(ctx context.Context, sessionID string) (ReplayResult, error) {
	if e == nil || ctx == nil {
		return ReplayResult{}, ErrInvalidEvent
	}
	session, err := e.Options.StateStore.GetSession(ctx, sessionID)
	if err != nil {
		return ReplayResult{}, err
	}
	if session.CurrentConnectionID == "" {
		return ReplayResult{}, ErrConnectionNotFound
	}
	return e.replaySession(ctx, sessionID, session.CurrentConnectionID, session.Generation)
}

func (e *Engine) replaySession(ctx context.Context, sessionID, connectionID string, generation uint64) (ReplayResult, error) {
	var result ReplayResult
	if !e.Options.Offline.Enabled {
		return result, nil
	}
	_, session, err := e.validateActiveBinding(ctx, connectionID)
	if err != nil {
		return result, err
	}
	if session.SessionID != sessionID || session.Generation != generation {
		return result, ErrStaleConnection
	}
	messages, err := e.Options.StateStore.ListPending(ctx, sessionID, wsstore.MaxBatchSize)
	if err != nil {
		return result, err
	}
	result.Truncated = len(messages) == wsstore.MaxBatchSize
	batchSize := e.Options.Offline.ReplayBatchSize
	replayNow := e.Options.Clock.Now()
	for start := 0; start < len(messages); start += batchSize {
		end := start + batchSize
		if end > len(messages) {
			end = len(messages)
		}
		result.Batches++
		for _, message := range messages[start:end] {
			if message.Generation > generation {
				result.Retained++
				continue
			}
			if e.Options.Offline.AckPolicy == AckPolicyClientAck &&
				message.Generation == generation && message.State == wsstore.OfflineMessageStateSent && message.SentAt != nil &&
				replayNow.Before(message.SentAt.Add(e.Options.Offline.AckTimeout)) {
				result.Retained++
				continue
			}
			// Re-read at every batch boundary and before each send so a reconnect
			// or cleanup during replay stops delivery to the old generation.
			currentConnection, currentSession, bindingErr := e.validateActiveBinding(ctx, connectionID)
			if bindingErr != nil {
				result.Failed++
				return result, bindingErr
			}
			if currentSession.Generation != generation || currentConnection.Generation != generation {
				result.Failed++
				return result, ErrStaleConnection
			}
			result.Attempted++
			if err := e.postEncoded(ctx, &currentConnection, message.MessageID, message.Envelope); err != nil {
				result.Failed++
				e.observability.log(ctx, LogEvent{Name: LogEventReplay, Outcome: "failure", ConnectionID: connectionID, SessionID: sessionID, MessageID: message.MessageID, ErrorCode: safeErrorCode(err), Generation: generation})
				return result, err
			}
			result.Sent++
			if marker, ok := e.Options.StateStore.(replaySentMarker); ok {
				err = marker.MarkSentForReplay(ctx, sessionID, message.MessageID, generation)
			} else {
				err = e.Options.StateStore.MarkSent(ctx, sessionID, message.MessageID, generation)
			}
			if err != nil {
				result.Failed++
				return result, err
			}
			if e.Options.Offline.AckPolicy == AckPolicyPostSuccess {
				if err := e.Options.StateStore.Ack(ctx, sessionID, message.MessageID, generation); err != nil {
					result.Failed++
					return result, err
				}
				result.Acknowledged++
			} else {
				result.Retained++
			}
			e.observability.record(ctx, Metric{Name: MetricReplay, Kind: MetricCounter, Value: 1, ConnectionID: connectionID, SessionID: sessionID})
		}
	}
	e.observability.log(ctx, LogEvent{Name: LogEventReplay, Outcome: "success", ConnectionID: connectionID, SessionID: sessionID, Count: result.Sent, Generation: generation})
	if depth, depthErr := e.offlineQueueDepth(ctx, sessionID); depthErr == nil {
		e.observability.record(ctx, Metric{Name: MetricOfflineQueueDepth, Kind: MetricGauge, Value: int64(depth), SessionID: sessionID})
	}
	return result, nil
}

func (e *Engine) acknowledge(ctx context.Context, connection wsstore.Connection, envelope Envelope) error {
	if !e.Options.Offline.Enabled {
		return ErrOfflineDisabled
	}
	if envelope.SessionID != "" && envelope.SessionID != connection.SessionID {
		return ErrAuthorizationFailed
	}
	return e.Options.StateStore.Ack(ctx, connection.SessionID, envelope.MessageID, connection.Generation)
}

func (e *Engine) offlineQueueDepth(ctx context.Context, sessionID string) (int, error) {
	messages, err := e.Options.StateStore.ListPending(ctx, sessionID, wsstore.MaxBatchSize)
	return len(messages), err
}
