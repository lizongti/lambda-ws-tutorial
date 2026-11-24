package ws

import (
	"context"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"time"
)

type replaySentMarker interface {
	MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error
}

type ReplayResult struct {
	Attempted, Sent, Acknowledged, Retained, Failed, Batches int
	Truncated                                                bool
}

func (e *Engine) replaySession(context.Context, string, string, uint64) (ReplayResult, error) {
	return ReplayResult{}, nil
}
func (e *Engine) acknowledge(ctx context.Context, connection wsstore.Connection, envelope Envelope) error {
	return e.Options.StateStore.Ack(ctx, connection.SessionID, envelope.MessageID, connection.Generation)
}
func (e *Engine) enqueueOffline(ctx context.Context, sessionID string, msg OutboundMessage, body []byte) error {
	now := time.Now()
	if e.Options.Clock != nil {
		now = e.Options.Clock.Now()
	}
	return e.Options.StateStore.Append(ctx, wsstore.OfflineMessage{SessionID: sessionID, MessageID: msg.MessageID, Generation: 1, Envelope: append([]byte(nil), body...), CreatedAt: now, ExpiresAt: now.Add(e.Options.Offline.Retention), State: wsstore.OfflineMessageStatePending})
}
