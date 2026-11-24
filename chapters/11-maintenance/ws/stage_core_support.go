package ws

import (
	"context"
	"crypto/sha256"
	"fmt"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"time"
)

type replaySentMarker interface {
	MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error
}

func (e *Engine) HandleConnect(ctx context.Context, event ConnectEvent) (ConnectResult, error) {
	if existing, err := e.Options.StateStore.GetConnection(ctx, event.ConnectionID); err == nil {
		return ConnectResult{ConnectionID: existing.ConnectionID, SessionID: existing.SessionID, Generation: existing.Generation}, nil
	}
	authenticator := e.Options.Authenticator
	if authenticator == nil {
		authenticator = NewAnonymousAuthenticator()
	}
	principal, err := authenticator.Authenticate(ctx, ConnectRequest{ConnectionID: event.ConnectionID, RequestID: event.RequestID})
	if err != nil {
		return ConnectResult{}, ErrAuthenticationFailed
	}
	now := e.Options.Clock.Now()
	sessionID := e.Options.IDGenerator.ID("session")
	credential := e.Options.IDGenerator.ID("resume")
	generation := uint64(1)
	session := wsstore.Session{SessionID: sessionID, PrincipalSubject: principal.Subject, CurrentConnectionID: event.ConnectionID, Generation: generation, ResumeCredentialHash: fmt.Sprintf("%x", sha256.Sum256([]byte(sessionID+credential))), LastActiveAt: now, ExpiresAt: now.Add(e.Options.Reconnect.SessionRetention)}
	if err := e.Options.StateStore.CreateSession(ctx, session); err != nil {
		return ConnectResult{}, err
	}
	connection := wsstore.Connection{ConnectionID: event.ConnectionID, SessionID: sessionID, PrincipalSubject: principal.Subject, Generation: generation, ConnectedAt: now, LastSeenAt: now, ExpiresAt: now.Add(e.Options.Reconnect.SessionRetention), State: wsstore.ConnectionStateActive}
	if err := e.Options.StateStore.PutConnection(ctx, connection); err != nil {
		return ConnectResult{}, err
	}
	return ConnectResult{ConnectionID: event.ConnectionID, SessionID: sessionID, Generation: generation, ResumeCredential: credential}, nil
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
