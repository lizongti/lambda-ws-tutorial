package ws

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

const maxResumeCredentialBytes = 4096

// HashResumeCredential returns a domain-separated SHA-256 representation. The
// session ID is included so a credential hash cannot be transplanted between
// logical sessions. No plaintext credential is retained by Engine.
func HashResumeCredential(sessionID, credential string) string {
	digest := sha256.Sum256([]byte("lambda/ws/resume-v1\x00" + sessionID + "\x00" + credential))
	return hex.EncodeToString(digest[:])
}

// HandleConnect authenticates before any state write, then creates a new
// session, resumes an existing one, or accepts an idempotent duplicate event.
func (e *Engine) HandleConnect(ctx context.Context, event ConnectEvent) (result ConnectResult, err error) {
	if e == nil || e.Options == nil {
		return ConnectResult{}, ErrInternal
	}
	defer func() {
		if recover() != nil {
			result = ConnectResult{}
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return ConnectResult{}, ErrInvalidEvent
	}
	if err := ValidateConnectionID(event.ConnectionID); err != nil {
		return ConnectResult{}, fmt.Errorf("%w: connectionId", ErrInvalidEvent)
	}
	if event.SessionID != "" {
		if err := ValidateSessionID(event.SessionID); err != nil {
			return ConnectResult{}, ErrInvalidResume
		}
	}
	if len(event.ResumeCredential) > maxResumeCredentialBytes {
		return ConnectResult{}, ErrInvalidResume
	}

	// The engine-level lock makes retries of the same physical connection
	// idempotent within one process. Shared backends still provide the atomic
	// session CAS required between processes.
	e.connectMu.Lock()
	defer e.connectMu.Unlock()

	authenticator := e.Options.Authenticator
	if nilDependency(authenticator) {
		authenticator = NewAnonymousAuthenticator()
	}
	principal, err := authenticator.Authenticate(ctx, ConnectRequest{
		ConnectionID:                    event.ConnectionID,
		RequestID:                       event.RequestID,
		Headers:                         cloneStringMap(event.Headers),
		QueryParameters:                 cloneStringMap(event.QueryParameters),
		MultiValueHeaders:               cloneStringSliceMap(event.MultiValueHeaders),
		MultiValueQueryStringParameters: cloneStringSliceMap(event.MultiValueQueryStringParameters),
		RequestContext:                  cloneAnyMap(event.RequestContext),
	})
	if err != nil || strings.TrimSpace(principal.Subject) == "" {
		e.observability.log(ctx, LogEvent{Name: LogEventConnect, Outcome: "denied", RequestID: event.RequestID, ConnectionID: event.ConnectionID, ErrorCode: "authentication_failed"})
		return ConnectResult{}, ErrAuthenticationFailed
	}
	principal = principal.Clone()
	if err := ctx.Err(); err != nil {
		return ConnectResult{}, err
	}

	if existing, getErr := e.Options.StateStore.GetConnection(ctx, event.ConnectionID); getErr == nil {
		return e.handleDuplicateConnect(ctx, event, principal, existing)
	} else if !wsstore.IsNotFound(getErr) {
		return ConnectResult{}, getErr
	}

	if event.ResumeCredential != "" || event.SessionID != "" {
		if !e.Options.Reconnect.Enabled {
			return ConnectResult{}, ErrReconnectDisabled
		}
		if event.ResumeCredential == "" || event.SessionID == "" {
			return ConnectResult{}, ErrInvalidResume
		}
		result, err = e.resumeSession(ctx, event, principal)
	} else {
		result, err = e.createSession(ctx, event, principal)
	}
	if err != nil {
		e.observability.log(ctx, LogEvent{Name: LogEventConnect, Outcome: "failure", RequestID: event.RequestID, ConnectionID: event.ConnectionID, ErrorCode: safeErrorCode(err)})
		return result, err
	}

	name := LogEventConnect
	if result.Reconnected {
		name = LogEventReconnect
	}
	e.observability.log(ctx, LogEvent{Name: name, Outcome: "success", RequestID: event.RequestID, ConnectionID: result.ConnectionID, SessionID: result.SessionID, Generation: result.Generation})
	e.observability.record(ctx, Metric{Name: MetricActiveConnections, Kind: MetricCounter, Value: 1, RequestID: event.RequestID, ConnectionID: result.ConnectionID, SessionID: result.SessionID})
	if e.Options.Offline.Enabled {
		replay, replayErr := e.replaySession(ctx, result.SessionID, result.ConnectionID, result.Generation)
		result.Replay = replay
		if replayErr != nil {
			return result, replayErr
		}
	}
	return result, nil
}

func (e *Engine) handleDuplicateConnect(ctx context.Context, event ConnectEvent, principal Principal, connection wsstore.Connection) (ConnectResult, error) {
	if connection.State != wsstore.ConnectionStateActive || connection.PrincipalSubject != principal.Subject ||
		(event.SessionID != "" && event.SessionID != connection.SessionID) {
		return ConnectResult{}, ErrConnectionConflict
	}
	session, err := e.Options.StateStore.GetSession(ctx, connection.SessionID)
	if err != nil {
		return ConnectResult{}, err
	}
	if session.CurrentConnectionID != connection.ConnectionID || session.Generation != connection.Generation ||
		session.PrincipalSubject != principal.Subject {
		return ConnectResult{}, ErrConnectionConflict
	}
	if event.ResumeCredential != "" && !resumeCredentialMatches(session, event.ResumeCredential) {
		return ConnectResult{}, ErrInvalidResume
	}
	result := ConnectResult{
		ConnectionID:     connection.ConnectionID,
		SessionID:        session.SessionID,
		Generation:       session.Generation,
		ResumeCredential: event.ResumeCredential,
	}
	if e.Options.Offline.Enabled {
		replay, replayErr := e.replaySession(ctx, session.SessionID, connection.ConnectionID, connection.Generation)
		result.Replay = replay
		if replayErr != nil {
			return result, replayErr
		}
	}
	e.observability.log(ctx, LogEvent{Name: LogEventConnect, Outcome: "duplicate", RequestID: event.RequestID, ConnectionID: connection.ConnectionID, SessionID: session.SessionID, Generation: session.Generation})
	return result, nil
}

func (e *Engine) createSession(ctx context.Context, event ConnectEvent, principal Principal) (ConnectResult, error) {
	now := e.Options.Clock.Now()
	for attempt := 0; attempt < 4; attempt++ {
		sessionID := e.Options.IDGenerator.ID("session")
		if err := ValidateSessionID(sessionID); err != nil {
			return ConnectResult{}, err
		}
		credential := e.Options.IDGenerator.ID("resume")
		if credential == "" || len(credential) > maxResumeCredentialBytes {
			return ConnectResult{}, ErrInternal
		}
		session := wsstore.Session{
			SessionID:            sessionID,
			PrincipalSubject:     principal.Subject,
			ResumeCredentialHash: HashResumeCredential(sessionID, credential),
			LastActiveAt:         now,
			ExpiresAt:            now.Add(e.Options.Reconnect.SessionRetention),
		}
		if err := e.Options.StateStore.CreateSession(ctx, session); err != nil {
			if wsstore.IsConditionConflict(err) {
				continue
			}
			return ConnectResult{}, err
		}
		bound, err := e.Options.StateStore.BindConnection(ctx, sessionID, event.ConnectionID, 0)
		if err != nil {
			return ConnectResult{}, err
		}
		connection := e.newConnection(event.ConnectionID, bound, now)
		if err := e.Options.StateStore.PutConnection(ctx, connection); err != nil {
			_ = e.Options.StateStore.UnbindConnectionIfMatch(ctx, sessionID, event.ConnectionID, bound.Generation)
			return ConnectResult{}, err
		}
		return ConnectResult{ConnectionID: event.ConnectionID, SessionID: sessionID, Generation: bound.Generation, ResumeCredential: credential}, nil
	}
	return ConnectResult{}, ErrConnectionConflict
}

func (e *Engine) resumeSession(ctx context.Context, event ConnectEvent, principal Principal) (ConnectResult, error) {
	session, err := e.Options.StateStore.GetSession(ctx, event.SessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return ConnectResult{}, errors.Join(ErrInvalidResume, err)
		}
		return ConnectResult{}, err
	}
	now := e.Options.Clock.Now()
	if session.PrincipalSubject != principal.Subject || !resumeCredentialMatches(session, event.ResumeCredential) ||
		(!session.ExpiresAt.IsZero() && !session.ExpiresAt.After(now)) {
		return ConnectResult{}, ErrInvalidResume
	}
	if !nilDependency(e.Options.Authorizer) {
		if err := e.Options.Authorizer.Authorize(ctx, principal.Clone(), Action{
			Name: ActionSessionResume, SessionID: session.SessionID, Resource: session.SessionID,
		}); err != nil {
			return ConnectResult{}, ErrAuthorizationFailed
		}
	}

	oldConnectionID := session.CurrentConnectionID
	if oldConnectionID != "" && !e.Options.Reconnect.ReplaceExisting {
		if old, getErr := e.Options.StateStore.GetConnection(ctx, oldConnectionID); getErr == nil && old.State == wsstore.ConnectionStateActive {
			return ConnectResult{}, ErrConnectionConflict
		} else if getErr != nil && !wsstore.IsNotFound(getErr) {
			return ConnectResult{}, getErr
		}
	}
	bound, err := e.Options.StateStore.BindConnection(ctx, session.SessionID, event.ConnectionID, session.Generation)
	if err != nil {
		if wsstore.IsConditionConflict(err) {
			return ConnectResult{}, errors.Join(ErrConnectionConflict, err)
		}
		return ConnectResult{}, err
	}
	connection := e.newConnection(event.ConnectionID, bound, now)
	if err := e.Options.StateStore.PutConnection(ctx, connection); err != nil {
		_ = e.Options.StateStore.UnbindConnectionIfMatch(ctx, session.SessionID, event.ConnectionID, bound.Generation)
		return ConnectResult{}, err
	}

	if oldConnectionID != "" && oldConnectionID != event.ConnectionID {
		if old, getErr := e.Options.StateStore.GetConnection(ctx, oldConnectionID); getErr == nil {
			// The session has already advanced. cleanupConnection's conditional
			// unbind therefore cannot clear the replacement binding.
			if cleanupErr := e.cleanupConnection(ctx, old, "replaced"); cleanupErr != nil && !isFencingError(cleanupErr) {
				e.observability.log(ctx, LogEvent{Name: LogEventReconnect, Outcome: "cleanup_failed", RequestID: event.RequestID, ConnectionID: old.ConnectionID, SessionID: session.SessionID, ErrorCode: safeErrorCode(cleanupErr)})
			}
		}
	}
	return ConnectResult{
		ConnectionID:     event.ConnectionID,
		SessionID:        session.SessionID,
		Generation:       bound.Generation,
		ResumeCredential: event.ResumeCredential,
		Reconnected:      true,
	}, nil
}

func (e *Engine) newConnection(connectionID string, session wsstore.Session, nowTime time.Time) wsstore.Connection {
	return wsstore.Connection{
		ConnectionID:     connectionID,
		SessionID:        session.SessionID,
		PrincipalSubject: session.PrincipalSubject,
		Generation:       session.Generation,
		ConnectedAt:      nowTime,
		LastSeenAt:       nowTime,
		ExpiresAt:        nowTime.Add(e.Options.Reconnect.SessionRetention),
		State:            wsstore.ConnectionStateActive,
	}
}

func resumeCredentialMatches(session wsstore.Session, credential string) bool {
	if credential == "" || len(credential) > maxResumeCredentialBytes || session.ResumeCredentialHash == "" {
		return false
	}
	actual := HashResumeCredential(session.SessionID, credential)
	return subtle.ConstantTimeCompare([]byte(actual), []byte(session.ResumeCredentialHash)) == 1
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneStringSliceMap(source map[string][]string) map[string][]string {
	if source == nil {
		return nil
	}
	clone := make(map[string][]string, len(source))
	for key, value := range source {
		clone[key] = append([]string(nil), value...)
	}
	return clone
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
