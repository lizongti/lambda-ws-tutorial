package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
	"github.com/aws/aws-lambda-go/events"
)

type wsLifecycleWriteGuardStore struct {
	wsstore.StateStore
	writes atomic.Int32
}

func (s *wsLifecycleWriteGuardStore) write() { s.writes.Add(1) }

func (s *wsLifecycleWriteGuardStore) PutConnection(ctx context.Context, connection wsstore.Connection) error {
	s.write()
	return s.StateStore.PutConnection(ctx, connection)
}

func (s *wsLifecycleWriteGuardStore) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	s.write()
	return s.StateStore.DeleteConnectionIfMatch(ctx, id, generation)
}

func (s *wsLifecycleWriteGuardStore) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	s.write()
	return s.StateStore.TouchConnection(ctx, id, generation, now)
}

func (s *wsLifecycleWriteGuardStore) CreateSession(ctx context.Context, session wsstore.Session) error {
	s.write()
	return s.StateStore.CreateSession(ctx, session)
}

func (s *wsLifecycleWriteGuardStore) BindConnection(ctx context.Context, sessionID, connectionID string, generation uint64) (wsstore.Session, error) {
	s.write()
	return s.StateStore.BindConnection(ctx, sessionID, connectionID, generation)
}

func (s *wsLifecycleWriteGuardStore) UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error {
	s.write()
	return s.StateStore.UnbindConnectionIfMatch(ctx, sessionID, connectionID, generation)
}

func (s *wsLifecycleWriteGuardStore) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	s.write()
	return s.StateStore.DeleteExpiredSessions(ctx, before, limit)
}

func (s *wsLifecycleWriteGuardStore) Join(ctx context.Context, member wsstore.RoomMember) error {
	s.write()
	return s.StateStore.Join(ctx, member)
}

func (s *wsLifecycleWriteGuardStore) Leave(ctx context.Context, roomID, sessionID string) error {
	s.write()
	return s.StateStore.Leave(ctx, roomID, sessionID)
}

func (s *wsLifecycleWriteGuardStore) RemoveConnection(ctx context.Context, connectionID string) error {
	s.write()
	return s.StateStore.RemoveConnection(ctx, connectionID)
}

func (s *wsLifecycleWriteGuardStore) Append(ctx context.Context, message wsstore.OfflineMessage) error {
	s.write()
	return s.StateStore.Append(ctx, message)
}

func (s *wsLifecycleWriteGuardStore) MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error {
	s.write()
	return s.StateStore.MarkSent(ctx, sessionID, messageID, generation)
}

func (s *wsLifecycleWriteGuardStore) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	s.write()
	return s.StateStore.Ack(ctx, sessionID, messageID, generation)
}

func (s *wsLifecycleWriteGuardStore) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	s.write()
	return s.StateStore.DeleteExpired(ctx, before, limit)
}

func (s *wsLifecycleWriteGuardStore) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	s.write()
	return s.StateStore.BeginIdempotency(ctx, record)
}

func (s *wsLifecycleWriteGuardStore) CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error {
	s.write()
	return s.StateStore.CompleteIdempotency(ctx, sessionID, messageID, response)
}

func (s *wsLifecycleWriteGuardStore) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	s.write()
	return s.StateStore.DeleteExpiredIdempotency(ctx, before, limit)
}

func TestWSLifecycleAuthenticationFailureHasNoWriteSideEffects(t *testing.T) {
	for _, test := range []struct {
		name          string
		authenticator ws.Authenticator
	}{
		{
			name: "authenticator error",
			authenticator: ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
				return ws.Principal{Subject: "must-not-be-persisted"}, errors.New("private authentication failure")
			}),
		},
		{
			name: "empty authenticated subject",
			authenticator: ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
				return ws.Principal{Subject: " \t\n"}, nil
			}),
		},
		{
			name: "authenticator panic",
			authenticator: ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
				panic("authenticator panic with private credential")
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
			ids := &wsSessionTestIDGenerator{}
			store := &wsLifecycleWriteGuardStore{StateStore: memory.New(memory.WithClock(clock.Now))}
			pusher := ws.NewFakePusher()
			options := wsSessionTestOptions(clock, ids)
			options.StateStore = store
			options.Pusher = pusher
			options.Authenticator = test.authenticator
			engine, err := ws.NewEngine(options)
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			event := wsSessionTestConnectEvent("connection-auth-rejected", "alice", "session-resume", "resume-secret")
			event.Headers["authorization"] = "Bearer private-credential"
			result, err := engine.HandleConnect(context.Background(), event)
			if test.name == "authenticator panic" {
				if !errors.Is(err, ws.ErrInternal) {
					t.Fatalf("HandleConnect(panic) error = %v, want ErrInternal", err)
				}
			} else if !errors.Is(err, ws.ErrAuthenticationFailed) {
				t.Fatalf("HandleConnect() error = %v, want ErrAuthenticationFailed", err)
			}
			if result != (ws.ConnectResult{}) {
				t.Fatalf("rejected connect result = %+v, want zero value", result)
			}
			if got := store.writes.Load(); got != 0 {
				t.Fatalf("authentication rejection performed %d state writes, want zero", got)
			}
			if got := pusher.CallCount(); got != 0 {
				t.Fatalf("authentication rejection performed %d pushes, want zero", got)
			}
			if _, getErr := store.GetConnection(context.Background(), event.ConnectionID); !wsstore.IsNotFound(getErr) {
				t.Fatalf("rejected connection was persisted: %v", getErr)
			}
		})
	}
}

func TestWSLifecycleDuplicateAndOutOfOrderConnectDisconnect(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()

	// A disconnect which arrives before its connect is an idempotent no-op.
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{ConnectionID: "connection-not-connected"}); err != nil {
		t.Fatalf("early HandleDisconnect() error = %v", err)
	}
	firstEvent := wsSessionTestConnectEvent("connection-first", "alice", "", "")
	first := mustWSSessionTestConnect(t, f.engine, firstEvent)
	duplicate := mustWSSessionTestConnect(t, f.engine, firstEvent)
	if duplicate.SessionID != first.SessionID || duplicate.Generation != first.Generation || duplicate.ConnectionID != first.ConnectionID {
		t.Fatalf("duplicate connect = %+v, want original binding %+v", duplicate, first)
	}

	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-second", "alice", first.SessionID, first.ResumeCredential,
	))
	// Late duplicate disconnects from the replaced connection cannot unbind the
	// replacement, regardless of how many times API Gateway retries them.
	for i := 0; i < 2; i++ {
		if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
			ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
		}); err != nil {
			t.Fatalf("late old disconnect %d error = %v", i+1, err)
		}
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != resumed.ConnectionID || stored.Generation != resumed.Generation {
		t.Fatalf("old disconnect changed replacement binding: %+v, %v", stored, err)
	}

	duplicateResume := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		resumed.ConnectionID, "alice", first.SessionID, first.ResumeCredential,
	))
	if duplicateResume.SessionID != resumed.SessionID || duplicateResume.Generation != resumed.Generation {
		t.Fatalf("duplicate resumed connect advanced state: %+v, want %+v", duplicateResume, resumed)
	}
	// A stale generation attached to the current physical ID is fenced too.
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: resumed.ConnectionID, SessionID: resumed.SessionID, Generation: first.Generation,
	}); err != nil {
		t.Fatalf("stale-generation disconnect error = %v", err)
	}
	if _, err := f.store.GetConnection(ctx, resumed.ConnectionID); err != nil {
		t.Fatalf("stale-generation disconnect removed replacement: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
			ConnectionID: resumed.ConnectionID, SessionID: resumed.SessionID, Generation: resumed.Generation,
		}); err != nil {
			t.Fatalf("current disconnect %d error = %v", i+1, err)
		}
	}
	if _, err := f.store.GetConnection(ctx, resumed.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("current connection remains after disconnect: %v", err)
	}
	stored, err = f.store.GetSession(ctx, resumed.SessionID)
	if err != nil || stored.CurrentConnectionID != "" || stored.Generation != resumed.Generation {
		t.Fatalf("session after duplicate disconnects = %+v, %v", stored, err)
	}
	if got := f.pusher.CallCount(); got != 0 {
		t.Fatalf("duplicate/out-of-order lifecycle emitted %d pushes, want none", got)
	}
}

func TestWSLifecycleNotFoundDisconnectRepairsSecondaryState(t *testing.T) {
	for _, test := range []struct {
		name         string
		withMetadata bool
	}{
		{name: "session and generation", withMetadata: true},
		{name: "connection only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newWSSessionTestFixture(t, nil)
			ctx := context.Background()
			connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-partial-disconnect", "alice", "", ""))
			if err := f.store.Join(ctx, wsstore.RoomMember{
				RoomID: "room-partial-disconnect", SessionID: connected.SessionID,
				ConnectionID: connected.ConnectionID, Generation: connected.Generation,
			}); err != nil {
				t.Fatal(err)
			}
			// Simulate the state left when physical deletion succeeds but both
			// secondary writes fail during the first disconnect attempt.
			if err := f.store.DeleteConnectionIfMatch(ctx, connected.ConnectionID, connected.Generation); err != nil {
				t.Fatal(err)
			}
			event := ws.DisconnectEvent{ConnectionID: connected.ConnectionID}
			if test.withMetadata {
				event.SessionID = connected.SessionID
				event.Generation = connected.Generation
			}
			if err := f.engine.HandleDisconnect(ctx, event); err != nil {
				t.Fatalf("HandleDisconnect(NotFound) error = %v", err)
			}

			retained, err := f.store.GetSession(ctx, connected.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if test.withMetadata && retained.CurrentConnectionID != "" {
				t.Fatalf("session remains bound after fenced recovery: %+v", retained)
			}
			if !test.withMetadata && retained.CurrentConnectionID != connected.ConnectionID {
				t.Fatalf("connection-only recovery unexpectedly unbound session: %+v", retained)
			}
			members, _, err := f.store.Members(ctx, "room-partial-disconnect", 10, "")
			if err != nil || len(members) != 0 {
				t.Fatalf("room after NotFound recovery = %+v, %v; want empty", members, err)
			}
			if retained.Generation != connected.Generation {
				t.Fatalf("recovery changed retained generation: %+v", retained)
			}
		})
	}
}

func wsLifecycleControlEvent(t *testing.T, connected ws.ConnectResult, message ws.OutboundMessage) ws.MessageEvent {
	t.Helper()
	message.SessionID = connected.SessionID
	body, err := message.Encode()
	if err != nil {
		t.Fatalf("Encode(control) error = %v", err)
	}
	return ws.MessageEvent{
		ConnectionID: connected.ConnectionID,
		RequestID:    message.RequestID,
		SessionID:    connected.SessionID,
		Generation:   connected.Generation,
		Body:         string(body),
	}
}

type wsLifecycleVanishingTouchStore struct {
	*memory.Store
}

func (s *wsLifecycleVanishingTouchStore) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	// Model a disconnect which wins after validateActiveBinding but before the
	// heartbeat write. The backend's raw not-found is a fencing outcome, not a
	// new framework failure or a reason to write to another connection.
	if err := s.Store.DeleteConnectionIfMatch(ctx, id, generation); err != nil {
		return err
	}
	return s.Store.TouchConnection(ctx, id, generation, now)
}

func TestWSLifecycleConnectionVanishingBeforeTouchIsFenced(t *testing.T) {
	clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
	ids := &wsSessionTestIDGenerator{}
	store := &wsLifecycleVanishingTouchStore{Store: memory.New(memory.WithClock(clock.Now))}
	pusher := ws.NewFakePusher()
	options := wsSessionTestOptions(clock, ids)
	options.StateStore = store
	options.Pusher = pusher
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	connected := mustWSSessionTestConnect(t, engine, wsSessionTestConnectEvent("connection-vanishing-touch", "alice", "", ""))
	for attempt := 0; attempt < 2; attempt++ {
		if err := engine.HandleMessage(context.Background(), wsLifecycleControlEvent(t, connected, ws.NewPing("request-vanishing-touch"))); err != nil {
			t.Fatalf("HandleMessage() attempt %d after concurrent removal error = %v, want fenced no-op", attempt+1, err)
		}
	}
	if got := pusher.CallCount(); got != 0 {
		t.Fatalf("vanished connection received %d pushes, want zero", got)
	}
	if _, err := store.GetConnection(context.Background(), connected.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("test setup did not remove connection: %v", err)
	}
}

func TestWSLifecyclePingPongAckAndGenerationFencing(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-control-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-control", 1))
	current := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-control-current", "alice", first.SessionID, first.ResumeCredential,
	))
	if got := f.pusher.CallCount(); got != 1 {
		t.Fatalf("initial replay calls = %d, want 1", got)
	}
	staleOnCurrent := current
	staleOnCurrent.Generation = first.Generation
	for _, test := range []struct {
		name      string
		connected ws.ConnectResult
		message   ws.OutboundMessage
	}{
		{name: "old ping", connected: first, message: ws.NewPing("request-old-ping")},
		{name: "stale generation ping", connected: staleOnCurrent, message: ws.NewPing("request-stale-ping")},
		{name: "old pong", connected: first, message: ws.NewPong("request-old-pong")},
		{name: "stale generation pong", connected: staleOnCurrent, message: ws.NewPong("request-stale-pong")},
		{name: "old ack", connected: first, message: ws.NewAck("request-old-ack", "message-control")},
		{name: "stale generation ack", connected: staleOnCurrent, message: ws.NewAck("request-stale-ack", "message-control")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, test.connected, test.message)); err != nil {
				t.Fatalf("stale control message error = %v", err)
			}
		})
	}
	if got := f.pusher.CallCount(); got != 1 {
		t.Fatalf("stale control messages emitted pushes: got %d total, want initial replay only", got)
	}
	pending, err := f.store.ListPending(ctx, current.SessionID, 10)
	if err != nil || len(pending) != 1 || pending[0].Generation != current.Generation || pending[0].State != wsstore.OfflineMessageStateSent {
		t.Fatalf("stale control messages changed queue: %+v, %v", pending, err)
	}
	connection, err := f.store.GetConnection(ctx, current.ConnectionID)
	if err != nil || !connection.LastSeenAt.Equal(f.base) {
		t.Fatalf("stale control messages touched current connection: %+v, %v", connection, err)
	}

	f.clock.Advance(time.Minute)
	if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, current, ws.NewPing("request-current-ping"))); err != nil {
		t.Fatalf("current ping error = %v", err)
	}
	calls := f.pusher.Calls()
	if len(calls) != 2 || calls[1].ConnectionID != current.ConnectionID {
		t.Fatalf("current ping calls = %+v, want one pong to current connection", calls)
	}
	pong, err := ws.DecodeEnvelope(calls[1].Body)
	if err != nil || pong.Type != ws.TypePong || pong.RequestID != "request-current-ping" || pong.SessionID != current.SessionID {
		t.Fatalf("pong envelope = %+v, %v", pong, err)
	}
	if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, current, ws.NewPong("request-current-pong"))); err != nil {
		t.Fatalf("current pong error = %v", err)
	}
	if got := f.pusher.CallCount(); got != 2 {
		t.Fatalf("pong produced a response push: total calls = %d", got)
	}
	connection, err = f.store.GetConnection(ctx, current.ConnectionID)
	if err != nil || !connection.LastSeenAt.Equal(f.base.Add(time.Minute)) {
		t.Fatalf("current ping/pong did not touch exactly current generation: %+v, %v", connection, err)
	}

	f.clock.Advance(time.Minute)
	mustWSOfflineTestAck(t, f.engine, current, "message-control")
	connection, err = f.store.GetConnection(ctx, current.ConnectionID)
	if err != nil || !connection.LastSeenAt.Equal(f.base.Add(2*time.Minute)) {
		t.Fatalf("successful ack did not touch current generation: %+v, %v", connection, err)
	}
	mustWSOfflineTestAck(t, f.engine, current, "message-control")
	pending, err = f.store.ListPending(ctx, current.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("queue after current and duplicate ack = %+v, %v", pending, err)
	}
	stored, err := f.store.GetSession(ctx, current.SessionID)
	if err != nil || stored.CurrentConnectionID != current.ConnectionID || stored.Generation != current.Generation {
		t.Fatalf("control messages changed current binding: %+v, %v", stored, err)
	}
}

func TestWSLifecycleOnlyValidMessagesRenewLastSeen(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-message-validation", "alice", "", ""))
	f.engine.Handle("/touch", func(*ws.Context) {})
	send := func(message ws.OutboundMessage, generation uint64, envelopeSessionID string) {
		t.Helper()
		message.SessionID = envelopeSessionID
		body, err := message.Encode()
		if err != nil {
			t.Fatalf("Encode(%s) error = %v", message.Type, err)
		}
		if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
			ConnectionID: connected.ConnectionID, RequestID: message.RequestID, SessionID: connected.SessionID,
			Generation: generation, Body: string(body),
		}); err != nil {
			t.Fatalf("HandleMessage(%s) error = %v", message.Type, err)
		}
	}
	requireLastSeen := func(want time.Time) {
		t.Helper()
		connection, err := f.store.GetConnection(ctx, connected.ConnectionID)
		if err != nil || !connection.LastSeenAt.Equal(want) {
			t.Fatalf("LastSeenAt = %s, want %s (connection %+v, error %v)", connection.LastSeenAt, want, connection, err)
		}
	}

	f.clock.Advance(time.Minute)
	for _, test := range []struct {
		name           string
		message        ws.OutboundMessage
		generation     uint64
		sessionID      string
		responsePushes int
	}{
		{name: "stale generation", message: ws.NewPing("request-invalid-stale"), generation: connected.Generation + 1, sessionID: connected.SessionID},
		{name: "envelope session mismatch", message: ws.NewPing("request-invalid-session"), generation: connected.Generation, sessionID: "session-mismatch", responsePushes: 1},
		{name: "inbound response type", message: ws.NewResponse("request-invalid-type", nil), generation: connected.Generation, sessionID: connected.SessionID, responsePushes: 1},
		{name: "rejected ack", message: ws.NewAck("request-invalid-ack", "message-not-acknowledgeable"), generation: connected.Generation, sessionID: connected.SessionID, responsePushes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := f.pusher.CallCount()
			send(test.message, test.generation, test.sessionID)
			if got := f.pusher.CallCount() - before; got != test.responsePushes {
				t.Fatalf("response pushes = %d, want %d", got, test.responsePushes)
			}
			requireLastSeen(f.base)
			if test.responsePushes != 0 {
				response, err := ws.DecodeEnvelope(f.pusher.Calls()[before].Body)
				if err != nil || response.Type != ws.TypeError || response.Error == nil || response.Error.Code == string(ws.ErrorCodeInternal) {
					t.Fatalf("rejection response = %+v, %v; want safe classified error", response, err)
				}
			}
		})
	}

	for _, test := range []struct {
		name    string
		message ws.OutboundMessage
	}{
		{name: "ping", message: ws.NewPing("request-valid-ping")},
		{name: "pong", message: ws.NewPong("request-valid-pong")},
		{name: "request", message: ws.OutboundMessage{Type: ws.TypeRequest, Path: "/touch", RequestID: "request-valid-request"}},
		{name: "event", message: ws.OutboundMessage{Type: ws.TypeEvent, Path: "/touch", RequestID: "request-valid-event"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.clock.Advance(time.Minute)
			send(test.message, connected.Generation, connected.SessionID)
			requireLastSeen(f.clock.Now())
		})
	}
}

func wsLifecycleGatewayRequest(connectionID, requestID, routeKey, body string) events.APIGatewayWebsocketProxyRequest {
	return events.APIGatewayWebsocketProxyRequest{
		Headers: map[string]string{"test-subject": "alice"},
		Body:    body,
		RequestContext: events.APIGatewayWebsocketProxyRequestContext{
			ConnectionID: connectionID,
			RequestID:    requestID,
			RouteKey:     routeKey,
		},
	}
}

func requireWSLifecycleSafeGatewayResponse(t *testing.T, response events.APIGatewayProxyResponse, err error, status int, secrets ...string) {
	t.Helper()
	if err != nil || response.StatusCode != status || response.Body != "" {
		t.Fatalf("API Gateway response = %+v, error = %v; want status %d, empty body, nil Lambda error", response, err, status)
	}
	diagnostic := fmt.Sprintf("%v|%+v|%#v|%v", response, response, response, err)
	for _, secret := range secrets {
		if secret != "" && strings.Contains(diagnostic, secret) {
			t.Fatalf("API Gateway response leaked %q: %s", secret, diagnostic)
		}
	}
}

type wsLifecycleInternalErrorStore struct {
	wsstore.StateStore
	err error
}

func (s *wsLifecycleInternalErrorStore) GetConnection(context.Context, string) (wsstore.Connection, error) {
	return wsstore.Connection{}, s.err
}

func TestWSLifecycleAPIGatewayErrorStatusMappingAndRedaction(t *testing.T) {
	t.Run("authentication rejection is unauthorized", func(t *testing.T) {
		const secret = "private-authenticator-and-credential"
		f := newWSSessionTestFixture(t, func(options *ws.Options) {
			options.Authenticator = ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
				return ws.Principal{}, errors.New(secret)
			})
		})
		request := wsLifecycleGatewayRequest("connection-gateway-denied", "request-gateway-denied", ws.RouteConnect, "")
		request.Headers["authorization"] = secret
		request.QueryStringParameters = map[string]string{"resumeCredential": secret}
		response, err := f.engine.HandleEvent(context.Background(), request)
		requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusUnauthorized, secret)
		if f.pusher.CallCount() != 0 {
			t.Fatal("authentication rejection emitted a WebSocket push")
		}
	})

	t.Run("protocol failure becomes safe message error", func(t *testing.T) {
		const secret = "private-malformed-protocol-input"
		f := newWSSessionTestFixture(t, nil)
		request := wsLifecycleGatewayRequest("connection-gateway-protocol", "request-gateway-protocol", ws.DefaultRouteKey, `{"version":1,"secret":"`+secret+`"`)
		response, err := f.engine.HandleEvent(context.Background(), request)
		// The protocol error is handled on the WebSocket, so the Lambda invocation
		// itself succeeds and the HTTP response never reflects client input.
		requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusOK, secret)
		calls := f.pusher.Calls()
		if len(calls) != 1 {
			t.Fatalf("protocol failure pushes = %d, want 1", len(calls))
		}
		envelope := wsEngineDecodeCall(t, calls[0])
		if envelope.Type != ws.TypeError || envelope.Error == nil || envelope.Error.Code != string(ws.ErrorCodeInvalidJSON) || envelope.Error.Message != "invalid JSON message" {
			t.Fatalf("protocol error envelope = %+v", envelope)
		}
		if body := string(calls[0].Body); strings.Contains(body, secret) {
			t.Fatalf("protocol error leaked request data: %s", body)
		}
	})

	t.Run("invalid event is bad request", func(t *testing.T) {
		f := newWSSessionTestFixture(t, nil)
		request := wsLifecycleGatewayRequest("invalid connection", "request-gateway-invalid", ws.DefaultRouteKey, `{"version":1,"type":"ping"}`)
		response, err := f.engine.HandleEvent(context.Background(), request)
		requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusBadRequest)
		if f.pusher.CallCount() != 0 {
			t.Fatal("invalid connection ID reached the pusher")
		}
	})

	t.Run("internal store failure is safe internal server error", func(t *testing.T) {
		const secret = "private-store-host-and-credential"
		clock := &wsSessionTestClock{now: time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)}
		ids := &wsSessionTestIDGenerator{}
		store := &wsLifecycleInternalErrorStore{
			StateStore: memory.New(memory.WithClock(clock.Now)),
			err:        errors.New(secret),
		}
		options := wsSessionTestOptions(clock, ids)
		options.StateStore = store
		options.Pusher = ws.NewFakePusher()
		engine, err := ws.NewEngine(options)
		if err != nil {
			t.Fatal(err)
		}
		request := wsLifecycleGatewayRequest("connection-gateway-internal", "request-gateway-internal", ws.RouteDisconnect, "")
		response, err := engine.HandleEvent(context.Background(), request)
		requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusInternalServerError, secret)
	})
}

func TestWSLifecycleAPIGatewayRejectsBase64BinaryAsSafeProtocolError(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	binaryBody := []byte{0xff, 0x00, 0x01, 0xfe, 0x7f}
	request := wsLifecycleGatewayRequest(
		"connection-gateway-binary", "request-gateway-binary", ws.DefaultRouteKey,
		base64.StdEncoding.EncodeToString(binaryBody),
	)
	request.IsBase64Encoded = true
	response, err := f.engine.HandleEvent(context.Background(), request)
	requireWSLifecycleSafeGatewayResponse(t, response, err, http.StatusOK, request.Body)
	calls := f.pusher.Calls()
	if len(calls) != 1 {
		t.Fatalf("binary rejection pushes = %d, want one protocol error", len(calls))
	}
	envelope := wsEngineDecodeCall(t, calls[0])
	if envelope.Type != ws.TypeError || envelope.Error == nil || envelope.Error.Code != string(ws.ErrorCodeInvalidPayload) || envelope.Error.Message != "invalid JSON payload" {
		t.Fatalf("binary rejection envelope = %+v, want invalid_payload", envelope)
	}
	if strings.Contains(string(calls[0].Body), request.Body) {
		t.Fatal("binary protocol error reflected the Base64 request body")
	}
}
