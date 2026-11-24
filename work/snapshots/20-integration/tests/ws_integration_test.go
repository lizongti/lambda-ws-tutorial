package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
	redisstore "github.com/aura-studio/lambda/ws/store/redis"
	"github.com/aws/aws-lambda-go/events"
)

type wsIntegrationBackend string

const (
	wsIntegrationMemory   wsIntegrationBackend = "memory"
	wsIntegrationDynamoDB wsIntegrationBackend = "dynamodb-fake"
	wsIntegrationRedis    wsIntegrationBackend = "redis-miniredis"
)

// The matrix exercises real Engine and StateStore code with process-local test
// dependencies. dynamodb-fake is not AWS/DynamoDB Local and redis-miniredis is
// not a real Redis server; neither name implies external service coverage.

type wsIntegrationFixture struct {
	engine  *ws.Engine
	store   wsstore.StateStore
	pusher  *ws.FakePusher
	clock   *wsSessionTestClock
	ids     *wsSessionTestIDGenerator
	base    time.Time
	backend wsIntegrationBackend
	options ws.Options
}

func newWSIntegrationFixture(t *testing.T, backend wsIntegrationBackend, configure func(*ws.Options)) *wsIntegrationFixture {
	t.Helper()
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := &wsSessionTestClock{now: base}
	ids := &wsSessionTestIDGenerator{}
	options := wsSessionTestOptions(clock, ids)
	options.Offline.Enabled = true
	options.Offline.AckPolicy = ws.AckPolicyClientAck
	options.Offline.MaxMessagesPerSession = 16
	options.Offline.Retention = 30 * time.Minute
	options.Offline.ReplayBatchSize = 2
	options.Push.MaxRetries = 0
	pusher := ws.NewFakePusher()
	options.Pusher = pusher
	if configure != nil {
		configure(options)
	}

	var backendStore wsstore.StateStore
	switch backend {
	case wsIntegrationMemory:
		options.State.Backend = ws.BackendMemory
	case wsIntegrationDynamoDB:
		options.State.Backend = ws.BackendDynamoDB
		options.State.TableName = "integration-ws"
		backendStore = dynamodbstore.New(
			dynamodbtest.New(), options.State.TableName,
			dynamodbstore.WithClock(clock.Now),
			dynamodbstore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			dynamodbstore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			dynamodbstore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		)
		options.StateStore = backendStore
	case wsIntegrationRedis:
		mr := miniredis.RunT(t)
		options.State.Backend = ws.BackendRedis
		options.State.RedisAddress = mr.Addr()
		options.State.RedisKeyPrefix = "integration:ws:"
	default:
		t.Fatalf("unknown WS integration backend %q", backend)
	}
	factory := ws.NewFactory(options)
	engine, err := factory.Build()
	if err != nil {
		t.Fatalf("Factory.Build(%s) error = %v", backend, err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Errorf("Factory.Close(%s) error = %v", backend, err)
		}
	})
	if backendStore == nil {
		backendStore = engine.StateStore()
	}
	switch backend {
	case wsIntegrationMemory:
		if _, ok := backendStore.(*memorystore.Store); !ok {
			t.Fatalf("memory integration store type = %T", backendStore)
		}
	case wsIntegrationDynamoDB:
		if _, ok := backendStore.(*dynamodbstore.Store); !ok {
			t.Fatalf("DynamoDB fake integration store type = %T", backendStore)
		}
	case wsIntegrationRedis:
		if _, ok := backendStore.(*redisstore.Store); !ok {
			t.Fatalf("miniredis integration store type = %T", backendStore)
		}
	}
	return &wsIntegrationFixture{
		engine: engine, store: backendStore, pusher: pusher,
		clock: clock, ids: ids, base: base, backend: backend, options: *options,
	}
}

func runWSIntegrationBackends(t *testing.T, test func(t *testing.T, f *wsIntegrationFixture)) {
	t.Helper()
	runWSIntegrationBackendsConfigured(t, nil, test)
}

func runWSIntegrationBackendsConfigured(t *testing.T, configure func(*ws.Options), test func(t *testing.T, f *wsIntegrationFixture)) {
	t.Helper()
	for _, backend := range []wsIntegrationBackend{wsIntegrationMemory, wsIntegrationDynamoDB, wsIntegrationRedis} {
		t.Run(string(backend), func(t *testing.T) {
			test(t, newWSIntegrationFixture(t, backend, configure))
		})
	}
}

// newEngine reuses the fixture's single coherent backend and dependencies while
// allowing a scenario to vary retry, authentication, or replacement policies.
func (f *wsIntegrationFixture) newEngine(t *testing.T, store wsstore.StateStore, configure func(*ws.Options)) *ws.Engine {
	t.Helper()
	options := f.options
	if store != nil {
		options.StateStore = store
	}
	if configure != nil {
		configure(&options)
	}
	engine, err := ws.NewEngine(&options)
	if err != nil {
		t.Fatalf("NewEngine(%s) error = %v", f.backend, err)
	}
	return engine
}

func wsIntegrationContext(connected ws.ConnectResult) *ws.Context {
	return &ws.Context{
		Context: context.Background(), ConnectionID: connected.ConnectionID,
		SessionID: connected.SessionID, Principal: ws.Principal{Subject: "alice"},
	}
}

func mustWSIntegrationDisconnect(t *testing.T, engine *ws.Engine, connected ws.ConnectResult) {
	t.Helper()
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{
		ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
	}); err != nil {
		t.Fatalf("HandleDisconnect(%s) error = %v", connected.ConnectionID, err)
	}
}

func mustWSIntegrationJoin(t *testing.T, engine *ws.Engine, connected ws.ConnectResult, roomID string) {
	t.Helper()
	if err := engine.JoinRoom(wsIntegrationContext(connected), roomID); err != nil {
		t.Fatalf("JoinRoom(%s, %s) error = %v", connected.SessionID, roomID, err)
	}
}

func wsIntegrationGatewayEvent(connectionID, requestID, routeKey, body string) events.APIGatewayWebsocketProxyRequest {
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

func (f *wsIntegrationFixture) onlySessionID(t *testing.T) string {
	t.Helper()
	f.ids.mu.Lock()
	defer f.ids.mu.Unlock()
	if got := f.ids.counter["session"]; got != 1 {
		t.Fatalf("generated session count = %d, want 1", got)
	}
	return "session-0001"
}

type wsIntegrationGetConnectionFaultStore struct {
	wsstore.StateStore
	mu       sync.Mutex
	failures []error
	calls    int
}

func (s *wsIntegrationGetConnectionFaultStore) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	s.mu.Lock()
	s.calls++
	if len(s.failures) != 0 {
		err := s.failures[0]
		s.failures = s.failures[1:]
		s.mu.Unlock()
		return wsstore.Connection{}, err
	}
	s.mu.Unlock()
	return s.StateStore.GetConnection(ctx, id)
}

func (s *wsIntegrationGetConnectionFaultStore) failNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, err)
}

func (s *wsIntegrationGetConnectionFaultStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestWSIntegrationConnectRequestReplyDisconnect(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		connect := wsIntegrationGatewayEvent("connection-e2e", "request-connect", ws.RouteConnect, "")
		response, err := f.engine.HandleAPIGatewayEvent(ctx, connect)
		if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
			t.Fatalf("connect response = %+v, error = %v", response, err)
		}
		sessionID := f.onlySessionID(t)
		session, err := f.store.GetSession(ctx, sessionID)
		if err != nil || session.CurrentConnectionID != "connection-e2e" || session.Generation != 1 {
			t.Fatalf("session after API Gateway connect = %+v, %v", session, err)
		}

		f.engine.Handle("/integration/echo", func(handlerCtx *ws.Context) {
			handlerCtx.SetResponse(handlerCtx.Payload)
		})
		body, err := ws.EncodeEnvelope(ws.Envelope{
			Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/integration/echo",
			RequestID: "request-e2e", MessageID: "message-e2e", SessionID: sessionID,
			Payload: json.RawMessage(`{"echo":"through-api-gateway-adapter"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		request := wsIntegrationGatewayEvent("connection-e2e", "gateway-request-e2e", ws.DefaultRouteKey, string(body))
		response, err = f.engine.HandleAPIGatewayEvent(ctx, request)
		if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
			t.Fatalf("request response = %+v, error = %v", response, err)
		}
		calls := f.pusher.Calls()
		if len(calls) != 1 || calls[0].ConnectionID != "connection-e2e" {
			t.Fatalf("request pushes = %+v, want one reply to connection-e2e", calls)
		}
		reply := wsEngineDecodeCall(t, calls[0])
		if reply.Type != ws.TypeResponse || reply.RequestID != "request-e2e" || reply.SessionID != sessionID ||
			!bytes.Contains(reply.Payload, []byte("through-api-gateway-adapter")) {
			t.Fatalf("end-to-end reply = %+v", reply)
		}

		var explicitReplyErr error
		f.engine.Handle("/integration/reply", func(handlerCtx *ws.Context) {
			handlerCtx.ConnectionID = "connection-untrusted"
			explicitReplyErr = f.engine.Reply(handlerCtx, json.RawMessage(`{"kind":"explicit-reply"}`))
		})
		f.engine.Handle("/integration/failure", func(handlerCtx *ws.Context) {
			handlerCtx.ConnectionID = "connection-untrusted"
			handlerCtx.AbortWithError(ws.NewHandlerError("integration_denied", "safe integration failure"))
		})
		if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, ws.ConnectResult{
			ConnectionID: "connection-e2e", SessionID: sessionID, Generation: 1,
		}, "message-explicit-reply", "request-explicit-reply", "/integration/reply", nil)); err != nil || explicitReplyErr != nil {
			t.Fatalf("explicit Reply error = %v / %v", err, explicitReplyErr)
		}
		if err := f.engine.Push(ctx, "connection-e2e", ws.NewEvent(
			"/integration/push", "request-active-push", json.RawMessage(`{"kind":"active-push"}`),
		)); err != nil {
			t.Fatalf("active Push() error = %v", err)
		}
		if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, ws.ConnectResult{
			ConnectionID: "connection-e2e", SessionID: sessionID, Generation: 1,
		}, "message-error", "request-error", "/integration/failure", nil)); err != nil {
			t.Fatalf("error Envelope request error = %v", err)
		}
		calls = f.pusher.Calls()
		if len(calls) != 4 {
			t.Fatalf("Reply/Push/error push count = %d, want 4", len(calls))
		}
		for index, wantType := range []string{ws.TypeResponse, ws.TypeResponse, ws.TypeEvent, ws.TypeError} {
			if calls[index].ConnectionID != "connection-e2e" {
				t.Errorf("push %d targeted %q, want connection-e2e", index, calls[index].ConnectionID)
			}
			envelope := wsEngineDecodeCall(t, calls[index])
			if envelope.Type != wantType || envelope.SessionID != sessionID {
				t.Errorf("push %d Envelope = %+v, want type %s and session %s", index, envelope, wantType, sessionID)
			}
		}
		errorEnvelope := wsEngineDecodeCall(t, calls[3])
		if errorEnvelope.RequestID != "request-error" || errorEnvelope.Error == nil ||
			errorEnvelope.Error.Code != "integration_denied" || errorEnvelope.Error.Message != "safe integration failure" {
			t.Fatalf("error Envelope = %+v", errorEnvelope)
		}

		disconnect := wsIntegrationGatewayEvent("connection-e2e", "request-disconnect", ws.RouteDisconnect, "")
		for attempt := 0; attempt < 2; attempt++ {
			response, err = f.engine.HandleAPIGatewayEvent(ctx, disconnect)
			if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
				t.Fatalf("disconnect %d response = %+v, error = %v", attempt+1, response, err)
			}
		}
		if _, err := f.store.GetConnection(ctx, "connection-e2e"); !wsstore.IsNotFound(err) {
			t.Fatalf("connection after disconnect = %v, want not-found", err)
		}
		session, err = f.store.GetSession(ctx, sessionID)
		if err != nil || session.CurrentConnectionID != "" || session.Generation != 1 {
			t.Fatalf("session after disconnect = %+v, %v", session, err)
		}
	})
}

func TestWSIntegrationLifecycleDuplicateHeartbeatAndGenerationFencing(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		event := wsSessionTestConnectEvent("connection-lifecycle-1", "alice", "", "")
		first := mustWSSessionTestConnect(t, f.engine, event)
		duplicate, err := f.engine.HandleConnect(ctx, event)
		if err != nil || duplicate.SessionID != first.SessionID || duplicate.Generation != first.Generation || duplicate.Reconnected {
			t.Fatalf("duplicate connect = %+v, %v; first = %+v", duplicate, err, first)
		}
		mustWSIntegrationDisconnect(t, f.engine, first)
		mustWSIntegrationDisconnect(t, f.engine, first)

		second := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
			"connection-lifecycle-2", "alice", first.SessionID, first.ResumeCredential,
		))
		if second.Generation != 2 || !second.Reconnected {
			t.Fatalf("first reconnect = %+v", second)
		}
		mustWSIntegrationDisconnect(t, f.engine, first) // late disconnect is fenced
		before := f.pusher.CallCount()
		f.clock.Advance(time.Minute)
		stalePing := wsLifecycleControlEvent(t, ws.ConnectResult{
			ConnectionID: second.ConnectionID, SessionID: second.SessionID, Generation: first.Generation,
		}, ws.NewPing("request-stale-ping"))
		if err := f.engine.HandleMessage(ctx, stalePing); err != nil {
			t.Fatalf("stale-generation heartbeat error = %v", err)
		}
		connection, err := f.store.GetConnection(ctx, second.ConnectionID)
		if err != nil || !connection.LastSeenAt.Equal(f.base) || f.pusher.CallCount() != before {
			t.Fatalf("stale heartbeat changed state or pushed: connection=%+v err=%v calls=%d", connection, err, f.pusher.CallCount())
		}
		if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, second, ws.NewPing("request-current-ping"))); err != nil {
			t.Fatalf("current heartbeat error = %v", err)
		}
		calls := f.pusher.Calls()
		if len(calls) != before+1 || calls[len(calls)-1].ConnectionID != second.ConnectionID {
			t.Fatalf("heartbeat pushes = %+v", calls)
		}
		pong := wsEngineDecodeCall(t, calls[len(calls)-1])
		connection, err = f.store.GetConnection(ctx, second.ConnectionID)
		if err != nil || pong.Type != ws.TypePong || !connection.LastSeenAt.Equal(f.base.Add(time.Minute)) {
			t.Fatalf("current heartbeat result: pong=%+v connection=%+v err=%v", pong, connection, err)
		}

		third := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
			"connection-lifecycle-3", "alice", first.SessionID, first.ResumeCredential,
		))
		mustWSIntegrationDisconnect(t, f.engine, second)
		if err := f.engine.HandleMessage(ctx, wsLifecycleControlEvent(t, second, ws.NewPing("request-replaced-ping"))); err != nil {
			t.Fatalf("replaced heartbeat error = %v", err)
		}
		stored, err := f.store.GetSession(ctx, first.SessionID)
		if err != nil || third.Generation != 3 || stored.CurrentConnectionID != third.ConnectionID || stored.Generation != third.Generation {
			t.Fatalf("binding after generation competition = %+v, %v; third=%+v", stored, err, third)
		}
	})
}

func TestWSIntegrationAnonymousModeRequiresDevelopment(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		options := f.options
		options.StateStore = f.store
		options.Authenticator = nil
		options.Auth.Development = false
		if _, err := ws.NewEngine(&options); !errors.Is(err, ws.ErrAuthenticatorRequired) {
			t.Fatalf("production anonymous NewEngine error = %v, want authenticator required", err)
		}
		options.Authenticator = ws.NewAnonymousAuthenticator()
		options.Auth.Anonymous = true
		if _, err := ws.NewEngine(&options); !errors.Is(err, ws.ErrAnonymousNotAllowed) {
			t.Fatalf("explicit production anonymous error = %v, want anonymous not allowed", err)
		}
		anonymousEngine := f.newEngine(t, f.store, func(options *ws.Options) {
			options.Authenticator = nil
			options.Auth.Development = true
		})
		connected := mustWSSessionTestConnect(t, anonymousEngine, wsSessionTestConnectEvent("connection-anonymous", "ignored", "", ""))
		session, err := f.store.GetSession(context.Background(), connected.SessionID)
		if err != nil || session.PrincipalSubject != ws.AnonymousSubject {
			t.Fatalf("anonymous development session = %+v, %v", session, err)
		}
	})
}

func TestWSIntegrationRejectsCrossPrincipalTransplantedAndExpiredCredentials(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-auth-original", "alice", "", ""))
		other := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-auth-other", "bob", "", ""))
		attempts := []ws.ConnectEvent{
			wsSessionTestConnectEvent("connection-cross-principal", "bob", first.SessionID, first.ResumeCredential),
			wsSessionTestConnectEvent("connection-wrong-credential", "alice", first.SessionID, "not-the-issued-credential"),
			wsSessionTestConnectEvent("connection-transplanted-credential", "bob", other.SessionID, first.ResumeCredential),
		}
		for _, attempt := range attempts {
			if _, err := f.engine.HandleConnect(ctx, attempt); !errors.Is(err, ws.ErrInvalidResume) {
				t.Fatalf("resume %s error = %v, want ErrInvalidResume", attempt.ConnectionID, err)
			}
			if _, err := f.store.GetConnection(ctx, attempt.ConnectionID); !wsstore.IsNotFound(err) {
				t.Fatalf("rejected resume %s left connection: %v", attempt.ConnectionID, err)
			}
		}
		f.clock.Advance(time.Hour)
		if _, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
			"connection-expired-credential", "alice", first.SessionID, first.ResumeCredential,
		)); !errors.Is(err, ws.ErrInvalidResume) {
			t.Fatalf("expired old credential error = %v, want ErrInvalidResume", err)
		}
		stored, err := f.store.GetSession(ctx, first.SessionID)
		if !wsstore.IsNotFound(err) {
			// Memory, the DynamoDB fake, and miniredis all enforce logical expiry.
			t.Fatalf("expired session remained readable: %+v, %v", stored, err)
		}
	})
}

func TestWSIntegrationReconnectReplacementPolicies(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replaceExisting=%t", replace), func(t *testing.T) {
			runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
				options.Reconnect.ReplaceExisting = replace
			}, func(t *testing.T, f *wsIntegrationFixture) {
				ctx := context.Background()
				first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-policy-old", "alice", "", ""))
				resumed, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
					"connection-policy-new", "alice", first.SessionID, first.ResumeCredential,
				))
				if !replace {
					if !errors.Is(err, ws.ErrConnectionConflict) {
						t.Fatalf("active replacement error = %v, want conflict", err)
					}
					stored, getErr := f.store.GetSession(ctx, first.SessionID)
					if getErr != nil || stored.CurrentConnectionID != first.ConnectionID || stored.Generation != 1 {
						t.Fatalf("rejected replacement changed session = %+v, %v", stored, getErr)
					}
					mustWSIntegrationDisconnect(t, f.engine, first)
					resumed, err = f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
						"connection-policy-new", "alice", first.SessionID, first.ResumeCredential,
					))
				}
				if err != nil || !resumed.Reconnected || resumed.Generation != 2 {
					t.Fatalf("replacement resume = %+v, %v", resumed, err)
				}
				if _, err := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(err) {
					t.Fatalf("old connection remains after allowed resume: %v", err)
				}
			})
		})
	}
}

func TestWSIntegrationStaleAndGoneCleanupPreserveSessionAndReenter(t *testing.T) {
	for _, scenario := range []string{"stale", "gone"} {
		t.Run(scenario, func(t *testing.T) {
			runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
				options.Heartbeat.Timeout = time.Minute
			}, func(t *testing.T, f *wsIntegrationFixture) {
				ctx := context.Background()
				connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-cleanup-"+scenario, "alice", "", ""))
				mustWSIntegrationJoin(t, f.engine, connected, "cleanup-room")
				if scenario == "stale" {
					f.clock.Advance(time.Minute + time.Second)
				} else {
					f.pusher.SetErrors(ws.NewPushError(ws.PushErrorGone, errors.New("private gateway detail")))
					delivery, err := f.engine.PushToSessionWithResult(ctx, connected.SessionID, wsOfflineTestMessage("message-gone-cleanup", 1))
					if err != nil || delivery.Status != ws.DeliveryOffline || !delivery.Gone {
						t.Fatalf("gone delivery = %+v, %v", delivery, err)
					}
				}
				if _, err := f.store.GetConnection(ctx, connected.ConnectionID); scenario == "gone" && !wsstore.IsNotFound(err) {
					t.Fatalf("gone connection remains: %v", err)
				}
				for run := 0; run < 2; run++ {
					result, err := f.engine.RunMaintenanceWithResult(ctx, f.clock.Now())
					if err != nil || result.Failed != 0 {
						t.Fatalf("maintenance run %d = %+v, %v", run+1, result, err)
					}
					if scenario == "stale" && ((run == 0 && result.StaleCleaned != 1) || (run == 1 && result.StaleCleaned != 0)) {
						t.Fatalf("stale maintenance run %d = %+v", run+1, result)
					}
				}
				if _, err := f.store.GetConnection(ctx, connected.ConnectionID); !wsstore.IsNotFound(err) {
					t.Fatalf("cleaned connection remains: %v", err)
				}
				session, err := f.store.GetSession(ctx, connected.SessionID)
				if err != nil || session.CurrentConnectionID != "" || session.Generation != connected.Generation {
					t.Fatalf("retained session after %s = %+v, %v", scenario, session, err)
				}
				members, _, err := f.store.Members(ctx, "cleanup-room", 10, "")
				if err != nil || len(members) != 0 {
					t.Fatalf("room members after %s cleanup = %+v, %v", scenario, members, err)
				}
				if scenario == "gone" {
					pending, err := f.store.ListPending(ctx, connected.SessionID, 10)
					if err != nil || len(pending) != 1 || pending[0].MessageID != "message-gone-cleanup" {
						t.Fatalf("offline message after gone cleanup = %+v, %v", pending, err)
					}
				}
			})
		})
	}
}

func TestWSIntegrationOfflineReplayAckAndIdempotency(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-durable-old", "alice", "", ""))
		if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
			ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
		}); err != nil {
			t.Fatalf("HandleDisconnect() error = %v", err)
		}
		if _, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage("message-durable", 1)); err != nil {
			t.Fatalf("offline PushToSession() error = %v", err)
		}
		current := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
			"connection-durable-new", "alice", first.SessionID, first.ResumeCredential,
		))
		if current.Replay.Attempted != 1 || current.Replay.Sent != 1 || current.Replay.Retained != 1 {
			t.Fatalf("reconnect replay = %+v", current.Replay)
		}
		mustWSOfflineTestAck(t, f.engine, current, "message-durable")
		pending, err := f.store.ListPending(ctx, current.SessionID, 10)
		if err != nil || len(pending) != 0 {
			t.Fatalf("pending after ack = %+v, %v", pending, err)
		}

		var executions atomic.Int32
		f.engine.Handle("/integration/idempotent", func(handlerCtx *ws.Context) {
			executions.Add(1)
			handlerCtx.SetResponse(map[string]string{"status": "stored"})
		})
		event := wsEngineMessageEvent(t, current, "message-durable-request", "request-durable", "/integration/idempotent", json.RawMessage(`{"value":1}`))
		if err := f.engine.HandleMessage(ctx, event); err != nil {
			t.Fatalf("first idempotent request error = %v", err)
		}
		if err := f.engine.HandleMessage(ctx, event); err != nil {
			t.Fatalf("replayed idempotent request error = %v", err)
		}
		calls := f.pusher.Calls()
		if len(calls) != 3 {
			t.Fatalf("durable idempotency push count = %d, want replay plus two replies", len(calls))
		}
		firstReply := wsEngineDecodeCall(t, calls[1])
		replayedReply := wsEngineDecodeCall(t, calls[2])
		if firstReply.Type != ws.TypeResponse || replayedReply.Type != ws.TypeResponse ||
			firstReply.RequestID != replayedReply.RequestID || firstReply.SessionID != replayedReply.SessionID {
			t.Fatalf("durable idempotency responses differ: first=%+v replay=%+v", firstReply, replayedReply)
		}
		assertWSProtocolJSONEqual(t, replayedReply.Payload, firstReply.Payload)
		if got := executions.Load(); got != 1 {
			t.Fatalf("idempotent handler executions = %d, want 1", got)
		}
		record, err := f.store.GetIdempotency(ctx, current.SessionID, "message-durable-request")
		if err != nil || record.State != wsstore.IdempotencyStateCompleted || len(record.Response) == 0 {
			t.Fatalf("idempotency record = %+v, %v", record, err)
		}
	})
}

func TestWSIntegrationOfflineAckPoliciesOrderingAndDeduplication(t *testing.T) {
	for _, policy := range []ws.AckPolicy{ws.AckPolicyPostSuccess, ws.AckPolicyClientAck} {
		t.Run(string(policy), func(t *testing.T) {
			runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
				options.Offline.AckPolicy = policy
			}, func(t *testing.T, f *wsIntegrationFixture) {
				ctx := context.Background()
				first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-ack-old", "alice", "", ""))
				mustWSIntegrationDisconnect(t, f.engine, first)
				wantIDs := []string{"message-ordered-1", "message-ordered-2", "message-ordered-3"}
				for index, id := range wantIDs {
					delivery, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage(id, index+1))
					if err != nil || delivery.Status != ws.DeliveryOffline {
						t.Fatalf("queue %s = %+v, %v", id, delivery, err)
					}
				}
				resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
					"connection-ack-new", "alice", first.SessionID, first.ResumeCredential,
				))
				calls := f.pusher.Calls()
				if len(calls) != 3 || resumed.Replay.Attempted != 3 || resumed.Replay.Sent != 3 {
					t.Fatalf("ordered replay = %+v, calls=%d", resumed.Replay, len(calls))
				}
				for index, call := range calls {
					envelope, position := wsOfflineTestEnvelopePosition(t, call.Body)
					if call.ConnectionID != resumed.ConnectionID || envelope.MessageID != wantIDs[index] || position != index+1 {
						t.Fatalf("replay call %d = %+v envelope=%+v position=%d", index, call, envelope, position)
					}
				}
				pending, err := f.store.ListPending(ctx, first.SessionID, 10)
				if err != nil {
					t.Fatal(err)
				}
				if policy == ws.AckPolicyPostSuccess {
					if len(pending) != 0 || resumed.Replay.Acknowledged != 3 || resumed.Replay.Retained != 0 {
						t.Fatalf("postSuccess replay/queue = %+v / %+v", resumed.Replay, pending)
					}
				} else {
					if len(pending) != 3 || resumed.Replay.Acknowledged != 0 || resumed.Replay.Retained != 3 {
						t.Fatalf("clientAck replay/queue = %+v / %+v", resumed.Replay, pending)
					}
					mustWSOfflineTestAck(t, f.engine, resumed, wantIDs[0])
					mustWSOfflineTestAck(t, f.engine, resumed, wantIDs[0])
					f.clock.Advance(f.options.Offline.AckTimeout)
					replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
					if err != nil || replay.Attempted != 2 {
						t.Fatalf("unacknowledged replay = %+v, %v", replay, err)
					}
					calls = f.pusher.Calls()
					for index, call := range calls[3:] {
						envelope, _ := wsOfflineTestEnvelopePosition(t, call.Body)
						if envelope.MessageID != wantIDs[index+1] {
							t.Fatalf("deduplicated redelivery %d ID = %q", index, envelope.MessageID)
						}
					}
					for _, id := range wantIDs[1:] {
						mustWSOfflineTestAck(t, f.engine, resumed, id)
					}
					pending, err = f.store.ListPending(ctx, first.SessionID, 10)
					if err != nil || len(pending) != 0 {
						t.Fatalf("queue after all client acks = %+v, %v", pending, err)
					}
				}
				before := f.pusher.CallCount()
				replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
				if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != before {
					t.Fatalf("acknowledged messages replayed: %+v, %v", replay, err)
				}
			})
		})
	}
}

func TestWSIntegrationOfflineReconnectInterruptionRetainsMessages(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-interrupt-old", "alice", "", ""))
		mustWSIntegrationDisconnect(t, f.engine, first)
		wantIDs := []string{"message-interrupt-1", "message-interrupt-2", "message-interrupt-3"}
		for index, id := range wantIDs {
			mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage(id, index+1))
		}
		f.pusher.SetErrors(nil, ws.NewPushError(ws.PushErrorGone, errors.New("replay target disappeared")))
		interrupted, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
			"connection-interrupt-mid", "alice", first.SessionID, first.ResumeCredential,
		))
		if !ws.IsPushGone(err) || interrupted.Replay.Attempted != 2 || interrupted.Replay.Sent != 1 || interrupted.Replay.Failed != 1 {
			t.Fatalf("interrupted reconnect = %+v, %v", interrupted, err)
		}
		pending, listErr := f.store.ListPending(ctx, first.SessionID, 10)
		if listErr != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), wantIDs) {
			t.Fatalf("messages lost during interrupted replay = %v, %v", wsOfflineTestMessageIDs(pending), listErr)
		}
		f.pusher.SetErrors()
		f.clock.Advance(f.options.Offline.AckTimeout)
		resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
			"connection-interrupt-final", "alice", first.SessionID, first.ResumeCredential,
		))
		if resumed.Generation != 3 || resumed.Replay.Attempted != 3 || resumed.Replay.Sent != 3 {
			t.Fatalf("resumed replay = %+v", resumed)
		}
		calls := f.pusher.Calls()
		for index, call := range calls[2:] {
			envelope, _ := wsOfflineTestEnvelopePosition(t, call.Body)
			if envelope.MessageID != wantIDs[index] || call.ConnectionID != resumed.ConnectionID {
				t.Fatalf("resumed replay call %d = %+v envelope=%+v", index, call, envelope)
			}
		}
		for _, id := range wantIDs {
			mustWSOfflineTestAck(t, f.engine, resumed, id)
		}
	})
}

func TestWSIntegrationOfflineExpirationAndOverflow(t *testing.T) {
	t.Run("expiration", func(t *testing.T) {
		runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
			options.Offline.Retention = 10 * time.Minute
		}, func(t *testing.T, f *wsIntegrationFixture) {
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-expiry-old", "alice", "", ""))
			mustWSIntegrationDisconnect(t, f.engine, first)
			mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-expired", 1))
			f.clock.Advance(5 * time.Minute)
			mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-live", 2))
			f.clock.Advance(6 * time.Minute)
			resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
				"connection-expiry-new", "alice", first.SessionID, first.ResumeCredential,
			))
			if resumed.Replay.Attempted != 1 || f.pusher.CallCount() != 1 {
				t.Fatalf("expired entry entered replay: %+v, calls=%d", resumed.Replay, f.pusher.CallCount())
			}
			envelope, _ := wsOfflineTestEnvelopePosition(t, f.pusher.Calls()[0].Body)
			if envelope.MessageID != "message-live" {
				t.Fatalf("replayed message = %q, want live", envelope.MessageID)
			}
		})
	})
	for _, policy := range []wsstore.OfflineOverflowPolicy{wsstore.OfflineOverflowRejectNew, wsstore.OfflineOverflowEvictOldest} {
		t.Run("overflow-"+string(policy), func(t *testing.T) {
			runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
				options.Offline.MaxMessagesPerSession = 2
				options.Offline.Overflow = policy
			}, func(t *testing.T, f *wsIntegrationFixture) {
				ctx := context.Background()
				first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-overflow-old", "alice", "", ""))
				mustWSIntegrationDisconnect(t, f.engine, first)
				for index := 1; index <= 2; index++ {
					mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage(fmt.Sprintf("message-overflow-%d", index), index))
				}
				delivery, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage("message-overflow-3", 3))
				if policy == wsstore.OfflineOverflowRejectNew && !wsstore.IsQuotaExceeded(err) {
					t.Fatalf("reject-new overflow = %+v, %v", delivery, err)
				}
				if policy == wsstore.OfflineOverflowEvictOldest && (err != nil || delivery.Status != ws.DeliveryOffline) {
					t.Fatalf("evict-oldest overflow = %+v, %v", delivery, err)
				}
				pending, listErr := f.store.ListPending(ctx, first.SessionID, 10)
				want := []string{"message-overflow-1", "message-overflow-2"}
				if policy == wsstore.OfflineOverflowEvictOldest {
					want = []string{"message-overflow-2", "message-overflow-3"}
				}
				if listErr != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), want) {
					t.Fatalf("overflow queue = %v, %v; want %v", wsOfflineTestMessageIDs(pending), listErr, want)
				}
			})
		})
	}
}

func TestWSIntegrationBroadcastIsolatesGoneAndTemporaryFailures(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		members := make([]ws.ConnectResult, 0, 4)
		for _, id := range []string{"gone", "temporary", "healthy-a", "healthy-b"} {
			connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-broadcast-"+id, "alice", "", ""))
			mustWSIntegrationJoin(t, f.engine, connected, "broadcast-room")
			members = append(members, connected)
		}
		f.pusher.SetHook(func(_ context.Context, input ws.PushInput) error {
			switch input.ConnectionID {
			case "connection-broadcast-gone":
				return ws.NewPushError(ws.PushErrorGone, errors.New("private gone detail"))
			case "connection-broadcast-temporary":
				return ws.NewPushError(ws.PushErrorUnavailable, errors.New("private temporary detail"))
			default:
				return nil
			}
		})
		result := f.engine.Broadcast(ctx, "broadcast-room", wsRoomTestMessage("message-broadcast-partial"), wsRoomTestBroadcastOptions())
		if result.Err != nil || result.Total != 4 || result.Succeeded != 2 || result.Gone != 1 || result.Offline != 1 || result.Failed != 1 {
			t.Fatalf("partial broadcast = %+v", result)
		}
		deliveries := make(map[string]ws.BroadcastDeliveryResult)
		for _, delivery := range result.Deliveries {
			deliveries[delivery.ConnectionID] = delivery
		}
		if got := deliveries["connection-broadcast-gone"]; got.Status != string(ws.DeliveryOffline) || !got.Gone || !got.Offline {
			t.Fatalf("gone delivery = %+v", got)
		}
		if got := deliveries["connection-broadcast-temporary"]; got.Status != "failed" || got.ErrorCode != "push_unavailable" {
			t.Fatalf("temporary delivery = %+v", got)
		}
		for _, connected := range members[2:] {
			if got := deliveries[connected.ConnectionID]; got.Status != string(ws.DeliverySent) {
				t.Fatalf("healthy delivery %s = %+v", connected.ConnectionID, got)
			}
		}
		if _, err := f.store.GetConnection(ctx, members[0].ConnectionID); !wsstore.IsNotFound(err) {
			t.Fatalf("gone broadcast target remains: %v", err)
		}
		if _, err := f.store.GetConnection(ctx, members[1].ConnectionID); err != nil {
			t.Fatalf("temporary failure removed target: %v", err)
		}
		pending, err := f.store.ListPending(ctx, members[0].SessionID, 10)
		if err != nil || len(pending) != 1 || pending[0].MessageID != "message-broadcast-partial" {
			t.Fatalf("gone target offline queue = %+v, %v", pending, err)
		}
		remaining, _, err := f.store.Members(ctx, "broadcast-room", 10, "")
		if err != nil || len(remaining) != 3 {
			t.Fatalf("room after partial broadcast = %+v, %v", remaining, err)
		}
	})
}

func TestWSIntegrationBroadcastSlowTargetsRespectConcurrencyLimit(t *testing.T) {
	const workerLimit = 2
	runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
		options.Push.Concurrency = workerLimit
		options.Rooms.BroadcastBatchSize = 8
	}, func(t *testing.T, f *wsIntegrationFixture) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for index := 0; index < 6; index++ {
			connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(fmt.Sprintf("connection-slow-%d", index), "alice", "", ""))
			mustWSIntegrationJoin(t, f.engine, connected, "slow-room")
		}
		var active, maximum atomic.Int32
		started := make(chan struct{}, 6)
		release := make(chan struct{})
		f.pusher.SetHook(func(ctx context.Context, _ ws.PushInput) error {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		resultCh := make(chan ws.BroadcastResult, 1)
		go func() {
			resultCh <- f.engine.Broadcast(ctx, "slow-room", wsRoomTestMessage("message-broadcast-slow"), wsRoomTestBroadcastOptions())
		}()
		for index := 0; index < workerLimit; index++ {
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("broadcast workers did not start")
			}
		}
		select {
		case <-started:
			t.Fatal("third push started while both workers were blocked")
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		result := <-resultCh
		if result.Err != nil || result.Total != 6 || result.Succeeded != 6 || result.Failed != 0 {
			t.Fatalf("slow broadcast = %+v", result)
		}
		if got := maximum.Load(); got != workerLimit {
			t.Fatalf("maximum push concurrency = %d, want %d", got, workerLimit)
		}
	})
}

func TestWSIntegrationProtocolAndPanicErrorsAreSafe(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		const secret = "integration-secret-must-not-reach-client"
		ctx := context.Background()
		connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-safe-errors", "alice", "", ""))
		if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
			ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
			RequestID: "request-malformed", Body: `{"version":1,"type":"request","path":"/broken","payload":"` + secret,
		}); err != nil {
			t.Fatalf("malformed protocol handling error = %v", err)
		}
		f.engine.Handle("/integration/panic", func(*ws.Context) { panic(secret) })
		if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, connected, "message-panic", "request-panic", "/integration/panic", json.RawMessage(`{"private":true}`))); err != nil {
			t.Fatalf("panic recovery error = %v", err)
		}
		calls := f.pusher.Calls()
		if len(calls) != 2 {
			t.Fatalf("safe error push count = %d, want 2", len(calls))
		}
		for index, call := range calls {
			if call.ConnectionID != connected.ConnectionID || bytes.Contains(call.Body, []byte(secret)) {
				t.Fatalf("unsafe or misdirected error push %d = %+v body=%s", index, call, call.Body)
			}
			envelope := wsEngineDecodeCall(t, call)
			if envelope.Type != ws.TypeError || envelope.Error == nil || envelope.SessionID != connected.SessionID {
				t.Fatalf("error Envelope %d = %+v", index, envelope)
			}
			if index == 0 && envelope.Error.Code != string(ws.ErrorCodeInvalidJSON) {
				t.Fatalf("protocol error = %+v", envelope.Error)
			}
			if index == 1 && envelope.Error.Code != "internal_error" {
				t.Fatalf("panic error = %+v", envelope.Error)
			}
		}
	})
}

func TestWSIntegrationStorageAndPusherRetriesAreBoundedAndClassified(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		const secret = "private-backend-fault-detail"
		ctx := context.Background()
		event := wsSessionTestConnectEvent("connection-retry", "alice", "", "")
		connected := mustWSSessionTestConnect(t, f.engine, event)
		faults := &wsIntegrationGetConnectionFaultStore{StateStore: f.store}
		var storeSleeps atomic.Int32
		storeEngine := f.newEngine(t, faults, func(options *ws.Options) {
			options.Push.MaxRetries = 2
			options.RetrySleeper = func(context.Context, time.Duration) error {
				storeSleeps.Add(1)
				return nil
			}
		})
		for range 2 {
			faults.failNext(wsstore.NewError(wsstore.ErrorCodeUnavailable, "GetConnection", errors.New(secret)))
		}
		duplicate, err := storeEngine.HandleConnect(ctx, event)
		if err != nil || duplicate.SessionID != connected.SessionID || faults.callCount() != 4 || storeSleeps.Load() != 2 {
			t.Fatalf("transient store retry = %+v, %v calls=%d sleeps=%d", duplicate, err, faults.callCount(), storeSleeps.Load())
		}
		faults.failNext(wsstore.NewError(wsstore.ErrorCodeInvalid, "GetConnection", errors.New(secret)))
		_, err = storeEngine.HandleConnect(ctx, event)
		if !wsstore.IsInvalid(err) || faults.callCount() != 5 || storeSleeps.Load() != 2 || strings.Contains(err.Error(), secret) {
			t.Fatalf("permanent store failure = %v calls=%d sleeps=%d", err, faults.callCount(), storeSleeps.Load())
		}

		var pushSleeps atomic.Int32
		pushEngine := f.newEngine(t, f.store, func(options *ws.Options) {
			options.Push.MaxRetries = 2
			options.RetrySleeper = func(context.Context, time.Duration) error {
				pushSleeps.Add(1)
				return nil
			}
		})
		message := ws.NewEvent("/integration/retry", "request-retry", json.RawMessage(`{"value":1}`))
		before := f.pusher.CallCount()
		f.pusher.SetErrors(
			ws.NewPushError(ws.PushErrorUnavailable, errors.New(secret)),
			ws.NewPushError(ws.PushErrorUnavailable, errors.New(secret)),
			ws.NewPushError(ws.PushErrorUnavailable, errors.New(secret)),
		)
		err = pushEngine.Push(ctx, connected.ConnectionID, message)
		if !ws.IsPushErrorKind(err, ws.PushErrorUnavailable) || f.pusher.CallCount() != before+3 || pushSleeps.Load() != 2 || strings.Contains(err.Error(), secret) {
			t.Fatalf("bounded push retry = %v calls=%d sleeps=%d", err, f.pusher.CallCount()-before, pushSleeps.Load())
		}
		permanentPusher := ws.NewFakePusher(ws.NewPushError(ws.PushErrorInvalid, errors.New(secret)))
		permanentEngine := f.newEngine(t, f.store, func(options *ws.Options) {
			options.Pusher = permanentPusher
			options.Push.MaxRetries = 2
			options.RetrySleeper = func(context.Context, time.Duration) error {
				pushSleeps.Add(1)
				return nil
			}
		})
		err = permanentEngine.Push(ctx, connected.ConnectionID, message)
		if !ws.IsPushErrorKind(err, ws.PushErrorInvalid) || permanentPusher.CallCount() != 1 || pushSleeps.Load() != 2 || strings.Contains(err.Error(), secret) {
			t.Fatalf("permanent push failure = %v calls=%d sleeps=%d", err, permanentPusher.CallCount(), pushSleeps.Load())
		}
	})
}

func TestWSIntegrationConcurrentDisconnectReconnectReplayAck(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for iteration := 0; iteration < 6; iteration++ {
			suffix := fmt.Sprintf("-%02d", iteration)
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-race-old"+suffix, "alice", "", ""))
			if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
				ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
			}); err != nil {
				t.Fatalf("initial disconnect error = %v", err)
			}
			messageID := "message-race" + suffix
			if _, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage(messageID, iteration+1)); err != nil {
				t.Fatalf("offline push error = %v", err)
			}
			second := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
				"connection-race-second"+suffix, "alice", first.SessionID, first.ResumeCredential,
			))
			ackEvent := wsLifecycleControlEvent(t, second, ws.NewAck("request-race-ack"+suffix, messageID))
			// Make the surviving generation-2 delivery eligible for the winning
			// reconnect to fence and resend it at generation 3.
			f.clock.Advance(f.options.Offline.AckTimeout)

			type outcome struct {
				kind      string
				connected ws.ConnectResult
				err       error
			}
			start := make(chan struct{})
			outcomes := make(chan outcome, 4)
			var wg sync.WaitGroup
			wg.Add(4)
			go func() {
				defer wg.Done()
				<-start
				err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
					ConnectionID: second.ConnectionID, SessionID: second.SessionID, Generation: second.Generation,
				})
				outcomes <- outcome{kind: "disconnect", err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				connected, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
					"connection-race-third"+suffix, "alice", first.SessionID, first.ResumeCredential,
				))
				outcomes <- outcome{kind: "reconnect", connected: connected, err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				_, err := f.engine.ReplayOffline(ctx, first.SessionID)
				outcomes <- outcome{kind: "replay", err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				outcomes <- outcome{kind: "ack", err: f.engine.HandleMessage(ctx, ackEvent)}
			}()
			close(start)
			wg.Wait()
			close(outcomes)

			var current ws.ConnectResult
			for outcome := range outcomes {
				switch outcome.kind {
				case "reconnect":
					if outcome.err != nil {
						t.Fatalf("iteration %d reconnect error = %v", iteration, outcome.err)
					}
					current = outcome.connected
				case "disconnect", "ack":
					if outcome.err != nil {
						t.Fatalf("iteration %d %s error = %v", iteration, outcome.kind, outcome.err)
					}
				case "replay":
					if outcome.err != nil && !errors.Is(outcome.err, ws.ErrConnectionNotFound) &&
						!errors.Is(outcome.err, ws.ErrSessionNotFound) && !errors.Is(outcome.err, ws.ErrStaleConnection) &&
						!wsstore.IsConditionConflict(outcome.err) && !wsstore.IsNotFound(outcome.err) {
						t.Fatalf("iteration %d replay returned non-fencing error = %v", iteration, outcome.err)
					}
				}
			}
			if current.ConnectionID != "connection-race-third"+suffix || current.Generation != 3 || !current.Reconnected {
				t.Fatalf("iteration %d winning reconnect = %+v", iteration, current)
			}
			stored, err := f.store.GetSession(ctx, first.SessionID)
			if err != nil || stored.CurrentConnectionID != current.ConnectionID || stored.Generation != current.Generation {
				t.Fatalf("iteration %d final binding = %+v, %v; current = %+v", iteration, stored, err, current)
			}
			connection, err := f.store.GetConnection(ctx, current.ConnectionID)
			if err != nil || connection.Generation != current.Generation || connection.State != wsstore.ConnectionStateActive {
				t.Fatalf("iteration %d current connection = %+v, %v", iteration, connection, err)
			}
			if _, err := f.store.GetConnection(ctx, second.ConnectionID); !wsstore.IsNotFound(err) {
				t.Fatalf("iteration %d replaced connection remains: %v", iteration, err)
			}

			// Any ack which won before reconnect may already have removed the entry.
			// Otherwise, a replay which overlapped the generation transition may have
			// retained the old sent state. Advance past ackTimeout and drain through
			// the winning Engine binding before issuing the final idempotent ack.
			f.clock.Advance(f.options.Offline.AckTimeout)
			if _, err := f.engine.ReplayOffline(ctx, first.SessionID); err != nil {
				t.Fatalf("iteration %d final replay error = %v", iteration, err)
			}
			mustWSOfflineTestAck(t, f.engine, current, messageID)
			pending, err := f.store.ListPending(ctx, first.SessionID, 10)
			if err != nil || len(pending) != 0 {
				t.Fatalf("iteration %d pending after winning ack = %+v, %v", iteration, pending, err)
			}
			beforeReplay := f.pusher.CallCount()
			replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
			if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != beforeReplay {
				t.Fatalf("iteration %d acknowledged message replayed again: %+v, %v", iteration, replay, err)
			}
		}
	})
}

func TestWSIntegrationRaceBroadcastAndCleanup(t *testing.T) {
	runWSIntegrationBackendsConfigured(t, func(options *ws.Options) {
		options.Push.Concurrency = 2
	}, func(t *testing.T, f *wsIntegrationFixture) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for iteration := 0; iteration < 6; iteration++ {
			suffix := fmt.Sprintf("-%02d", iteration)
			connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-broadcast-race"+suffix, "alice", "", ""))
			roomID := "broadcast-race-room" + suffix
			mustWSIntegrationJoin(t, f.engine, connected, roomID)
			start := make(chan struct{})
			disconnectResult := make(chan error, 1)
			broadcastResult := make(chan ws.BroadcastResult, 1)
			go func() {
				<-start
				disconnectResult <- f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
					ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
				})
			}()
			go func() {
				<-start
				broadcastResult <- f.engine.Broadcast(ctx, roomID, wsRoomTestMessage("message-broadcast-race"+suffix), wsRoomTestBroadcastOptions())
			}()
			close(start)
			if err := <-disconnectResult; err != nil {
				t.Fatalf("iteration %d disconnect race error = %v", iteration, err)
			}
			result := <-broadcastResult
			if result.Err != nil || result.Failed != 0 || result.Total > 1 ||
				(result.Total == 1 && result.Succeeded+result.Offline != 1) {
				t.Fatalf("iteration %d broadcast/cleanup result = %+v", iteration, result)
			}
			if _, err := f.store.GetConnection(ctx, connected.ConnectionID); !wsstore.IsNotFound(err) {
				t.Fatalf("iteration %d disconnected connection remains: %v", iteration, err)
			}
			session, err := f.store.GetSession(ctx, connected.SessionID)
			if err != nil || session.CurrentConnectionID != "" {
				t.Fatalf("iteration %d session after broadcast race = %+v, %v", iteration, session, err)
			}
			members, _, err := f.store.Members(ctx, roomID, 10, "")
			if err != nil || len(members) != 0 {
				t.Fatalf("iteration %d room after cleanup = %+v, %v", iteration, members, err)
			}
		}
	})
}
