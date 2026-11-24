package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
)

func wsEngineMessageEvent(t *testing.T, connected ws.ConnectResult, messageID, requestID, path string, payload json.RawMessage) ws.MessageEvent {
	t.Helper()
	body, err := ws.EncodeEnvelope(ws.Envelope{
		Version:   ws.ProtocolVersion,
		Type:      ws.TypeRequest,
		Path:      path,
		RequestID: requestID,
		MessageID: messageID,
		SessionID: connected.SessionID,
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("EncodeEnvelope(request) error = %v", err)
	}
	return ws.MessageEvent{
		ConnectionID: connected.ConnectionID,
		RequestID:    requestID,
		SessionID:    connected.SessionID,
		Generation:   connected.Generation,
		Body:         string(body),
	}
}

func wsEngineDecodeCall(t *testing.T, call ws.PushInput) ws.Envelope {
	t.Helper()
	envelope, err := ws.DecodeEnvelope(call.Body)
	if err != nil {
		t.Fatalf("DecodeEnvelope(push) error = %v; body = %q", err, call.Body)
	}
	return *envelope
}

func TestWSEngineReplyPushAndErrorsKeepTrustedConnectionTarget(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-origin", "alice", "", ""))

	var replyErr error
	f.engine.Handle("/reply", func(handlerCtx *ws.Context) {
		handlerCtx.ConnectionID = "connection-attacker"
		handlerCtx.SessionID = "session-attacker"
		replyErr = f.engine.Reply(handlerCtx, map[string]string{"kind": "explicit-reply"})
	})
	f.engine.Handle("/failure", func(handlerCtx *ws.Context) {
		handlerCtx.ConnectionID = "connection-attacker"
		handlerCtx.SessionID = "session-attacker"
		handlerCtx.AbortWithError(ws.NewHandlerError("expected_failure", "safe public failure"))
	})

	if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, connected, "message-reply", "request-reply", "/reply", nil)); err != nil {
		t.Fatalf("HandleMessage(Reply) error = %v", err)
	}
	if replyErr != nil {
		t.Fatalf("Reply() error = %v", replyErr)
	}
	if err := f.engine.Push(ctx, connected.ConnectionID, ws.NewEvent("/push", "request-push", json.RawMessage(`{"kind":"active-push"}`))); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, connected, "message-error", "request-error", "/failure", nil)); err != nil {
		t.Fatalf("HandleMessage(error) error = %v", err)
	}

	calls := f.pusher.Calls()
	if len(calls) != 3 {
		t.Fatalf("push calls = %d, want Reply + Push + error (3): %+v", len(calls), calls)
	}
	wantTypes := []string{ws.TypeResponse, ws.TypeEvent, ws.TypeError}
	wantRequestIDs := []string{"request-reply", "request-push", "request-error"}
	for i, call := range calls {
		if call.ConnectionID != connected.ConnectionID {
			t.Errorf("call %d targeted %q, want originating %q", i, call.ConnectionID, connected.ConnectionID)
		}
		envelope := wsEngineDecodeCall(t, call)
		if envelope.Type != wantTypes[i] || envelope.RequestID != wantRequestIDs[i] || envelope.SessionID != connected.SessionID {
			t.Errorf("call %d envelope = %+v, want type=%q requestId=%q sessionId=%q", i, envelope, wantTypes[i], wantRequestIDs[i], connected.SessionID)
		}
	}
	if got := wsEngineDecodeCall(t, calls[2]).Error; got == nil || got.Code != "expected_failure" || got.Message != "safe public failure" {
		t.Fatalf("error response body = %+v, want safe application error", got)
	}

	beforeUnknown := f.pusher.CallCount()
	unknownMessage := ws.NewEvent("/unknown", "request-unknown", json.RawMessage(`{"unknown":true}`))
	if err := f.engine.Push(ctx, "connection-unknown", unknownMessage); !errors.Is(err, ws.ErrConnectionNotFound) {
		t.Fatalf("Push(unknown) error = %v, want ErrConnectionNotFound", err)
	}
	if err := f.engine.Reply(&ws.Context{
		Context: ctx, ConnectionID: "connection-unknown", SessionID: connected.SessionID, RequestID: "request-unknown-reply",
	}, map[string]bool{"unknown": true}); !errors.Is(err, ws.ErrConnectionNotFound) {
		t.Fatalf("Reply(unknown) error = %v, want ErrConnectionNotFound", err)
	}
	if _, err := f.engine.PushToSessionWithResult(ctx, "session-unknown", unknownMessage); !errors.Is(err, ws.ErrSessionNotFound) {
		t.Fatalf("PushToSession(unknown) error = %v, want ErrSessionNotFound", err)
	}
	if got := f.pusher.CallCount(); got != beforeUnknown {
		t.Fatalf("unknown targets caused %d replacement pushes, want none", got-beforeUnknown)
	}
	if _, err := f.store.GetConnection(ctx, connected.ConnectionID); err != nil {
		t.Fatalf("unknown-target attempts damaged origin: %v", err)
	}
}

func TestWSEngineIdempotencyReplayAndConcurrentDuplicates(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-idempotency", "alice", "", ""))

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var handlerCalls atomic.Int32
	f.engine.Handle("/work", func(handlerCtx *ws.Context) {
		handlerCalls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		handlerCtx.SetResponse(map[string]string{"result": "completed-once"})
	})
	event := wsEngineMessageEvent(t, connected, "message-work", "request-work", "/work", json.RawMessage(`{"input":"same"}`))

	ownerResult := make(chan error, 1)
	go func() { ownerResult <- f.engine.HandleMessage(ctx, event) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first handler did not start")
	}

	const duplicates = 8
	startDuplicates := make(chan struct{})
	duplicateResults := make(chan error, duplicates)
	for i := 0; i < duplicates; i++ {
		go func() {
			<-startDuplicates
			duplicateResults <- f.engine.HandleMessage(ctx, event)
		}()
	}
	close(startDuplicates)
	for i := 0; i < duplicates; i++ {
		select {
		case err := <-duplicateResults:
			if err != nil {
				t.Fatalf("in-flight duplicate %d error = %v", i, err)
			}
		case <-ctx.Done():
			t.Fatal("in-flight duplicate did not finish while owner was blocked")
		}
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler executions while owner blocked = %d, want 1", got)
	}
	if got := f.pusher.CallCount(); got != 0 {
		t.Fatalf("in-flight duplicates emitted %d responses before completion", got)
	}
	close(release)
	if err := <-ownerResult; err != nil {
		t.Fatalf("owning HandleMessage() error = %v", err)
	}

	firstCalls := f.pusher.Calls()
	if len(firstCalls) != 1 {
		t.Fatalf("calls after owning request = %d, want 1", len(firstCalls))
	}
	first := wsEngineDecodeCall(t, firstCalls[0])
	if first.Type != ws.TypeResponse || first.RequestID != "request-work" || !bytes.Contains(first.Payload, []byte("completed-once")) {
		t.Fatalf("first response = %+v", first)
	}

	if err := f.engine.HandleMessage(ctx, event); err != nil {
		t.Fatalf("completed duplicate HandleMessage() error = %v", err)
	}
	calls := f.pusher.Calls()
	if len(calls) != 2 || !bytes.Equal(calls[0].Body, calls[1].Body) {
		t.Fatalf("completed replay did not return the exact stored response: %+v", calls)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler executions after completed replay = %d, want 1", got)
	}
}

func TestWSEngineHandlerPanicRecoveryIsSafeAndReplayable(t *testing.T) {
	const panicSecret = "panic-must-not-reach-client"
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-panic", "alice", "", ""))
	var panicCalls atomic.Int32
	f.engine.Handle("/panic", func(*ws.Context) {
		panicCalls.Add(1)
		panic(panicSecret)
	})
	event := wsEngineMessageEvent(t, connected, "message-panic", "request-panic", "/panic", json.RawMessage(`{"private":"request payload"}`))

	for attempt := 0; attempt < 2; attempt++ {
		if err := f.engine.HandleMessage(ctx, event); err != nil {
			t.Fatalf("HandleMessage(panic) attempt %d error = %v, want recovered error response", attempt+1, err)
		}
	}
	calls := f.pusher.Calls()
	if len(calls) != 2 || !bytes.Equal(calls[0].Body, calls[1].Body) {
		t.Fatalf("panic response was not idempotently replayed: %+v", calls)
	}
	envelope := wsEngineDecodeCall(t, calls[0])
	if envelope.Type != ws.TypeError || envelope.Error == nil || envelope.Error.Code != "internal_error" || envelope.Error.Message == panicSecret {
		t.Fatalf("panic response = %+v, want safe internal_error", envelope)
	}
	if bytes.Contains(calls[0].Body, []byte(panicSecret)) || bytes.Contains(calls[0].Body, []byte("request payload")) {
		t.Fatalf("panic response leaked panic value or request payload: %s", calls[0].Body)
	}
	if got := panicCalls.Load(); got != 1 {
		t.Fatalf("panic handler calls = %d, want one execution plus stored replay", got)
	}

	f.engine.Handle("/healthy", func(handlerCtx *ws.Context) { handlerCtx.SetResponse(map[string]bool{"ok": true}) })
	if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, connected, "message-healthy", "request-healthy", "/healthy", nil)); err != nil {
		t.Fatalf("request after recovered panic error = %v", err)
	}
	if got := wsEngineDecodeCall(t, f.pusher.Calls()[2]); got.Type != ws.TypeResponse {
		t.Fatalf("response after panic = %+v, want normal response", got)
	}
}

type wsEngineObservabilityRecorder struct {
	mu      sync.Mutex
	logs    []ws.LogEvent
	metrics []ws.Metric
}

func (r *wsEngineObservabilityRecorder) Log(_ context.Context, event ws.LogEvent) {
	r.mu.Lock()
	r.logs = append(r.logs, event)
	r.mu.Unlock()
}

func (r *wsEngineObservabilityRecorder) Record(_ context.Context, metric ws.Metric) {
	r.mu.Lock()
	r.metrics = append(r.metrics, metric)
	r.mu.Unlock()
}

func (r *wsEngineObservabilityRecorder) snapshot() ([]ws.LogEvent, []ws.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ws.LogEvent(nil), r.logs...), append([]ws.Metric(nil), r.metrics...)
}

func TestWSEngineLogsAndMetricsExcludeCredentialsAndPayloads(t *testing.T) {
	const (
		headerSecret     = "header-credential-do-not-log"
		querySecret      = "query-credential-do-not-log"
		claimSecret      = "claim-value-do-not-log"
		resumeSecret     = "resume-credential-do-not-log"
		payloadSecret    = "message-payload-do-not-log"
		internalSecret   = "wrapped-internal-error-do-not-log"
		pushSecret       = "push-backend-credential-do-not-log"
		authFailureToken = "denied-credential-do-not-log"
	)
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Authenticator = ws.AuthenticatorFunc(func(_ context.Context, request ws.ConnectRequest) (ws.Principal, error) {
			if request.Headers["deny"] == "true" {
				return ws.Principal{}, errors.New(authFailureToken)
			}
			return ws.Principal{Subject: "alice", Claims: map[string]string{"private": claimSecret}}, nil
		})
		options.Push.MaxRetries = 0
	})
	recorder := &wsEngineObservabilityRecorder{}
	f.engine.SetLogger(recorder)
	f.engine.SetMetrics(recorder)
	ctx := context.Background()
	connect := wsSessionTestConnectEvent("connection-observability", "alice", "", "")
	connect.Headers["authorization"] = headerSecret
	connect.QueryParameters = map[string]string{"token": querySecret}
	connected := mustWSSessionTestConnect(t, f.engine, connect)

	f.engine.Handle("/private", func(handlerCtx *ws.Context) {
		handlerCtx.AbortWithError(ws.WrapHandlerError("denied", "public denial", errors.New(internalSecret)))
	})
	event := wsEngineMessageEvent(t, connected, "message-private", "request-private", "/private", json.RawMessage(`{"secret":"`+payloadSecret+`"}`))
	if err := f.engine.HandleMessage(ctx, event); err != nil {
		t.Fatalf("HandleMessage(private) error = %v", err)
	}

	f.pusher.SetHook(func(context.Context, ws.PushInput) error {
		return ws.NewPushError(ws.PushErrorUnavailable, errors.New(pushSecret))
	})
	if err := f.engine.Push(ctx, connected.ConnectionID, ws.NewEvent("/private-push", "request-private-push", json.RawMessage(`{"secret":"`+payloadSecret+`"}`))); err == nil {
		t.Fatal("Push() with scripted backend failure unexpectedly succeeded")
	}
	denied := wsSessionTestConnectEvent("connection-denied-observability", "alice", "session-resume-secret", resumeSecret)
	denied.Headers["deny"] = "true"
	denied.Headers["authorization"] = authFailureToken
	if _, err := f.engine.HandleConnect(ctx, denied); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("denied HandleConnect() error = %v, want ErrAuthenticationFailed", err)
	}

	logs, metrics := recorder.snapshot()
	if len(logs) < 4 || len(metrics) < 2 {
		t.Fatalf("observability test did not exercise enough events: logs=%d metrics=%d", len(logs), len(metrics))
	}
	var diagnostic strings.Builder
	for _, event := range logs {
		fmt.Fprintf(&diagnostic, "%s|%#v|%+v\n", event.String(), event, event)
	}
	for _, metric := range metrics {
		fmt.Fprintf(&diagnostic, "%#v|%+v\n", metric, metric)
	}
	for _, secret := range []string{headerSecret, querySecret, claimSecret, resumeSecret, payloadSecret, internalSecret, pushSecret, authFailureToken} {
		if strings.Contains(diagnostic.String(), secret) {
			t.Errorf("logs or metrics leaked %q:\n%s", secret, diagnostic.String())
		}
	}
}

func TestWSEnginePanickingObservabilityHooksDoNotBreakLifecycle(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	f.engine.SetLogger(ws.LoggerFunc(func(context.Context, ws.LogEvent) { panic("logger panic") }))
	f.engine.SetMetrics(ws.MetricsFunc(func(context.Context, ws.Metric) { panic("metrics panic") }))
	ctx := context.Background()
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-hooks", "alice", "", ""))
	if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
		RequestID: "request-ping-hooks", Body: `{"version":1,"type":"ping","requestId":"request-ping-hooks"}`,
	}); err != nil {
		t.Fatalf("HandleMessage() with panicking hooks error = %v", err)
	}
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
	}); err != nil {
		t.Fatalf("HandleDisconnect() with panicking hooks error = %v", err)
	}
}

func TestWSEnginePushToSessionSuccessTargetsCurrentBinding(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-session-old", "alice", "", ""))
	current := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-session-current", "alice", first.SessionID, first.ResumeCredential,
	))
	message := ws.NewEvent("/session/success", "request-session-success", json.RawMessage(`{"delivered":true}`))
	message.MessageID = "message-session-success"
	result, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, message)
	if err != nil {
		t.Fatalf("PushToSessionWithResult() error = %v", err)
	}
	if result.Status != ws.DeliverySent || result.SessionID != first.SessionID || result.ConnectionID != current.ConnectionID ||
		result.MessageID != message.MessageID || result.Gone || result.Offline {
		t.Fatalf("delivery result = %+v, want live delivery to current connection", result)
	}
	calls := f.pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != current.ConnectionID {
		t.Fatalf("push calls = %+v, want exactly current Connection_ID %q", calls, current.ConnectionID)
	}
	envelope := wsEngineDecodeCall(t, calls[0])
	if envelope.Type != ws.TypeEvent || envelope.SessionID != first.SessionID || envelope.RequestID != message.RequestID || envelope.MessageID != message.MessageID {
		t.Fatalf("delivered envelope = %+v", envelope)
	}
}

func TestWSEnginePushToSessionAcrossEnginesUsesSharedCurrentConnection(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	firstEngine := f.engine
	secondEngine := f.newEngine(t, nil)
	first := mustWSSessionTestConnect(t, firstEngine, wsSessionTestConnectEvent("connection-shared-old", "alice", "", ""))
	// Advance the binding through a different Engine, then send through the
	// original instance. Delivery must be resolved from shared state, not affinity.
	current := mustWSSessionTestConnect(t, secondEngine, wsSessionTestConnectEvent(
		"connection-shared-current", "alice", first.SessionID, first.ResumeCredential,
	))
	message := ws.NewEvent("/session/shared", "request-session-shared", json.RawMessage(`{"engine":2}`))
	message.MessageID = "message-session-shared"
	if err := firstEngine.PushToSession(ctx, first.SessionID, message); err != nil {
		t.Fatalf("PushToSession() through original Engine error = %v", err)
	}
	calls := f.pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != current.ConnectionID {
		t.Fatalf("cross-Engine push calls = %+v, want current Connection_ID %q", calls, current.ConnectionID)
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != current.ConnectionID || stored.Generation != current.Generation {
		t.Fatalf("shared binding after delivery = %+v, %v; current = %+v", stored, err, current)
	}
}
