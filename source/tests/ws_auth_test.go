package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

type wsAuthCountingStore struct {
	wsstore.StateStore
	putConnections  int
	createSessions  int
	joins           int
	offlineAppends  int
	idempotencyRuns int
}

func (s *wsAuthCountingStore) PutConnection(ctx context.Context, connection wsstore.Connection) error {
	s.putConnections++
	return s.StateStore.PutConnection(ctx, connection)
}

func (s *wsAuthCountingStore) CreateSession(ctx context.Context, session wsstore.Session) error {
	s.createSessions++
	return s.StateStore.CreateSession(ctx, session)
}

func (s *wsAuthCountingStore) Join(ctx context.Context, member wsstore.RoomMember) error {
	s.joins++
	return s.StateStore.Join(ctx, member)
}

func (s *wsAuthCountingStore) Append(ctx context.Context, message wsstore.OfflineMessage) error {
	s.offlineAppends++
	return s.StateStore.Append(ctx, message)
}

func (s *wsAuthCountingStore) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	s.idempotencyRuns++
	return s.StateStore.BeginIdempotency(ctx, record)
}

func TestWSAuthenticationFailureHasNoStateOrDeliverySideEffects(t *testing.T) {
	clock := &wsSessionTestClock{}
	ids := &wsSessionTestIDGenerator{}
	options := wsSessionTestOptions(clock, ids)
	options.Authenticator = ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{}, errors.New("private authentication backend detail")
	})
	store := &wsAuthCountingStore{StateStore: memory.New(memory.WithClock(clock.Now))}
	pusher := ws.NewFakePusher()
	options.StateStore = store
	options.Pusher = pusher
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}

	event := wsSessionTestConnectEvent("connection-denied-auth", "alice", "session-forbidden", "resume-secret")
	if _, err := engine.HandleConnect(context.Background(), event); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("HandleConnect() error = %v, want ErrAuthenticationFailed", err)
	}
	if store.putConnections != 0 || store.createSessions != 0 || store.joins != 0 || store.offlineAppends != 0 || store.idempotencyRuns != 0 {
		t.Fatalf("denied connect wrote state: %+v", store)
	}
	if pusher.CallCount() != 0 {
		t.Fatal("denied connect emitted an application push")
	}
}

func TestWSDevelopmentWithoutAuthenticatorUsesAnonymousIdentity(t *testing.T) {
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Auth.Development = true
		options.Authenticator = nil
	})
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-development-anonymous", "ignored", "", ""))
	session, err := f.store.GetSession(context.Background(), connected.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if session.PrincipalSubject != ws.AnonymousSubject {
		t.Fatalf("development principal = %q, want %q", session.PrincipalSubject, ws.AnonymousSubject)
	}
}

func TestWSProtectedPathUsesAuthenticatedPrincipalAndSession(t *testing.T) {
	type authorization struct {
		subject   string
		sessionID string
		path      string
	}
	var authorized []authorization
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Auth.Protected = true
		options.Authorizer = ws.AuthorizerFunc(func(_ context.Context, principal ws.Principal, action ws.Action) error {
			authorized = append(authorized, authorization{subject: principal.Subject, sessionID: action.SessionID, path: action.Path})
			if principal.Subject != "alice" {
				return errors.New("private policy detail")
			}
			return nil
		})
	})
	alice := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-auth-alice", "alice", "", ""))
	bob := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-auth-bob", "bob", "", ""))
	handlerCalls := 0
	f.engine.Handle("/protected", func(ctx *ws.Context) {
		handlerCalls++
		ctx.SetResponse(map[string]string{"subject": ctx.Principal.Subject})
	})

	if err := f.engine.HandleMessage(context.Background(), wsEngineMessageEvent(t, alice, "message-auth-alice", "request-auth-alice", "/protected", nil)); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.HandleMessage(context.Background(), wsEngineMessageEvent(t, bob, "message-auth-bob", "request-auth-bob", "/protected", nil)); err != nil {
		t.Fatal(err)
	}
	if handlerCalls != 1 {
		t.Fatalf("protected handler calls = %d, want alice only", handlerCalls)
	}
	if len(authorized) != 2 || authorized[0] != (authorization{subject: "alice", sessionID: alice.SessionID, path: "/protected"}) ||
		authorized[1] != (authorization{subject: "bob", sessionID: bob.SessionID, path: "/protected"}) {
		t.Fatalf("authorization actions = %+v", authorized)
	}
	calls := f.pusher.Calls()
	if len(calls) != 2 {
		t.Fatalf("push calls = %d, want response and denial", len(calls))
	}
	if got := wsEngineDecodeCall(t, calls[0]); got.SessionID != alice.SessionID || got.Type != ws.TypeResponse {
		t.Fatalf("alice response = %+v", got)
	}
	if got := wsEngineDecodeCall(t, calls[1]); got.SessionID != bob.SessionID || got.Type != ws.TypeError || got.Error == nil ||
		got.Error.Code != "authorization_denied" || got.Error.Message != "access denied" {
		t.Fatalf("bob denial = %+v", got)
	}
}

func TestWSProductionAnonymousEngineInitializationIsRejected(t *testing.T) {
	for _, test := range []struct {
		name          string
		anonymousFlag bool
		authenticator ws.Authenticator
	}{
		{name: "anonymous implementation", authenticator: ws.NewAnonymousAuthenticator()},
		{name: "explicit anonymous implementation and flag", anonymousFlag: true, authenticator: ws.NewAnonymousAuthenticator()},
		{name: "anonymous flag without authenticator", anonymousFlag: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := ws.NewOptions()
			options.StateStore = memory.New()
			options.Pusher = ws.NewFakePusher()
			options.Authenticator = test.authenticator
			options.Auth = ws.AuthConfig{Anonymous: test.anonymousFlag}
			engine, err := ws.NewEngine(options)
			if engine != nil || !errors.Is(err, ws.ErrAnonymousNotAllowed) {
				t.Fatalf("NewEngine() = %+v, %v; want nil engine and ErrAnonymousNotAllowed", engine, err)
			}
		})
	}
}

func TestWSAuthorizationReceivesIsolatedPrincipalClaims(t *testing.T) {
	principal := ws.Principal{Subject: "alice", Claims: map[string]string{"role": "player"}}
	authorizations := 0
	authorizer := ws.AuthorizerFunc(func(_ context.Context, received ws.Principal, _ ws.Action) error {
		authorizations++
		if received.Claims["role"] != "player" {
			t.Fatalf("authorizer %d received mutated role %q", authorizations, received.Claims["role"])
		}
		received.Claims["role"] = "admin"
		received.Claims["authorizer-only"] = "private"
		return nil
	})
	router := ws.NewRouter()
	router.Handle("/protected", ws.RequireAuthorization(authorizer), func(ctx *ws.Context) {
		if ctx.Principal.Claims["role"] != "player" {
			t.Fatalf("handler principal role = %q, want player", ctx.Principal.Claims["role"])
		}
		if _, leaked := ctx.Principal.Claims["authorizer-only"]; leaked {
			t.Fatal("authorizer mutation leaked into the request principal")
		}
		ctx.SetResponse("ok")
	})
	for i := 0; i < 2; i++ {
		ctx := &ws.Context{Path: "/protected", Principal: principal}
		router.Dispatch(ctx)
		if ctx.Err() != nil || ctx.Response() != "ok" {
			t.Fatalf("dispatch %d response = %#v, error = %v", i+1, ctx.Response(), ctx.Err())
		}
	}
	if authorizations != 2 || principal.Claims["role"] != "player" || len(principal.Claims) != 1 {
		t.Fatalf("authorizations = %d, original principal = %+v", authorizations, principal)
	}
}

func TestWSAuthorizationRunsBeforeRouterAndHandler(t *testing.T) {
	var calls []string
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Auth.Protected = true
		options.Authorizer = ws.AuthorizerFunc(func(_ context.Context, _ ws.Principal, action ws.Action) error {
			calls = append(calls, "authorize:"+action.Path)
			if action.Path == "/denied" {
				return errors.New("private authorization policy")
			}
			return nil
		})
	})
	connected := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-auth-order", "alice", "", ""))
	f.engine.Use(func(ctx *ws.Context) { calls = append(calls, "middleware:"+ctx.RoutePath()) })
	f.engine.Handle("/allowed", func(ctx *ws.Context) {
		calls = append(calls, "handler:/allowed")
		ctx.SetResponse("ok")
	})
	f.engine.Handle("/denied", func(*ws.Context) { calls = append(calls, "handler:/denied") })
	if err := f.engine.HandleMessage(context.Background(), wsEngineMessageEvent(t, connected, "message-auth-order-allowed", "request-auth-order-allowed", "/allowed", nil)); err != nil {
		t.Fatal(err)
	}
	if err := f.engine.HandleMessage(context.Background(), wsEngineMessageEvent(t, connected, "message-auth-order-denied", "request-auth-order-denied", "/denied", nil)); err != nil {
		t.Fatal(err)
	}
	want := []string{"authorize:/allowed", "middleware:/allowed", "handler:/allowed", "authorize:/denied"}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("authorization/handler order = %v, want %v", calls, want)
	}
	pushes := f.pusher.Calls()
	if len(pushes) != 2 {
		t.Fatalf("authorization responses = %+v, want 2", pushes)
	}
	denial := wsEngineDecodeCall(t, pushes[1])
	if denial.Error == nil || denial.Error.Code != "authorization_denied" {
		t.Fatalf("authorization denial = %+v", denial)
	}
}

func TestWSAuthenticationLogsRedactCredentialsAndClaims(t *testing.T) {
	const (
		headerSecret = "header-auth-secret"
		querySecret  = "query-auth-secret"
		claimSecret  = "claim-auth-secret"
		denialSecret = "authentication-backend-secret"
		resumeSecret = "resume-auth-secret"
	)
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Authenticator = ws.AuthenticatorFunc(func(_ context.Context, request ws.ConnectRequest) (ws.Principal, error) {
			if request.Headers["deny"] == "true" {
				return ws.Principal{}, errors.New(denialSecret)
			}
			return ws.Principal{Subject: "alice", Claims: map[string]string{"credential": claimSecret}}, nil
		})
	})
	recorder := &wsEngineObservabilityRecorder{}
	f.engine.SetLogger(recorder)
	accepted := wsSessionTestConnectEvent("connection-auth-log-accepted", "alice", "", "")
	accepted.Headers["authorization"] = headerSecret
	accepted.QueryParameters = map[string]string{"token": querySecret}
	mustWSSessionTestConnect(t, f.engine, accepted)
	denied := wsSessionTestConnectEvent("connection-auth-log-denied", "alice", "session-private", resumeSecret)
	denied.Headers["deny"] = "true"
	denied.Headers["authorization"] = headerSecret
	if _, err := f.engine.HandleConnect(context.Background(), denied); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("denied HandleConnect() error = %v", err)
	}
	logs, _ := recorder.snapshot()
	if len(logs) < 2 {
		t.Fatalf("captured %d authentication logs, want accepted and denied", len(logs))
	}
	var diagnostic strings.Builder
	for _, event := range logs {
		fmt.Fprintf(&diagnostic, "%v|%+v|%#v\n", event, event, event)
	}
	for _, secret := range []string{headerSecret, querySecret, claimSecret, denialSecret, resumeSecret} {
		if strings.Contains(diagnostic.String(), secret) {
			t.Errorf("authentication logs leaked %q: %s", secret, diagnostic.String())
		}
	}
}
