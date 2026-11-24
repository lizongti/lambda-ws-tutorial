package ws_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/aura-studio/lambda/ws"
)

func TestAuthenticatorAndAuthorizerFunctionAdapters(t *testing.T) {
	request := ws.ConnectRequest{
		ConnectionID:    "connection-1",
		Headers:         map[string]string{"Authorization": "Bearer secret"},
		QueryParameters: map[string]string{"tenant": "tenant-1"},
		RequestContext:  map[string]any{"stage": "test"},
	}
	authenticator := ws.AuthenticatorFunc(func(_ context.Context, got ws.ConnectRequest) (ws.Principal, error) {
		if !reflect.DeepEqual(got, request) {
			t.Fatalf("request = %#v, want original metadata", got)
		}
		return ws.Principal{Subject: "subject-1", Claims: map[string]string{"tenant": "tenant-1"}}, nil
	})
	principal, err := authenticator.Authenticate(context.Background(), request)
	if err != nil || principal.Subject != "subject-1" || principal.Claims["tenant"] != "tenant-1" {
		t.Fatalf("principal = %#v, err = %v", principal, err)
	}

	var action ws.Action
	authorizer := ws.AuthorizerFunc(func(_ context.Context, got ws.Principal, gotAction ws.Action) error {
		if got.Subject != principal.Subject {
			t.Fatalf("subject = %q", got.Subject)
		}
		action = gotAction
		return nil
	})
	wantAction := ws.Action{Name: ws.ActionRoomJoin, RoomID: "lobby"}
	if err := authorizer.Authorize(context.Background(), principal, wantAction); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(action, wantAction) {
		t.Fatalf("action = %#v, want %#v", action, wantAction)
	}
}

func TestRequireAuthorizationOrderAndDenial(t *testing.T) {
	t.Run("allowed", func(t *testing.T) {
		var calls []string
		authorizer := ws.AuthorizerFunc(func(_ context.Context, principal ws.Principal, action ws.Action) error {
			calls = append(calls, "authorize")
			if principal.Subject != "subject-1" || action.Name != ws.ActionPathAccess || action.Path != "/protected" {
				t.Fatalf("principal = %v, action = %#v", principal, action)
			}
			return nil
		})
		router := ws.NewRouter()
		router.Use(
			func(*ws.Context) { calls = append(calls, "before") },
			ws.RequireAuthorization(authorizer),
			func(*ws.Context) { calls = append(calls, "after") },
		)
		router.Handle("/protected", func(*ws.Context) { calls = append(calls, "handler") })
		ctx := &ws.Context{Path: "/protected", Principal: ws.Principal{Subject: "subject-1"}}
		router.Dispatch(ctx)
		if want := []string{"before", "authorize", "after", "handler"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	})

	t.Run("denied stops chain and redacts cause", func(t *testing.T) {
		const secret = "DynamoDB token=secret policy=private"
		authorizer := ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error {
			return errors.New(secret)
		})
		router := ws.NewRouter()
		handlerCalled := false
		router.Use(ws.RequireAuthorization(authorizer))
		router.Handle("/protected", func(*ws.Context) { handlerCalled = true })
		ctx := &ws.Context{Path: "/protected", Principal: ws.Principal{Subject: "subject-1"}}
		router.Dispatch(ctx)

		if handlerCalled || !ctx.IsAborted() {
			t.Fatalf("handlerCalled = %v, aborted = %v", handlerCalled, ctx.IsAborted())
		}
		if got := ws.HandlerErrorCode(ctx.Err()); got != "authorization_denied" {
			t.Fatalf("code = %q", got)
		}
		formatted := fmt.Sprintf("%v %+v %#v", ctx.Err(), ctx.Err(), ctx.Err())
		if strings.Contains(formatted, secret) || strings.Contains(formatted, "DynamoDB") {
			t.Fatalf("authorization error leaked details: %s", formatted)
		}
	})

	t.Run("missing principal fails before authorizer", func(t *testing.T) {
		called := false
		authorizer := ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error {
			called = true
			return nil
		})
		ctx := &ws.Context{Path: "/protected"}
		ws.RequireAuthorization(authorizer)(ctx)
		if called || ws.HandlerErrorCode(ctx.Err()) != "authentication_required" {
			t.Fatalf("called = %v, err = %v", called, ctx.Err())
		}
	})
}

func TestAuthorizationUsesOriginalWildcardRoutePath(t *testing.T) {
	var authorizedPath string
	authorizer := ws.AuthorizerFunc(func(_ context.Context, _ ws.Principal, action ws.Action) error {
		authorizedPath = action.Path
		return nil
	})
	router := ws.NewRouter()
	router.Handle("/api/*path", ws.RequireAuthorization(authorizer))
	ctx := &ws.Context{Path: "/api/private/resource", Principal: ws.Principal{Subject: "subject-1"}}
	router.Dispatch(ctx)
	if authorizedPath != "/api/private/resource" {
		t.Fatalf("authorized path = %q", authorizedPath)
	}
}

func TestAnonymousAuthenticationRestrictions(t *testing.T) {
	anonymous := ws.NewAnonymousAuthenticator()
	principal, err := anonymous.Authenticate(context.Background(), ws.ConnectRequest{
		Headers: map[string]string{"Authorization": "must-not-be-used"},
	})
	if err != nil || principal.Subject != ws.AnonymousSubject {
		t.Fatalf("anonymous principal = %v, err = %v", principal, err)
	}

	if err := ws.ValidateAuthConfig(ws.AuthConfig{Development: true, Anonymous: true}, anonymous); err != nil {
		t.Fatalf("explicit development anonymous rejected: %v", err)
	}
	for name, config := range map[string]ws.AuthConfig{
		"production anonymous flag": {Anonymous: true},
		"production anonymous impl": {},
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(ws.ValidateAuthConfig(config, anonymous), ws.ErrAnonymousNotAllowed) {
				t.Fatalf("err = %v, want ErrAnonymousNotAllowed", ws.ValidateAuthConfig(config, anonymous))
			}
		})
	}
	if !errors.Is(
		ws.ValidateAuthConfig(ws.AuthConfig{Development: true}, anonymous),
		ws.ErrAnonymousNotExplicit,
	) {
		t.Fatal("implicit development anonymous was accepted")
	}
	if !errors.Is(
		ws.ValidateAuthConfig(ws.AuthConfig{Development: true, Anonymous: true}, nil),
		ws.ErrAnonymousNotExplicit,
	) {
		t.Fatal("anonymous flag without explicit authenticator was accepted")
	}
}

func TestAuthConfigRequiresAuthenticator(t *testing.T) {
	if !errors.Is(ws.ValidateProductionAuth(nil), ws.ErrAuthenticatorRequired) {
		t.Fatal("production configuration accepted a nil authenticator")
	}
	if !errors.Is(
		ws.ValidateAuthConfig(ws.AuthConfig{Development: true, Protected: true}, nil),
		ws.ErrAuthenticatorRequired,
	) {
		t.Fatal("protected development configuration accepted a nil authenticator")
	}
	authenticator := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{Subject: "subject-1"}, nil
	})
	if err := ws.ValidateProductionAuth(authenticator); err != nil {
		t.Fatalf("valid production authenticator rejected: %v", err)
	}
}

func TestAuthFormattingDoesNotLeakSensitiveValues(t *testing.T) {
	const secret = "raw-access-token-and-resume-secret"
	request := ws.ConnectRequest{
		ConnectionID:    "connection-safe",
		RequestID:       "request-safe",
		Headers:         map[string]string{"Authorization": secret},
		QueryParameters: map[string]string{"resume": secret},
		RequestContext:  map[string]any{"identity": secret},
	}
	principal := ws.Principal{Subject: "subject-safe", Claims: map[string]string{"credential": secret}}
	wrapped := ws.WrapHandlerError("internal_error", "internal handler error", errors.New(secret))

	for name, formatted := range map[string]string{
		"request":   fmt.Sprintf("%v %+v %#v", request, request, request),
		"principal": fmt.Sprintf("%v %+v %#v", principal, principal, principal),
		"error":     fmt.Sprintf("%v %+v %#v", wrapped, wrapped, wrapped),
	} {
		if strings.Contains(formatted, secret) {
			t.Errorf("%s formatting leaked secret: %s", name, formatted)
		}
	}
}

func TestPrincipalCloneIsolatesClaims(t *testing.T) {
	original := ws.Principal{Subject: "subject-1", Claims: map[string]string{"role": "player"}}
	clone := original.Clone()
	clone.Claims["role"] = "admin"
	if original.Claims["role"] != "player" {
		t.Fatal("Clone shared its Claims map with the original")
	}
}
