package tests

import (
	"context"
	"errors"
	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"testing"
)

func TestChapter10AuthenticationAndAuthorization(t *testing.T) {
	if err := ws.ValidateProductionAuth(nil); !errors.Is(err, ws.ErrAuthenticatorRequired) {
		t.Fatalf("production auth err=%v", err)
	}
	denied := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{}, errors.New("private token detail")
	})
	engine, _, store := newChapter09Engine(t, func(o *ws.Options) { o.Authenticator = denied; o.Auth.Anonymous = false })
	if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "denied-connection"}); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("connect err=%v", err)
	}
	if _, err := store.GetConnection(context.Background(), "denied-connection"); !wsstore.IsNotFound(err) {
		t.Fatalf("failed auth created state: %v", err)
	}
	ctx := &ws.Context{Context: context.Background(), Path: "/private", Principal: ws.Principal{Subject: "alice"}}
	ws.RequireAuthorization(ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error { return errors.New("denied") }))(ctx)
	if !ctx.IsAborted() || ws.HandlerErrorCode(ctx.Err()) != "authorization_denied" {
		t.Fatalf("authorization state=%v err=%v", ctx.IsAborted(), ctx.Err())
	}
}
