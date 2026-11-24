package tests

import (
	"context"
	"github.com/aura-studio/lambda/ws"
	"testing"
)

func TestChapter07RouterMiddlewareWildcardAndAbort(t *testing.T) {
	router := ws.NewRouter()
	order := []string{}
	router.Use(func(ctx *ws.Context) { order = append(order, "middleware") })
	router.Handle("/echo/*path", func(ctx *ws.Context) {
		order = append(order, "handler")
		if ctx.PathParam() != "/42" {
			t.Fatalf("path param=%q", ctx.PathParam())
		}
		ctx.SetResponse(map[string]any{"ok": true})
	})
	ctx := &ws.Context{Context: context.Background(), Path: "/echo/42"}
	router.Dispatch(ctx)
	if len(order) != 2 || order[0] != "middleware" || !ctx.HasResponse() {
		t.Fatalf("order=%v response=%t", order, ctx.HasResponse())
	}
	router.NoRoute(func(ctx *ws.Context) { ctx.AbortWithError(ws.NewHandlerError("route_not_found", "no route")) })
	missing := &ws.Context{Context: context.Background(), Path: "/missing"}
	router.Dispatch(missing)
	if !missing.IsAborted() || ws.HandlerErrorCode(missing.Err()) != "route_not_found" {
		t.Fatalf("missing state=%v err=%v", missing.IsAborted(), missing.Err())
	}
}
