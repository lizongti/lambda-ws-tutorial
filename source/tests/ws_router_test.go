package tests

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aura-studio/lambda/ws"
)

func TestRouterExactMatchAndMiddlewareOrder(t *testing.T) {
	router := ws.NewRouter()
	var calls []string
	router.Use(
		func(*ws.Context) { calls = append(calls, "first") },
		nil,
		func(*ws.Context) { calls = append(calls, "second") },
	)
	router.Handle("/room/join", nil, func(ctx *ws.Context) {
		calls = append(calls, "handler")
		ctx.SetResponse("joined")
	})

	ctx := &ws.Context{Path: "room//join/"}
	router.Dispatch(ctx)

	if want := []string{"first", "second", "handler"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if ctx.RoutePath() != "/room/join" {
		t.Fatalf("RoutePath = %q, want /room/join", ctx.RoutePath())
	}
	if !ctx.HasResponse() || ctx.Response() != "joined" {
		t.Fatalf("response = %#v, set = %v", ctx.Response(), ctx.HasResponse())
	}
}

func TestRouterWildcardAndRegistrationOrder(t *testing.T) {
	router := ws.NewRouter()
	var wildcardParam, routePath string
	router.Handle("/api/*path", func(ctx *ws.Context) {
		wildcardParam = ctx.PathParam()
		routePath = ctx.RoutePath()
	})
	router.Handle("/api/exact", func(ctx *ws.Context) {
		t.Fatal("later exact route must not replace the first match")
	})

	ctx := &ws.Context{Path: "/api/pkg/v1/route"}
	router.Dispatch(ctx)
	if wildcardParam != "/pkg/v1/route" || ctx.Path != "/pkg/v1/route" {
		t.Fatalf("param = %q, Path = %q", wildcardParam, ctx.Path)
	}
	if routePath != "/api/pkg/v1/route" {
		t.Fatalf("RoutePath = %q", routePath)
	}

	base := &ws.Context{Path: "/api/"}
	router.Dispatch(base)
	if base.Err() != nil || base.PathParam() != "/" {
		t.Fatalf("base param = %q, err = %v", base.PathParam(), base.Err())
	}
}

func TestRouterAbortAndErrorTerminateChains(t *testing.T) {
	t.Run("abort preserves response", func(t *testing.T) {
		router := ws.NewRouter()
		called := false
		router.Use(func(ctx *ws.Context) {
			ctx.AbortWithResponse(map[string]string{"status": "done"})
		})
		router.Handle("/test", func(*ws.Context) { called = true })
		ctx := &ws.Context{Path: "/test"}
		router.Dispatch(ctx)
		if called || !ctx.IsAborted() || !ctx.HasResponse() {
			t.Fatalf("called = %v, aborted = %v, response = %v", called, ctx.IsAborted(), ctx.HasResponse())
		}
	})

	t.Run("middleware error skips route", func(t *testing.T) {
		router := ws.NewRouter()
		called := false
		router.Use(func(ctx *ws.Context) { ctx.SetError(errors.New("stop")) })
		router.Handle("/test", func(*ws.Context) { called = true })
		ctx := &ws.Context{Path: "/test"}
		router.Dispatch(ctx)
		if called || ctx.Err() == nil {
			t.Fatalf("called = %v, err = %v", called, ctx.Err())
		}
	})

	t.Run("handler error skips remaining handlers", func(t *testing.T) {
		router := ws.NewRouter()
		called := false
		router.Handle("/test",
			func(ctx *ws.Context) { ctx.SetError(errors.New("stop")) },
			func(*ws.Context) { called = true },
		)
		ctx := &ws.Context{Path: "/test"}
		router.Dispatch(ctx)
		if called {
			t.Fatal("handler after error was called")
		}
	})
}

func TestRouterNoRoute(t *testing.T) {
	t.Run("default safe error", func(t *testing.T) {
		ctx := &ws.Context{Path: "/missing"}
		ws.NewRouter().Dispatch(ctx)
		if got := ws.HandlerErrorCode(ctx.Err()); got != "route_not_found" {
			t.Fatalf("code = %q, want route_not_found", got)
		}
		if strings.Contains(ctx.Err().Error(), "/missing") {
			t.Fatalf("error exposed request path: %v", ctx.Err())
		}
	})

	t.Run("custom chain", func(t *testing.T) {
		router := ws.NewRouter()
		called := false
		router.NoRoute(func(ctx *ws.Context) {
			called = true
			ctx.SetResponse("fallback")
		})
		ctx := &ws.Context{Path: "/missing"}
		router.Dispatch(ctx)
		if !called || ctx.Response() != "fallback" || ctx.Err() != nil {
			t.Fatalf("called = %v, response = %#v, err = %v", called, ctx.Response(), ctx.Err())
		}
	})
}

func TestRouterRecoversPanicWithoutLeakingDetails(t *testing.T) {
	router := ws.NewRouter()
	const secret = "token=super-secret stack=private"
	router.Handle("/panic", func(*ws.Context) { panic(secret) })
	router.Handle("/healthy", func(ctx *ws.Context) { ctx.SetResponse("ok") })

	ctx := &ws.Context{Path: "/panic"}
	router.Dispatch(ctx)
	if got := ws.HandlerErrorCode(ctx.Err()); got != "internal_error" {
		t.Fatalf("code = %q, want internal_error", got)
	}
	if strings.Contains(ctx.Err().Error(), secret) || !ctx.IsAborted() {
		t.Fatalf("unsafe panic state: err = %v, aborted = %v", ctx.Err(), ctx.IsAborted())
	}

	healthy := &ws.Context{Path: "/healthy"}
	router.Dispatch(healthy)
	if healthy.Err() != nil || healthy.Response() != "ok" {
		t.Fatalf("router unusable after panic: response = %#v, err = %v", healthy.Response(), healthy.Err())
	}
}

func TestRouterRouteKeyMapping(t *testing.T) {
	router := ws.NewRouter()
	router.MapRouteKey("join", "/room/join")
	router.Handle("/room/join", func(ctx *ws.Context) { ctx.SetResponse("mapped") })
	router.Handle("/client/path", func(ctx *ws.Context) { ctx.SetResponse("default") })

	mapped := &ws.Context{Path: "/client/path"}
	router.DispatchRoute(mapped, "join")
	if mapped.Response() != "mapped" || mapped.RoutePath() != "/room/join" {
		t.Fatalf("mapped response = %#v, path = %q", mapped.Response(), mapped.RoutePath())
	}

	fallback := &ws.Context{Path: "/client/path"}
	router.DispatchRoute(fallback, "unknown")
	if fallback.Response() != "default" {
		t.Fatalf("unknown route key response = %#v", fallback.Response())
	}

	defaultRoute := &ws.Context{Path: "/client/path"}
	router.DispatchRoute(defaultRoute, ws.DefaultRouteKey)
	if defaultRoute.Response() != "default" {
		t.Fatalf("$default response = %#v", defaultRoute.Response())
	}
}

func TestRouterRejectsInvalidPathBeforeHandlers(t *testing.T) {
	router := ws.NewRouter()
	called := false
	router.Use(func(*ws.Context) { called = true })
	router.Handle("/*path", func(*ws.Context) { called = true })

	for _, requestPath := range []string{
		strings.Repeat("a", ws.MaxPathLength+1),
		"/private?token=secret",
		"/bad#fragment",
		string([]byte{'/', 0xff}),
	} {
		ctx := &ws.Context{Path: requestPath}
		router.Dispatch(ctx)
		if got := ws.HandlerErrorCode(ctx.Err()); got != "invalid_path" {
			t.Errorf("path code = %q, want invalid_path", got)
		}
	}
	if called {
		t.Fatal("invalid path reached middleware or handler")
	}
}

func TestContextRetainsOriginatingConnectionID(t *testing.T) {
	router := ws.NewRouter()
	router.Handle("/test", func(ctx *ws.Context) { ctx.ConnectionID = "attacker-selected" })
	ctx := &ws.Context{ConnectionID: "connection-from-event", Path: "/test"}
	router.Dispatch(ctx)
	if got := ctx.OriginatingConnectionID(); got != "connection-from-event" {
		t.Fatalf("OriginatingConnectionID = %q", got)
	}
}
