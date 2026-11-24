package ws

import (
	"path"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultRouteKey is API Gateway's catch-all WebSocket route.
	DefaultRouteKey = "$default"
	// MaxPathLength is the maximum accepted UTF-8 path size in bytes.
	MaxPathLength = 2048
)

type HandlerFunc func(*Context)

type route struct {
	pattern  string
	handlers []HandlerFunc
}

// Router dispatches normalized paths in registration order. Registrations and
// route-key mappings may be performed concurrently with dispatch; handlers run
// without the router lock held.
type Router struct {
	mu        sync.RWMutex
	pre       []HandlerFunc
	routes    []route
	noRoute   []HandlerFunc
	routeKeys map[string]string
}

func NewRouter() *Router {
	return &Router{routeKeys: make(map[string]string)}
}

func (r *Router) Use(handlers ...HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pre = append(r.pre, compactHandlers(handlers)...)
}

// Handle registers a route. A terminal *path captures the remaining path,
// including its leading slash. Registration-order matching is intentional and
// matches the existing event and reqresp routers.
func (r *Router) Handle(pattern string, handlers ...HandlerFunc) {
	normalized, err := normalizePattern(pattern)
	if err != nil {
		panic("ws: invalid route pattern")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route{
		pattern:  normalized,
		handlers: compactHandlers(handlers),
	})
}

// NoRoute replaces the current unmatched-path handler chain.
func (r *Router) NoRoute(handlers ...HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noRoute = compactHandlers(handlers)
}

// MapRouteKey maps a custom API Gateway route key to a trusted internal path.
// $default always uses the Envelope path and cannot be overridden.
func (r *Router) MapRouteKey(routeKey, internalPath string) {
	if routeKey == "" || routeKey == DefaultRouteKey {
		panic("ws: invalid route key mapping")
	}
	normalized, err := NormalizePath(internalPath)
	if err != nil {
		panic("ws: invalid route key path")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.routeKeys == nil {
		r.routeKeys = make(map[string]string)
	}
	r.routeKeys[routeKey] = normalized
}

// ResolvePath applies a custom route-key mapping. Unknown route keys follow
// the $default rule and use the Envelope path.
func (r *Router) ResolvePath(routeKey, envelopePath string) (string, error) {
	if routeKey != "" && routeKey != DefaultRouteKey {
		r.mu.RLock()
		mapped, ok := r.routeKeys[routeKey]
		r.mu.RUnlock()
		if ok {
			return NormalizePath(mapped)
		}
	}
	return NormalizePath(envelopePath)
}

// DispatchRoute resolves an API Gateway route key before dispatching.
func (r *Router) DispatchRoute(ctx *Context, routeKey string) {
	if ctx == nil {
		return
	}
	resolved, err := r.ResolvePath(routeKey, ctx.Path)
	if err != nil {
		ctx.AbortWithError(err)
		return
	}
	ctx.Path = resolved
	r.Dispatch(ctx)
}

// Dispatch executes global middleware followed by the first matching route or
// NoRoute. Abort and errors terminate the chain. A panic is recovered at this
// boundary and converted to a safe internal_error result.
func (r *Router) Dispatch(ctx *Context) {
	if ctx == nil {
		return
	}

	defer func() {
		if recover() != nil {
			ctx.AbortWithError(NewHandlerError("internal_error", "internal handler error"))
		}
	}()

	ctx.originatingConnectionID = ctx.ConnectionID
	normalized, err := NormalizePath(ctx.Path)
	if err != nil {
		ctx.AbortWithError(err)
		return
	}
	ctx.Path = normalized
	ctx.routePath = normalized
	ctx.pathParam = ""

	r.mu.RLock()
	pre := append([]HandlerFunc(nil), r.pre...)
	routes := append([]route(nil), r.routes...)
	noRoute := append([]HandlerFunc(nil), r.noRoute...)
	r.mu.RUnlock()

	if !runHandlers(ctx, pre) {
		return
	}

	matched := false
	var handlers []HandlerFunc
	param := ""
	for _, rt := range routes {
		if param, matched = MatchPattern(rt.pattern, normalized); matched {
			handlers = rt.handlers
			break
		}
	}

	if !matched {
		handlers = noRoute
	}
	if len(handlers) == 0 {
		ctx.AbortWithError(NewHandlerError("route_not_found", "no route for path"))
		return
	}

	ctx.pathParam = param
	if matched && param != "" {
		ctx.Path = param
	}
	runHandlers(ctx, handlers)
}

func runHandlers(ctx *Context, handlers []HandlerFunc) bool {
	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		handler(ctx)
		if ctx.IsAborted() || ctx.Err() != nil {
			return false
		}
	}
	return true
}

func compactHandlers(handlers []HandlerFunc) []HandlerFunc {
	out := make([]HandlerFunc, 0, len(handlers))
	for _, handler := range handlers {
		if handler != nil {
			out = append(out, handler)
		}
	}
	return out
}

// MatchPattern matches an exact path or a pattern with one terminal *path.
// The wildcard's base path matches with a captured value of "/".
func MatchPattern(pattern, requestPath string) (param string, ok bool) {
	if strings.HasSuffix(pattern, "*path") {
		prefix := strings.TrimSuffix(pattern, "*path")
		if prefix == "" {
			prefix = "/"
		}
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		base := strings.TrimSuffix(prefix, "/")
		if base == "" {
			base = "/"
		}
		if requestPath == base || (base == "/" && requestPath == "") {
			return "/", true
		}
		if !strings.HasPrefix(requestPath, prefix) {
			return "", false
		}
		rest := strings.TrimPrefix(requestPath, prefix)
		if rest == "" {
			return "/", true
		}
		return "/" + rest, true
	}
	return "", pattern == requestPath
}

// NormalizePath canonicalizes a route path and rejects ambiguous or oversized
// input. It follows the HTTP module's slash, leading-slash and trailing-slash
// conventions, and additionally cleans dot segments.
func NormalizePath(requestPath string) (string, error) {
	invalid := func() (string, error) {
		return "", NewHandlerError("invalid_path", "invalid request path")
	}
	if len(requestPath) > MaxPathLength || !utf8.ValidString(requestPath) {
		return invalid()
	}
	for _, r := range requestPath {
		if r < 0x20 || r == 0x7f || r == '?' || r == '#' {
			return invalid()
		}
	}

	requestPath = strings.ReplaceAll(requestPath, "\\", "/")
	if requestPath == "" {
		requestPath = "/"
	}
	if !strings.HasPrefix(requestPath, "/") {
		requestPath = "/" + requestPath
	}
	requestPath = path.Clean(requestPath)
	if len(requestPath) > MaxPathLength {
		return invalid()
	}
	return requestPath, nil
}

func normalizePattern(pattern string) (string, error) {
	wildcard := strings.HasSuffix(pattern, "*path")
	if strings.Contains(pattern, "*path") && !wildcard {
		return "", NewHandlerError("invalid_path", "invalid request path")
	}
	if wildcard {
		pattern = strings.TrimSuffix(pattern, "*path")
	}
	normalized, err := NormalizePath(pattern)
	if err != nil {
		return "", err
	}
	if wildcard {
		if normalized == "/" {
			return "/*path", nil
		}
		return normalized + "/*path", nil
	}
	if strings.Contains(normalized, "*path") {
		return "", NewHandlerError("invalid_path", "invalid request path")
	}
	return normalized, nil
}
