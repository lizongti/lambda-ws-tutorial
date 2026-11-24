const tests = {
1: `package tests

import (
    "encoding/json"
    "strings"
    "testing"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter01EnvelopeRoundTripAndValidation(t *testing.T) {
    original := ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/echo", RequestID: "req-1", MessageID: "msg-1", Payload: json.RawMessage(` + "`" + `{"text":"你好"}` + "`" + `)}
    body, err := ws.EncodeEnvelope(original)
    if err != nil { t.Fatal(err) }
    decoded, err := ws.DecodeEnvelopeWithLimit(body, ws.MaxMessageBytes)
    if err != nil { t.Fatal(err) }
    if decoded.Path != original.Path || string(decoded.Payload) != string(original.Payload) { t.Fatalf("round trip mismatch: %#v", decoded) }
    if _, err := ws.DecodeEnvelopeWithLimit([]byte(` + "`" + `{"version":1,"type":"request"}` + "`" + `), ws.MaxMessageBytes); err == nil { t.Fatal("missing path was accepted") }
    if _, err := ws.DecodeEnvelopeWithLimit([]byte(` + "`" + `{"version":2,"type":"request","path":"/"}` + "`" + `), ws.MaxMessageBytes); err == nil { t.Fatal("unsupported version was accepted") }
    if _, err := ws.DecodeEnvelopeWithLimit([]byte(strings.Repeat(" ", ws.MaxMessageBytes+1)), ws.MaxMessageBytes); err == nil { t.Fatal("oversized input was accepted") }
}
`,
2: `package tests

import (
    "context"
    "testing"
    "time"
    "github.com/aura-studio/lambda/ws"
)

type chapter02Pusher struct{}
func (chapter02Pusher) PostToConnection(context.Context, string, []byte) error { return nil }

func TestChapter02OptionsAndYAML(t *testing.T) {
    options := ws.NewOptions(
        ws.WithDevelopment(true),
        ws.WithAuthenticator(ws.NewAnonymousAuthenticator()),
        ws.WithPusher(chapter02Pusher{}),
        ws.OptionFunc(func(o *ws.Options) { o.Auth.Anonymous = true }),
        ws.WithConfig([]byte("heartbeat:\\n  interval: 10s\\n  timeout: 30s\\n")),
    )
    if err := options.Validate(); err != nil { t.Fatal(err) }
    if options.Heartbeat.Interval != 10*time.Second || options.Heartbeat.Timeout != 30*time.Second { t.Fatalf("YAML override failed: %#v", options.Heartbeat) }
    options.SharedDeployment = true
    if err := options.Validate(); err == nil { t.Fatal("shared memory deployment was accepted") }
}

func TestChapter02EndpointValidation(t *testing.T) {
    endpoint, err := ws.BuildAPIGatewayManagementEndpoint("api", "cn-north-1", "prod")
    if err != nil || endpoint != "https://api.execute-api.cn-north-1.amazonaws.com.cn/prod" { t.Fatalf("endpoint=%q err=%v", endpoint, err) }
    if _, err := ws.ValidateAPIGatewayManagementEndpoint("https://user:pass@example.com/prod"); err == nil { t.Fatal("credential endpoint accepted") }
}
`,
3: `package tests

import (
    "errors"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter03StoreModelsAndErrors(t *testing.T) {
    err := wsstore.NewError(wsstore.ErrorCodeConditionConflict, "BindConnection", errors.New("private backend detail"))
    if !wsstore.IsConditionConflict(err) || wsstore.IsRetryable(err) { t.Fatalf("wrong classification: %v", err) }
    if err.Error() == "" || err.Error() == "private backend detail" { t.Fatalf("unsafe error text: %q", err) }
    now := time.Now()
    session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 2, LastActiveAt: now}
    connection := wsstore.Connection{ConnectionID: "c1", SessionID: session.SessionID, Generation: session.Generation}
    if connection.SessionID != session.SessionID || connection.Generation != 2 { t.Fatal("model binding mismatch") }
    var store wsstore.StateStore
    if store != nil { t.Fatal("nil interface must remain nil") }
}
`,
4: `package tests

import (
    "context"
    "sync"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
    memorystore "github.com/aura-studio/lambda/ws/store/memory"
)

func chapter04Store() (*memorystore.Store, time.Time) {
    now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
    return memorystore.New(memorystore.WithClock(func() time.Time { return now })), now
}

func TestChapter04ConnectionSessionAndRace(t *testing.T) {
    ctx := context.Background()
    store, now := chapter04Store()
    session := wsstore.Session{SessionID: "s1", PrincipalSubject: "alice", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
    if err := store.CreateSession(ctx, session); err != nil { t.Fatal(err) }
    connection := wsstore.Connection{ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "alice", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}
    if err := store.PutConnection(ctx, connection); err != nil { t.Fatal(err) }
    bound, err := store.BindConnection(ctx, "s1", "c2", 1)
    if err != nil || bound.Generation != 2 || bound.CurrentConnectionID != "c2" { t.Fatalf("bind=%#v err=%v", bound, err) }
    if err := store.UnbindConnectionIfMatch(ctx, "s1", "c1", 1); !wsstore.IsConditionConflict(err) { t.Fatalf("stale unbind err=%v", err) }
    var wg sync.WaitGroup
    for i := 0; i < 8; i++ { wg.Add(1); go func() { defer wg.Done(); _ = store.TouchConnection(ctx, "c1", 1, now.Add(time.Second)) }() }
    wg.Wait()
    got, err := store.GetConnection(ctx, "c1")
    if err != nil || !got.LastSeenAt.Equal(now.Add(time.Second)) { t.Fatalf("touch=%#v err=%v", got, err) }
}
`,
5: `package tests

import (
    "context"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter05RoomOfflineAndIdempotency(t *testing.T) {
    ctx := context.Background()
    store, now := chapter04Store()
    session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
    if err := store.CreateSession(ctx, session); err != nil { t.Fatal(err) }
    if err := store.Join(ctx, wsstore.RoomMember{RoomID: "lobby", SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil { t.Fatal(err) }
    members, cursor, err := store.Members(ctx, "lobby", 10, "")
    if err != nil || len(members) != 1 || cursor != "" { t.Fatalf("members=%v cursor=%q err=%v", members, cursor, err) }
    if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1, Envelope: []byte(` + "`" + `{"ok":true}` + "`" + `), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    pending, err := store.ListPending(ctx, "s1", 10)
    if err != nil || len(pending) != 1 || pending[0].Sequence != 1 { t.Fatalf("pending=%#v err=%v", pending, err) }
    if err := store.MarkSent(ctx, "s1", "m1", 1); err != nil { t.Fatal(err) }
    if err := store.Ack(ctx, "s1", "m1", 1); err != nil { t.Fatal(err) }
    record, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "incoming-1", ExpiresAt: now.Add(time.Minute)})
    if err != nil || !acquired || record.MessageID != "incoming-1" { t.Fatalf("begin=%#v acquired=%t err=%v", record, acquired, err) }
    if _, acquired, err = store.BeginIdempotency(ctx, record); err != nil || acquired { t.Fatalf("duplicate acquired=%t err=%v", acquired, err) }
}
`,
6: `package tests

import (
    "context"
    "errors"
    "testing"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter06FakePusherAndErrors(t *testing.T) {
    gone := ws.NewPushError(ws.PushErrorGone, errors.New("410"))
    temporary := ws.NewPushError(ws.PushErrorThrottled, errors.New("limited"))
    if ws.IsRetryablePushError(gone) || !ws.IsPushGone(gone) || !ws.IsRetryablePushError(temporary) { t.Fatal("push error classification mismatch") }
    pusher := ws.NewFakePusher(nil)
    if err := pusher.PostToConnection(context.Background(), "c1", []byte(` + "`" + `{"ok":true}` + "`" + `)); err != nil { t.Fatal(err) }
    calls := pusher.Calls()
    if len(calls) != 1 || calls[0].ConnectionID != "c1" || string(calls[0].Body) != ` + "`" + `{"ok":true}` + "`" + ` { t.Fatalf("calls=%#v", calls) }
    calls[0].Body[0] = 'X'
    if string(pusher.Calls()[0].Body)[0] == 'X' { t.Fatal("recorded body aliases caller mutation") }
}
`,
7: `package tests

import (
    "context"
    "testing"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter07RouterMiddlewareWildcardAndAbort(t *testing.T) {
    router := ws.NewRouter()
    order := []string{}
    router.Use(func(ctx *ws.Context) { order = append(order, "middleware") })
    router.Handle("/echo/*path", func(ctx *ws.Context) {
        order = append(order, "handler")
        if ctx.PathParam() != "/42" { t.Fatalf("path param=%q", ctx.PathParam()) }
        ctx.SetResponse(map[string]any{"ok": true})
    })
    ctx := &ws.Context{Context: context.Background(), Path: "/echo/42"}
    router.Dispatch(ctx)
    if len(order) != 2 || order[0] != "middleware" || !ctx.HasResponse() { t.Fatalf("order=%v response=%t", order, ctx.HasResponse()) }
    router.NoRoute(func(ctx *ws.Context) { ctx.AbortWithError(ws.NewHandlerError("route_not_found", "no route")) })
    missing := &ws.Context{Context: context.Background(), Path: "/missing"}
    router.Dispatch(missing)
    if !missing.IsAborted() || ws.HandlerErrorCode(missing.Err()) != "route_not_found" { t.Fatalf("missing state=%v err=%v", missing.IsAborted(), missing.Err()) }
}
`,
8: `package tests

import (
    "testing"
    "github.com/aura-studio/lambda/ws"
    "github.com/aws/aws-lambda-go/events"
)

func TestChapter08APIGatewayAdapters(t *testing.T) {
    request := events.APIGatewayWebsocketProxyRequest{Body: ` + "`" + `{"version":1,"type":"ping"}` + "`" + `, Headers: map[string]string{"X-WS-Resume-Credential": "secret"}, QueryStringParameters: map[string]string{"sessionId": "s1"}}
    request.RequestContext.ConnectionID = "c1"
    request.RequestContext.RequestID = "r1"
    request.RequestContext.RouteKey = ws.RouteConnect
    request.RequestContext.ConnectedAt = 1000
    connected := ws.ConnectEventFromAPIGateway(request)
    if connected.ConnectionID != "c1" || connected.SessionID != "s1" || connected.ResumeCredential != "secret" { t.Fatalf("connect=%#v", connected) }
    if connected.String() == "" || containsCredential(connected.String(), "secret") { t.Fatalf("credential leaked: %q", connected.String()) }
    request.RequestContext.RouteKey = ws.DefaultRouteKey
    message := ws.MessageEventFromAPIGateway(request)
    if message.Body != request.Body || message.ConnectionID != "c1" { t.Fatalf("message=%#v", message) }
}

func containsCredential(text, credential string) bool {
    for i := 0; i+len(credential) <= len(text); i++ { if text[i:i+len(credential)] == credential { return true } }
    return false
}
`,
9: `package tests

import (
    "context"
    "encoding/json"
    "testing"
    "time"
    "sync/atomic"
    "github.com/aura-studio/lambda/ws"
    wsstore "github.com/aura-studio/lambda/ws/store"
    memorystore "github.com/aura-studio/lambda/ws/store/memory"
)

type chapter09Clock struct{ now time.Time }
func (c *chapter09Clock) Now() time.Time { return c.now }
type chapter09IDs struct{ n atomic.Int64 }
func (i *chapter09IDs) ID(prefix string) string { n := i.n.Add(1); return prefix + "-" + time.Unix(n, 0).Format("150405") }

func newChapter09Engine(t *testing.T, configure func(*ws.Options)) (*ws.Engine, *ws.FakePusher, wsstore.StateStore) {
    t.Helper()
    now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
    store := memorystore.New(memorystore.WithClock(func() time.Time { return now }))
    pusher := ws.NewFakePusher()
    options := ws.NewOptions(ws.WithDevelopment(true), ws.WithAuthenticator(ws.NewAnonymousAuthenticator()), ws.WithStateStore(store), ws.WithPusher(pusher), ws.WithClock(&chapter09Clock{now: now}), ws.WithIDGenerator(&chapter09IDs{}), ws.WithRetrySleeper(func(context.Context, time.Duration) error { return nil }), ws.OptionFunc(func(o *ws.Options) { o.Auth.Anonymous = true; o.Push.MaxRetries = 0 }))
    if configure != nil { configure(options) }
    engine, err := ws.NewEngine(options)
    if err != nil { t.Fatal(err) }
    return engine, pusher, store
}

func chapter09Seed(t *testing.T, engine *ws.Engine, store wsstore.StateStore, connectionID, sessionID string) {
    t.Helper()
    now := engine.Clock.Now()
    if err := store.CreateSession(context.Background(), wsstore.Session{SessionID: sessionID, PrincipalSubject: "anonymous", CurrentConnectionID: connectionID, Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    if err := store.PutConnection(context.Background(), wsstore.Connection{ConnectionID: connectionID, SessionID: sessionID, PrincipalSubject: "anonymous", Generation: 1, LastSeenAt: now, ExpiresAt: now.Add(time.Hour), State: wsstore.ConnectionStateActive}); err != nil { t.Fatal(err) }
}

func TestChapter09SameConnectionReply(t *testing.T) {
    engine, pusher, store := newChapter09Engine(t, nil)
    chapter09Seed(t, engine, store, "connection-A", "session-A")
    engine.Handle("/echo", func(ctx *ws.Context) {
        if err := engine.Reply(ctx, map[string]string{"text": "hello"}); err != nil { t.Fatal(err) }
    })
    body, _ := json.Marshal(ws.Envelope{Version: 1, Type: ws.TypeRequest, Path: "/echo", RequestID: "r1", MessageID: "m1"})
    if err := engine.HandleMessage(context.Background(), ws.MessageEvent{ConnectionID: "connection-A", RouteKey: ws.DefaultRouteKey, Body: string(body)}); err != nil { t.Fatal(err) }
    calls := pusher.Calls()
    if len(calls) != 1 || calls[0].ConnectionID != "connection-A" { t.Fatalf("calls=%#v", calls) }
}
`,
10: `package tests

import (
    "context"
    "errors"
    "testing"
    "github.com/aura-studio/lambda/ws"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter10AuthenticationAndAuthorization(t *testing.T) {
    if err := ws.ValidateProductionAuth(nil); !errors.Is(err, ws.ErrAuthenticatorRequired) { t.Fatalf("production auth err=%v", err) }
    denied := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) { return ws.Principal{}, errors.New("private token detail") })
    engine, _, store := newChapter09Engine(t, func(o *ws.Options) { o.Authenticator = denied; o.Auth.Anonymous = false })
    if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "denied-connection"}); !errors.Is(err, ws.ErrAuthenticationFailed) { t.Fatalf("connect err=%v", err) }
    if _, err := store.GetConnection(context.Background(), "denied-connection"); !wsstore.IsNotFound(err) { t.Fatalf("failed auth created state: %v", err) }
    ctx := &ws.Context{Context: context.Background(), Path: "/private", Principal: ws.Principal{Subject: "alice"}}
    ws.RequireAuthorization(ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error { return errors.New("denied") }))(ctx)
    if !ctx.IsAborted() || ws.HandlerErrorCode(ctx.Err()) != "authorization_denied" { t.Fatalf("authorization state=%v err=%v", ctx.IsAborted(), ctx.Err()) }
}
`,
11: `package tests

import (
    "context"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter11HeartbeatAndMaintenance(t *testing.T) {
    engine, _, store := newChapter09Engine(t, nil)
    now := engine.Clock.Now()
    chapter09Seed(t, engine, store, "stale", "stale-session")
    stale, _ := store.GetConnection(context.Background(), "stale")
    stale.LastSeenAt = now.Add(-2 * engine.HeartbeatTimeout())
    if err := store.PutConnection(context.Background(), stale); err != nil { t.Fatal(err) }
    result, err := engine.RunMaintenanceWithResult(context.Background(), now)
    if err != nil { t.Fatal(err) }
    if result.StaleCleaned != 1 { t.Fatalf("maintenance=%#v", result) }
    if _, err := store.GetConnection(context.Background(), "stale"); !wsstore.IsNotFound(err) { t.Fatalf("stale connection remains: %v", err) }
    if engine.HeartbeatInterval() <= 0 || engine.HeartbeatTimeout() <= engine.HeartbeatInterval() { t.Fatal("invalid heartbeat configuration") }
    _ = time.Second
}
`,
12: `package tests

import (
    "context"
    "testing"
    "github.com/aura-studio/lambda/ws"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter12ReconnectAndStaleDisconnect(t *testing.T) {
    engine, _, store := newChapter09Engine(t, nil)
    first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c1"})
    if err != nil || first.ResumeCredential == "" { t.Fatalf("first=%#v err=%v", first, err) }
    if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil { t.Fatal(err) }
    second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c2", SessionID: first.SessionID, ResumeCredential: first.ResumeCredential})
    if err != nil { t.Fatal(err) }
    if !second.Reconnected || second.SessionID != first.SessionID || second.Generation <= first.Generation { t.Fatalf("second=%#v first=%#v", second, first) }
    if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil { t.Fatal(err) }
    if _, err := store.GetConnection(context.Background(), "c2"); err != nil { t.Fatalf("stale disconnect removed c2: %v", err) }
    if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c3", SessionID: first.SessionID, ResumeCredential: "wrong"}); err == nil { t.Fatal("wrong credential accepted") }
    _ = wsstore.ConnectionStateActive
}
`,
13: `package tests

import (
    "context"
    "testing"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter13OfflineReplay(t *testing.T) {
    engine, pusher, store := newChapter09Engine(t, func(o *ws.Options) { o.Offline.AckPolicy = ws.AckPolicyPostSuccess })
    connected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c1"})
    if err != nil { t.Fatal(err) }
    if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "offline-c1", SessionID: connected.SessionID, Generation: connected.Generation}); err != nil { t.Fatal(err) }
    if err := engine.PushToSession(context.Background(), connected.SessionID, ws.OutboundMessage{Type: ws.TypeEvent, Path: "/notice", Payload: []byte(` + "`" + `{"n":1}` + "`" + `)}); err != nil { t.Fatal(err) }
    pending, err := store.ListPending(context.Background(), connected.SessionID, 10)
    if err != nil || len(pending) != 1 { t.Fatalf("pending=%#v err=%v", pending, err) }
    reconnected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c2", SessionID: connected.SessionID, ResumeCredential: connected.ResumeCredential})
    if err != nil { t.Fatal(err) }
    if reconnected.Replay.Sent != 1 || pusher.CallCount() != 1 { t.Fatalf("replay=%#v calls=%d", reconnected.Replay, pusher.CallCount()) }
    pending, err = store.ListPending(context.Background(), connected.SessionID, 10)
    if err != nil || len(pending) != 0 { t.Fatalf("postSuccess queue remains=%#v err=%v", pending, err) }
}
`,
14: `package tests

import (
    "context"
    "testing"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter14RoomBroadcast(t *testing.T) {
    engine, pusher, _ := newChapter09Engine(t, nil)
    first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c1"})
    if err != nil { t.Fatal(err) }
    second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c2"})
    if err != nil { t.Fatal(err) }
    for _, item := range []ws.ConnectResult{first, second} {
        ctx := &ws.Context{Context: context.Background(), ConnectionID: item.ConnectionID, SessionID: item.SessionID, Principal: ws.Principal{Subject: "anonymous"}}
        if err := engine.JoinRoom(ctx, "lobby"); err != nil { t.Fatal(err) }
    }
    result := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message", Payload: []byte(` + "`" + `{"text":"hello"}` + "`" + `)}, ws.BroadcastOptions{})
    if result.Err != nil || result.Succeeded != 2 || pusher.CallCount() != 2 { t.Fatalf("broadcast=%#v calls=%d", result, pusher.CallCount()) }
    excluded := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message"}, ws.BroadcastOptions{ExcludeSender: true, SenderSessionID: first.SessionID})
    if excluded.Err != nil || excluded.Succeeded != 1 { t.Fatalf("excluded=%#v", excluded) }
}
`,
15: `package tests

import (
    "context"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
    dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
    "github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
)

func chapter15Store(t *testing.T) (*dynamodbstore.Store, time.Time) {
    t.Helper()
    now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
    return dynamodbstore.New(dynamodbtest.New(), "tutorial", dynamodbstore.WithClock(func() time.Time { return now })), now
}

func TestChapter15DynamoDBConnectionSession(t *testing.T) {
    ctx := context.Background()
    store, now := chapter15Store(t)
    if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-s1", CurrentConnectionID: "ddb-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "ddb-c1", SessionID: "ddb-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil { t.Fatal(err) }
    bound, err := store.BindConnection(ctx, "ddb-s1", "ddb-c2", 1)
    if err != nil || bound.Generation != 2 { t.Fatalf("bind=%#v err=%v", bound, err) }
    if err := store.UnbindConnectionIfMatch(ctx, "ddb-s1", "ddb-c1", 1); !wsstore.IsConditionConflict(err) { t.Fatalf("stale unbind=%v", err) }
}
`,
16: `package tests

import (
    "context"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter16DynamoDBRoomOfflineIdempotency(t *testing.T) {
    ctx := context.Background()
    store, now := chapter15Store(t)
    if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-feature", CurrentConnectionID: "ddb-feature-c", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    if err := store.Join(ctx, wsstore.RoomMember{RoomID: "r", SessionID: "ddb-feature", ConnectionID: "ddb-feature-c", Generation: 1}); err != nil { t.Fatal(err) }
    if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "ddb-feature", MessageID: "m", Generation: 1, Envelope: []byte(` + "`" + `{"ok":true}` + "`" + `), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    pending, err := store.ListPending(ctx, "ddb-feature", 10)
    if err != nil || len(pending) != 1 { t.Fatalf("pending=%#v err=%v", pending, err) }
    _, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "ddb-feature", MessageID: "i", ExpiresAt: now.Add(time.Minute)})
    if err != nil || !acquired { t.Fatalf("acquired=%t err=%v", acquired, err) }
    members, _, err := store.Members(ctx, "r", 10, "")
    if err != nil || len(members) != 1 { t.Fatalf("members=%#v err=%v", members, err) }
}
`,
17: `package tests

import (
    "context"
    "testing"
    "time"
    "github.com/alicebob/miniredis/v2"
    wsstore "github.com/aura-studio/lambda/ws/store"
    redisstore "github.com/aura-studio/lambda/ws/store/redis"
    goredis "github.com/redis/go-redis/v9"
)

func chapter17Store(t *testing.T) (*redisstore.Store, time.Time) {
    t.Helper()
    server := miniredis.RunT(t)
    client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
    t.Cleanup(func() { _ = client.Close() })
    now := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
    return redisstore.New(client, redisstore.WithClock(func() time.Time { return now })), now
}

func TestChapter17RedisConnectionSession(t *testing.T) {
    ctx := context.Background()
    store, now := chapter17Store(t)
    if err := store.CreateSession(ctx, wsstore.Session{SessionID: "redis-s1", CurrentConnectionID: "redis-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "redis-c1", SessionID: "redis-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil { t.Fatal(err) }
    bound, err := store.BindConnection(ctx, "redis-s1", "redis-c2", 1)
    if err != nil || bound.Generation != 2 { t.Fatalf("bind=%#v err=%v", bound, err) }
    if err := store.UnbindConnectionIfMatch(ctx, "redis-s1", "redis-c1", 1); !wsstore.IsConditionConflict(err) { t.Fatalf("stale unbind=%v", err) }
}
`,
18: `package tests

import (
    "context"
    "testing"
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)

func TestChapter18RedisRoomOfflineIdempotency(t *testing.T) {
    ctx := context.Background()
    store, now := chapter17Store(t)
    if err := store.CreateSession(ctx, wsstore.Session{SessionID: "redis-feature", CurrentConnectionID: "redis-feature-c", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    if err := store.Join(ctx, wsstore.RoomMember{RoomID: "r", SessionID: "redis-feature", ConnectionID: "redis-feature-c", Generation: 1}); err != nil { t.Fatal(err) }
    if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "redis-feature", MessageID: "m", Generation: 1, Envelope: []byte(` + "`" + `{"ok":true}` + "`" + `), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil { t.Fatal(err) }
    pending, err := store.ListPending(ctx, "redis-feature", 10)
    if err != nil || len(pending) != 1 { t.Fatalf("pending=%#v err=%v", pending, err) }
    _, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "redis-feature", MessageID: "i", ExpiresAt: now.Add(time.Minute)})
    if err != nil || !acquired { t.Fatalf("acquired=%t err=%v", acquired, err) }
    members, _, err := store.Members(ctx, "r", 10, "")
    if err != nil || len(members) != 1 { t.Fatalf("members=%#v err=%v", members, err) }
}
`,
19: `package tests

import (
    "context"
    "testing"
    "github.com/aura-studio/lambda/server"
    "github.com/aura-studio/lambda/ws"
)

func TestChapter19ServerOptionsFactoryAndObservability(t *testing.T) {
    options := &server.Options{}
    server.WithLambdaType("ws").Apply(options)
    server.WithWsOptions(ws.WithDevelopment(true)).Apply(options)
    if options.Lambda != "ws" || len(options.Ws) != 1 { t.Fatalf("server options=%#v", options) }
    engine, _, _ := newChapter09Engine(t, nil)
    events := []ws.LogEvent{}
    engine.SetLogger(ws.LoggerFunc(func(_ context.Context, event ws.LogEvent) { events = append(events, event) }))
    if engine.StateStore() == nil { t.Fatal("factory engine has no state store") }
}
`,
};

export function chapterTest(number) {
  const text = tests[number];
  if (!text) throw new Error(`missing chapter test ${number}`);
  return text;
}

export function chapterReadme(chapter) {
  const number = String(chapter.number).padStart(2, '0');
  const lines = [
    `# 第 ${number} 节：${chapter.title}`, '',
    '这是本教程的累计完整代码快照，不需要复制其他章节的文件。', '',
    '## 环境与运行', '',
    '- Go 1.24.1（工具链固定为本地版本）。',
    '- 首次构建执行 `go mod download` 获取模块依赖。',
    `- 验证：\`${chapter.testCommand}\``,
  ];
  if (chapter.race) lines.push('- 并发验证：`go test -mod=readonly -race -count=1 ./...`');
  if (chapter.number === 20) lines.push('- 静态检查：`go vet -mod=readonly ./...`', '- 关键并发场景：对对应测试追加 `-run` 与 `-count=20`。');
  lines.push('', '## 本阶段阅读方式', '', `先阅读同编号的 ${number}-${chapter.slug}.pdf，再查看 \`ws/\` 与 \`tests/\`。PDF 中的代码摘录来自本 ZIP；后续章节可能重构本阶段的临时衔接实现。`, '', '## 验证边界', '', '所有测试均在本地执行。DynamoDB 使用进程内 `dynamodbtest.Fake`，Redis 使用 miniredis，API Gateway 使用 SDK fake/FakePusher；它们不等同于真实 AWS、DynamoDB Local、Redis 或 API Gateway 网络验证。', '');
  return lines.join('\n');
}
