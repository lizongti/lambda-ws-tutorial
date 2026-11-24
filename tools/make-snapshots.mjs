import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { root, chapters, chapterKey, walk, writeFile } from './common.mjs';
import { chapterTest, chapterReadme } from './chapter-tests.mjs';

const source = path.join(root, 'source');
const only = process.argv.includes('--only') ? Number(process.argv[process.argv.indexOf('--only') + 1]) : null;

const runtimeDependencies = `package ws

import (
    "context"
    "errors"
    "fmt"
    "net/url"
    "strings"
    "time"
)

const (
    DefaultPushInitialBackoff = 50 * time.Millisecond
    DefaultPushMaxBackoff = time.Second
)

type Pusher interface { PostToConnection(context.Context, string, []byte) error }
type SleepFunc func(context.Context, time.Duration) error
func sleepWithContext(ctx context.Context, delay time.Duration) error {
    timer := time.NewTimer(delay)
    defer timer.Stop()
    select { case <-timer.C: return nil; case <-ctx.Done(): return ctx.Err() }
}

func BuildAPIGatewayManagementEndpoint(apiID, region, stage string) (string, error) {
    if apiID == "" || region == "" || stage == "" || strings.ContainsAny(apiID + region + stage, "/ ?#") {
        return "", errors.New("invalid API Gateway endpoint components")
    }
    domain := "amazonaws.com"
    if strings.HasPrefix(region, "cn-") { domain = "amazonaws.com.cn" }
    return fmt.Sprintf("https://%s.execute-api.%s.%s/%s", apiID, region, domain, stage), nil
}

func ValidateAPIGatewayManagementEndpoint(endpoint string) (string, error) {
    u, err := url.Parse(endpoint)
    if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" || u.Path == "/" {
        return "", errors.New("invalid API Gateway management endpoint")
    }
    return strings.TrimSuffix(endpoint, "/"), nil
}

` + `package ws

import (
    "context"
    "errors"
    "fmt"
)

const (
    ActionPathAccess = "path.access"
    ActionRoomJoin = "room.join"
    ActionRoomLeave = "room.leave"
    ActionRoomBroadcast = "room.broadcast"
    ActionSessionResume = "session.resume"
    AnonymousSubject = "anonymous"
)

var (
    ErrAuthenticatorRequired = errors.New("ws: authenticator required")
    ErrAnonymousNotAllowed = errors.New("ws: anonymous authentication is development-only")
    ErrAnonymousNotExplicit = errors.New("ws: anonymous authentication must be explicitly configured")
)

type Principal struct { Subject string; Claims map[string]string }
func (p Principal) Clone() Principal {
    clone := Principal{Subject: p.Subject}
    if p.Claims != nil { clone.Claims = map[string]string{}; for k, v := range p.Claims { clone.Claims[k] = v } }
    return clone
}
func (p Principal) String() string { return fmt.Sprintf("subject=%q", p.Subject) }
func (p Principal) GoString() string { return p.String() }
type ConnectRequest struct {
    ConnectionID string
    RequestID string
    Headers map[string]string
    QueryParameters map[string]string
    MultiValueHeaders map[string][]string
    MultiValueQueryStringParameters map[string][]string
    RequestContext map[string]any
}
type Action struct { Name, Resource, Path, RoomID, SessionID string }
type Authenticator interface { Authenticate(context.Context, ConnectRequest) (Principal, error) }
type AuthenticatorFunc func(context.Context, ConnectRequest) (Principal, error)
func (f AuthenticatorFunc) Authenticate(ctx context.Context, req ConnectRequest) (Principal, error) { return f(ctx, req) }
type Authorizer interface { Authorize(context.Context, Principal, Action) error }
type AuthorizerFunc func(context.Context, Principal, Action) error
func (f AuthorizerFunc) Authorize(ctx context.Context, p Principal, a Action) error { return f(ctx, p, a) }
type AnonymousAuthenticator struct{}
func NewAnonymousAuthenticator() *AnonymousAuthenticator { return &AnonymousAuthenticator{} }
func (*AnonymousAuthenticator) Authenticate(context.Context, ConnectRequest) (Principal, error) { return Principal{Subject: AnonymousSubject}, nil }
func (*AnonymousAuthenticator) developmentAnonymous() {}
type AuthConfig struct { Development, Required, Protected, Anonymous bool }
func ValidateAuthConfig(config AuthConfig, authenticator Authenticator) error {
    _, anonymous := authenticator.(interface{ developmentAnonymous() })
    if !config.Development && (config.Anonymous || anonymous) { return ErrAnonymousNotAllowed }
    if config.Anonymous != anonymous { return ErrAnonymousNotExplicit }
    if !config.Development && authenticator == nil { return ErrAuthenticatorRequired }
    if (config.Required || config.Protected) && authenticator == nil { return ErrAuthenticatorRequired }
    return nil
}
func ValidateProductionAuth(authenticator Authenticator) error { return ValidateAuthConfig(AuthConfig{}, authenticator) }
`;

const authOnly = runtimeDependencies.substring(runtimeDependencies.indexOf('package ws', 10));
const pusherOnly = runtimeDependencies.substring(0, runtimeDependencies.indexOf('package ws', 10));
const stage02Store = `package store

const MaxBatchSize = 1000

type OfflineOverflowPolicy string
const (
    OfflineOverflowRejectNew OfflineOverflowPolicy = "reject-new"
    OfflineOverflowEvictOldest OfflineOverflowPolicy = "evict-oldest"
)
type StateStore interface{}
`;

const stage08Engine = `package ws

import (
    "context"
    "errors"
)

var (
    ErrAuthenticationFailed = errors.New("ws: authentication failed")
    ErrAuthorizationFailed = errors.New("ws: authorization failed")
    ErrInvalidEvent = errors.New("ws: invalid event")
    ErrConnectionNotFound = errors.New("ws: connection not found")
    ErrSessionNotFound = errors.New("ws: session not found")
    ErrStaleConnection = errors.New("ws: stale connection")
    ErrInvalidResume = errors.New("ws: invalid resume credential")
    ErrReconnectDisabled = errors.New("ws: reconnect is disabled")
    ErrConnectionConflict = errors.New("ws: connection binding conflict")
)

type ReplayResult struct { Attempted, Sent, Acknowledged, Retained, Failed, Batches int; Truncated bool }
type Engine struct{}
func (*Engine) HandleConnect(_ context.Context, event ConnectEvent) (ConnectResult, error) {
    return ConnectResult{ConnectionID: event.ConnectionID}, nil
}
func (*Engine) HandleDisconnect(context.Context, DisconnectEvent) error { return nil }
func (*Engine) HandleMessage(context.Context, MessageEvent) error { return nil }
`;

const stage08Factory = `package ws

import "io"

type Factory struct{ options *Options }
func NewFactory(options *Options) *Factory { return &Factory{options: options} }
func (f *Factory) Build() (*Engine, error) { return &Engine{}, nil }
func (f *Factory) Close() error { return nil }
var _ io.Closer = (*Factory)(nil)
`;

const stageClones = `package ws

func cloneStringMap(source map[string]string) map[string]string {
    if source == nil { return nil }
    clone := make(map[string]string, len(source))
    for key, value := range source { clone[key] = value }
    return clone
}
func cloneStringSliceMap(source map[string][]string) map[string][]string {
    if source == nil { return nil }
    clone := make(map[string][]string, len(source))
    for key, values := range source { clone[key] = append([]string(nil), values...) }
    return clone
}
func cloneAnyMap(source map[string]any) map[string]any {
    if source == nil { return nil }
    clone := make(map[string]any, len(source))
    for key, value := range source { clone[key] = value }
    return clone
}
`;

const stage09Factory = `package ws

type Factory struct{ options *Options }
func NewFactory(options *Options) *Factory { return &Factory{options: options} }
func (f *Factory) Build() (*Engine, error) { return NewEngine(f.options) }
func (f *Factory) Close() error { return nil }
`;

function coreSupport(number) {
  const parts = [`package ws

import (
    "context"
    ${number < 12 ? '"crypto/sha256"\n    "fmt"' : ''}
    "time"
    wsstore "github.com/aura-studio/lambda/ws/store"
)
`];
  if (number < 13) parts.push(`type replaySentMarker interface {
    MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error
}
`);
  if (number < 11) parts.push(`type MaintenanceResult struct {
    StaleScanned, StaleCleaned, ExpiredScanned, ExpiredCleaned int
    Skipped, Conflicts, Failed int
    ExpiredSessionBatches, ExpiredOfflineBatches, ExpiredIdempotencyBatches int
}
`);
  if (number < 12) parts.push(`func (e *Engine) HandleConnect(ctx context.Context, event ConnectEvent) (ConnectResult, error) {
    if existing, err := e.Options.StateStore.GetConnection(ctx, event.ConnectionID); err == nil {
        return ConnectResult{ConnectionID: existing.ConnectionID, SessionID: existing.SessionID, Generation: existing.Generation}, nil
    }
    authenticator := e.Options.Authenticator
    if authenticator == nil { authenticator = NewAnonymousAuthenticator() }
    principal, err := authenticator.Authenticate(ctx, ConnectRequest{ConnectionID: event.ConnectionID, RequestID: event.RequestID})
    if err != nil { return ConnectResult{}, ErrAuthenticationFailed }
    now := e.Options.Clock.Now()
    sessionID := e.Options.IDGenerator.ID("session")
    credential := e.Options.IDGenerator.ID("resume")
    generation := uint64(1)
    session := wsstore.Session{SessionID: sessionID, PrincipalSubject: principal.Subject, CurrentConnectionID: event.ConnectionID, Generation: generation, ResumeCredentialHash: fmt.Sprintf("%x", sha256.Sum256([]byte(sessionID + credential))), LastActiveAt: now, ExpiresAt: now.Add(e.Options.Reconnect.SessionRetention)}
    if err := e.Options.StateStore.CreateSession(ctx, session); err != nil { return ConnectResult{}, err }
    connection := wsstore.Connection{ConnectionID: event.ConnectionID, SessionID: sessionID, PrincipalSubject: principal.Subject, Generation: generation, ConnectedAt: now, LastSeenAt: now, ExpiresAt: now.Add(e.Options.Reconnect.SessionRetention), State: wsstore.ConnectionStateActive}
    if err := e.Options.StateStore.PutConnection(ctx, connection); err != nil { return ConnectResult{}, err }
    return ConnectResult{ConnectionID: event.ConnectionID, SessionID: sessionID, Generation: generation, ResumeCredential: credential}, nil
}
`);
  if (number < 11) parts.push(`func (e *Engine) HandleDisconnect(ctx context.Context, event DisconnectEvent) error {
    connection, err := e.Options.StateStore.GetConnection(ctx, event.ConnectionID)
    if err != nil { return nil }
    return e.cleanupConnection(ctx, connection, "disconnect")
}
func (e *Engine) cleanupConnection(ctx context.Context, connection wsstore.Connection, reason string) error {
    if err := e.Options.StateStore.DeleteConnectionIfMatch(ctx, connection.ConnectionID, connection.Generation); err != nil && !wsstore.IsNotFound(err) { return err }
    if err := e.Options.StateStore.UnbindConnectionIfMatch(ctx, connection.SessionID, connection.ConnectionID, connection.Generation); err != nil && !wsstore.IsNotFound(err) { return err }
    return e.Options.StateStore.RemoveConnection(ctx, connection.ConnectionID)
}
`);
  if (number < 13) parts.push(`type ReplayResult struct { Attempted, Sent, Acknowledged, Retained, Failed, Batches int; Truncated bool }
func (e *Engine) replaySession(context.Context, string, string, uint64) (ReplayResult, error) { return ReplayResult{}, nil }
func (e *Engine) acknowledge(ctx context.Context, connection wsstore.Connection, envelope Envelope) error {
    return e.Options.StateStore.Ack(ctx, connection.SessionID, envelope.MessageID, connection.Generation)
}
func (e *Engine) enqueueOffline(ctx context.Context, sessionID string, msg OutboundMessage, body []byte) error {
    now := time.Now()
    if e.Options.Clock != nil { now = e.Options.Clock.Now() }
    return e.Options.StateStore.Append(ctx, wsstore.OfflineMessage{SessionID: sessionID, MessageID: msg.MessageID, Generation: 1, Envelope: append([]byte(nil), body...), CreatedAt: now, ExpiresAt: now.Add(e.Options.Offline.Retention), State: wsstore.OfflineMessageStatePending})
}
`);
  return parts.join('\n');
}

const chapterFiles = {
  1: ['ws/protocol.go'],
  2: ['ws/options.go', 'ws/default_config.go', 'ws/config.go'],
  3: ['ws/store/store.go'],
  4: ['ws/store/memory/store.go', 'ws/store/memory/connection_session.go'],
  5: ['ws/store/memory/store.go', 'ws/store/memory/idempotency.go', 'ws/store/memory/offline.go', 'ws/store/memory/room.go'],
  6: ['ws/pusher.go', 'ws/apigateway_pusher.go'],
  7: ['ws/context.go', 'ws/router.go'],
  8: ['ws/events.go', 'ws/server.go'],
  9: ['ws/engine.go', 'ws/retry_store.go', 'ws/observability.go'],
  10: ['ws/auth.go'],
  11: ['ws/heartbeat.go', 'ws/maintenance.go'],
  12: ['ws/session.go'],
  13: ['ws/offline.go'],
  14: ['ws/room.go'],
  15: ['ws/store/dynamodb/store.go', 'ws/store/dynamodb/codec.go', 'ws/store/dynamodb/connection_session.go', 'ws/store/dynamodb/dynamodbtest/fake.go'],
  16: ['ws/store/dynamodb/store.go', 'ws/store/dynamodb/idempotency.go', 'ws/store/dynamodb/offline.go', 'ws/store/dynamodb/room.go'],
  17: ['ws/store/redis/store.go', 'ws/store/redis/connection_session.go'],
  18: ['ws/store/redis/store.go', 'ws/store/redis/idempotency.go', 'ws/store/redis/offline.go', 'ws/store/redis/room.go'],
  19: ['ws/factory.go'],
  20: [],
};

const localPackages = ['dynamic', 'event', 'http', 'reqresp', 'server', 'sqs'];
const testNames = {
  1: 'ws_protocol_test.go', 2: 'ws_config_test.go', 3: 'ws_store_contract_test.go',
  4: 'ws_memory_connection_test.go', 5: 'ws_memory_features_test.go', 6: 'ws_pusher_test.go',
  7: 'ws_router_test.go', 8: 'ws_lifecycle_test.go', 9: 'ws_engine_test.go', 10: 'ws_auth_test.go',
  11: 'ws_maintenance_test.go', 12: 'ws_reconnect_test.go', 13: 'ws_offline_test.go',
  14: 'ws_room_test.go', 15: 'ws_dynamodb_connection_test.go', 16: 'ws_dynamodb_features_test.go',
  17: 'ws_redis_connection_test.go', 18: 'ws_redis_features_test.go', 19: 'ws_server_test.go',
};

function copySource(relative, target, transform) {
  const from = path.join(source, relative);
  if (fs.statSync(from).isDirectory()) {
    for (const file of walk(from)) copySource(`${relative}/${file}`, target);
    return;
  }
  let contents = fs.readFileSync(from);
  if (transform) contents = transform(contents.toString('utf8'));
  writeFile(path.join(target, relative), contents);
}

for (const chapter of chapters) {
  if (only && chapter.number !== only) continue;
  const key = chapterKey(chapter);
  const target = path.join(root, 'work', 'snapshots', key);
  fs.rmSync(target, { recursive: true, force: true });
  fs.mkdirSync(target, { recursive: true });
  for (const file of ['go.mod', 'go.sum']) copySource(file, target);

  if (chapter.fullSourceCopy) {
    for (const directory of ['ws', ...localPackages]) copySource(directory, target);
    for (const file of fs.readdirSync(path.join(source, 'tests'))) copySource(`tests/${file}`, target);
    writeFile(path.join(target, 'README.md'), chapterReadme(chapter));
    console.log(`materialized ${key} from pristine source`);
    continue;
  }

  for (let number = 1; number <= chapter.number; number++) {
    for (const relative of chapterFiles[number]) {
      const stripStateStore = ((number === 4 && chapter.number === 4) || (number === 15 && chapter.number === 15) || (number === 17 && chapter.number === 17)) && relative.endsWith('/store.go');
      copySource(relative, target, stripStateStore ? text => text.replace('var _ wsstore.StateStore = (*Store)(nil)\n', '') : undefined);
    }
  }

  if (chapter.number >= 2 && chapter.number < 6) {
    writeFile(path.join(target, 'ws/stage_runtime_dependencies.go'), pusherOnly);
    writeFile(path.join(target, 'ws/stage_auth_dependencies.go'), authOnly);
  }
  if (chapter.number === 2) writeFile(path.join(target, 'ws/store/store.go'), stage02Store);
  if (chapter.number >= 6 && chapter.number < 10) writeFile(path.join(target, 'ws/stage_auth_dependencies.go'), authOnly);
  if (chapter.number >= 8 && chapter.number < 12) writeFile(path.join(target, 'ws/stage_clones.go'), stageClones);
  if (chapter.number === 8) {
    writeFile(path.join(target, 'ws/stage08_engine.go'), stage08Engine);
    writeFile(path.join(target, 'ws/stage08_factory.go'), stage08Factory);
  }
  if (chapter.number >= 9 && chapter.number < 13) writeFile(path.join(target, 'ws/stage_core_support.go'), coreSupport(chapter.number));
  if (chapter.number >= 9 && chapter.number < 19) writeFile(path.join(target, 'ws/stage_factory.go'), stage09Factory);

  if (chapter.number >= 8) copySource('dynamic', target);
  if (chapter.number >= 19) for (const directory of localPackages) copySource(directory, target);
  for (let number = 1; number <= chapter.number && number <= 19; number++) {
    writeFile(path.join(target, 'tests', testNames[number]), chapterTest(number));
  }
  writeFile(path.join(target, 'README.md'), chapterReadme(chapter));
  const goFiles = walk(target).filter(file => file.endsWith('.go')).map(file => path.join(target, file));
  const formatted = spawnSync('gofmt', ['-w', ...goFiles], { encoding: 'utf8' });
  if (formatted.status !== 0) throw new Error(`gofmt ${key}: ${formatted.stderr}`);
  console.log(`materialized ${key}`);
}
