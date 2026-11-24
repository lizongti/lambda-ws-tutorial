package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

func TestWSConfigDefaults(t *testing.T) {
	options := ws.NewOptions()
	if options.State.Backend != ws.BackendMemory || options.SharedDeployment {
		t.Fatalf("unexpected state defaults: %v", options.State)
	}
	if options.Auth.Development || options.Auth.Required || options.Auth.Anonymous {
		t.Fatalf("authentication defaults must be production-safe: %+v", options.Auth)
	}
	if options.Heartbeat.Interval != 30*time.Second || options.Heartbeat.Timeout != 90*time.Second || options.Heartbeat.MaintenanceBatchSize != 100 {
		t.Fatalf("unexpected heartbeat defaults: %+v", options.Heartbeat)
	}
	if !options.Reconnect.Enabled || !options.Reconnect.ReplaceExisting || options.Reconnect.SessionRetention != 24*time.Hour {
		t.Fatalf("unexpected reconnect defaults: %+v", options.Reconnect)
	}
	if !options.Offline.Enabled || options.Offline.AckPolicy != ws.AckPolicyClientAck ||
		options.Offline.MaxMessagesPerSession != 1000 || options.Offline.Retention != 24*time.Hour ||
		options.Offline.Overflow != wsstore.OfflineOverflowRejectNew || options.Offline.ReplayBatchSize != 100 {
		t.Fatalf("unexpected offline defaults: %+v", options.Offline)
	}
	if !options.Rooms.Enabled || options.Rooms.MaxMembers != 10000 || options.Rooms.BroadcastBatchSize != 100 {
		t.Fatalf("unexpected room defaults: %+v", options.Rooms)
	}
	if options.Push.Concurrency != 16 || options.Push.MaxRetries != 2 ||
		options.Push.InitialBackoff != ws.DefaultPushInitialBackoff || options.Push.MaxBackoff != ws.DefaultPushMaxBackoff {
		t.Fatalf("unexpected push defaults: %+v", options.Push)
	}
	if options.Message.MaxBytes != ws.MaxMessageBytes || options.Message.IdempotencyWindow != 5*time.Minute {
		t.Fatalf("unexpected message defaults: %+v", options.Message)
	}
	if options.Clock == nil || options.IDGenerator == nil || options.RetrySleeper == nil {
		t.Fatal("default clock, ID generator, and retry sleeper must be usable")
	}
	if got := options.IDGenerator.ID("session"); !strings.HasPrefix(got, "session-") {
		t.Fatalf("default generated ID = %q", got)
	}

	development := ws.NewOptions(ws.WithDevelopment(true), ws.WithPusher(ws.NewFakePusher()))
	if err := development.Validate(); err != nil {
		t.Fatalf("development defaults should validate: %v", err)
	}
	if err := ws.Validate(development); err != nil {
		t.Fatalf("package Validate should delegate to Options.Validate: %v", err)
	}
}

func TestWSConfigYAMLOverridesEveryGroup(t *testing.T) {
	config := []byte(`state:
  backend: redis
  redisAddress: localhost:6379
  redisKeyPrefix: example:ws
  sharedDeployment: true
apiGateway:
  managementEndpoint: https://api.example.com/production
auth:
  development: true
  required: true
  protected: true
  anonymous: false
heartbeat:
  interval: 45s
  timeout: 2m
  maintenanceBatchSize: 25
reconnect:
  enabled: false
  sessionRetention: 12h
  replaceExisting: false
offline:
  enabled: false
  ackPolicy: postSuccess
  maxMessagesPerSession: 50
  retention: 6h
  overflow: evict-oldest
  replayBatchSize: 20
rooms:
  enabled: false
  maxMembers: 250
  broadcastBatchSize: 10
push:
  concurrency: 4
  maxRetries: 0
  initialBackoff: 25ms
  maxBackoff: 2s
message:
  maxBytes: 16384
  idempotencyWindow: 10m
`)
	authenticator := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{Subject: "subject"}, nil
	})
	options := ws.NewOptions(ws.WithConfig(config), ws.WithAuthenticator(authenticator))

	if options.State.Backend != ws.BackendRedis || options.State.RedisAddress != "localhost:6379" ||
		options.State.RedisKeyPrefix != "example:ws" || !options.SharedDeployment {
		t.Fatalf("state YAML was not applied: %v", options.State)
	}
	if options.APIGateway.ManagementEndpoint != "https://api.example.com/production" {
		t.Fatalf("apiGateway YAML was not applied: %v", options.APIGateway)
	}
	if !options.Auth.Development || !options.Auth.Required || !options.Auth.Protected || options.Auth.Anonymous {
		t.Fatalf("auth YAML was not applied: %+v", options.Auth)
	}
	if options.Heartbeat.Interval != 45*time.Second || options.Heartbeat.Timeout != 2*time.Minute || options.Heartbeat.MaintenanceBatchSize != 25 {
		t.Fatalf("heartbeat YAML was not applied: %+v", options.Heartbeat)
	}
	if options.Reconnect.Enabled || options.Reconnect.ReplaceExisting || options.Reconnect.SessionRetention != 12*time.Hour {
		t.Fatalf("reconnect YAML was not applied: %+v", options.Reconnect)
	}
	if options.Offline.Enabled || options.Offline.AckPolicy != ws.AckPolicyPostSuccess ||
		options.Offline.MaxMessagesPerSession != 50 || options.Offline.Retention != 6*time.Hour ||
		options.Offline.Overflow != wsstore.OfflineOverflowEvictOldest || options.Offline.ReplayBatchSize != 20 {
		t.Fatalf("offline YAML was not applied: %+v", options.Offline)
	}
	if options.Rooms.Enabled || options.Rooms.MaxMembers != 250 || options.Rooms.BroadcastBatchSize != 10 {
		t.Fatalf("rooms YAML was not applied: %+v", options.Rooms)
	}
	if options.Push.Concurrency != 4 || options.Push.MaxRetries != 0 ||
		options.Push.InitialBackoff != 25*time.Millisecond || options.Push.MaxBackoff != 2*time.Second {
		t.Fatalf("push YAML was not applied: %+v", options.Push)
	}
	if options.Message.MaxBytes != 16384 || options.Message.IdempotencyWindow != 10*time.Minute {
		t.Fatalf("message YAML was not applied: %+v", options.Message)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("complete YAML should validate: %v", err)
	}
}

func TestWSConfigFileAndOptionOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lambda.yaml")
	contents := []byte(`lambda: ws
sharedDeployment: false
ws:
  auth:
    development: true
    required: false
  reconnect:
    enabled: false
  rooms:
    enabled: false
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	options := ws.NewOptions(
		ws.WithDevelopment(false),
		ws.WithConfigFile(path),
		ws.WithConfig([]byte("reconnect:\n  enabled: true\n")),
		ws.WithPusher(ws.NewFakePusher()),
	)
	if !options.Auth.Development || options.Auth.Required {
		t.Fatalf("later YAML must preserve explicit false and override earlier options: %+v", options.Auth)
	}
	if !options.Reconnect.Enabled || options.Rooms.Enabled {
		t.Fatalf("later options must be selective: reconnect=%+v rooms=%+v", options.Reconnect, options.Rooms)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("file options should validate: %v", err)
	}
	if got := ws.DefaultConfigCandidates(); len(got) != 4 || got[0] != "ws.yaml" || got[3] != filepath.FromSlash("ws/ws.yml") {
		t.Fatalf("unexpected default config candidates: %v", got)
	}
}

func TestWSConfigBackendsAndSharedDeployment(t *testing.T) {
	development := []ws.Option{ws.WithDevelopment(true), ws.WithPusher(ws.NewFakePusher())}
	if err := ws.NewOptions(development...).Validate(); err != nil {
		t.Fatalf("single-process memory should validate: %v", err)
	}

	sharedMemory := append(development, ws.WithSharedDeployment(true))
	err := ws.NewOptions(sharedMemory...).Validate()
	if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "memory") || !strings.Contains(err.Error(), "shared") {
		t.Fatalf("shared memory error = %v", err)
	}

	dynamoDB := append(development, ws.WithStateBackend(ws.BackendDynamoDB))
	if err := ws.NewOptions(dynamoDB...).Validate(); err == nil || !strings.Contains(err.Error(), "state.tableName") {
		t.Fatalf("dynamodb missing table error = %v", err)
	}
	withTable := ws.NewOptions(append(dynamoDB, ws.WithConfig([]byte("state:\n  tableName: connections\n")))...)
	if err := withTable.Validate(); err != nil {
		t.Fatalf("complete dynamodb config should validate: %v", err)
	}

	redis := append(development, ws.WithStateBackend(ws.BackendRedis))
	if err := ws.NewOptions(redis...).Validate(); err == nil || !strings.Contains(err.Error(), "state.redisAddress") {
		t.Fatalf("redis missing address error = %v", err)
	}
	withAddress := ws.NewOptions(append(redis, ws.WithConfig([]byte("state:\n  redisAddress: localhost:6379\n")))...)
	if err := withAddress.Validate(); err != nil {
		t.Fatalf("complete redis config should validate: %v", err)
	}

	unknown := ws.NewOptions(development...)
	unknown.State.Backend = ws.Backend("other")
	if err := unknown.Validate(); !errors.Is(err, ws.ErrInvalidConfig) {
		t.Fatalf("unknown backend error = %v", err)
	}
}

func TestWSConfigProductionAuthentication(t *testing.T) {
	production := ws.NewOptions(ws.WithPusher(ws.NewFakePusher()))
	if err := production.Validate(); !errors.Is(err, ws.ErrAuthenticatorRequired) {
		t.Fatalf("production without authenticator error = %v", err)
	}

	authenticator := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{Subject: "subject"}, nil
	})
	if err := ws.NewOptions(ws.WithPusher(ws.NewFakePusher()), ws.WithAuthenticator(authenticator)).Validate(); err != nil {
		t.Fatalf("authenticated production config should validate: %v", err)
	}

	anonymous := ws.NewOptions(ws.WithPusher(ws.NewFakePusher()), ws.WithAuthenticator(ws.NewAnonymousAuthenticator()))
	if err := anonymous.Validate(); !errors.Is(err, ws.ErrAnonymousNotAllowed) {
		t.Fatalf("production anonymous error = %v", err)
	}

	developmentAnonymous := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithPusher(ws.NewFakePusher()),
		ws.WithAuthenticator(ws.NewAnonymousAuthenticator()),
		ws.WithConfig([]byte("auth:\n  anonymous: true\n")),
	)
	if err := developmentAnonymous.Validate(); err != nil {
		t.Fatalf("explicit development anonymous config should validate: %v", err)
	}
}

func TestWSConfigAPIGatewayValidation(t *testing.T) {
	base := []ws.Option{ws.WithDevelopment(true)}
	fromParts := ws.NewOptions(append(base, ws.WithConfig([]byte(`apiGateway:
  apiId: api-1
  region: us-east-1
  stage: production
`)))...)
	if err := fromParts.Validate(); err != nil {
		t.Fatalf("valid API Gateway parts should validate: %v", err)
	}
	explicit := ws.NewOptions(append(base, ws.WithConfig([]byte("apiGateway:\n  managementEndpoint: https://api.example.com/production\n")))...)
	if err := explicit.Validate(); err != nil {
		t.Fatalf("valid explicit endpoint should validate: %v", err)
	}
	partial := ws.NewOptions(append(base, ws.WithConfig([]byte("apiGateway:\n  region: us-east-1\n")))...)
	if err := partial.Validate(); !errors.Is(err, ws.ErrInvalidConfig) {
		t.Fatalf("partial API Gateway config error = %v", err)
	}
	if err := ws.NewOptions(ws.WithDevelopment(true), ws.WithPusher(ws.NewFakePusher())).Validate(); err != nil {
		t.Fatalf("injected Pusher should not require AWS endpoint settings: %v", err)
	}
}

func TestWSConfigDependencyInjection(t *testing.T) {
	stateStore := memory.New()
	pusher := ws.NewFakePusher()
	authenticator := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{Subject: "subject"}, nil
	})
	authorizer := ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error { return nil })
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := ws.ClockFunc(func() time.Time { return now })
	generator := ws.IDGeneratorFunc(func(prefix string) string { return prefix + "-fixed" })
	sleeperCalled := false
	sleeper := ws.SleepFunc(func(context.Context, time.Duration) error {
		sleeperCalled = true
		return nil
	})
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithStateStore(stateStore),
		ws.WithPusher(pusher),
		ws.WithAuthenticator(authenticator),
		ws.WithAuthorizer(authorizer),
		ws.WithClock(clock),
		ws.WithIDGenerator(generator),
		ws.WithRetrySleeper(sleeper),
	)
	if options.StateStore != stateStore || options.Pusher != pusher || options.Authenticator == nil || options.Authorizer == nil {
		t.Fatal("injected store, pusher, authenticator, or authorizer was not retained")
	}
	if !options.Clock.Now().Equal(now) || options.IDGenerator.ID("message") != "message-fixed" {
		t.Fatal("injected clock or ID generator was not used")
	}
	if err := options.RetrySleeper(context.Background(), time.Nanosecond); err != nil || !sleeperCalled {
		t.Fatalf("injected retry sleeper was not used: called=%t err=%v", sleeperCalled, err)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("injected options should validate: %v", err)
	}
}

func TestWSConfigRejectsInvalidScalars(t *testing.T) {
	tests := map[string]string{
		"heartbeat duration zero":   "heartbeat:\n  interval: 0s\n",
		"heartbeat duration syntax": "heartbeat:\n  timeout: not-a-duration\n",
		"reconnect negative":        "reconnect:\n  sessionRetention: -1s\n",
		"offline queue zero":        "offline:\n  maxMessagesPerSession: 0\n",
		"offline ack enum":          "offline:\n  ackPolicy: invalid\n",
		"offline overflow enum":     "offline:\n  overflow: invalid\n",
		"rooms negative":            "rooms:\n  maxMembers: -1\n",
		"push concurrency zero":     "push:\n  concurrency: 0\n",
		"push retries negative":     "push:\n  maxRetries: -1\n",
		"message size zero":         "message:\n  maxBytes: 0\n",
		"state backend enum":        "state:\n  backend: invalid\n",
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			panicValue := wsConfigPanic(t, ws.WithConfig([]byte(config)))
			if !strings.Contains(fmt.Sprintf("%v", panicValue), "ws") {
				t.Fatalf("panic should identify ws configuration: %v", panicValue)
			}
		})
	}
}

func TestWSConfigErrorsAndFormattingRedactSensitiveValues(t *testing.T) {
	const secret = "do-not-expose-this-secret"
	panicValue := wsConfigPanic(t, ws.WithConfig([]byte("auth:\n  token: "+secret+"\n")))
	if text := fmt.Sprintf("%v", panicValue); strings.Contains(text, secret) {
		t.Fatalf("YAML error exposed a sensitive value: %s", text)
	}

	options := ws.NewOptions(ws.WithDevelopment(true), ws.WithConfig([]byte(`apiGateway:
  managementEndpoint: https://user:password@example.com/production
`)))
	if err := options.Validate(); err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "user") {
		t.Fatalf("endpoint error exposed credentials: %v", err)
	}

	options = ws.NewOptions(ws.WithConfig([]byte("state:\n  redisAddress: redis://user:" + secret + "@localhost:6379\n")))
	for _, text := range []string{fmt.Sprintf("%v", options), fmt.Sprintf("%#v", options), fmt.Sprintf("%v", options.State)} {
		if strings.Contains(text, secret) {
			t.Fatalf("formatted options exposed a sensitive value: %s", text)
		}
	}
}

func wsConfigPanic(t *testing.T, option ws.Option) (recovered any) {
	t.Helper()
	defer func() {
		recovered = recover()
		if recovered == nil {
			t.Fatal("expected configuration Option to panic")
		}
	}()
	_ = ws.NewOptions(option)
	return nil
}
