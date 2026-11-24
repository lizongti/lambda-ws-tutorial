package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aura-studio/lambda/event"
	"github.com/aura-studio/lambda/server"
	"github.com/aura-studio/lambda/ws"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
	redisstore "github.com/aura-studio/lambda/ws/store/redis"
)

func TestWSServerConfigRoutingAndOptions(t *testing.T) {
	config := []byte(`lambda: ws
ws:
  state:
    backend: memory
  auth:
    development: true
  heartbeat:
    interval: 10s
    timeout: 30s
  push:
    concurrency: 3
`)

	options := &server.Options{}
	server.WithServeConfig(config).Apply(options)
	if options.Lambda != "ws" {
		t.Fatalf("Lambda = %q, want ws", options.Lambda)
	}
	if len(options.Ws) != 1 {
		t.Fatalf("len(Ws) = %d, want 1", len(options.Ws))
	}

	pusher := ws.NewFakePusher()
	server.WithWsOptions(
		ws.WithPusher(pusher),
		ws.WithConfig([]byte("rooms:\n  enabled: false\n")),
	).Apply(options)
	wsOptions := ws.NewOptions(options.Ws...)
	if !wsOptions.Auth.Development || wsOptions.State.Backend != ws.BackendMemory {
		t.Fatalf("WS YAML was not routed: %+v", wsOptions)
	}
	if wsOptions.Heartbeat.Interval != 10*time.Second || wsOptions.Heartbeat.Timeout != 30*time.Second {
		t.Fatalf("heartbeat config was not retained: %+v", wsOptions.Heartbeat)
	}
	if wsOptions.Push.Concurrency != 3 || wsOptions.Rooms.Enabled {
		t.Fatalf("explicit WS options were not appended in order: push=%+v rooms=%+v", wsOptions.Push, wsOptions.Rooms)
	}
	if wsOptions.Pusher != pusher {
		t.Fatal("WithWsOptions did not retain the injected pusher")
	}
	if err := wsOptions.Validate(); err != nil {
		t.Fatalf("routed WS config should validate: %v", err)
	}
}

func TestWSServerConfigRejectsInvalidWSYAML(t *testing.T) {
	options := &server.Options{}
	server.WithServeConfig([]byte("lambda: ws\nws:\n  unknownSetting: true\n")).Apply(options)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("invalid WS YAML should panic when its option is applied")
		}
		if message := fmt.Sprint(recovered); !strings.Contains(message, "ws.WithConfig") {
			t.Fatalf("panic = %q, want WS config error context", message)
		}
	}()
	_ = ws.NewOptions(options.Ws...)
}

func TestWSFactoryDefaultsToMemoryAndDoesNotMutateOptions(t *testing.T) {
	pusher := ws.NewFakePusher()
	options := ws.NewOptions(ws.WithDevelopment(true), ws.WithPusher(pusher))
	factory := ws.NewFactory(options)
	engine, err := factory.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if _, ok := engine.StateStore().(*memorystore.Store); !ok {
		t.Fatalf("default StateStore type = %T, want memory.Store", engine.StateStore())
	}
	if engine.Pusher != pusher {
		t.Fatalf("Pusher = %T, want injected fake", engine.Pusher)
	}
	if options.StateStore != nil {
		t.Fatal("Factory.Build mutated the caller's options")
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("second Close() error: %v", err)
	}
}

func TestWSFactoryExplicitDependenciesTakePrecedence(t *testing.T) {
	// If either default AWS-backed constructor were used, this malformed shared
	// config would make initialization fail. Explicit dependencies must avoid it.
	malformedAWSConfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(malformedAWSConfig, []byte("[default\ninvalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", malformedAWSConfig)

	stateStore := memorystore.New()
	pusher := ws.NewFakePusher()
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithConfig([]byte("state:\n  backend: dynamodb\n  tableName: shared-connections\n")),
		ws.WithStateStore(stateStore),
		ws.WithPusher(pusher),
	)
	factory := ws.NewFactory(options)
	engine, err := factory.Build()
	if err != nil {
		t.Fatalf("Build() with explicit dependencies error: %v", err)
	}
	if engine.StateStore() != stateStore || engine.Pusher != pusher {
		t.Fatalf("factory replaced explicit dependencies: store=%T pusher=%T", engine.StateStore(), engine.Pusher)
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestWSFactorySelectsDynamoDB(t *testing.T) {
	setWSOfflineAWSConfig(t)
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithPusher(ws.NewFakePusher()),
		ws.WithConfig([]byte("state:\n  backend: dynamodb\n  tableName: shared-connections\n  sharedDeployment: true\n")),
	)
	factory := ws.NewFactory(options)
	engine, err := factory.Build()
	if err != nil {
		t.Fatalf("DynamoDB Build() error: %v", err)
	}
	if _, ok := engine.StateStore().(*dynamodbstore.Store); !ok {
		t.Fatalf("StateStore type = %T, want dynamodb.Store", engine.StateStore())
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("DynamoDB factory Close() error: %v", err)
	}
}

func TestWSFactorySelectsRedisAndClosesClient(t *testing.T) {
	for _, test := range []struct {
		name    string
		address string
	}{
		{name: "URL", address: "redis://:test-password@127.0.0.1:6379/2"},
		{name: "HostPort", address: "127.0.0.1:6379"},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := ws.NewOptions(
				ws.WithDevelopment(true),
				ws.WithPusher(ws.NewFakePusher()),
				ws.WithConfig([]byte(fmt.Sprintf("state:\n  backend: redis\n  redisAddress: %q\n  redisKeyPrefix: %q\n  sharedDeployment: true\n", test.address, "integration:ws:"))),
			)
			factory := ws.NewFactory(options)
			engine, err := factory.Build()
			if err != nil {
				t.Fatalf("Redis Build() error: %v", err)
			}
			if _, ok := engine.StateStore().(*redisstore.Store); !ok {
				t.Fatalf("StateStore type = %T, want redis.Store", engine.StateStore())
			}
			// Construction and Close do not issue Redis commands or Ping.
			if err := factory.Close(); err != nil {
				t.Fatalf("Redis factory Close() error: %v", err)
			}
			if err := factory.Close(); err != nil {
				t.Fatalf("second Redis factory Close() error: %v", err)
			}
		})
	}
}

func TestWSFactoryBuildsAPIGatewayPusher(t *testing.T) {
	setWSOfflineAWSConfig(t)
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithConfig([]byte(`apiGateway:
  apiId: api-1
  region: us-east-1
  stage: integration
push:
  maxRetries: 0
`)),
	)
	factory := ws.NewFactory(options)
	engine, err := factory.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	pusher, ok := engine.Pusher.(*ws.APIGatewayPusher)
	if !ok {
		t.Fatalf("Pusher type = %T, want APIGatewayPusher", engine.Pusher)
	}
	if got, want := pusher.Endpoint(), "https://api-1.execute-api.us-east-1.amazonaws.com/integration"; got != want {
		t.Fatalf("Endpoint() = %q, want %q", got, want)
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestWSFactoryRejectsInvalidConfiguration(t *testing.T) {
	t.Run("SharedMemory", func(t *testing.T) {
		options := ws.NewOptions(
			ws.WithDevelopment(true),
			ws.WithPusher(ws.NewFakePusher()),
			ws.WithSharedDeployment(true),
		)
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "shared") {
			t.Fatalf("shared memory error = %v", err)
		}
	})

	t.Run("ProductionAuthentication", func(t *testing.T) {
		options := ws.NewOptions(ws.WithPusher(ws.NewFakePusher()))
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrAuthenticatorRequired) {
			t.Fatalf("production authentication error = %v", err)
		}
	})

	t.Run("IncompleteDynamoDB", func(t *testing.T) {
		options := ws.NewOptions(
			ws.WithDevelopment(true),
			ws.WithPusher(ws.NewFakePusher()),
			ws.WithStateBackend(ws.BackendDynamoDB),
		)
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "state.tableName") {
			t.Fatalf("DynamoDB config error = %v", err)
		}
	})

	t.Run("IncompleteRedis", func(t *testing.T) {
		options := ws.NewOptions(
			ws.WithDevelopment(true),
			ws.WithPusher(ws.NewFakePusher()),
			ws.WithStateBackend(ws.BackendRedis),
		)
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "state.redisAddress") {
			t.Fatalf("Redis config error = %v", err)
		}
	})

	t.Run("MalformedRedisURLRedactsCredentials", func(t *testing.T) {
		const secret = "do-not-expose-this-password"
		options := ws.NewOptions(
			ws.WithDevelopment(true),
			ws.WithPusher(ws.NewFakePusher()),
			ws.WithStateBackend(ws.BackendRedis),
			ws.WithConfig([]byte(fmt.Sprintf("state:\n  redisAddress: %q\n", "redis://:"+secret+"@localhost:not-a-port"))),
		)
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) {
			t.Fatalf("malformed Redis URL error = %v", err)
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "not-a-port") {
			t.Fatalf("Redis error exposed configuration: %v", err)
		}
	})

	t.Run("InjectedMemoryCannotBypassSharedValidation", func(t *testing.T) {
		options := ws.NewOptions(
			ws.WithDevelopment(true),
			ws.WithPusher(ws.NewFakePusher()),
			ws.WithStateStore(memorystore.New()),
			ws.WithConfig([]byte("state:\n  backend: redis\n  redisAddress: 127.0.0.1:6379\n  sharedDeployment: true\n")),
		)
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "memory") {
			t.Fatalf("injected shared memory error = %v", err)
		}
	})

	t.Run("MissingPusherConfiguration", func(t *testing.T) {
		_, err := ws.NewFactory(ws.NewOptions(ws.WithDevelopment(true))).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) || !strings.Contains(err.Error(), "apiGateway") {
			t.Fatalf("missing pusher config error = %v", err)
		}
	})

	t.Run("UnknownBackend", func(t *testing.T) {
		options := ws.NewOptions(ws.WithDevelopment(true), ws.WithPusher(ws.NewFakePusher()))
		options.State.Backend = ws.Backend("unknown")
		_, err := ws.NewFactory(options).Build()
		if !errors.Is(err, ws.ErrInvalidConfig) {
			t.Fatalf("unknown backend error = %v", err)
		}
	})
}

type wsCloseTrackingStore struct {
	*memorystore.Store
	closeCalls int
}

func (s *wsCloseTrackingStore) Close() error {
	s.closeCalls++
	return nil
}

func TestWSFactoryDoesNotCloseInjectedStateStore(t *testing.T) {
	stateStore := &wsCloseTrackingStore{Store: memorystore.New()}
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithStateStore(stateStore),
		ws.WithPusher(ws.NewFakePusher()),
	)
	factory := ws.NewFactory(options)
	if _, err := factory.Build(); err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if stateStore.closeCalls != 0 {
		t.Fatalf("injected StateStore Close calls = %d, want 0", stateStore.closeCalls)
	}
}

func TestWSCloseWithoutServe(t *testing.T) {
	if err := ws.Close(); err != nil {
		t.Fatalf("Close() without Serve error: %v", err)
	}
	if err := ws.Close(); err != nil {
		t.Fatalf("second Close() error: %v", err)
	}
}

func TestWSServerConfigPreservesOtherModules(t *testing.T) {
	for _, test := range []struct {
		name       string
		config     string
		lambdaType string
		optionLen  func(*server.Options) int
	}{
		{name: "Event", config: "lambda: event\nevent:\n  mode:\n    debug: true\n", lambdaType: "event", optionLen: func(o *server.Options) int { return len(o.Event) }},
		{name: "HTTP", config: "lambda: http\nhttp: {}\n", lambdaType: "http", optionLen: func(o *server.Options) int { return len(o.Http) }},
		{name: "SQS", config: "lambda: sqs\nsqs: {}\n", lambdaType: "sqs", optionLen: func(o *server.Options) int { return len(o.Sqs) }},
		{name: "ReqResp", config: "lambda: reqresp\nreqresp: {}\n", lambdaType: "reqresp", optionLen: func(o *server.Options) int { return len(o.ReqResp) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := &server.Options{}
			server.WithServeConfig([]byte(test.config)).Apply(options)
			if options.Lambda != test.lambdaType || test.optionLen(options) != 1 {
				t.Fatalf("non-WS route changed: lambda=%q options=%d", options.Lambda, test.optionLen(options))
			}
			if len(options.Ws) != 0 {
				t.Fatalf("non-WS route populated WS options: %d", len(options.Ws))
			}
			if test.lambdaType == "event" && !event.NewOptions(options.Event...).DebugMode {
				t.Fatal("event options regressed after WS integration")
			}
		})
	}
}

func TestWSServerLogCorrelationAndSecretRedaction(t *testing.T) {
	const (
		tokenSecret    = "server-token-credential-do-not-log"
		resumeSecret   = "server-resume-credential-do-not-log"
		payloadSecret  = "server-message-payload-do-not-log"
		internalSecret = "server-internal-error-do-not-log"
	)
	f := newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Authenticator = ws.AuthenticatorFunc(func(_ context.Context, request ws.ConnectRequest) (ws.Principal, error) {
			if request.Headers["deny"] == "true" {
				return ws.Principal{}, errors.New(internalSecret)
			}
			return ws.Principal{Subject: "alice", Claims: map[string]string{"private": internalSecret}}, nil
		})
	})
	recorder := &wsEngineObservabilityRecorder{}
	f.engine.SetLogger(recorder)
	ctx := context.Background()

	connect := wsSessionTestConnectEvent("connection-server-logging", "alice", "", "")
	connect.RequestID = "request-server-connect"
	connect.Headers["authorization"] = tokenSecret
	connect.QueryParameters = map[string]string{"token": tokenSecret}
	connected := mustWSSessionTestConnect(t, f.engine, connect)

	f.engine.Handle("/server-private", func(handlerCtx *ws.Context) {
		handlerCtx.AbortWithError(ws.WrapHandlerError("denied", "public denial", errors.New(internalSecret)))
	})
	if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, connected, "message-server-private", "request-server-message", "/server-private", json.RawMessage(`{"secret":"`+payloadSecret+`"}`))); err != nil {
		t.Fatalf("HandleMessage(private) error = %v", err)
	}
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: connected.ConnectionID, SessionID: connected.SessionID, Generation: connected.Generation,
		RequestID: "request-server-disconnect",
	}); err != nil {
		t.Fatalf("HandleDisconnect() error = %v", err)
	}
	// A duplicate lifecycle event exercises correlation-field sanitization even
	// when a caller supplies control characters and an internal secret.
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: connected.ConnectionID, RequestID: "unsafe-request-id\n" + internalSecret,
	}); err != nil {
		t.Fatalf("duplicate HandleDisconnect() error = %v", err)
	}
	denied := wsSessionTestConnectEvent("connection-server-denied", "alice", "session-server-denied", resumeSecret)
	denied.RequestID = "request-server-denied"
	denied.Headers["deny"] = "true"
	denied.Headers["authorization"] = tokenSecret
	if _, err := f.engine.HandleConnect(ctx, denied); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("denied HandleConnect() error = %v, want ErrAuthenticationFailed", err)
	}

	logs, _ := recorder.snapshot()
	assertLog := func(name, outcome, requestID, connectionID string) {
		t.Helper()
		for _, event := range logs {
			if event.Name == name && event.Outcome == outcome && event.RequestID == requestID && event.ConnectionID == connectionID {
				return
			}
		}
		t.Errorf("missing correlated log name=%q outcome=%q requestId=%q connectionId=%q; logs=%+v", name, outcome, requestID, connectionID, logs)
	}
	assertLog(ws.LogEventConnect, "success", "request-server-connect", connected.ConnectionID)
	assertLog(ws.LogEventDisconnect, "success", "request-server-disconnect", connected.ConnectionID)
	assertLog(ws.LogEventDisconnect, "duplicate", "[redacted]", connected.ConnectionID)
	assertLog(ws.LogEventConnect, "denied", "request-server-denied", denied.ConnectionID)

	var diagnostic strings.Builder
	for _, event := range logs {
		fmt.Fprintf(&diagnostic, "%s|%#v|%+v\n", event.String(), event, event)
	}
	for _, secret := range []string{tokenSecret, resumeSecret, connected.ResumeCredential, payloadSecret, internalSecret} {
		if secret == "" {
			t.Fatal("test did not issue a resume credential")
		}
		if strings.Contains(diagnostic.String(), secret) {
			t.Errorf("server logs leaked %q:\n%s", secret, diagnostic.String())
		}
	}
}

func setWSOfflineAWSConfig(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "test-session-token")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
}
