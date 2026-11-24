package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi/types"
	"github.com/aws/smithy-go"
)

var _ ws.Pusher = (*ws.APIGatewayPusher)(nil)
var _ ws.Pusher = (*ws.FakePusher)(nil)

type wsPusherRecordedPost struct {
	connectionID string
	body         []byte
}

type wsPusherFakeClient struct {
	mu         sync.Mutex
	script     []error
	calls      []wsPusherRecordedPost
	hook       func(context.Context, *apigatewaymanagementapi.PostToConnectionInput) error
	mutateData bool
}

func (c *wsPusherFakeClient) PostToConnection(ctx context.Context, input *apigatewaymanagementapi.PostToConnectionInput, _ ...func(*apigatewaymanagementapi.Options)) (*apigatewaymanagementapi.PostToConnectionOutput, error) {
	connectionID := ""
	if input.ConnectionId != nil {
		connectionID = *input.ConnectionId
	}
	c.mu.Lock()
	index := len(c.calls)
	c.calls = append(c.calls, wsPusherRecordedPost{connectionID: connectionID, body: bytes.Clone(input.Data)})
	var err error
	if index < len(c.script) {
		err = c.script[index]
	}
	hook := c.hook
	mutate := c.mutateData
	c.mu.Unlock()

	if mutate && len(input.Data) > 0 {
		input.Data[0] = 'X'
	}
	if hook != nil {
		if hookErr := hook(ctx, input); hookErr != nil {
			return nil, hookErr
		}
	}
	if err != nil {
		return nil, err
	}
	return &apigatewaymanagementapi.PostToConnectionOutput{}, nil
}

func (c *wsPusherFakeClient) Calls() []wsPusherRecordedPost {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls := make([]wsPusherRecordedPost, len(c.calls))
	for i, call := range c.calls {
		calls[i] = wsPusherRecordedPost{connectionID: call.connectionID, body: bytes.Clone(call.body)}
	}
	return calls
}

func wsPusherTestConfig() ws.APIGatewayPusherConfig {
	return ws.APIGatewayPusherConfig{ManagementEndpoint: "https://example.com/prod"}
}

func newWSPusherForTest(t *testing.T, client ws.PostToConnectionClient, cfg ws.APIGatewayPusherConfig) *ws.APIGatewayPusher {
	t.Helper()
	pusher, err := ws.NewAPIGatewayPusher(client, cfg)
	if err != nil {
		t.Fatalf("NewAPIGatewayPusher() error = %v", err)
	}
	return pusher
}

func requireWSPusherErrorKind(t *testing.T, err error, kind ws.PushErrorKind, retryable bool) {
	t.Helper()
	var pushErr *ws.PushError
	if !errors.As(err, &pushErr) {
		t.Fatalf("error %T(%v) is not PushError", err, err)
	}
	if pushErr.Kind != kind || pushErr.Retryable != retryable {
		t.Fatalf("PushError = {Kind:%q Retryable:%v}, want {%q %v}", pushErr.Kind, pushErr.Retryable, kind, retryable)
	}
}

func TestWSPusherEndpoints(t *testing.T) {
	for _, test := range []struct {
		name                 string
		apiID, region, stage string
		want                 string
	}{
		{name: "standard partition", apiID: "abc123", region: "us-east-1", stage: "prod", want: "https://abc123.execute-api.us-east-1.amazonaws.com/prod"},
		{name: "China partition", apiID: "api_1", region: "cn-north-1", stage: "stage_1", want: "https://api_1.execute-api.cn-north-1.amazonaws.com.cn/stage_1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := ws.BuildAPIGatewayManagementEndpoint(test.apiID, test.region, test.stage)
			if err != nil || endpoint != test.want {
				t.Fatalf("endpoint = %q, error = %v; want %q", endpoint, err, test.want)
			}
		})
	}

	endpoint, err := ws.ValidateAPIGatewayManagementEndpoint("https://example.com/prod///")
	if err != nil || endpoint != "https://example.com/prod" {
		t.Fatalf("normalized endpoint = %q, error = %v", endpoint, err)
	}
	pusher := newWSPusherForTest(t, &wsPusherFakeClient{}, ws.APIGatewayPusherConfig{APIID: "api-1", Region: "eu-west-1", Stage: "prod"})
	if got, want := pusher.Endpoint(), "https://api-1.execute-api.eu-west-1.amazonaws.com/prod"; got != want {
		t.Fatalf("APIGatewayPusher.Endpoint() = %q, want %q", got, want)
	}
	for _, endpoint := range []string{
		"", "http://example.com/prod", "example.com/prod", "https://example.com",
		"https://user:secret@example.com/prod", "https://example.com/prod?stage=other",
		"https://example.com/prod#fragment", "https://example.com/../prod", "https://example.com/prod%2fadmin",
	} {
		if _, err := ws.ValidateAPIGatewayManagementEndpoint(endpoint); err == nil {
			t.Errorf("ValidateAPIGatewayManagementEndpoint(%q) unexpectedly succeeded", endpoint)
		} else {
			requireWSPusherErrorKind(t, err, ws.PushErrorInvalid, false)
		}
	}
}

func TestWSPusherRequestUsesTrustedConnectionAndImmutableBody(t *testing.T) {
	client := &wsPusherFakeClient{mutateData: true}
	pusher := newWSPusherForTest(t, client, wsPusherTestConfig())
	body := []byte(`{"connectionId":"untrusted","payload":{"ok":true}}`)
	original := bytes.Clone(body)

	result, err := pusher.Push(context.Background(), ws.PushInput{
		ConnectionID: "trusted=Connection_ID=", MessageID: "message-7", Body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ConnectionID != "trusted=Connection_ID=" || result.MessageID != "message-7" || result.Attempts != 1 {
		t.Fatalf("PushResult = %+v", result)
	}
	if !bytes.Equal(body, original) {
		t.Fatalf("client mutated caller body: got %q, want %q", body, original)
	}
	calls := client.Calls()
	if len(calls) != 1 || calls[0].connectionID != "trusted=Connection_ID=" || !bytes.Equal(calls[0].body, original) {
		t.Fatalf("SDK request = %+v, want trusted ID and original body", calls)
	}
}

type wsPusherHTTPStatusError struct{ status int }

func (e wsPusherHTTPStatusError) Error() string       { return "status error" }
func (e wsPusherHTTPStatusError) HTTPStatusCode() int { return e.status }

func TestWSPusherErrorMappingIsStableAndSafe(t *testing.T) {
	cause := errors.New("sensitive SDK detail")
	for _, test := range []struct {
		name      string
		err       error
		kind      ws.PushErrorKind
		retryable bool
	}{
		{name: "modeled gone", err: &types.GoneException{}, kind: ws.PushErrorGone},
		{name: "modeled throttled", err: &types.LimitExceededException{}, kind: ws.PushErrorThrottled, retryable: true},
		{name: "modeled too large", err: &types.PayloadTooLargeException{}, kind: ws.PushErrorInvalid},
		{name: "modeled forbidden", err: &types.ForbiddenException{}, kind: ws.PushErrorInvalid},
		{name: "generic gone", err: &smithy.GenericAPIError{Code: "GoneException"}, kind: ws.PushErrorGone},
		{name: "generic throttling", err: &smithy.GenericAPIError{Code: "ThrottlingException"}, kind: ws.PushErrorThrottled, retryable: true},
		{name: "generic unavailable", err: &smithy.GenericAPIError{Code: "ServiceUnavailableException"}, kind: ws.PushErrorUnavailable, retryable: true},
		{name: "server fault", err: &smithy.GenericAPIError{Code: "Unexpected", Fault: smithy.FaultServer}, kind: ws.PushErrorUnavailable, retryable: true},
		{name: "HTTP gone", err: wsPusherHTTPStatusError{status: 410}, kind: ws.PushErrorGone},
		{name: "HTTP throttled", err: wsPusherHTTPStatusError{status: 429}, kind: ws.PushErrorThrottled, retryable: true},
		{name: "HTTP unavailable", err: wsPusherHTTPStatusError{status: 503}, kind: ws.PushErrorUnavailable, retryable: true},
		{name: "HTTP invalid", err: wsPusherHTTPStatusError{status: 400}, kind: ws.PushErrorInvalid},
		{name: "network", err: &net.DNSError{Err: "timeout", Name: "example.com", IsTimeout: true}, kind: ws.PushErrorUnavailable, retryable: true},
		{name: "unknown", err: cause, kind: ws.PushErrorUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ws.ClassifyPushError(test.err)
			requireWSPusherErrorKind(t, err, test.kind, test.retryable)
			if !errors.Is(err, test.err) {
				t.Fatalf("classified error does not wrap cause %T", test.err)
			}
		})
	}
	if got := ws.ClassifyPushError(nil); got != nil {
		t.Fatalf("ClassifyPushError(nil) = %v", got)
	}
	if got := ws.NewPushError(ws.PushErrorUnknown, cause).Error(); bytes.Contains([]byte(got), []byte("sensitive")) {
		t.Fatalf("PushError.Error() leaked cause: %q", got)
	}
}

func TestWSPusherRetriesWithBoundedBackoffAndAttemptCount(t *testing.T) {
	client := &wsPusherFakeClient{mutateData: true, script: []error{
		&types.LimitExceededException{},
		&smithy.GenericAPIError{Code: "ServiceUnavailableException"},
		nil,
	}}
	var delays []time.Duration
	cfg := wsPusherTestConfig()
	cfg.MaxRetries = 2
	cfg.InitialBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 15 * time.Millisecond
	cfg.Sleep = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	pusher := newWSPusherForTest(t, client, cfg)
	body := []byte("original")
	result, err := pusher.Push(context.Background(), ws.PushInput{ConnectionID: "connection", MessageID: "message", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 3 || len(client.Calls()) != 3 {
		t.Fatalf("attempts/calls = %d/%d, want 3/3", result.Attempts, len(client.Calls()))
	}
	if want := []time.Duration{10 * time.Millisecond, 15 * time.Millisecond}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	for i, call := range client.Calls() {
		if string(call.body) != "original" {
			t.Errorf("call %d body = %q, want original", i+1, call.body)
		}
	}
	if string(body) != "original" {
		t.Fatalf("caller body = %q", body)
	}
}

func TestWSPusherRetryBoundsAndPermanentErrors(t *testing.T) {
	t.Run("retry budget exhausted", func(t *testing.T) {
		client := &wsPusherFakeClient{script: []error{&types.LimitExceededException{}, &types.LimitExceededException{}, &types.LimitExceededException{}}}
		sleeps := 0
		cfg := wsPusherTestConfig()
		cfg.MaxRetries = 2
		cfg.Sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
		pusher := newWSPusherForTest(t, client, cfg)
		result, err := pusher.Push(context.Background(), ws.PushInput{ConnectionID: "connection", Body: []byte("data")})
		requireWSPusherErrorKind(t, err, ws.PushErrorThrottled, true)
		if result.Attempts != 3 || len(client.Calls()) != 3 || sleeps != 2 {
			t.Fatalf("attempts/calls/sleeps = %d/%d/%d, want 3/3/2", result.Attempts, len(client.Calls()), sleeps)
		}
	})

	for _, test := range []struct {
		name string
		err  error
		kind ws.PushErrorKind
	}{
		{name: "gone", err: &types.GoneException{}, kind: ws.PushErrorGone},
		{name: "invalid", err: &types.PayloadTooLargeException{}, kind: ws.PushErrorInvalid},
		{name: "unknown", err: errors.New("unexpected"), kind: ws.PushErrorUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &wsPusherFakeClient{script: []error{test.err}}
			sleeps := 0
			cfg := wsPusherTestConfig()
			cfg.MaxRetries = 5
			cfg.Sleep = func(context.Context, time.Duration) error { sleeps++; return nil }
			pusher := newWSPusherForTest(t, client, cfg)
			result, err := pusher.Push(context.Background(), ws.PushInput{ConnectionID: "connection", Body: []byte("data")})
			requireWSPusherErrorKind(t, err, test.kind, false)
			if result.Attempts != 1 || len(client.Calls()) != 1 || sleeps != 0 {
				t.Fatalf("attempts/calls/sleeps = %d/%d/%d, want 1/1/0", result.Attempts, len(client.Calls()), sleeps)
			}
		})
	}
}

func TestWSPusherCancellationStopsRequestAndBackoff(t *testing.T) {
	t.Run("pre-canceled", func(t *testing.T) {
		client := &wsPusherFakeClient{}
		pusher := newWSPusherForTest(t, client, wsPusherTestConfig())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := pusher.Push(ctx, ws.PushInput{ConnectionID: "connection", Body: []byte("data")})
		if !errors.Is(err, context.Canceled) || result.Attempts != 0 || len(client.Calls()) != 0 {
			t.Fatalf("result = %+v, error = %v, calls = %d", result, err, len(client.Calls()))
		}
	})

	t.Run("during SDK request", func(t *testing.T) {
		started := make(chan struct{})
		client := &wsPusherFakeClient{}
		client.hook = func(ctx context.Context, _ *apigatewaymanagementapi.PostToConnectionInput) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		pusher := newWSPusherForTest(t, client, wsPusherTestConfig())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := pusher.Push(ctx, ws.PushInput{ConnectionID: "connection", Body: []byte("data")})
			done <- err
		}()
		<-started
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Push did not return after cancellation")
		}
	})

	t.Run("during default backoff", func(t *testing.T) {
		started := make(chan struct{})
		client := &wsPusherFakeClient{script: []error{&types.LimitExceededException{}}}
		client.hook = func(context.Context, *apigatewaymanagementapi.PostToConnectionInput) error {
			close(started)
			return nil
		}
		cfg := wsPusherTestConfig()
		cfg.MaxRetries = 1
		cfg.InitialBackoff = time.Hour
		cfg.MaxBackoff = time.Hour
		pusher := newWSPusherForTest(t, client, cfg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := pusher.Push(ctx, ws.PushInput{ConnectionID: "connection", Body: []byte("data")})
			done <- err
		}()
		<-started
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("default backoff did not respect cancellation")
		}
	})
}

func TestWSFakePusherConcurrentRecordingAndSnapshotIsolation(t *testing.T) {
	fake := ws.NewFakePusher()
	const count = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := fake.Push(context.Background(), ws.PushInput{
				ConnectionID: fmt.Sprintf("connection-%02d", i), MessageID: fmt.Sprintf("message-%02d", i), Body: []byte(fmt.Sprintf("payload-%02d", i)),
			})
			errs <- err
		}(i)
	}
	close(start)
	for i := 0; i < count; i++ {
		_ = fake.Calls()
		_ = fake.CallCount()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	calls := fake.Calls()
	if len(calls) != count || fake.CallCount() != count {
		t.Fatalf("calls/count = %d/%d, want %d", len(calls), fake.CallCount(), count)
	}
	seen := make(map[string]string, count)
	for _, call := range calls {
		if previous, duplicate := seen[call.ConnectionID]; duplicate {
			t.Fatalf("duplicate call %q: previous body %q", call.ConnectionID, previous)
		}
		seen[call.ConnectionID] = string(call.Body)
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("connection-%02d", i)
		if want := fmt.Sprintf("payload-%02d", i); seen[id] != want {
			t.Errorf("recorded body for %s = %q, want %q", id, seen[id], want)
		}
	}
	calls[0].Body[0] = 'X'
	if bytes.Equal(calls[0].Body, fake.Calls()[0].Body) && calls[0].Body[0] == 'X' {
		t.Fatal("Calls snapshot aliases the internal record")
	}
}

func TestWSFakePusherScriptAndCancellation(t *testing.T) {
	gone := ws.NewPushError(ws.PushErrorGone, errors.New("private gone cause"))
	fake := ws.NewFakePusher(gone, nil)
	body := []byte("payload")
	if _, err := fake.Push(context.Background(), ws.PushInput{ConnectionID: "one", Body: body}); !ws.IsPushGone(err) {
		t.Fatalf("first error = %v, want gone", err)
	}
	body[0] = 'X'
	if _, err := fake.Push(context.Background(), ws.PushInput{ConnectionID: "two", Body: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	if got := string(fake.Calls()[0].Body); got != "payload" {
		t.Fatalf("recorded body aliases caller: %q", got)
	}

	fake = ws.NewFakePusher()
	started := make(chan struct{})
	fake.SetHook(func(ctx context.Context, _ ws.PushInput) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := fake.Push(ctx, ws.PushInput{ConnectionID: "connection", Body: []byte("payload")})
		done <- err
	}()
	<-started
	if fake.CallCount() != 1 {
		t.Fatal("blocked call was not recorded")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FakePusher remained blocked after cancellation")
	}
}
