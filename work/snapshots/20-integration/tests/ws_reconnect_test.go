package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

type wsSessionTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *wsSessionTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *wsSessionTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type wsSessionTestIDGenerator struct {
	mu      sync.Mutex
	counter map[string]uint64
}

func (g *wsSessionTestIDGenerator) ID(prefix string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.counter == nil {
		g.counter = make(map[string]uint64)
	}
	g.counter[prefix]++
	return fmt.Sprintf("%s-%04d", prefix, g.counter[prefix])
}

type wsSessionTestFixture struct {
	engine *ws.Engine
	store  *memory.Store
	pusher *ws.FakePusher
	clock  *wsSessionTestClock
	ids    *wsSessionTestIDGenerator
	base   time.Time
}

func wsSessionTestOptions(clock *wsSessionTestClock, ids *wsSessionTestIDGenerator) *ws.Options {
	options := ws.NewOptions()
	options.Clock = clock
	options.IDGenerator = ids
	options.Authenticator = ws.AuthenticatorFunc(func(_ context.Context, request ws.ConnectRequest) (ws.Principal, error) {
		subject := request.Headers["test-subject"]
		if subject == "" {
			subject = "alice"
		}
		return ws.Principal{Subject: subject, Claims: map[string]string{"source": "fixed-test-authenticator"}}, nil
	})
	options.Reconnect.SessionRetention = time.Hour
	options.Offline.Enabled = false
	options.Push.MaxRetries = 0
	return options
}

func newWSSessionTestFixture(t *testing.T, configure func(*ws.Options)) *wsSessionTestFixture {
	t.Helper()
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := &wsSessionTestClock{now: base}
	ids := &wsSessionTestIDGenerator{}
	options := wsSessionTestOptions(clock, ids)
	if configure != nil {
		configure(options)
	}
	store := memory.New(
		memory.WithClock(clock.Now),
		memory.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
		memory.WithOfflineOverflowPolicy(options.Offline.Overflow),
	)
	pusher := ws.NewFakePusher()
	options.StateStore = store
	options.Pusher = pusher
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return &wsSessionTestFixture{engine: engine, store: store, pusher: pusher, clock: clock, ids: ids, base: base}
}

func (f *wsSessionTestFixture) newEngine(t *testing.T, configure func(*ws.Options)) *ws.Engine {
	t.Helper()
	options := wsSessionTestOptions(f.clock, f.ids)
	if configure != nil {
		configure(options)
	}
	options.StateStore = f.store
	options.Pusher = f.pusher
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatalf("NewEngine(shared fixture) error = %v", err)
	}
	return engine
}

func wsSessionTestConnectEvent(connectionID, subject, sessionID, credential string) ws.ConnectEvent {
	return ws.ConnectEvent{
		ConnectionID:     connectionID,
		RequestID:        "request-" + connectionID,
		SessionID:        sessionID,
		ResumeCredential: credential,
		Headers:          map[string]string{"test-subject": subject},
	}
}

func mustWSSessionTestConnect(t *testing.T, engine *ws.Engine, event ws.ConnectEvent) ws.ConnectResult {
	t.Helper()
	result, err := engine.HandleConnect(context.Background(), event)
	if err != nil {
		t.Fatalf("HandleConnect(%s) error = %v", event.ConnectionID, err)
	}
	return result
}

func TestWSConnectCreatesDistinctSessionsAndProtectsResumeCredential(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()

	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-1", "alice", "", ""))
	if first.SessionID == "" || first.ResumeCredential == "" {
		t.Fatalf("first connect did not issue session credentials: %+v", first)
	}
	if first.ConnectionID != "connection-1" || first.Generation != 1 || first.Reconnected {
		t.Fatalf("first connect result = %+v, want a new generation-1 session", first)
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if stored.PrincipalSubject != "alice" || stored.CurrentConnectionID != first.ConnectionID || stored.Generation != 1 {
		t.Fatalf("stored session = %+v, want alice bound to connection-1", stored)
	}
	wantHash := ws.HashResumeCredential(first.SessionID, first.ResumeCredential)
	if stored.ResumeCredentialHash != wantHash || stored.ResumeCredentialHash == first.ResumeCredential {
		t.Fatalf("resume credential was not stored as its domain-separated hash")
	}
	if strings.Contains(fmt.Sprintf("%+v", stored), first.ResumeCredential) ||
		strings.Contains(first.String(), first.ResumeCredential) {
		t.Fatal("plaintext resume credential is exposed by stored state or result formatting")
	}
	connection, err := f.store.GetConnection(ctx, first.ConnectionID)
	if err != nil || connection.SessionID != first.SessionID || connection.PrincipalSubject != "alice" ||
		connection.Generation != 1 || connection.State != wsstore.ConnectionStateActive {
		t.Fatalf("stored connection = %+v, %v", connection, err)
	}

	second := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-2", "alice", "", ""))
	if second.SessionID == first.SessionID || second.ResumeCredential == first.ResumeCredential {
		t.Fatalf("credential-less connect reused state: first=%+v second=%+v", first, second)
	}
	firstAfter, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || firstAfter.CurrentConnectionID != first.ConnectionID || firstAfter.Generation != 1 {
		t.Fatalf("second new session changed the first binding: %+v, %v", firstAfter, err)
	}
}

func TestWSReconnectResumesSessionAndFencesOldEvents(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))

	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	if !resumed.Reconnected || resumed.SessionID != first.SessionID || resumed.Generation != 2 ||
		resumed.ResumeCredential != first.ResumeCredential {
		t.Fatalf("resume result = %+v, want the same session at generation 2", resumed)
	}
	if _, err := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("replaced connection still exists: %v", err)
	}

	ping := `{"version":1,"type":"ping","requestId":"old-request"}`
	if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation, Body: ping,
	}); err != nil {
		t.Fatalf("late old message should be fenced without a framework error: %v", err)
	}
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
	}); err != nil {
		t.Fatalf("late old disconnect should be an idempotent no-op: %v", err)
	}

	// A stale generation is ignored even if a caller attaches it to the current
	// physical connection ID; it must not touch or write the replacement.
	f.clock.Advance(time.Minute)
	if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: resumed.ConnectionID, SessionID: resumed.SessionID, Generation: first.Generation, Body: ping,
	}); err != nil {
		t.Fatalf("stale-generation message error = %v", err)
	}
	currentConnection, err := f.store.GetConnection(ctx, resumed.ConnectionID)
	if err != nil || !currentConnection.LastSeenAt.Equal(f.base) {
		t.Fatalf("stale event touched replacement: %+v, %v", currentConnection, err)
	}
	if f.pusher.CallCount() != 0 {
		t.Fatalf("stale events produced %d pushes, want none", f.pusher.CallCount())
	}

	if err := f.engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: resumed.ConnectionID, SessionID: resumed.SessionID, Generation: resumed.Generation, Body: ping,
	}); err != nil {
		t.Fatalf("current connection message error = %v", err)
	}
	calls := f.pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != resumed.ConnectionID {
		t.Fatalf("current pong calls = %+v, want one push to replacement", calls)
	}
	envelope, err := ws.DecodeEnvelope(calls[0].Body)
	if err != nil || envelope.Type != ws.TypePong || envelope.SessionID != resumed.SessionID {
		t.Fatalf("current pong envelope = %+v, %v", envelope, err)
	}
	stored, err := f.store.GetSession(ctx, resumed.SessionID)
	if err != nil || stored.CurrentConnectionID != resumed.ConnectionID || stored.Generation != resumed.Generation {
		t.Fatalf("old events changed current binding: %+v, %v", stored, err)
	}
}

func TestWSResumeRejectsInvalidPrincipalCredentialExpiryAndAuthorization(t *testing.T) {
	tests := []struct {
		name       string
		subject    string
		sessionID  string
		credential string
		advance    time.Duration
		configure  func(*ws.Options)
		want       error
	}{
		{name: "wrong credential", subject: "alice", sessionID: "<current>", credential: "wrong-credential", want: ws.ErrInvalidResume},
		{name: "cross principal", subject: "bob", sessionID: "<current>", credential: "<valid>", want: ws.ErrInvalidResume},
		{name: "unknown session", subject: "alice", sessionID: "session-does-not-exist", credential: "<valid>", want: ws.ErrInvalidResume},
		{name: "missing credential", subject: "alice", sessionID: "<current>", credential: "", want: ws.ErrInvalidResume},
		{name: "missing session", subject: "alice", sessionID: "", credential: "<valid>", want: ws.ErrInvalidResume},
		{name: "expired at retention boundary", subject: "alice", sessionID: "<current>", credential: "<valid>", advance: time.Hour, want: ws.ErrInvalidResume},
		{
			name: "session authorization denied", subject: "alice", sessionID: "<current>", credential: "<valid>",
			configure: func(options *ws.Options) {
				options.Authorizer = ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error {
					return errors.New("private policy detail")
				})
			},
			want: ws.ErrAuthorizationFailed,
		},
		{
			name: "reconnect disabled", subject: "alice", sessionID: "<current>", credential: "<valid>",
			configure: func(options *ws.Options) { options.Reconnect.Enabled = false },
			want:      ws.ErrReconnectDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newWSSessionTestFixture(t, tt.configure)
			ctx := context.Background()
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-original", "alice", "", ""))
			before, err := f.store.GetSession(ctx, first.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if tt.advance != 0 {
				f.clock.Advance(tt.advance)
			}
			sessionID, credential := tt.sessionID, tt.credential
			if sessionID == "<current>" {
				sessionID = first.SessionID
			}
			if credential == "<valid>" {
				credential = first.ResumeCredential
			}
			result, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent("connection-attempt", tt.subject, sessionID, credential))
			if !errors.Is(err, tt.want) {
				t.Fatalf("resume error = %v, want %v (result %+v)", err, tt.want, result)
			}
			if _, getErr := f.store.GetConnection(ctx, "connection-attempt"); !wsstore.IsNotFound(getErr) {
				t.Fatalf("rejected resume left a new connection: %v", getErr)
			}
			if tt.advance == 0 {
				after, getErr := f.store.GetSession(ctx, first.SessionID)
				if getErr != nil || after.CurrentConnectionID != before.CurrentConnectionID ||
					after.Generation != before.Generation || after.ResumeCredentialHash != before.ResumeCredentialHash {
					t.Fatalf("rejected resume mutated session: before=%+v after=%+v err=%v", before, after, getErr)
				}
				original, getErr := f.store.GetConnection(ctx, first.ConnectionID)
				if getErr != nil || original.State != wsstore.ConnectionStateActive || original.Generation != first.Generation {
					t.Fatalf("rejected resume damaged original connection: %+v, %v", original, getErr)
				}
			}
		})
	}
}

func TestWSReconnectReplaceExistingPolicy(t *testing.T) {
	tests := []struct {
		name            string
		replaceExisting bool
		disconnectFirst bool
		wantConflict    bool
	}{
		{name: "disabled rejects active replacement", replaceExisting: false, wantConflict: true},
		{name: "disabled resumes offline session", replaceExisting: false, disconnectFirst: true},
		{name: "enabled replaces active connection", replaceExisting: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newWSSessionTestFixture(t, func(options *ws.Options) {
				options.Reconnect.ReplaceExisting = tt.replaceExisting
			})
			ctx := context.Background()
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
			if tt.disconnectFirst {
				if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
					ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
				}); err != nil {
					t.Fatalf("HandleDisconnect() error = %v", err)
				}
			}

			result, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
				"connection-new", "alice", first.SessionID, first.ResumeCredential,
			))
			if tt.wantConflict {
				if !errors.Is(err, ws.ErrConnectionConflict) {
					t.Fatalf("active replacement error = %v, want ErrConnectionConflict", err)
				}
				stored, getErr := f.store.GetSession(ctx, first.SessionID)
				if getErr != nil || stored.CurrentConnectionID != first.ConnectionID || stored.Generation != first.Generation {
					t.Fatalf("conflicting replacement changed binding: %+v, %v", stored, getErr)
				}
				if _, getErr := f.store.GetConnection(ctx, "connection-new"); !wsstore.IsNotFound(getErr) {
					t.Fatalf("conflicting replacement left new connection: %v", getErr)
				}
				if _, getErr := f.store.GetConnection(ctx, first.ConnectionID); getErr != nil {
					t.Fatalf("conflicting replacement removed active connection: %v", getErr)
				}
				return
			}
			if err != nil || !result.Reconnected || result.SessionID != first.SessionID || result.Generation != 2 {
				t.Fatalf("resume result = %+v, error = %v", result, err)
			}
			if _, getErr := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(getErr) {
				t.Fatalf("old connection remains after resume: %v", getErr)
			}
			current, getErr := f.store.GetConnection(ctx, result.ConnectionID)
			if getErr != nil || current.Generation != 2 || current.State != wsstore.ConnectionStateActive {
				t.Fatalf("replacement connection = %+v, %v", current, getErr)
			}
		})
	}
}

func TestWSDisconnectGrantsFullRetentionAndReconnectRenewsActivity(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))

	f.clock.Advance(55 * time.Minute)
	disconnectedAt := f.clock.Now()
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
	}); err != nil {
		t.Fatalf("HandleDisconnect() error = %v", err)
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || !stored.LastActiveAt.Equal(disconnectedAt) ||
		!stored.ExpiresAt.Equal(disconnectedAt.Add(time.Hour)) {
		t.Fatalf("disconnect did not grant a full retention window: %+v, %v", stored, err)
	}

	// This is beyond the original creation-time expiry, but inside the complete
	// retention window granted at disconnect.
	f.clock.Advance(50 * time.Minute)
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	stored, err = f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != resumed.ConnectionID ||
		!stored.LastActiveAt.Equal(f.clock.Now()) || !stored.ExpiresAt.Equal(f.clock.Now().Add(time.Hour)) {
		t.Fatalf("reconnect did not renew activity and expiry: %+v, %v", stored, err)
	}
}

func TestWSReconnectRejectedAfterDisconnectRetentionWindow(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	f.clock.Advance(55 * time.Minute)
	if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
	}); err != nil {
		t.Fatalf("HandleDisconnect() error = %v", err)
	}

	f.clock.Advance(time.Hour)
	_, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
		"connection-too-late", "alice", first.SessionID, first.ResumeCredential,
	))
	if !errors.Is(err, ws.ErrInvalidResume) {
		t.Fatalf("resume outside disconnect retention error = %v, want ErrInvalidResume", err)
	}
}

func TestWSStaleGenerationUnbindDoesNotRenewCurrentSession(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	f.clock.Advance(10 * time.Minute)
	mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	before, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil {
		t.Fatal(err)
	}

	f.clock.Advance(10 * time.Minute)
	if err := f.store.UnbindConnectionIfMatch(ctx, first.SessionID, first.ConnectionID, first.Generation); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind error = %v, want condition conflict", err)
	}
	after, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || after.CurrentConnectionID != before.CurrentConnectionID || after.Generation != before.Generation ||
		!after.LastActiveAt.Equal(before.LastActiveAt) || !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatalf("stale unbind renewed current session: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestWSConcurrentReconnectUsesSessionCAS(t *testing.T) {
	f := newWSSessionTestFixture(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-original", "alice", "", ""))

	const contenders = 12
	allAuthenticated := make(chan struct{})
	start := make(chan struct{})
	var authenticated atomic.Int32
	authorizer := ws.AuthorizerFunc(func(ctx context.Context, principal ws.Principal, action ws.Action) error {
		if principal.Subject != "alice" || action.Name != ws.ActionSessionResume || action.SessionID != first.SessionID {
			return errors.New("unexpected resume authorization")
		}
		// Every contender has read the same generation before any is allowed to
		// bind. This makes the CAS assertion deterministic across Engine instances.
		if authenticated.Add(1) == contenders {
			close(allAuthenticated)
		}
		select {
		case <-allAuthenticated:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	engines := make([]*ws.Engine, contenders)
	for i := range engines {
		engines[i] = f.newEngine(t, func(options *ws.Options) { options.Authorizer = authorizer })
	}

	type outcome struct {
		connectionID string
		result       ws.ConnectResult
		err          error
	}
	outcomes := make(chan outcome, contenders)
	for i, engine := range engines {
		connectionID := fmt.Sprintf("connection-contender-%02d", i)
		go func() {
			<-start
			result, err := engine.HandleConnect(ctx, wsSessionTestConnectEvent(
				connectionID, "alice", first.SessionID, first.ResumeCredential,
			))
			outcomes <- outcome{connectionID: connectionID, result: result, err: err}
		}()
	}
	close(start)

	winner := ""
	for i := 0; i < contenders; i++ {
		outcome := <-outcomes
		if outcome.err == nil {
			if winner != "" {
				t.Fatalf("multiple concurrent resumes succeeded: %q and %q", winner, outcome.connectionID)
			}
			winner = outcome.connectionID
			if !outcome.result.Reconnected || outcome.result.Generation != 2 || outcome.result.SessionID != first.SessionID {
				t.Fatalf("winning resume = %+v", outcome.result)
			}
		} else if !errors.Is(outcome.err, ws.ErrConnectionConflict) {
			t.Fatalf("losing resume error = %v, want ErrConnectionConflict", outcome.err)
		}
	}
	if winner == "" {
		t.Fatal("no concurrent resume succeeded")
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != winner || stored.Generation != 2 {
		t.Fatalf("session after CAS race = %+v, %v; winner=%q", stored, err, winner)
	}
	if _, err := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("original connection survived replacement: %v", err)
	}
	for i := 0; i < contenders; i++ {
		connectionID := fmt.Sprintf("connection-contender-%02d", i)
		connection, getErr := f.store.GetConnection(ctx, connectionID)
		if connectionID == winner {
			if getErr != nil || connection.Generation != 2 || connection.State != wsstore.ConnectionStateActive {
				t.Fatalf("winning connection = %+v, %v", connection, getErr)
			}
		} else if !wsstore.IsNotFound(getErr) {
			t.Fatalf("losing connection %q was persisted: %+v, %v", connectionID, connection, getErr)
		}
	}
}
