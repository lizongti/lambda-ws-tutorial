package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

const wsRoomTestRoomID = "room"

type wsRoomTestIDGenerator struct {
	next atomic.Uint64
}

func (g *wsRoomTestIDGenerator) ID(prefix string) string {
	return fmt.Sprintf("%s-%04d", prefix, g.next.Add(1))
}

type wsRoomTestStore struct {
	*memory.Store
	mu                 sync.Mutex
	removedConnections []string
	joinIfMatchHook    func()
	leaveIfMatchHook   func()
}

func (s *wsRoomTestStore) JoinIfMatch(ctx context.Context, member wsstore.RoomMember) error {
	if s.joinIfMatchHook != nil {
		s.joinIfMatchHook()
	}
	return s.Store.JoinIfMatch(ctx, member)
}

func (s *wsRoomTestStore) LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error {
	if s.leaveIfMatchHook != nil {
		s.leaveIfMatchHook()
	}
	return s.Store.LeaveIfMatch(ctx, roomID, sessionID, connectionID, generation)
}

func (s *wsRoomTestStore) RemoveConnection(ctx context.Context, connectionID string) error {
	if err := s.Store.RemoveConnection(ctx, connectionID); err != nil {
		return err
	}
	s.mu.Lock()
	s.removedConnections = append(s.removedConnections, connectionID)
	s.mu.Unlock()
	return nil
}

func (s *wsRoomTestStore) removedConnectionIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.removedConnections...)
}

type wsLegacyRoomStore struct {
	wsstore.StateStore
	joinCalls  int
	leaveCalls int
}

func (s *wsLegacyRoomStore) Join(ctx context.Context, member wsstore.RoomMember) error {
	s.joinCalls++
	return s.StateStore.Join(ctx, member)
}

func (s *wsLegacyRoomStore) Leave(ctx context.Context, roomID, sessionID string) error {
	s.leaveCalls++
	return s.StateStore.Leave(ctx, roomID, sessionID)
}

type wsRoomTestFixture struct {
	engine *ws.Engine
	store  *wsRoomTestStore
	pusher *ws.FakePusher
	now    time.Time
}

func newWSRoomTestFixture(t *testing.T, options ...ws.Option) *wsRoomTestFixture {
	t.Helper()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	store := &wsRoomTestStore{Store: memory.New(memory.WithClock(func() time.Time { return now }))}
	pusher := ws.NewFakePusher()
	baseOptions := []ws.Option{
		ws.WithStateStore(store),
		ws.WithPusher(pusher),
		ws.WithAuthenticator(ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
			return ws.Principal{Subject: "room-test"}, nil
		})),
		ws.WithClock(ws.ClockFunc(func() time.Time { return now })),
		ws.WithIDGenerator(&wsRoomTestIDGenerator{}),
		ws.WithRetrySleeper(func(context.Context, time.Duration) error { return nil }),
	}
	engine, err := ws.NewEngine(ws.NewOptions(append(baseOptions, options...)...))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return &wsRoomTestFixture{engine: engine, store: store, pusher: pusher, now: now}
}

func (f *wsRoomTestFixture) addSession(t *testing.T, sessionID string) *ws.Context {
	t.Helper()
	ctx := context.Background()
	subject := "subject-" + sessionID
	if err := f.store.CreateSession(ctx, wsstore.Session{
		SessionID: sessionID, PrincipalSubject: subject, LastActiveAt: f.now, ExpiresAt: f.now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession(%q) error = %v", sessionID, err)
	}
	connectionID := "conn-" + sessionID
	bound, err := f.store.BindConnection(ctx, sessionID, connectionID, 0)
	if err != nil {
		t.Fatalf("BindConnection(%q) error = %v", sessionID, err)
	}
	if err := f.store.PutConnection(ctx, wsstore.Connection{
		ConnectionID: connectionID, SessionID: sessionID, PrincipalSubject: subject,
		Generation: bound.Generation, ConnectedAt: f.now, LastSeenAt: f.now,
		ExpiresAt: f.now.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}); err != nil {
		t.Fatalf("PutConnection(%q) error = %v", connectionID, err)
	}
	return &ws.Context{
		Context: ctx, ConnectionID: connectionID, SessionID: sessionID,
		Principal: ws.Principal{Subject: subject},
	}
}

func (f *wsRoomTestFixture) addMember(t *testing.T, sessionID string) *ws.Context {
	t.Helper()
	ctx := f.addSession(t, sessionID)
	if err := f.engine.JoinRoom(ctx, wsRoomTestRoomID); err != nil {
		t.Fatalf("JoinRoom(%q) error = %v", sessionID, err)
	}
	return ctx
}

func (f *wsRoomTestFixture) reconnectSession(t *testing.T, old *ws.Context, connectionID string, expectedGeneration uint64) *ws.Context {
	t.Helper()
	bound, err := f.store.BindConnection(context.Background(), old.SessionID, connectionID, expectedGeneration)
	if err != nil {
		t.Fatalf("BindConnection(%q) reconnect error = %v", old.SessionID, err)
	}
	if err := f.store.PutConnection(context.Background(), wsstore.Connection{
		ConnectionID: connectionID, SessionID: old.SessionID, PrincipalSubject: old.Principal.Subject,
		Generation: bound.Generation, ConnectedAt: f.now, LastSeenAt: f.now,
		ExpiresAt: f.now.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}); err != nil {
		t.Fatalf("PutConnection(%q) reconnect error = %v", connectionID, err)
	}
	return &ws.Context{
		Context: context.Background(), ConnectionID: connectionID, SessionID: old.SessionID,
		Principal: old.Principal.Clone(),
	}
}

func wsRoomTestMessage(messageID string) ws.OutboundMessage {
	msg := ws.NewEvent("/room/events", "request-room", json.RawMessage(`{"value":1}`))
	msg.MessageID = messageID
	return msg
}

func wsRoomTestBroadcastOptions() ws.BroadcastOptions {
	return ws.BroadcastOptions{Principal: ws.Principal{Subject: "room-admin"}}
}

func assertWSRoomTestCounts(t *testing.T, result ws.BroadcastResult, total, succeeded, gone, offline, failed int) {
	t.Helper()
	if result.Total != total || result.Succeeded != succeeded || result.Gone != gone ||
		result.Offline != offline || result.Failed != failed {
		t.Fatalf("Broadcast counts = {total:%d succeeded:%d gone:%d offline:%d failed:%d}, want {%d %d %d %d %d}; result = %+v",
			result.Total, result.Succeeded, result.Gone, result.Offline, result.Failed,
			total, succeeded, gone, offline, failed, result)
	}
	if len(result.Deliveries) != total {
		t.Fatalf("len(Deliveries) = %d, want %d; deliveries = %+v", len(result.Deliveries), total, result.Deliveries)
	}
}

func wsRoomTestMemberIDs(members []wsstore.RoomMember) []string {
	ids := make([]string, len(members))
	for i, member := range members {
		ids[i] = member.SessionID
	}
	return ids
}

func assertWSRoomTestSessionIDs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("session IDs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("session IDs = %v, want %v", got, want)
		}
	}
}

func TestWSRoomJoinLeaveAuthorizationAndIdempotency(t *testing.T) {
	var mu sync.Mutex
	var actions []ws.Action
	authorizer := ws.AuthorizerFunc(func(_ context.Context, principal ws.Principal, action ws.Action) error {
		if principal.Subject != "subject-"+action.SessionID {
			return errors.New("wrong principal")
		}
		mu.Lock()
		actions = append(actions, action)
		mu.Unlock()
		if action.Name == ws.ActionRoomJoin && action.SessionID == "s03" {
			return errors.New("join denied")
		}
		if action.Name == ws.ActionRoomLeave && action.SessionID == "s02" {
			return errors.New("leave denied")
		}
		return nil
	})
	f := newWSRoomTestFixture(t, ws.WithAuthorizer(authorizer))
	ctx1 := f.addSession(t, "s01")
	ctx2 := f.addSession(t, "s02")
	ctx3 := f.addSession(t, "s03")

	for i := 0; i < 2; i++ {
		if err := f.engine.JoinRoom(ctx1, wsRoomTestRoomID); err != nil {
			t.Fatalf("JoinRoom(s01) call %d error = %v", i+1, err)
		}
	}
	if err := f.engine.JoinRoom(ctx2, wsRoomTestRoomID); err != nil {
		t.Fatalf("JoinRoom(s02) error = %v", err)
	}
	if err := f.engine.JoinRoom(ctx3, wsRoomTestRoomID); !errors.Is(err, ws.ErrAuthorizationFailed) {
		t.Fatalf("denied JoinRoom(s03) error = %v, want ErrAuthorizationFailed", err)
	}

	members, next, err := f.store.Members(context.Background(), wsRoomTestRoomID, 10, "")
	if err != nil || next != "" {
		t.Fatalf("Members() next = %q, error = %v", next, err)
	}
	assertWSRoomTestSessionIDs(t, wsRoomTestMemberIDs(members), "s01", "s02")

	if err := f.engine.LeaveRoom(ctx2, wsRoomTestRoomID); !errors.Is(err, ws.ErrAuthorizationFailed) {
		t.Fatalf("denied LeaveRoom(s02) error = %v, want ErrAuthorizationFailed", err)
	}
	for i := 0; i < 2; i++ {
		if err := f.engine.LeaveRoom(ctx1, wsRoomTestRoomID); err != nil {
			t.Fatalf("LeaveRoom(s01) call %d error = %v", i+1, err)
		}
	}
	members, _, err = f.store.Members(context.Background(), wsRoomTestRoomID, 10, "")
	if err != nil {
		t.Fatalf("Members() after leave error = %v", err)
	}
	assertWSRoomTestSessionIDs(t, wsRoomTestMemberIDs(members), "s02")

	mu.Lock()
	gotActions := append([]ws.Action(nil), actions...)
	mu.Unlock()
	wantActions := []ws.Action{
		{Name: ws.ActionRoomJoin, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s01"},
		{Name: ws.ActionRoomJoin, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s01"},
		{Name: ws.ActionRoomJoin, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s02"},
		{Name: ws.ActionRoomJoin, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s03"},
		{Name: ws.ActionRoomLeave, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s02"},
		{Name: ws.ActionRoomLeave, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s01"},
		{Name: ws.ActionRoomLeave, Resource: wsRoomTestRoomID, RoomID: wsRoomTestRoomID, SessionID: "s01"},
	}
	if len(gotActions) != len(wantActions) {
		t.Fatalf("authorization actions = %+v, want %+v", gotActions, wantActions)
	}
	for i := range wantActions {
		if gotActions[i] != wantActions[i] {
			t.Fatalf("authorization action %d = %+v, want %+v", i, gotActions[i], wantActions[i])
		}
	}
}

func TestWSRoomDelayedGenerationCannotDamageReplacementMember(t *testing.T) {
	for _, operation := range []string{"join", "leave"} {
		t.Run(operation, func(t *testing.T) {
			f := newWSRoomTestFixture(t)
			oldCtx := f.addSession(t, "s01")
			if operation == "leave" {
				if err := f.engine.JoinRoom(oldCtx, wsRoomTestRoomID); err != nil {
					t.Fatalf("initial JoinRoom error = %v", err)
				}
			}

			// The hook runs after JoinRoom/LeaveRoom captured c-old/1 but before
			// the store's atomic conditional write. The replacement Join therefore
			// deterministically completes before the old operation resumes.
			reconnectAndJoin := func() {
				newCtx := f.reconnectSession(t, oldCtx, "conn-s01-new", 1)
				if err := f.engine.JoinRoom(newCtx, wsRoomTestRoomID); err != nil {
					t.Fatalf("replacement JoinRoom error = %v", err)
				}
			}
			if operation == "join" {
				f.store.joinIfMatchHook = func() {
					f.store.joinIfMatchHook = nil
					reconnectAndJoin()
				}
			} else {
				f.store.leaveIfMatchHook = func() {
					f.store.leaveIfMatchHook = nil
					reconnectAndJoin()
				}
			}

			var err error
			if operation == "join" {
				err = f.engine.JoinRoom(oldCtx, wsRoomTestRoomID)
			} else {
				err = f.engine.LeaveRoom(oldCtx, wsRoomTestRoomID)
			}
			if !wsstore.IsConditionConflict(err) {
				t.Fatalf("delayed %s error = %v, want condition conflict", operation, err)
			}
			members, next, err := f.store.Members(context.Background(), wsRoomTestRoomID, 10, "")
			if err != nil || next != "" || len(members) != 1 ||
				members[0].ConnectionID != "conn-s01-new" || members[0].Generation != 2 {
				t.Fatalf("replacement member after delayed %s = %+v, next %q, %v", operation, members, next, err)
			}
		})
	}
}

func TestWSRoomLegacyStoreFallsBackToUnconditionalMethods(t *testing.T) {
	var legacy *wsLegacyRoomStore
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		legacy = &wsLegacyRoomStore{StateStore: options.StateStore}
		options.StateStore = legacy
	}))
	ctx := f.addSession(t, "s01")
	if err := f.engine.JoinRoom(ctx, wsRoomTestRoomID); err != nil {
		t.Fatalf("legacy JoinRoom error = %v", err)
	}
	for index := 0; index < 2; index++ {
		if err := f.engine.LeaveRoom(ctx, wsRoomTestRoomID); err != nil {
			t.Fatalf("legacy LeaveRoom call %d error = %v", index+1, err)
		}
	}
	if legacy.joinCalls != 1 || legacy.leaveCalls != 2 {
		t.Fatalf("legacy Join/Leave calls = %d/%d, want 1/2", legacy.joinCalls, legacy.leaveCalls)
	}
}

func TestWSRoomMemberLimitAcrossPagesAndIdempotentJoinWhenFull(t *testing.T) {
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Rooms.MaxMembers = 2
		options.Rooms.BroadcastBatchSize = 1
	}))
	ctx1 := f.addSession(t, "s01")
	f.addMember(t, "s02")
	ctx3 := f.addSession(t, "s03")
	if err := f.engine.JoinRoom(ctx1, wsRoomTestRoomID); err != nil {
		t.Fatalf("JoinRoom(s01) error = %v", err)
	}
	if err := f.engine.JoinRoom(ctx3, wsRoomTestRoomID); !errors.Is(err, ws.ErrRoomFull) {
		t.Fatalf("JoinRoom(s03) error = %v, want ErrRoomFull", err)
	}
	if err := f.engine.JoinRoom(ctx1, wsRoomTestRoomID); err != nil {
		t.Fatalf("idempotent JoinRoom(s01) in full room error = %v", err)
	}

	members, next, err := f.store.Members(context.Background(), wsRoomTestRoomID, 10, "")
	if err != nil || next != "" {
		t.Fatalf("Members() next = %q, error = %v", next, err)
	}
	assertWSRoomTestSessionIDs(t, wsRoomTestMemberIDs(members), "s01", "s02")
}

func TestWSRoomBroadcastPaginationDeliversEveryMemberOnce(t *testing.T) {
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Rooms.BroadcastBatchSize = 2
		options.Push.Concurrency = 1
	}))
	for _, sessionID := range []string{"s01", "s02", "s03", "s04", "s05"} {
		f.addMember(t, sessionID)
	}

	result := f.engine.Broadcast(context.Background(), wsRoomTestRoomID, wsRoomTestMessage("broadcast-pages"), wsRoomTestBroadcastOptions())
	if result.Err != nil {
		t.Fatalf("Broadcast() error = %v", result.Err)
	}
	assertWSRoomTestCounts(t, result, 5, 5, 0, 0, 0)
	calls := f.pusher.Calls()
	if len(calls) != 5 {
		t.Fatalf("push calls = %d, want 5", len(calls))
	}
	seen := make(map[string]int, len(calls))
	for _, call := range calls {
		seen[call.ConnectionID]++
	}
	for _, sessionID := range []string{"s01", "s02", "s03", "s04", "s05"} {
		if seen["conn-"+sessionID] != 1 {
			t.Fatalf("push count for %s = %d, want 1; calls = %+v", sessionID, seen["conn-"+sessionID], calls)
		}
	}
	for _, delivery := range result.Deliveries {
		if delivery.Status != string(ws.DeliverySent) || delivery.MessageID != "broadcast-pages" || delivery.ConnectionID == "" {
			t.Fatalf("unexpected delivery = %+v", delivery)
		}
	}
}

func TestWSRoomBroadcastSenderExclusionSkipsEmptySelectedPages(t *testing.T) {
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Rooms.BroadcastBatchSize = 1
		options.Push.Concurrency = 1
	}))
	for _, sessionID := range []string{"s01", "s02", "s03"} {
		f.addMember(t, sessionID)
	}
	opts := wsRoomTestBroadcastOptions()
	opts.ExcludeSender = true
	opts.SenderSessionID = "s02"
	opts.SenderConnectionID = "conn-s02"

	result := f.engine.Broadcast(context.Background(), wsRoomTestRoomID, wsRoomTestMessage("broadcast-exclude"), opts)
	if result.Err != nil {
		t.Fatalf("Broadcast() error = %v", result.Err)
	}
	assertWSRoomTestCounts(t, result, 2, 2, 0, 0, 0)
	calls := f.pusher.Calls()
	if len(calls) != 2 {
		t.Fatalf("push calls = %d, want 2: %+v", len(calls), calls)
	}
	for _, call := range calls {
		if call.ConnectionID == "conn-s02" {
			t.Fatalf("sender received excluded broadcast: %+v", call)
		}
	}
	seen := map[string]bool{}
	for _, delivery := range result.Deliveries {
		seen[delivery.SessionID] = true
		if delivery.SessionID == "s02" {
			t.Fatalf("sender appears in deliveries: %+v", result.Deliveries)
		}
	}
	if !seen["s01"] || !seen["s03"] {
		t.Fatalf("deliveries = %+v, want s01 and s03", result.Deliveries)
	}
}

func TestWSRoomBroadcastEmptyRoom(t *testing.T) {
	f := newWSRoomTestFixture(t)
	result := f.engine.Broadcast(context.Background(), "empty-room", wsRoomTestMessage("broadcast-empty"), wsRoomTestBroadcastOptions())
	if result.Err != nil {
		t.Fatalf("Broadcast(empty) error = %v", result.Err)
	}
	assertWSRoomTestCounts(t, result, 0, 0, 0, 0, 0)
	if calls := f.pusher.CallCount(); calls != 0 {
		t.Fatalf("push calls = %d, want 0", calls)
	}
}

func TestWSRoomBroadcastBoundsPushConcurrency(t *testing.T) {
	const (
		memberCount = 8
		workerLimit = 3
	)
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Push.Concurrency = workerLimit
	}))
	for i := 1; i <= memberCount; i++ {
		f.addMember(t, fmt.Sprintf("s%02d", i))
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	started := make(chan struct{}, memberCount)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorkers := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWorkers()
	f.pusher.SetHook(func(ctx context.Context, _ ws.PushInput) error {
		current := active.Add(1)
		for {
			maximum := maxActive.Load()
			if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			active.Add(-1)
			return nil
		case <-ctx.Done():
			active.Add(-1)
			return ctx.Err()
		}
	})

	resultCh := make(chan ws.BroadcastResult, 1)
	go func() {
		resultCh <- f.engine.Broadcast(context.Background(), wsRoomTestRoomID, wsRoomTestMessage("broadcast-concurrency"), wsRoomTestBroadcastOptions())
	}()
	for i := 0; i < workerLimit; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d pushes started, want %d concurrent workers", i, workerLimit)
		}
	}
	select {
	case <-started:
		t.Fatal("a fourth push started while all three workers were blocked")
	case <-time.After(50 * time.Millisecond):
	}
	releaseWorkers()

	var result ws.BroadcastResult
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast did not finish after releasing push hooks")
	}
	if result.Err != nil {
		t.Fatalf("Broadcast() error = %v", result.Err)
	}
	assertWSRoomTestCounts(t, result, memberCount, memberCount, 0, 0, 0)
	if got := maxActive.Load(); got != workerLimit {
		t.Fatalf("maximum concurrent pushes = %d, want %d", got, workerLimit)
	}
	if calls := f.pusher.CallCount(); calls != memberCount {
		t.Fatalf("push calls = %d, want %d", calls, memberCount)
	}
}

func TestWSRoomBroadcastPartialGoneAndTemporaryFailure(t *testing.T) {
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Push.Concurrency = 1
		options.Push.MaxRetries = 0
	}))
	for _, sessionID := range []string{"s01", "s02", "s03"} {
		f.addMember(t, sessionID)
	}
	f.pusher.SetErrors(
		ws.NewPushError(ws.PushErrorGone, errors.New("connection disappeared")),
		ws.NewPushError(ws.PushErrorUnavailable, errors.New("temporary backend failure")),
		nil,
	)

	result := f.engine.Broadcast(context.Background(), wsRoomTestRoomID, wsRoomTestMessage("broadcast-partial"), wsRoomTestBroadcastOptions())
	if result.Err != nil {
		t.Fatalf("Broadcast() room-level error = %v, want independent delivery failures", result.Err)
	}
	assertWSRoomTestCounts(t, result, 3, 1, 1, 1, 1)
	deliveries := make(map[string]ws.BroadcastDeliveryResult, len(result.Deliveries))
	for _, delivery := range result.Deliveries {
		deliveries[delivery.SessionID] = delivery
	}
	if got := deliveries["s01"]; got.Status != string(ws.DeliveryOffline) || !got.Gone || !got.Offline || got.ErrorCode != "" {
		t.Fatalf("gone delivery = %+v, want successful offline fallback with Gone set", got)
	}
	if got := deliveries["s02"]; got.Status != "failed" || got.Gone || got.Offline || got.ErrorCode != "push_unavailable" {
		t.Fatalf("temporary failure delivery = %+v", got)
	}
	if got := deliveries["s03"]; got.Status != string(ws.DeliverySent) || got.Gone || got.Offline || got.ErrorCode != "" {
		t.Fatalf("successful delivery = %+v", got)
	}

	if _, err := f.store.GetConnection(context.Background(), "conn-s01"); !wsstore.IsNotFound(err) {
		t.Fatalf("gone connection still exists or unexpected error = %v", err)
	}
	if removed := f.store.removedConnectionIDs(); len(removed) != 1 || removed[0] != "conn-s01" {
		t.Fatalf("room membership cleanup calls = %v, want only conn-s01", removed)
	}
	goneSession, err := f.store.GetSession(context.Background(), "s01")
	if err != nil || goneSession.CurrentConnectionID != "" {
		t.Fatalf("gone session binding = %+v, error = %v; want retained unbound session", goneSession, err)
	}
	temporarySession, err := f.store.GetSession(context.Background(), "s02")
	if err != nil || temporarySession.CurrentConnectionID != "conn-s02" {
		t.Fatalf("temporary failure changed binding = %+v, error = %v", temporarySession, err)
	}
	if _, err := f.store.GetConnection(context.Background(), "conn-s02"); err != nil {
		t.Fatalf("temporary failure removed connection: %v", err)
	}

	goneQueue, err := f.store.ListPending(context.Background(), "s01", 10)
	if err != nil || len(goneQueue) != 1 || goneQueue[0].MessageID != "broadcast-partial" {
		t.Fatalf("gone member offline queue = %+v, error = %v", goneQueue, err)
	}
	for _, sessionID := range []string{"s02", "s03"} {
		queue, err := f.store.ListPending(context.Background(), sessionID, 10)
		if err != nil || len(queue) != 0 {
			t.Fatalf("offline queue for %s = %+v, error = %v; want empty", sessionID, queue, err)
		}
	}
	members, _, err := f.store.Members(context.Background(), wsRoomTestRoomID, 10, "")
	if err != nil {
		t.Fatalf("Members() after cleanup error = %v", err)
	}
	assertWSRoomTestSessionIDs(t, wsRoomTestMemberIDs(members), "s02", "s03")
}

func TestWSRoomBroadcastOfflineEnqueueWhenBindingChangesDuringPage(t *testing.T) {
	f := newWSRoomTestFixture(t, ws.OptionFunc(func(options *ws.Options) {
		options.Push.Concurrency = 1
	}))
	f.addMember(t, "s01")
	f.addMember(t, "s02")
	f.pusher.SetHook(func(_ context.Context, input ws.PushInput) error {
		if input.ConnectionID != "conn-s01" {
			return nil
		}
		return f.store.UnbindConnectionIfMatch(context.Background(), "s02", "conn-s02", 1)
	})

	result := f.engine.Broadcast(context.Background(), wsRoomTestRoomID, wsRoomTestMessage("broadcast-offline"), wsRoomTestBroadcastOptions())
	if result.Err != nil {
		t.Fatalf("Broadcast() error = %v", result.Err)
	}
	assertWSRoomTestCounts(t, result, 2, 1, 0, 1, 0)
	deliveries := make(map[string]ws.BroadcastDeliveryResult, len(result.Deliveries))
	for _, delivery := range result.Deliveries {
		deliveries[delivery.SessionID] = delivery
	}
	if got := deliveries["s01"]; got.Status != string(ws.DeliverySent) || got.Offline {
		t.Fatalf("live delivery = %+v", got)
	}
	if got := deliveries["s02"]; got.Status != string(ws.DeliveryOffline) || !got.Offline || got.Gone || got.ErrorCode != "" {
		t.Fatalf("unbound delivery = %+v, want offline without Gone", got)
	}
	queue, err := f.store.ListPending(context.Background(), "s02", 10)
	if err != nil || len(queue) != 1 {
		t.Fatalf("offline queue = %+v, error = %v, want one message", queue, err)
	}
	queued := queue[0]
	if queued.MessageID != "broadcast-offline" || queued.Generation != 1 || queued.State != wsstore.OfflineMessageStatePending {
		t.Fatalf("queued message = %+v", queued)
	}
	var envelope ws.Envelope
	if err := json.Unmarshal(queued.Envelope, &envelope); err != nil {
		t.Fatalf("queued envelope is invalid JSON: %v", err)
	}
	if envelope.SessionID != "s02" || envelope.MessageID != "broadcast-offline" || envelope.Type != ws.TypeEvent {
		t.Fatalf("queued envelope = %+v", envelope)
	}
}
