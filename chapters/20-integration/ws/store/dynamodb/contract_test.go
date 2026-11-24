package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type contractFactory func(t *testing.T, clock *testClock) wsstore.StateStore

type contractFixture struct {
	store wsstore.StateStore
	clock *testClock
	base  time.Time
}

func newContractFixture(t *testing.T, factory contractFactory) contractFixture {
	t.Helper()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{now: base}
	return contractFixture{store: factory(t, clock), clock: clock, base: base}
}

func TestFakeStateStoreContract(t *testing.T) {
	runStateStoreContract(t, func(t *testing.T, clock *testClock) wsstore.StateStore {
		t.Helper()
		return New(dynamodbtest.New(), "ws-test", WithClock(clock.Now), WithOfflineQueueLimit(2), WithIdempotencyWindow(time.Minute))
	})
}

func runStateStoreContract(t *testing.T, factory contractFactory) {
	t.Helper()
	t.Run("errors and empty results", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		if _, err := f.store.GetConnection(ctx, "missing"); !wsstore.IsNotFound(err) || !errors.Is(err, wsstore.ErrNotFound) {
			t.Fatalf("GetConnection error = %v", err)
		}
		if members, next, err := f.store.Members(ctx, "empty", 1, ""); err != nil || len(members) != 0 || next != "" {
			t.Fatalf("Members(empty) = %v, %q, %v", members, next, err)
		}
		if messages, err := f.store.ListPending(ctx, "missing", 10); err != nil || len(messages) != 0 {
			t.Fatalf("ListPending(missing) = %v, %v", messages, err)
		}
		for _, limit := range []int{0, wsstore.MaxBatchSize + 1} {
			if _, _, err := f.store.Members(ctx, "room", limit, ""); !wsstore.IsInvalid(err) {
				t.Fatalf("Members(limit=%d) error = %v", limit, err)
			}
		}
	})

	t.Run("connection generations stale ordering and query pages", func(t *testing.T) {
		f := newContractFixture(t, factory)
		if store, ok := f.store.(*Store); ok {
			if fake, ok := store.client.(*dynamodbtest.Fake); ok {
				fake.SetMaxQueryItems(1)
			}
		}
		ctx := context.Background()
		connection := wsstore.Connection{
			ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "user", Generation: 1,
			ConnectedAt: f.base, LastSeenAt: f.base, ExpiresAt: f.base.Add(time.Hour), State: wsstore.ConnectionStateActive,
		}
		if err := f.store.PutConnection(ctx, connection); err != nil {
			t.Fatal(err)
		}
		if err := f.store.TouchConnection(ctx, "c1", 0, f.base.Add(time.Second)); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale touch = %v", err)
		}
		if err := f.store.TouchConnection(ctx, "c1", 1, f.base); err != nil {
			t.Fatalf("monotonic no-op touch = %v", err)
		}
		if err := f.store.TouchConnection(ctx, "c1", 1, f.base.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := f.store.GetConnection(ctx, "c1")
		if err != nil || !got.LastSeenAt.Equal(f.base.Add(time.Second)) {
			t.Fatalf("touched connection = %+v, %v", got, err)
		}
		replacement := connection
		replacement.Generation = 2
		replacement.LastSeenAt = f.base.Add(2 * time.Second)
		if err := f.store.PutConnection(ctx, replacement); err != nil {
			t.Fatal(err)
		}
		if err := f.store.PutConnection(ctx, connection); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale put = %v", err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 1); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale delete = %v", err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
			t.Fatal(err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
			t.Fatalf("duplicate delete = %v", err)
		}
		for _, stale := range []wsstore.Connection{
			{ConnectionID: "b", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
			{ConnectionID: "c", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
			{ConnectionID: "a", Generation: 1, LastSeenAt: f.base.Add(-2 * time.Second)},
			{ConnectionID: "closed", Generation: 1, LastSeenAt: f.base.Add(-3 * time.Second), State: wsstore.ConnectionStateClosed},
			{ConnectionID: "live", Generation: 1, LastSeenAt: f.base},
		} {
			if err := f.store.PutConnection(ctx, stale); err != nil {
				t.Fatal(err)
			}
		}
		page, err := f.store.ListStaleConnections(ctx, f.base, 2)
		if err != nil || len(page) != 2 || page[0].ConnectionID != "a" || page[1].ConnectionID != "b" {
			t.Fatalf("stale page = %+v, %v", page, err)
		}
		page, err = f.store.ListStaleConnections(ctx, f.base, 10)
		if err != nil || len(page) != 3 || page[0].ConnectionID != "a" || page[1].ConnectionID != "b" || page[2].ConnectionID != "c" {
			t.Fatalf("all stale = %+v, %v", page, err)
		}
	})

	t.Run("connection expiration is indexed bounded and conditional", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		expirationStore, ok := f.store.(wsstore.ConnectionExpirationStore)
		if !ok {
			t.Fatal("store does not implement ConnectionExpirationStore")
		}
		for _, connection := range []wsstore.Connection{
			{ConnectionID: "expired-a", Generation: 1, LastSeenAt: f.base.Add(50 * time.Minute), ExpiresAt: f.base.Add(time.Hour)},
			{ConnectionID: "expired-b", Generation: 1, LastSeenAt: f.base.Add(50 * time.Minute), ExpiresAt: f.base.Add(2 * time.Hour)},
			{ConnectionID: "live", Generation: 1, LastSeenAt: f.base.Add(50 * time.Minute), ExpiresAt: f.base.Add(4 * time.Hour)},
		} {
			if err := f.store.PutConnection(ctx, connection); err != nil {
				t.Fatal(err)
			}
		}
		cutoff := f.base.Add(3 * time.Hour)
		stale, err := f.store.ListStaleConnections(ctx, f.base.Add(10*time.Minute), 10)
		if err != nil || len(stale) != 0 {
			t.Fatalf("recently seen expired connections must not rely on stale scan: %+v, %v", stale, err)
		}
		page, err := expirationStore.ListExpiredConnections(ctx, cutoff, 1)
		if err != nil || len(page) != 1 || page[0].ConnectionID != "expired-a" {
			t.Fatalf("first expired connection page = %+v, %v", page, err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "expired-a", 1); err != nil {
			t.Fatal(err)
		}
		page, err = expirationStore.ListExpiredConnections(ctx, cutoff, 1)
		if err != nil || len(page) != 1 || page[0].ConnectionID != "expired-b" {
			t.Fatalf("second expired connection page = %+v, %v", page, err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "expired-b", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("mismatched expired generation delete = %v", err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "expired-b", 1); err != nil {
			t.Fatal(err)
		}
		page, err = expirationStore.ListExpiredConnections(ctx, cutoff, 10)
		if err != nil || len(page) != 0 {
			t.Fatalf("live or deleted connections in expired page = %+v, %v", page, err)
		}
	})

	t.Run("session CAS unbind expiry and owned cleanup", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		session := wsstore.Session{SessionID: "s1", PrincipalSubject: "user", ResumeCredentialHash: "sha256:hash", ExpiresAt: f.base.Add(time.Hour)}
		if err := f.store.CreateSession(ctx, session); err != nil {
			t.Fatal(err)
		}
		if err := f.store.CreateSession(ctx, session); !wsstore.IsConditionConflict(err) {
			t.Fatalf("duplicate create = %v", err)
		}
		const contenders = 16
		var wg sync.WaitGroup
		errs := make(chan error, contenders)
		for index := 0; index < contenders; index++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				_, err := f.store.BindConnection(ctx, "s1", fmt.Sprintf("c-%d", index), 0)
				errs <- err
			}(index)
		}
		wg.Wait()
		close(errs)
		successes, conflicts := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				successes++
			case wsstore.IsConditionConflict(err):
				conflicts++
			default:
				t.Fatalf("concurrent bind = %v", err)
			}
		}
		if successes != 1 || conflicts != contenders-1 {
			t.Fatalf("bind successes/conflicts = %d/%d", successes, conflicts)
		}
		bound, err := f.store.GetSession(ctx, "s1")
		if err != nil || bound.Generation != 1 || bound.CurrentConnectionID == "" {
			t.Fatalf("bound = %+v, %v", bound, err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", "old", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("late unbind = %v", err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", bound.CurrentConnectionID, 1); err != nil {
			t.Fatal(err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", bound.CurrentConnectionID, 1); err != nil {
			t.Fatalf("duplicate unbind = %v", err)
		}
		rebound, err := f.store.BindConnection(ctx, "s1", "new", 1)
		if err != nil || rebound.Generation != 2 || rebound.CurrentConnectionID != "new" {
			t.Fatalf("rebound = %+v, %v", rebound, err)
		}
		if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "owned", SessionID: "s1", ConnectionID: "new", Generation: 2}); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "owned-message", Generation: 2}); err != nil {
			t.Fatal(err)
		}
		if _, acquired, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "owned-inbound"}); err != nil || !acquired {
			t.Fatalf("owned idempotency = %v, %v", acquired, err)
		}
		if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(2*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetSession(ctx, "s1"); !wsstore.IsNotFound(err) {
			t.Fatalf("expired session = %v", err)
		}
		if pending, err := f.store.ListPending(ctx, "s1", 10); err != nil || len(pending) != 0 {
			t.Fatalf("owned offline = %+v, %v", pending, err)
		}
		if _, err := f.store.GetIdempotency(ctx, "s1", "owned-inbound"); !wsstore.IsNotFound(err) {
			t.Fatalf("owned idempotency remains = %v", err)
		}
		if members, _, err := f.store.Members(ctx, "owned", 10, ""); err != nil || len(members) != 0 {
			t.Fatalf("owned room remains = %+v, %v", members, err)
		}

		for id, offset := range map[string]time.Duration{"exp-a": time.Hour, "exp-b": 2 * time.Hour, "live": 4 * time.Hour} {
			if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: id, ExpiresAt: f.base.Add(offset)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(3*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetSession(ctx, "exp-a"); !wsstore.IsNotFound(err) {
			t.Fatalf("first expired session = %v", err)
		}
		if _, err := f.store.GetSession(ctx, "exp-b"); err != nil {
			t.Fatalf("expiry batch exceeded limit = %v", err)
		}
		f.clock.Advance(5 * time.Hour)
		if _, err := f.store.GetSession(ctx, "live"); !wsstore.IsNotFound(err) {
			t.Fatalf("logical session expiry = %v", err)
		}
		if _, err := f.store.BindConnection(ctx, "live", "c", 0); !wsstore.IsNotFound(err) {
			t.Fatalf("expired bind = %v", err)
		}
	})

	t.Run("room idempotency generation cursors and reverse cleanup", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		for _, id := range []string{"s1", "s2", "s3"} {
			if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: id, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.BindConnection(ctx, id, "c-"+id, 0); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range []string{"s3", "s1", "s2"} {
			member := wsstore.RoomMember{RoomID: "room", SessionID: id, ConnectionID: "c-" + id, Generation: 1, JoinedAt: f.base}
			if err := f.store.Join(ctx, member); err != nil {
				t.Fatal(err)
			}
			member.JoinedAt = f.base.Add(time.Hour)
			if err := f.store.Join(ctx, member); err != nil {
				t.Fatalf("idempotent join = %v", err)
			}
		}
		first, cursor, err := f.store.Members(ctx, "room", 2, "")
		if err != nil || len(first) != 2 || first[0].SessionID != "s1" || first[1].SessionID != "s2" || cursor != "s2" || !first[0].JoinedAt.Equal(f.base) {
			t.Fatalf("first room page = %+v, cursor %q, %v", first, cursor, err)
		}
		second, next, err := f.store.Members(ctx, "room", 2, cursor)
		if err != nil || len(second) != 1 || second[0].SessionID != "s3" || next != "" {
			t.Fatalf("second room page = %+v, %q, %v", second, next, err)
		}
		if _, err := f.store.BindConnection(ctx, "s2", "c-new", 1); err != nil {
			t.Fatal(err)
		}
		members, _, err := f.store.Members(ctx, "room", 10, "")
		if err != nil || len(members) != 2 {
			t.Fatalf("stale member returned = %+v, %v", members, err)
		}
		if err := f.store.RemoveConnection(ctx, "c-s1"); err != nil {
			t.Fatal(err)
		}
		members, _, _ = f.store.Members(ctx, "room", 10, "")
		if len(members) != 1 || members[0].SessionID != "s3" {
			t.Fatalf("reverse cleanup = %+v", members)
		}
		if err := f.store.Leave(ctx, "room", "s3"); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Leave(ctx, "room", "s3"); err != nil {
			t.Fatalf("duplicate leave = %v", err)
		}
	})

	t.Run("offline sequence replay ack overflow and input isolation", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		envelope := json.RawMessage(`{"value":"original"}`)
		for _, id := range []string{"m1", "m2"} {
			if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, Sequence: 999, Generation: 0, Envelope: envelope, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
		envelope[2] = 'X'
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3", Generation: 0}); !wsstore.IsQuotaExceeded(err) {
			t.Fatalf("overflow = %v", err)
		}
		if _, err := f.store.BindConnection(ctx, "s1", "c1", 0); err != nil {
			t.Fatal(err)
		}
		if err := f.store.MarkSent(ctx, "s1", "m1", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale MarkSent = %v", err)
		}
		if err := f.store.MarkSent(ctx, "s1", "m1", 1); err != nil {
			t.Fatal(err)
		}
		pending, err := f.store.ListPending(ctx, "s1", 1)
		if err != nil || len(pending) != 1 || pending[0].MessageID != "m1" || pending[0].Sequence != 1 || pending[0].State != wsstore.OfflineMessageStateSent || pending[0].Generation != 1 || pending[0].SentAt == nil {
			t.Fatalf("sent pending = %+v, %v", pending, err)
		}
		if string(pending[0].Envelope) != `{"value":"original"}` {
			t.Fatalf("input envelope aliased = %s", pending[0].Envelope)
		}
		originalSentAt := *pending[0].SentAt
		pending[0].Envelope[2] = 'Y'
		changed := f.base.Add(10 * time.Hour)
		pending[0].SentAt = &changed
		pending, _ = f.store.ListPending(ctx, "s1", 1)
		if string(pending[0].Envelope) != `{"value":"original"}` || !pending[0].SentAt.Equal(originalSentAt) {
			t.Fatalf("output aliases stored state = %+v", pending[0])
		}
		if err := f.store.Ack(ctx, "s1", "m1", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale ack = %v", err)
		}
		if err := f.store.Ack(ctx, "s1", "m1", 1); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Ack(ctx, "s1", "m1", 1); err != nil {
			t.Fatalf("duplicate ack = %v", err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3", Generation: 1}); err != nil {
			t.Fatal(err)
		}
		pending, err = f.store.ListPending(ctx, "s1", 10)
		if err != nil || len(pending) != 2 || pending[0].MessageID != "m2" || pending[0].Sequence != 2 || pending[1].MessageID != "m3" || pending[1].Sequence != 3 {
			t.Fatalf("pending after ack/append = %+v, %v", pending, err)
		}
	})

	t.Run("offline evict oldest and concurrent sequence CAS", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		store, ok := f.store.(*Store)
		if !ok {
			t.Skip("backend-specific overflow configuration")
		}
		store.offlineOverflowPolicy = wsstore.OfflineOverflowEvictOldest
		if err := store.CreateSession(ctx, wsstore.Session{SessionID: "evict"}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"m1", "m2", "m3"} {
			if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "evict", MessageID: id}); err != nil {
				t.Fatal(err)
			}
		}
		pending, err := store.ListPending(ctx, "evict", 10)
		if err != nil || len(pending) != 2 || pending[0].MessageID != "m2" || pending[0].Sequence != 2 || pending[1].MessageID != "m3" || pending[1].Sequence != 3 {
			t.Fatalf("evicted queue = %+v, %v", pending, err)
		}

		const count = 32
		store.offlineQueueLimit = count
		if err := store.CreateSession(ctx, wsstore.Session{SessionID: "concurrent"}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, count)
		for index := 0; index < count; index++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				errs <- store.Append(ctx, wsstore.OfflineMessage{SessionID: "concurrent", MessageID: fmt.Sprintf("m-%02d", index)})
			}(index)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent append = %v", err)
			}
		}
		pending, err = store.ListPending(ctx, "concurrent", count)
		if err != nil || len(pending) != count {
			t.Fatalf("concurrent queue length = %d, %v", len(pending), err)
		}
		for index, msg := range pending {
			if msg.Sequence != uint64(index+1) {
				t.Fatalf("sequence at %d = %d", index, msg.Sequence)
			}
		}
	})

	t.Run("offline expiration batches and logical reads", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
			t.Fatal(err)
		}
		for index, id := range []string{"m1", "m2"} {
			if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, Generation: 0, ExpiresAt: f.base.Add(time.Duration(index+1) * time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.store.DeleteExpired(ctx, f.base.Add(2*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3", Generation: 0}); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m4", Generation: 0}); !wsstore.IsQuotaExceeded(err) {
			t.Fatalf("expiry removed more than one = %v", err)
		}
		f.clock.Advance(3 * time.Hour)
		pending, err := f.store.ListPending(ctx, "s1", 10)
		if err != nil || len(pending) != 1 || pending[0].MessageID != "m3" {
			t.Fatalf("expired pending = %+v, %v", pending, err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m4", Generation: 0}); err != nil {
			t.Fatalf("expired entry consumed capacity = %v", err)
		}
	})

	t.Run("idempotency reservation response and window", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		const contenders = 16
		var acquired atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, contenders)
		for index := 0; index < contenders; index++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound"})
				if ok {
					acquired.Add(1)
				}
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent begin = %v", err)
			}
		}
		if acquired.Load() != 1 {
			t.Fatalf("acquired = %d", acquired.Load())
		}
		response := json.RawMessage(`{"ok":true}`)
		if err := f.store.CompleteIdempotency(ctx, "s1", "inbound", response); err != nil {
			t.Fatal(err)
		}
		response[2] = 'X'
		record, err := f.store.GetIdempotency(ctx, "s1", "inbound")
		if err != nil || record.State != wsstore.IdempotencyStateCompleted || string(record.Response) != `{"ok":true}` {
			t.Fatalf("completed record = %+v, %v", record, err)
		}
		record.Response[2] = 'Y'
		record, _ = f.store.GetIdempotency(ctx, "s1", "inbound")
		if string(record.Response) != `{"ok":true}` {
			t.Fatalf("response aliased = %s", record.Response)
		}
		f.clock.Advance(2 * time.Minute)
		if _, err := f.store.GetIdempotency(ctx, "s1", "inbound"); !wsstore.IsNotFound(err) {
			t.Fatalf("expired get = %v", err)
		}
		_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound"})
		if err != nil || !ok {
			t.Fatalf("window reuse = %v, %v", ok, err)
		}
	})

	t.Run("idempotency expiration batches", func(t *testing.T) {
		f := newContractFixture(t, factory)
		ctx := context.Background()
		for index, id := range []string{"m1", "m2"} {
			_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: id, ExpiresAt: f.base.Add(time.Duration(index+1) * time.Hour)})
			if err != nil || !ok {
				t.Fatalf("begin %s = %v, %v", id, ok, err)
			}
		}
		if err := f.store.DeleteExpiredIdempotency(ctx, f.base.Add(2*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetIdempotency(ctx, "s1", "m1"); !wsstore.IsNotFound(err) {
			t.Fatalf("first expiry = %v", err)
		}
		if _, err := f.store.GetIdempotency(ctx, "s1", "m2"); err != nil {
			t.Fatalf("cleanup exceeded limit = %v", err)
		}
	})
}
