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

	"github.com/alicebob/miniredis/v2"
	wsstore "github.com/aura-studio/lambda/ws/store"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
	"github.com/aura-studio/lambda/ws/store/memory"
	redisstore "github.com/aura-studio/lambda/ws/store/redis"
	goredis "github.com/redis/go-redis/v9"
)

type wsStoreTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *wsStoreTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *wsStoreTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type wsStoreTestIDGenerator struct {
	next atomic.Uint64
}

func (g *wsStoreTestIDGenerator) ID(prefix string) string {
	return fmt.Sprintf("%s-%04d", prefix, g.next.Add(1))
}

type wsStoreContractFactory func(t *testing.T, clock *wsStoreTestClock, ids *wsStoreTestIDGenerator) wsstore.StateStore

type wsStoreFixture struct {
	store wsstore.StateStore
	clock *wsStoreTestClock
	ids   *wsStoreTestIDGenerator
	base  time.Time
}

func newWSStoreFixture(t *testing.T, factory wsStoreContractFactory) wsStoreFixture {
	t.Helper()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &wsStoreTestClock{now: base}
	ids := &wsStoreTestIDGenerator{}
	return wsStoreFixture{store: factory(t, clock, ids), clock: clock, ids: ids, base: base}
}

func TestMemoryStateStoreContract(t *testing.T) {
	runWSStoreContract(t, func(t *testing.T, clock *wsStoreTestClock, ids *wsStoreTestIDGenerator) wsstore.StateStore {
		t.Helper()
		return memory.New(
			memory.WithClock(clock.Now),
			memory.WithOfflineQueueLimit(2),
			memory.WithOfflineOverflowPolicy(wsstore.OfflineOverflowRejectNew),
			memory.WithIdempotencyWindow(time.Minute),
		)
	})
}

func TestDynamoDBStateStoreContract(t *testing.T) {
	runWSStoreContract(t, func(t *testing.T, clock *wsStoreTestClock, ids *wsStoreTestIDGenerator) wsstore.StateStore {
		t.Helper()
		return dynamodbstore.New(
			dynamodbtest.New(),
			"ws-contract-test",
			dynamodbstore.WithClock(clock.Now),
			dynamodbstore.WithOfflineQueueLimit(2),
			dynamodbstore.WithOfflineOverflowPolicy(wsstore.OfflineOverflowRejectNew),
			dynamodbstore.WithIdempotencyWindow(time.Minute),
		)
	})
}

func TestRedisStateStoreContract(t *testing.T) {
	runWSStoreContract(t, func(t *testing.T, clock *wsStoreTestClock, ids *wsStoreTestIDGenerator) wsstore.StateStore {
		t.Helper()
		mr := miniredis.RunT(t)
		client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		return redisstore.New(
			client,
			redisstore.WithClock(clock.Now),
			redisstore.WithOfflineQueueLimit(2),
			redisstore.WithOfflineOverflowPolicy(wsstore.OfflineOverflowRejectNew),
			redisstore.WithIdempotencyWindow(time.Minute),
		)
	})
}

// runWSStoreContract runs the same observable contract against every backend.
// DynamoDB is an in-memory SDK fake and Redis is miniredis, not live services.
func runWSStoreContract(t *testing.T, factory wsStoreContractFactory) {
	t.Helper()
	t.Run("errors and empty results", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		_, err := f.store.GetConnection(ctx, "missing")
		if !wsstore.IsNotFound(err) || !errors.Is(err, wsstore.ErrNotFound) {
			t.Fatalf("GetConnection error = %v, want classified not-found", err)
		}
		if members, next, err := f.store.Members(ctx, "empty", 1, ""); err != nil || len(members) != 0 || next != "" {
			t.Fatalf("Members(empty) = %v, %q, %v", members, next, err)
		}
		if messages, err := f.store.ListPending(ctx, "missing", 10); err != nil || len(messages) != 0 {
			t.Fatalf("ListPending(missing) = %v, %v", messages, err)
		}
		if _, _, err := f.store.Members(ctx, "room", 0, ""); !wsstore.IsInvalid(err) {
			t.Fatalf("Members(limit=0) error = %v, want invalid", err)
		}
		if _, _, err := f.store.Members(ctx, "room", wsstore.MaxBatchSize+1, ""); !wsstore.IsInvalid(err) {
			t.Fatalf("Members(over-limit) error = %v, want invalid", err)
		}
		unavailable := wsstore.NewError(wsstore.ErrorCodeUnavailable, "test", errors.New("backend detail"))
		if !wsstore.IsUnavailable(unavailable) || !wsstore.IsRetryable(unavailable) {
			t.Fatalf("unavailable classification failed: %v", unavailable)
		}
		if wsstore.IsRetryable(wsstore.NewError(wsstore.ErrorCodeInvalid, "test", nil)) {
			t.Fatal("invalid error must not be retryable")
		}
	})

	t.Run("connection generations and stale pages", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		c := wsstore.Connection{
			ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "user", Generation: 1,
			ConnectedAt: f.base, LastSeenAt: f.base, ExpiresAt: f.base.Add(time.Hour), State: wsstore.ConnectionStateActive,
		}
		if err := f.store.PutConnection(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := f.store.TouchConnection(ctx, "c1", 0, f.base.Add(time.Second)); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale touch error = %v", err)
		}
		if err := f.store.TouchConnection(ctx, "c1", 1, f.base.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		got, err := f.store.GetConnection(ctx, "c1")
		if err != nil || !got.LastSeenAt.Equal(f.base.Add(time.Second)) {
			t.Fatalf("GetConnection after touch = %+v, %v", got, err)
		}
		replacement := c
		replacement.Generation = 2
		replacement.LastSeenAt = f.base.Add(2 * time.Second)
		if err := f.store.PutConnection(ctx, replacement); err != nil {
			t.Fatal(err)
		}
		if err := f.store.PutConnection(ctx, c); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale PutConnection error = %v", err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 1); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale delete error = %v", err)
		}
		got, _ = f.store.GetConnection(ctx, "c1")
		if got.Generation != 2 {
			t.Fatalf("replacement generation = %d, want 2", got.Generation)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
			t.Fatal(err)
		}
		if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
			t.Fatalf("duplicate delete error = %v", err)
		}

		for _, stale := range []wsstore.Connection{
			{ConnectionID: "b", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
			{ConnectionID: "c", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
			{ConnectionID: "a", Generation: 1, LastSeenAt: f.base.Add(-2 * time.Second)},
			{ConnectionID: "live", Generation: 1, LastSeenAt: f.base},
		} {
			if err := f.store.PutConnection(ctx, stale); err != nil {
				t.Fatal(err)
			}
		}
		page, err := f.store.ListStaleConnections(ctx, f.base, 2)
		if err != nil || len(page) != 2 || page[0].ConnectionID != "a" || page[1].ConnectionID != "b" {
			t.Fatalf("ListStaleConnections page = %+v, %v", page, err)
		}
		page, err = f.store.ListStaleConnections(ctx, f.base, 10)
		if err != nil || len(page) != 3 || page[2].ConnectionID != "c" {
			t.Fatalf("ListStaleConnections full page = %+v, %v", page, err)
		}
	})

	t.Run("session CAS unbind and expiry", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		session := wsstore.Session{SessionID: "s1", PrincipalSubject: "user", ResumeCredentialHash: "sha256:hash", ExpiresAt: f.base.Add(time.Hour)}
		if err := f.store.CreateSession(ctx, session); err != nil {
			t.Fatal(err)
		}
		if err := f.store.CreateSession(ctx, session); !wsstore.IsConditionConflict(err) {
			t.Fatalf("duplicate CreateSession error = %v", err)
		}

		const contenders = 16
		var wg sync.WaitGroup
		errs := make(chan error, contenders)
		for i := 0; i < contenders; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := f.store.BindConnection(ctx, "s1", fmt.Sprintf("c-%d", i), 0)
				errs <- err
			}(i)
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
				t.Fatalf("concurrent BindConnection error = %v", err)
			}
		}
		if successes != 1 || conflicts != contenders-1 {
			t.Fatalf("concurrent bind successes/conflicts = %d/%d", successes, conflicts)
		}
		bound, err := f.store.GetSession(ctx, "s1")
		if err != nil || bound.Generation != 1 || bound.CurrentConnectionID == "" {
			t.Fatalf("bound session = %+v, %v", bound, err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", "old-connection", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("late unbind error = %v", err)
		}
		bound, _ = f.store.GetSession(ctx, "s1")
		if bound.CurrentConnectionID == "" || bound.Generation != 1 {
			t.Fatalf("late unbind changed binding: %+v", bound)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", bound.CurrentConnectionID, 1); err != nil {
			t.Fatal(err)
		}
		rebound, err := f.store.BindConnection(ctx, "s1", "new-connection", 1)
		if err != nil || rebound.Generation != 2 || rebound.CurrentConnectionID != "new-connection" {
			t.Fatalf("rebind = %+v, %v", rebound, err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", "new-connection", 2); err != nil {
			t.Fatal(err)
		}
		if err := f.store.UnbindConnectionIfMatch(ctx, "s1", "new-connection", 2); err != nil {
			t.Fatalf("duplicate UnbindConnectionIfMatch error = %v", err)
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
			t.Fatalf("first expired session was not deleted: %v", err)
		}
		if _, err := f.store.GetSession(ctx, "exp-b"); err != nil {
			t.Fatalf("expiry batch exceeded limit: %v", err)
		}
		if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(3*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetSession(ctx, "live"); err != nil {
			t.Fatalf("live session deleted: %v", err)
		}
		f.clock.Advance(5 * time.Hour)
		if _, err := f.store.GetSession(ctx, "live"); !wsstore.IsNotFound(err) {
			t.Fatalf("expired GetSession error = %v", err)
		}
		if _, err := f.store.BindConnection(ctx, "live", "c", 0); !wsstore.IsNotFound(err) {
			t.Fatalf("expired BindConnection error = %v", err)
		}
	})

	t.Run("room idempotency generation and cursors", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
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
			if err := f.store.Join(ctx, member); err != nil {
				t.Fatalf("idempotent Join error = %v", err)
			}
		}
		first, cursor, err := f.store.Members(ctx, "room", 2, "")
		if err != nil || len(first) != 2 || first[0].SessionID != "s1" || first[1].SessionID != "s2" || cursor != "s2" {
			t.Fatalf("first room page = %+v, cursor %q, %v", first, cursor, err)
		}
		second, next, err := f.store.Members(ctx, "room", 2, cursor)
		if err != nil || len(second) != 1 || second[0].SessionID != "s3" || next != "" {
			t.Fatalf("second room page = %+v, cursor %q, %v", second, next, err)
		}
		if _, err := f.store.BindConnection(ctx, "s2", "c-new", 1); err != nil {
			t.Fatal(err)
		}
		members, _, err := f.store.Members(ctx, "room", 10, "")
		if err != nil || len(members) != 2 {
			t.Fatalf("stale-generation member was returned: %+v, %v", members, err)
		}
		if err := f.store.RemoveConnection(ctx, "c-s1"); err != nil {
			t.Fatal(err)
		}
		members, _, _ = f.store.Members(ctx, "room", 10, "")
		if len(members) != 1 || members[0].SessionID != "s3" {
			t.Fatalf("RemoveConnection affected unrelated members: %+v", members)
		}
		if err := f.store.Leave(ctx, "room", "s3"); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Leave(ctx, "room", "s3"); err != nil {
			t.Fatalf("duplicate Leave error = %v", err)
		}
	})

	t.Run("room binding CAS fences delayed join and leave", func(t *testing.T) {
		for _, operation := range []string{"join", "leave"} {
			t.Run(operation, func(t *testing.T) {
				f := newWSStoreFixture(t, factory)
				ctx := context.Background()
				bindingStore, ok := f.store.(wsstore.RoomBindingStore)
				if !ok {
					t.Fatal("built-in store does not implement RoomBindingStore")
				}
				if err := f.store.CreateSession(ctx, wsstore.Session{
					SessionID: "s1", PrincipalSubject: "user", LastActiveAt: f.base, ExpiresAt: f.base.Add(time.Hour),
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.BindConnection(ctx, "s1", "c-old", 0); err != nil {
					t.Fatal(err)
				}
				oldMember := wsstore.RoomMember{
					RoomID: "room", SessionID: "s1", ConnectionID: "c-old", Generation: 1, JoinedAt: f.base,
				}
				if operation == "leave" {
					if err := bindingStore.JoinIfMatch(ctx, oldMember); err != nil {
						t.Fatalf("initial JoinIfMatch error = %v", err)
					}
				}

				// The old operation has captured c-old/1. Reconnect and complete the
				// replacement Join before allowing that old operation to resume.
				if _, err := f.store.BindConnection(ctx, "s1", "c-new", 1); err != nil {
					t.Fatal(err)
				}
				newMember := wsstore.RoomMember{
					RoomID: "room", SessionID: "s1", ConnectionID: "c-new", Generation: 2, JoinedAt: f.base.Add(time.Second),
				}
				if err := bindingStore.JoinIfMatch(ctx, newMember); err != nil {
					t.Fatalf("replacement JoinIfMatch error = %v", err)
				}

				var err error
				if operation == "join" {
					err = bindingStore.JoinIfMatch(ctx, oldMember)
				} else {
					err = bindingStore.LeaveIfMatch(ctx, "room", "s1", "c-old", 1)
				}
				if !wsstore.IsConditionConflict(err) {
					t.Fatalf("delayed %s error = %v, want condition conflict", operation, err)
				}
				members, next, err := f.store.Members(ctx, "room", 10, "")
				if err != nil || next != "" || len(members) != 1 ||
					members[0].ConnectionID != "c-new" || members[0].Generation != 2 {
					t.Fatalf("delayed %s damaged replacement member = %+v, next %q, %v", operation, members, next, err)
				}

				if err := bindingStore.LeaveIfMatch(ctx, "room", "s1", "c-new", 2); err != nil {
					t.Fatalf("current LeaveIfMatch error = %v", err)
				}
				if err := bindingStore.LeaveIfMatch(ctx, "room", "s1", "c-new", 2); err != nil {
					t.Fatalf("idempotent LeaveIfMatch with absent member error = %v", err)
				}
			})
		}
	})

	t.Run("offline sequence retry ack and overflow", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"m1", "m2"} {
			msg := wsstore.OfflineMessage{SessionID: "s1", MessageID: id, Sequence: 999, Generation: 0, Envelope: json.RawMessage(`{"value":"original"}`), ExpiresAt: f.base.Add(time.Hour)}
			if err := f.store.Append(ctx, msg); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3", Generation: 0}); !wsstore.IsQuotaExceeded(err) {
			t.Fatalf("queue overflow error = %v", err)
		}
		if _, err := f.store.BindConnection(ctx, "s1", "c1", 0); err != nil {
			t.Fatal(err)
		}
		if err := f.store.MarkSent(ctx, "s1", "m1", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale MarkSent error = %v", err)
		}
		if err := f.store.MarkSent(ctx, "s1", "m1", 1); err != nil {
			t.Fatal(err)
		}
		pending, err := f.store.ListPending(ctx, "s1", 1)
		if err != nil || len(pending) != 1 || pending[0].MessageID != "m1" || pending[0].Sequence != 1 || pending[0].State != wsstore.OfflineMessageStateSent || pending[0].Generation != 1 || pending[0].SentAt == nil {
			t.Fatalf("sent message pending state = %+v, %v", pending, err)
		}
		originalSentAt := *pending[0].SentAt
		pending[0].Envelope[2] = 'X'
		changed := f.base.Add(10 * time.Hour)
		pending[0].SentAt = &changed
		pending, _ = f.store.ListPending(ctx, "s1", 1)
		if string(pending[0].Envelope) != `{"value":"original"}` || !pending[0].SentAt.Equal(originalSentAt) {
			t.Fatalf("returned offline message aliases stored state: %+v", pending[0])
		}
		if err := f.store.Ack(ctx, "s1", "m1", 0); !wsstore.IsConditionConflict(err) {
			t.Fatalf("stale Ack error = %v", err)
		}
		if err := f.store.Ack(ctx, "s1", "m1", 1); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Ack(ctx, "s1", "m1", 1); err != nil {
			t.Fatalf("duplicate Ack error = %v", err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3", Generation: 1}); err != nil {
			t.Fatal(err)
		}
		pending, err = f.store.ListPending(ctx, "s1", 10)
		if err != nil || len(pending) != 2 || pending[0].MessageID != "m2" || pending[0].Sequence != 2 || pending[1].MessageID != "m3" || pending[1].Sequence != 3 {
			t.Fatalf("pending order after ack/append = %+v, %v", pending, err)
		}
	})

	t.Run("offline expiration batches", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
			t.Fatal(err)
		}
		for i, id := range []string{"m1", "m2"} {
			if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, Generation: 0, ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour)}); err != nil {
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
			t.Fatalf("expiry batch removed more than one entry: %v", err)
		}
		f.clock.Advance(3 * time.Hour)
		pending, err := f.store.ListPending(ctx, "s1", 10)
		// m4 was rejected and m2 is expired, leaving only m3.
		if err != nil || len(pending) != 1 || pending[0].MessageID != "m3" {
			t.Fatalf("expired messages returned: %+v, %v", pending, err)
		}
	})

	t.Run("idempotency reservation response and window", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		messageID := f.ids.ID("inbound")
		const contenders = 16
		var acquired atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, contenders)
		for i := 0; i < contenders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: messageID})
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
				t.Fatalf("concurrent BeginIdempotency error = %v", err)
			}
		}
		if acquired.Load() != 1 {
			t.Fatalf("idempotency reservations acquired = %d, want 1", acquired.Load())
		}
		response := json.RawMessage(`{"ok":true}`)
		if err := f.store.CompleteIdempotency(ctx, "s1", messageID, response); err != nil {
			t.Fatal(err)
		}
		response[2] = 'X'
		record, err := f.store.GetIdempotency(ctx, "s1", messageID)
		if err != nil || record.State != wsstore.IdempotencyStateCompleted || string(record.Response) != `{"ok":true}` {
			t.Fatalf("completed idempotency record = %+v, %v", record, err)
		}
		record.Response[2] = 'Y'
		record, _ = f.store.GetIdempotency(ctx, "s1", messageID)
		if string(record.Response) != `{"ok":true}` {
			t.Fatalf("idempotency response aliases stored bytes: %s", record.Response)
		}
		f.clock.Advance(2 * time.Minute)
		if _, err := f.store.GetIdempotency(ctx, "s1", messageID); !wsstore.IsNotFound(err) {
			t.Fatalf("expired idempotency Get error = %v", err)
		}
		_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: messageID})
		if err != nil || !ok {
			t.Fatalf("message ID was not reusable after its window: acquired=%v, %v", ok, err)
		}
	})

	t.Run("idempotency expiration batches", func(t *testing.T) {
		f := newWSStoreFixture(t, factory)
		ctx := context.Background()
		for i, id := range []string{"m1", "m2"} {
			_, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: id, ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour)})
			if err != nil || !ok {
				t.Fatalf("BeginIdempotency(%s) = %v, %v", id, ok, err)
			}
		}
		if err := f.store.DeleteExpiredIdempotency(ctx, f.base.Add(2*time.Hour), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.GetIdempotency(ctx, "s1", "m1"); !wsstore.IsNotFound(err) {
			t.Fatalf("first idempotency record was not deleted: %v", err)
		}
		if _, err := f.store.GetIdempotency(ctx, "s1", "m2"); err != nil {
			t.Fatalf("idempotency cleanup exceeded limit: %v", err)
		}
	})
}
