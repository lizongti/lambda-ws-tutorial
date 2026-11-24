package redis

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
	goredis "github.com/redis/go-redis/v9"
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

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type fixture struct {
	store *Store
	redis *miniredis.Miniredis
	clock *testClock
	base  time.Time
}

func newFixture(t *testing.T, options ...Option) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &testClock{now: base}
	allOptions := []Option{
		WithKeyPrefix("contract:"),
		WithClock(clock.Now),
		WithOfflineQueueLimit(2),
		WithOfflineOverflowPolicy(wsstore.OfflineOverflowRejectNew),
		WithIdempotencyWindow(time.Minute),
	}
	allOptions = append(allOptions, options...)
	return &fixture{
		store: New(client, allOptions...), redis: mr, clock: clock, base: base,
	}
}

func TestStateStoreErrorsEmptyResultsAndContext(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.store.GetConnection(ctx, "missing"); !wsstore.IsNotFound(err) || !errors.Is(err, wsstore.ErrNotFound) {
		t.Fatalf("GetConnection error = %v, want not-found", err)
	}
	if members, next, err := f.store.Members(ctx, "empty", 1, ""); err != nil || len(members) != 0 || next != "" {
		t.Fatalf("Members(empty) = %v, %q, %v", members, next, err)
	}
	if messages, err := f.store.ListPending(ctx, "missing", 10); err != nil || len(messages) != 0 {
		t.Fatalf("ListPending(missing) = %v, %v", messages, err)
	}
	for _, limit := range []int{0, -1, wsstore.MaxBatchSize + 1} {
		if _, _, err := f.store.Members(ctx, "room", limit, ""); !wsstore.IsInvalid(err) {
			t.Fatalf("Members(limit=%d) error = %v, want invalid", limit, err)
		}
		if err := f.store.DeleteExpired(ctx, f.base, limit); !wsstore.IsInvalid(err) {
			t.Fatalf("DeleteExpired(limit=%d) error = %v, want invalid", limit, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.store.GetSession(cancelled, "s1"); !wsstore.IsUnavailable(err) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled GetSession error = %v, want unavailable wrapping cancellation", err)
	}
	if err := f.store.PutConnection(cancelled, wsstore.Connection{ConnectionID: "c1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled PutConnection error = %v", err)
	}
}

func TestConnectionGenerationStaleIndexAndTTL(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	c := wsstore.Connection{
		ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "user", Generation: 1,
		ConnectedAt: f.base, LastSeenAt: f.base, ExpiresAt: f.base.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}
	if err := f.store.PutConnection(ctx, c); err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
	if ttl := f.redis.TTL(f.store.connectionKey("c1")); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("connection TTL = %s, want (0, 1h]", ttl)
	}
	if err := f.store.TouchConnection(ctx, "c1", 0, f.base.Add(time.Second)); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale Touch error = %v", err)
	}
	if err := f.store.TouchConnection(ctx, "c1", 1, f.base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := f.store.TouchConnection(ctx, "c1", 1, f.base); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.GetConnection(ctx, "c1")
	if err != nil || !got.LastSeenAt.Equal(f.base.Add(time.Second)) {
		t.Fatalf("monotonic Touch result = %+v, %v", got, err)
	}
	replacement := c
	replacement.Generation = 2
	replacement.LastSeenAt = f.base.Add(2 * time.Second)
	if err := f.store.PutConnection(ctx, replacement); err != nil {
		t.Fatalf("%v (cause: %v)", err, errors.Unwrap(err))
	}
	if err := f.store.PutConnection(ctx, c); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale PutConnection error = %v", err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale Delete error = %v", err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "c1", 2); err != nil {
		t.Fatalf("duplicate Delete error = %v", err)
	}
	for _, stale := range []wsstore.Connection{
		{ConnectionID: "b", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
		{ConnectionID: "c", Generation: 1, LastSeenAt: f.base.Add(-time.Second)},
		{ConnectionID: "a", Generation: 1, LastSeenAt: f.base.Add(-2 * time.Second)},
		{ConnectionID: "live", Generation: 1, LastSeenAt: f.base},
		{ConnectionID: "closed", Generation: 1, LastSeenAt: f.base.Add(-3 * time.Second), State: wsstore.ConnectionStateClosed},
	} {
		if err := f.store.PutConnection(ctx, stale); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.ListStaleConnections(ctx, f.base, 2)
	if err != nil || len(page) != 2 || page[0].ConnectionID != "a" || page[1].ConnectionID != "b" {
		t.Fatalf("first stale page = %+v, %v", page, err)
	}
	page, err = f.store.ListStaleConnections(ctx, f.base, 10)
	if err != nil || len(page) != 3 || page[2].ConnectionID != "c" {
		t.Fatalf("full stale page = %+v, %v", page, err)
	}
	f.redis.FastForward(2 * time.Hour)
	if _, err := f.store.GetConnection(ctx, "c1"); !wsstore.IsNotFound(err) {
		t.Fatalf("deleted connection error = %v", err)
	}
}

func TestConnectionFreshnessDeleteFencesTouchedStaleSnapshot(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, id := range []string{"touched", "untouched"} {
		if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: id, Generation: 1, LastSeenAt: f.base}); err != nil {
			t.Fatal(err)
		}
	}
	snapshots, err := f.store.ListStaleConnections(ctx, f.base.Add(time.Minute), 10)
	if err != nil || len(snapshots) != 2 {
		t.Fatalf("stale snapshots = %+v, %v", snapshots, err)
	}
	byID := make(map[string]wsstore.Connection, len(snapshots))
	for _, snapshot := range snapshots {
		byID[snapshot.ConnectionID] = snapshot
	}

	if err := f.store.TouchConnection(ctx, "touched", 1, f.base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	touchedSnapshot := byID["touched"]
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, touchedSnapshot.ConnectionID, touchedSnapshot.Generation, touchedSnapshot.LastSeenAt); !wsstore.IsConditionConflict(err) {
		t.Fatalf("delete after Touch error = %v, want condition conflict", err)
	}
	touched, err := f.store.GetConnection(ctx, "touched")
	if err != nil || !touched.LastSeenAt.Equal(f.base.Add(time.Second)) {
		t.Fatalf("touched connection after stale delete = %+v, %v", touched, err)
	}

	untouchedSnapshot := byID["untouched"]
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, untouchedSnapshot.ConnectionID, untouchedSnapshot.Generation, untouchedSnapshot.LastSeenAt); err != nil {
		t.Fatalf("untouched stale delete error = %v", err)
	}
	if _, err := f.store.GetConnection(ctx, "untouched"); !wsstore.IsNotFound(err) {
		t.Fatalf("untouched stale connection remains: %v", err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, touched.ConnectionID, touched.Generation, touched.LastSeenAt); err != nil {
		t.Fatalf("fresh snapshot delete error = %v", err)
	}
}

func TestConnectionExpirationIndexIsBoundedAndConditional(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
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
	page, err := f.store.ListExpiredConnections(ctx, cutoff, 1)
	if err != nil || len(page) != 1 || page[0].ConnectionID != "expired-a" {
		t.Fatalf("first expired connection page = %+v, %v", page, err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "expired-a", 1); err != nil {
		t.Fatal(err)
	}
	page, err = f.store.ListExpiredConnections(ctx, cutoff, 1)
	if err != nil || len(page) != 1 || page[0].ConnectionID != "expired-b" {
		t.Fatalf("second expired connection page = %+v, %v", page, err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "expired-b", 0); !wsstore.IsConditionConflict(err) {
		t.Fatalf("mismatched expired generation delete = %v", err)
	}
	if err := f.store.DeleteConnectionIfMatch(ctx, "expired-b", 1); err != nil {
		t.Fatal(err)
	}
	page, err = f.store.ListExpiredConnections(ctx, cutoff, 10)
	if err != nil || len(page) != 0 {
		t.Fatalf("live or deleted connections in expired page = %+v, %v", page, err)
	}
}

func TestSessionCASRetentionExpiryAndOwnedCleanup(t *testing.T) {
	f := newFixture(t)
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
			t.Fatalf("concurrent Bind error = %v", err)
		}
	}
	if successes != 1 || conflicts != contenders-1 {
		t.Fatalf("Bind successes/conflicts = %d/%d", successes, conflicts)
	}
	bound, err := f.store.GetSession(ctx, "s1")
	if err != nil || bound.Generation != 1 || bound.CurrentConnectionID == "" {
		t.Fatalf("bound session = %+v, %v", bound, err)
	}
	if err := f.store.UnbindConnectionIfMatch(ctx, "s1", "late", 0); !wsstore.IsConditionConflict(err) {
		t.Fatalf("late Unbind error = %v", err)
	}
	if err := f.store.UnbindConnectionIfMatch(ctx, "s1", bound.CurrentConnectionID, 1); err != nil {
		t.Fatal(err)
	}
	unbound, err := f.store.GetSession(ctx, "s1")
	if err != nil || unbound.CurrentConnectionID != "" || unbound.Generation != 1 {
		t.Fatalf("session was not retained after Unbind: %+v, %v", unbound, err)
	}
	if err := f.store.UnbindConnectionIfMatch(ctx, "s1", bound.CurrentConnectionID, 1); err != nil {
		t.Fatalf("duplicate Unbind error = %v", err)
	}
	rebound, err := f.store.BindConnection(ctx, "s1", "new", 1)
	if err != nil || rebound.Generation != 2 || rebound.CurrentConnectionID != "new" {
		t.Fatalf("rebind = %+v, %v", rebound, err)
	}

	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "room", SessionID: "s1", ConnectionID: "new", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "offline", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound"}); err != nil || !acquired {
		t.Fatalf("BeginIdempotency = %v, %v", acquired, err)
	}
	if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(2*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetSession(ctx, "s1"); !wsstore.IsNotFound(err) {
		t.Fatalf("expired session still exists: %v", err)
	}
	if pending, err := f.store.ListPending(ctx, "s1", 10); err != nil || len(pending) != 0 {
		t.Fatalf("owned offline state remains: %v, %v", pending, err)
	}
	if _, err := f.store.GetIdempotency(ctx, "s1", "inbound"); !wsstore.IsNotFound(err) {
		t.Fatalf("owned idempotency state remains: %v", err)
	}
	if members, _, err := f.store.Members(ctx, "room", 10, ""); err != nil || len(members) != 0 {
		t.Fatalf("owned room state remains: %v, %v", members, err)
	}
	if f.redis.Exists(f.store.offlineSequenceKey("s1")) || f.redis.Exists(f.store.sessionRoomsKey("s1")) || f.redis.Exists(f.store.sessionIdempotencyKey("s1")) {
		t.Fatal("session cleanup left secondary keys")
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
		t.Fatalf("first expiry batch deleted wrong session: %v", err)
	}
	if _, err := f.store.GetSession(ctx, "exp-b"); err != nil {
		t.Fatalf("expiry batch exceeded limit: %v", err)
	}
	f.clock.Advance(5 * time.Hour)
	if _, err := f.store.GetSession(ctx, "live"); !wsstore.IsNotFound(err) {
		t.Fatalf("logical expiry was not applied: %v", err)
	}
	if _, err := f.store.BindConnection(ctx, "live", "c", 0); !wsstore.IsNotFound(err) {
		t.Fatalf("expired Bind error = %v", err)
	}
}

func TestRoomIdempotencyGenerationCursorAndReverseCleanup(t *testing.T) {
	f := newFixture(t)
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
			t.Fatalf("idempotent Join error = %v", err)
		}
	}
	first, cursor, err := f.store.Members(ctx, "room", 2, "")
	if err != nil || len(first) != 2 || first[0].SessionID != "s1" || first[1].SessionID != "s2" || cursor != "s2" {
		t.Fatalf("first room page = %+v, cursor %q, %v", first, cursor, err)
	}
	if !first[0].JoinedAt.Equal(f.base) {
		t.Fatalf("Join changed JoinedAt: %s", first[0].JoinedAt)
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
		t.Fatalf("stale generation member returned: %+v, %v", members, err)
	}
	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "room", SessionID: "s2", ConnectionID: "c-new", Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RemoveConnection(ctx, "c-s2"); err != nil {
		t.Fatal(err)
	}
	members, _, _ = f.store.Members(ctx, "room", 10, "")
	if len(members) != 3 {
		t.Fatalf("old reverse index removed replacement member: %+v", members)
	}
	if err := f.store.RemoveConnection(ctx, "c-s1"); err != nil {
		t.Fatal(err)
	}
	members, _, _ = f.store.Members(ctx, "room", 10, "")
	if len(members) != 2 {
		t.Fatalf("RemoveConnection affected unrelated members: %+v", members)
	}
	if err := f.store.Leave(ctx, "room", "s3"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Leave(ctx, "room", "s3"); err != nil {
		t.Fatalf("duplicate Leave error = %v", err)
	}
}

func TestRoomSecondaryKeyTTLRefreshAndPhysicalExpiry(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BindConnection(ctx, "s1", "c1", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: "c1", SessionID: "s1", Generation: 1, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	member := wsstore.RoomMember{RoomID: "room", SessionID: "s1", ConnectionID: "c1", Generation: 1}
	if err := f.store.Join(ctx, member); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{f.store.roomMembersKey("room"), f.store.roomIndexKey("room"), f.store.connectionRoomsKey("c1"), f.store.sessionRoomsKey("s1")} {
		if ttl := f.redis.TTL(key); ttl <= 0 || ttl > time.Hour {
			t.Fatalf("initial member/reverse-index TTL for %q = %s, want (0, 1h]", key, ttl)
		}
	}

	f.redis.FastForward(30 * time.Minute)
	if err := f.store.Join(ctx, member); err != nil {
		t.Fatalf("Join TTL refresh error = %v", err)
	}
	for _, key := range []string{f.store.roomMembersKey("room"), f.store.roomIndexKey("room"), f.store.connectionRoomsKey("c1"), f.store.sessionRoomsKey("s1")} {
		if ttl := f.redis.TTL(key); ttl <= 0 || ttl > 30*time.Minute {
			t.Fatalf("refreshed member/reverse-index TTL for %q = %s, want (0, 30m]", key, ttl)
		}
	}

	f.redis.FastForward(31 * time.Minute)
	for _, key := range []string{f.store.roomMembersKey("room"), f.store.roomIndexKey("room"), f.store.connectionRoomsKey("c1"), f.store.sessionRoomsKey("s1")} {
		if f.redis.Exists(key) {
			t.Fatalf("key %q survived its member physical expiry", key)
		}
	}
}

func TestRoomSharedTTLDoesNotExpireLongerLivedMembers(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, item := range []struct {
		sessionID    string
		connectionID string
		lifetime     time.Duration
	}{
		{sessionID: "long", connectionID: "c-long", lifetime: 3 * time.Hour},
		{sessionID: "short", connectionID: "c-short", lifetime: time.Hour},
	} {
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: item.sessionID, ExpiresAt: f.base.Add(item.lifetime)}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.BindConnection(ctx, item.sessionID, item.connectionID, 0); err != nil {
			t.Fatal(err)
		}
		if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: item.connectionID, SessionID: item.sessionID, Generation: 1, ExpiresAt: f.base.Add(item.lifetime)}); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "shared", SessionID: item.sessionID, ConnectionID: item.connectionID, Generation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{f.store.roomMembersKey("shared"), f.store.roomIndexKey("shared")} {
		if ttl := f.redis.TTL(key); ttl <= 2*time.Hour || ttl > 3*time.Hour {
			t.Fatalf("shared room TTL after shorter Join = %s, want (2h, 3h]", ttl)
		}
	}

	f.redis.FastForward(30 * time.Minute)
	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "shared", SessionID: "short", ConnectionID: "c-short", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if ttl := f.redis.TTL(f.store.roomMembersKey("shared")); ttl <= 2*time.Hour {
		t.Fatalf("short member refresh shortened shared room TTL to %s", ttl)
	}
	f.redis.FastForward(31 * time.Minute)
	for _, key := range []string{f.store.connectionRoomsKey("c-short"), f.store.sessionRoomsKey("short")} {
		if f.redis.Exists(key) {
			t.Fatalf("short-lived reverse index %q did not physically expire", key)
		}
	}
	members, next, err := f.store.Members(ctx, "shared", 10, "")
	if err != nil || next != "" || len(members) != 1 || members[0].SessionID != "long" {
		t.Fatalf("members after short lifecycle expiry = %+v, next %q, %v", members, next, err)
	}
	for _, key := range []string{f.store.roomMembersKey("shared"), f.store.roomIndexKey("shared"), f.store.connectionRoomsKey("c-long"), f.store.sessionRoomsKey("long")} {
		if !f.redis.Exists(key) {
			t.Fatalf("long-lived room key %q expired with short-lived member", key)
		}
	}

	f.redis.FastForward(2 * time.Hour)
	for _, key := range []string{f.store.roomMembersKey("shared"), f.store.roomIndexKey("shared"), f.store.connectionRoomsKey("c-long"), f.store.sessionRoomsKey("long")} {
		if f.redis.Exists(key) {
			t.Fatalf("room key %q survived the longest member lifecycle", key)
		}
	}
}

func TestRoomNonExpiringMemberPersistsPreviouslyFiniteSharedKeys(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "finite", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BindConnection(ctx, "finite", "c-finite", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: "c-finite", SessionID: "finite", Generation: 1, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "mixed", SessionID: "finite", ConnectionID: "c-finite", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{f.store.roomMembersKey("mixed"), f.store.roomIndexKey("mixed")} {
		if ttl := f.redis.TTL(key); ttl <= 0 || ttl > time.Hour {
			t.Fatalf("finite member shared TTL for %q = %s, want (0, 1h]", key, ttl)
		}
	}

	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "permanent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BindConnection(ctx, "permanent", "c-permanent", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: "c-permanent", SessionID: "permanent", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "mixed", SessionID: "permanent", ConnectionID: "c-permanent", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{f.store.roomMembersKey("mixed"), f.store.roomIndexKey("mixed")} {
		ttl, err := f.store.client.PTTL(ctx, key).Result()
		if err != nil || ttl != -1 {
			t.Fatalf("shared TTL after non-expiring Join for %q = %s, %v; want no expiration", key, ttl, err)
		}
	}

	f.redis.FastForward(2 * time.Hour)
	members, next, err := f.store.Members(ctx, "mixed", 10, "")
	if err != nil || next != "" || len(members) != 1 || members[0].SessionID != "permanent" {
		t.Fatalf("members after finite lifecycle expiry = %+v, next %q, %v", members, next, err)
	}
	for _, key := range []string{f.store.roomMembersKey("mixed"), f.store.roomIndexKey("mixed")} {
		if !f.redis.Exists(key) {
			t.Fatalf("shared room key %q expired with non-expiring member", key)
		}
	}
}

func TestRoomLeaveAndRemoveConnectionDeleteEmptyIndexes(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BindConnection(ctx, "s1", "c1", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: "c1", SessionID: "s1", Generation: 1, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	for _, roomID := range []string{"room-a", "room-b"} {
		if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: roomID, SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.Leave(ctx, "room-a", "s1"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{f.store.roomMembersKey("room-a"), f.store.roomIndexKey("room-a")} {
		if f.redis.Exists(key) {
			t.Fatalf("Leave left empty room key %q", key)
		}
	}
	if !f.redis.Exists(f.store.sessionRoomsKey("s1")) || !f.redis.Exists(f.store.connectionRoomsKey("c1")) {
		t.Fatal("Leave removed reverse indexes that still reference room-b")
	}
	if err := f.store.RemoveConnection(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{f.store.roomMembersKey("room-b"), f.store.roomIndexKey("room-b"), f.store.sessionRoomsKey("s1"), f.store.connectionRoomsKey("c1")} {
		if f.redis.Exists(key) {
			t.Fatalf("RemoveConnection left empty secondary key %q", key)
		}
	}
}

func TestOfflineSequenceSentAckAndRejectNew(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	envelope := json.RawMessage(`{"value":"original"}`)
	for _, id := range []string{"m1", "m2"} {
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, Sequence: 999, Envelope: envelope, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	envelope[2] = 'X'
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3"}); !wsstore.IsQuotaExceeded(err) {
		t.Fatalf("queue overflow error = %v", err)
	}
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1"}); !wsstore.IsConditionConflict(err) {
		t.Fatalf("duplicate message ID error = %v", err)
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
		t.Fatalf("sent pending state = %+v, %v", pending, err)
	}
	if string(pending[0].Envelope) != `{"value":"original"}` {
		t.Fatalf("Append retained caller envelope: %s", pending[0].Envelope)
	}
	originalSentAt := *pending[0].SentAt
	pending[0].Envelope[2] = 'Y'
	changed := f.base.Add(10 * time.Hour)
	pending[0].SentAt = &changed
	f.clock.Advance(time.Minute)
	if err := f.store.MarkSent(ctx, "s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	pending, _ = f.store.ListPending(ctx, "s1", 1)
	if string(pending[0].Envelope) != `{"value":"original"}` || !pending[0].SentAt.Equal(originalSentAt) {
		t.Fatalf("returned message aliases state or duplicate MarkSent changed SentAt: %+v", pending[0])
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
		t.Fatalf("pending order = %+v, %v", pending, err)
	}
}

func TestOfflineEvictOldestAndConcurrentSequences(t *testing.T) {
	t.Run("evict oldest", func(t *testing.T) {
		f := newFixture(t, WithOfflineOverflowPolicy(wsstore.OfflineOverflowEvictOldest))
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1"}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"m1", "m2", "m3"} {
			if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id}); err != nil {
				t.Fatal(err)
			}
		}
		pending, err := f.store.ListPending(ctx, "s1", 10)
		if err != nil || len(pending) != 2 || pending[0].MessageID != "m2" || pending[0].Sequence != 2 || pending[1].Sequence != 3 {
			t.Fatalf("evict-oldest queue = %+v, %v", pending, err)
		}
	})

	t.Run("concurrent append", func(t *testing.T) {
		const count = 64
		f := newFixture(t, WithOfflineQueueLimit(count))
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1"}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, count)
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs <- f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: fmt.Sprintf("m-%02d", i)})
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent Append error = %v", err)
			}
		}
		pending, err := f.store.ListPending(ctx, "s1", count)
		if err != nil || len(pending) != count {
			t.Fatalf("concurrent pending length = %d, %v", len(pending), err)
		}
		seen := make(map[string]struct{}, count)
		for i, msg := range pending {
			if msg.Sequence != uint64(i+1) {
				t.Fatalf("message %d sequence = %d", i, msg.Sequence)
			}
			if _, ok := seen[msg.MessageID]; ok {
				t.Fatalf("duplicate message %q", msg.MessageID)
			}
			seen[msg.MessageID] = struct{}{}
		}
	})
}

func TestOfflineExpirationAndPhysicalTTL(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"m1", "m2"} {
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	if ttl := f.redis.TTL(f.store.offlineMessageKey("s1", "m1")); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("offline message TTL = %s", ttl)
	}
	if err := f.store.DeleteExpired(ctx, f.base.Add(2*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m3"}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m4"}); !wsstore.IsQuotaExceeded(err) {
		t.Fatalf("expiry batch removed more than one entry: %v", err)
	}
	f.clock.Advance(3 * time.Hour)
	pending, err := f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].MessageID != "m3" {
		t.Fatalf("expired messages returned: %+v, %v", pending, err)
	}
	if f.redis.Exists(f.store.offlineMessageKey("s1", "m2")) {
		t.Fatal("expired offline body was not removed")
	}
}

func TestIdempotencyAtomicReservationResponseTTLAndBatches(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	const contenders = 16
	var acquired atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
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
			t.Fatalf("concurrent BeginIdempotency error = %v", err)
		}
	}
	if acquired.Load() != 1 {
		t.Fatalf("reservations acquired = %d", acquired.Load())
	}
	if ttl := f.redis.TTL(f.store.idempotencyKey("s1", "inbound")); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("idempotency TTL = %s", ttl)
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
		t.Fatalf("response aliases stored bytes: %s", record.Response)
	}
	f.clock.Advance(2 * time.Minute)
	if _, err := f.store.GetIdempotency(ctx, "s1", "inbound"); !wsstore.IsNotFound(err) {
		t.Fatalf("expired GetIdempotency error = %v", err)
	}
	if _, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound"}); err != nil || !ok {
		t.Fatalf("message ID was not reusable: acquired=%v, %v", ok, err)
	}
	if err := f.store.DeleteExpiredIdempotency(ctx, f.base.Add(4*time.Minute), 10); err != nil {
		t.Fatal(err)
	}

	for i, id := range []string{"m1", "m2"} {
		if _, ok, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "batch", MessageID: id, ExpiresAt: f.base.Add(time.Duration(i+1) * time.Hour)}); err != nil || !ok {
			t.Fatalf("BeginIdempotency(%s) = %v, %v", id, ok, err)
		}
	}
	if err := f.store.DeleteExpiredIdempotency(ctx, f.base.Add(2*time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetIdempotency(ctx, "batch", "m1"); !wsstore.IsNotFound(err) {
		t.Fatalf("first expired idempotency record remains: %v", err)
	}
	if _, err := f.store.GetIdempotency(ctx, "batch", "m2"); err != nil {
		t.Fatalf("idempotency cleanup exceeded limit: %v", err)
	}
}

func TestPhysicalExpiryMaintenanceAndStaleIndexCleanup(t *testing.T) {
	t.Run("session secondary state", func(t *testing.T) {
		f := newFixture(t)
		ctx := context.Background()
		if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.BindConnection(ctx, "s1", "c1", 0); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "room", SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1}); err != nil {
			t.Fatal(err)
		}
		if _, acquired, err := f.store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound"}); err != nil || !acquired {
			t.Fatalf("BeginIdempotency = %v, %v", acquired, err)
		}
		f.redis.FastForward(2 * time.Hour)
		if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(2*time.Hour), 10); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{
			f.store.sessionRoomsKey("s1"), f.store.connectionRoomsKey("c1"), f.store.offlineQueueKey("s1"),
			f.store.offlineSequenceKey("s1"), f.store.offlineMessageKey("s1", "m1"), f.store.sessionIdempotencyKey("s1"),
			f.store.roomMembersKey("room"), f.store.roomIndexKey("room"), f.store.offlineExpiryKey(),
			f.store.idempotencyExpiryKey(), f.store.sessionExpiryKey(),
		} {
			if f.redis.Exists(key) {
				t.Fatalf("physical session expiry left key %q", key)
			}
		}
	})

	t.Run("stale connection index", func(t *testing.T) {
		f := newFixture(t)
		ctx := context.Background()
		for _, id := range []string{"a", "b", "c"} {
			if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: id, LastSeenAt: f.base, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
		f.redis.FastForward(2 * time.Hour)
		if _, err := f.store.ListStaleConnections(ctx, f.base.Add(time.Second), 2); err != nil {
			t.Fatal(err)
		}
		count, err := f.store.client.ZCard(ctx, f.store.staleConnectionsKey()).Result()
		if err != nil || count != 0 {
			t.Fatalf("orphaned stale index entries = %d, %v", count, err)
		}
	})
}

func TestKeyPrefixAndCorruptDataErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.store.PutConnection(ctx, wsstore.Connection{ConnectionID: "c1"}); err != nil {
		t.Fatal(err)
	}
	if !f.redis.Exists("contract:connection:c1") || f.redis.Exists(DefaultKeyPrefix+"connection:c1") {
		t.Fatal("WithKeyPrefix was not applied to connection keys")
	}
	if err := f.redis.Set("contract:session:broken", "not-json"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.GetSession(ctx, "broken"); wsstore.ErrorCodeOf(err) != wsstore.ErrorCodeUnknown {
		t.Fatalf("corrupt session error = %v, want unknown data error", err)
	}
}
