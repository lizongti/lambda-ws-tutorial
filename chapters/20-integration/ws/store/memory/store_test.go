package memory_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/memory"
)

func TestOfflineEvictOldestAndInputIsolation(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	s := memory.New(
		memory.WithClock(func() time.Time { return base }),
		memory.WithOfflineQueueLimit(2),
		memory.WithOfflineOverflowPolicy(wsstore.OfflineOverflowEvictOldest),
	)
	ctx := context.Background()
	if err := s.CreateSession(ctx, wsstore.Session{SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	envelope := json.RawMessage(`{"kind":"original"}`)
	if err := s.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Envelope: envelope}); err != nil {
		t.Fatal(err)
	}
	envelope[2] = 'X'
	pending, err := s.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || string(pending[0].Envelope) != `{"kind":"original"}` {
		t.Fatalf("Append retained caller envelope bytes: %+v, %v", pending, err)
	}
	for _, id := range []string{"m2", "m3"} {
		if err := s.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id}); err != nil {
			t.Fatal(err)
		}
	}
	pending, err = s.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 2 || pending[0].MessageID != "m2" || pending[0].Sequence != 2 || pending[1].MessageID != "m3" || pending[1].Sequence != 3 {
		t.Fatalf("evict-oldest queue = %+v, %v", pending, err)
	}
}

func TestConcurrentAppendAssignsUniqueOrderedSequences(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	const count = 64
	s := memory.New(
		memory.WithClock(func() time.Time { return base }),
		memory.WithOfflineQueueLimit(count),
	)
	ctx := context.Background()
	if err := s.CreateSession(ctx, wsstore.Session{SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- s.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: fmt.Sprintf("m-%02d", i)})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Append error = %v", err)
		}
	}
	pending, err := s.ListPending(ctx, "s1", count)
	if err != nil || len(pending) != count {
		t.Fatalf("ListPending length = %d, %v", len(pending), err)
	}
	seen := make(map[string]struct{}, count)
	for i, msg := range pending {
		if msg.Sequence != uint64(i+1) {
			t.Fatalf("message at index %d has sequence %d", i, msg.Sequence)
		}
		if _, exists := seen[msg.MessageID]; exists {
			t.Fatalf("duplicate message ID %q", msg.MessageID)
		}
		seen[msg.MessageID] = struct{}{}
	}
}

func TestConcurrentConnectionReadsAndMonotonicTouch(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	s := memory.New(memory.WithClock(func() time.Time { return base }))
	ctx := context.Background()
	if err := s.PutConnection(ctx, wsstore.Connection{ConnectionID: "c1", Generation: 1, LastSeenAt: base}); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const iterations = 100
	var wg sync.WaitGroup
	errs := make(chan error, workers*iterations*2)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				now := base.Add(time.Duration(worker*iterations+i) * time.Millisecond)
				if err := s.TouchConnection(ctx, "c1", 1, now); err != nil {
					errs <- err
				}
				if _, err := s.GetConnection(ctx, "c1"); err != nil {
					errs <- err
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent connection operation error = %v", err)
	}
	connection, err := s.GetConnection(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	latest := connection.LastSeenAt
	if err := s.TouchConnection(ctx, "c1", 1, base); err != nil {
		t.Fatal(err)
	}
	connection, _ = s.GetConnection(ctx, "c1")
	if !connection.LastSeenAt.Equal(latest) {
		t.Fatalf("older heartbeat moved LastSeenAt backwards: got %s, want %s", connection.LastSeenAt, latest)
	}
}

func TestConnectionFreshnessDeleteFencesTouchedStaleSnapshot(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	s := memory.New(memory.WithClock(func() time.Time { return base }))
	ctx := context.Background()
	for _, id := range []string{"touched", "untouched"} {
		if err := s.PutConnection(ctx, wsstore.Connection{ConnectionID: id, Generation: 1, LastSeenAt: base}); err != nil {
			t.Fatal(err)
		}
	}
	snapshots, err := s.ListStaleConnections(ctx, base.Add(time.Minute), 10)
	if err != nil || len(snapshots) != 2 {
		t.Fatalf("stale snapshots = %+v, %v", snapshots, err)
	}
	byID := make(map[string]wsstore.Connection, len(snapshots))
	for _, snapshot := range snapshots {
		byID[snapshot.ConnectionID] = snapshot
	}

	if err := s.TouchConnection(ctx, "touched", 1, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	touchedSnapshot := byID["touched"]
	if err := s.DeleteConnectionIfMatchAndLastSeen(ctx, touchedSnapshot.ConnectionID, touchedSnapshot.Generation, touchedSnapshot.LastSeenAt); !wsstore.IsConditionConflict(err) {
		t.Fatalf("delete after Touch error = %v, want condition conflict", err)
	}
	touched, err := s.GetConnection(ctx, "touched")
	if err != nil || !touched.LastSeenAt.Equal(base.Add(time.Second)) {
		t.Fatalf("touched connection after stale delete = %+v, %v", touched, err)
	}

	untouchedSnapshot := byID["untouched"]
	if err := s.DeleteConnectionIfMatchAndLastSeen(ctx, untouchedSnapshot.ConnectionID, untouchedSnapshot.Generation, untouchedSnapshot.LastSeenAt); err != nil {
		t.Fatalf("untouched stale delete error = %v", err)
	}
	if _, err := s.GetConnection(ctx, "untouched"); !wsstore.IsNotFound(err) {
		t.Fatalf("untouched stale connection remains: %v", err)
	}
	if err := s.DeleteConnectionIfMatchAndLastSeen(ctx, touched.ConnectionID, touched.Generation, touched.LastSeenAt); err != nil {
		t.Fatalf("fresh snapshot delete error = %v", err)
	}
}

func TestExpiredConnectionDeleteStillFencesGeneration(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	now := base
	s := memory.New(memory.WithClock(func() time.Time { return now }))
	ctx := context.Background()
	connection := wsstore.Connection{
		ConnectionID: "expired-generation", Generation: 1, ConnectedAt: base, LastSeenAt: base,
		ExpiresAt: base.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}
	if err := s.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	connection.Generation = 2
	if err := s.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	now = base.Add(2 * time.Hour)
	if err := s.DeleteConnectionIfMatch(ctx, connection.ConnectionID, 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("late generation delete error = %v, want condition conflict", err)
	}
	remaining, err := s.ListExpiredConnections(ctx, now, 10)
	if err != nil || len(remaining) != 1 || remaining[0].Generation != 2 {
		t.Fatalf("expired replacement after late delete = %+v, %v", remaining, err)
	}
	if err := s.DeleteConnectionIfMatch(ctx, connection.ConnectionID, 2); err != nil {
		t.Fatalf("matching expired generation delete error = %v", err)
	}
	remaining, err = s.ListExpiredConnections(ctx, now, 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expired connection was not physically deleted: %+v, %v", remaining, err)
	}
}

func TestExpiredSessionCleanupRemovesOwnedIndexes(t *testing.T) {
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	now := base
	s := memory.New(memory.WithClock(func() time.Time { return now }))
	ctx := context.Background()
	if err := s.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindConnection(ctx, "s1", "c1", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Join(ctx, wsstore.RoomMember{RoomID: "room", SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := s.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "inbound-1"}); err != nil || !acquired {
		t.Fatalf("BeginIdempotency = %v, %v", acquired, err)
	}
	now = base.Add(2 * time.Hour)
	if err := s.DeleteExpiredSessions(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSession(ctx, "s1"); !wsstore.IsNotFound(err) {
		t.Fatalf("expired session still exists: %v", err)
	}
	if pending, err := s.ListPending(ctx, "s1", 10); err != nil || len(pending) != 0 {
		t.Fatalf("expired session offline index remains: %+v, %v", pending, err)
	}
	if _, err := s.GetIdempotency(ctx, "s1", "inbound-1"); !wsstore.IsNotFound(err) {
		t.Fatalf("expired session idempotency index remains: %v", err)
	}
	if members, _, err := s.Members(ctx, "room", 10, ""); err != nil || len(members) != 0 {
		t.Fatalf("expired session room index remains: %+v, %v", members, err)
	}
}
