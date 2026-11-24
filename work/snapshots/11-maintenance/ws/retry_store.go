package ws

import (
	"context"
	"encoding/json"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

type retryingStateStore struct {
	delegate wsstore.StateStore
	options  *Options
}

func (s *retryingStateStore) retry(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 0; attempt <= s.options.Push.MaxRetries; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		err = operation()
		if !wsstore.IsRetryable(err) || attempt == s.options.Push.MaxRetries {
			return err
		}
		if sleepErr := s.options.RetrySleeper(ctx, retryDelay(s.options.Push, attempt+1)); sleepErr != nil {
			return sleepErr
		}
	}
	return err
}

func retryStoreValue[T any](ctx context.Context, store *retryingStateStore, operation func() (T, error)) (T, error) {
	var value T
	err := store.retry(ctx, func() error {
		var err error
		value, err = operation()
		return err
	})
	return value, err
}

func (s *retryingStateStore) PutConnection(ctx context.Context, connection wsstore.Connection) error {
	return s.retry(ctx, func() error { return s.delegate.PutConnection(ctx, connection) })
}

func (s *retryingStateStore) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	return retryStoreValue(ctx, s, func() (wsstore.Connection, error) { return s.delegate.GetConnection(ctx, id) })
}

func (s *retryingStateStore) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	return s.retry(ctx, func() error { return s.delegate.DeleteConnectionIfMatch(ctx, id, generation) })
}

func (s *retryingStateStore) DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error {
	freshnessStore, ok := s.delegate.(wsstore.ConnectionFreshnessStore)
	if !ok {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, "DeleteConnectionIfMatchAndLastSeen", nil)
	}
	return s.retry(ctx, func() error {
		return freshnessStore.DeleteConnectionIfMatchAndLastSeen(ctx, id, generation, lastSeen)
	})
}

func (s *retryingStateStore) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	return s.retry(ctx, func() error { return s.delegate.TouchConnection(ctx, id, generation, now) })
}

func (s *retryingStateStore) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	return retryStoreValue(ctx, s, func() ([]wsstore.Connection, error) {
		return s.delegate.ListStaleConnections(ctx, before, limit)
	})
}

func (s *retryingStateStore) ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	expirationStore, ok := s.delegate.(wsstore.ConnectionExpirationStore)
	if !ok {
		return []wsstore.Connection{}, nil
	}
	return retryStoreValue(ctx, s, func() ([]wsstore.Connection, error) {
		return expirationStore.ListExpiredConnections(ctx, before, limit)
	})
}

func (s *retryingStateStore) CreateSession(ctx context.Context, session wsstore.Session) error {
	return s.retry(ctx, func() error { return s.delegate.CreateSession(ctx, session) })
}

func (s *retryingStateStore) GetSession(ctx context.Context, id string) (wsstore.Session, error) {
	return retryStoreValue(ctx, s, func() (wsstore.Session, error) { return s.delegate.GetSession(ctx, id) })
}

func (s *retryingStateStore) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	return retryStoreValue(ctx, s, func() (wsstore.Session, error) {
		return s.delegate.BindConnection(ctx, sessionID, connectionID, expectedGeneration)
	})
}

func (s *retryingStateStore) UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error {
	return s.retry(ctx, func() error { return s.delegate.UnbindConnectionIfMatch(ctx, sessionID, connectionID, generation) })
}

func (s *retryingStateStore) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	return s.retry(ctx, func() error { return s.delegate.DeleteExpiredSessions(ctx, before, limit) })
}

func (s *retryingStateStore) Join(ctx context.Context, member wsstore.RoomMember) error {
	return s.retry(ctx, func() error { return s.delegate.Join(ctx, member) })
}

func (s *retryingStateStore) JoinIfMatch(ctx context.Context, member wsstore.RoomMember) error {
	bindingStore, ok := s.delegate.(wsstore.RoomBindingStore)
	if !ok {
		return s.Join(ctx, member)
	}
	return s.retry(ctx, func() error { return bindingStore.JoinIfMatch(ctx, member) })
}

func (s *retryingStateStore) Leave(ctx context.Context, roomID, sessionID string) error {
	return s.retry(ctx, func() error { return s.delegate.Leave(ctx, roomID, sessionID) })
}

func (s *retryingStateStore) LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error {
	bindingStore, ok := s.delegate.(wsstore.RoomBindingStore)
	if !ok {
		return s.Leave(ctx, roomID, sessionID)
	}
	return s.retry(ctx, func() error { return bindingStore.LeaveIfMatch(ctx, roomID, sessionID, connectionID, generation) })
}

func (s *retryingStateStore) Members(ctx context.Context, roomID string, limit int, cursor string) ([]wsstore.RoomMember, string, error) {
	type page struct {
		members []wsstore.RoomMember
		next    string
	}
	result, err := retryStoreValue(ctx, s, func() (page, error) {
		members, next, err := s.delegate.Members(ctx, roomID, limit, cursor)
		return page{members: members, next: next}, err
	})
	return result.members, result.next, err
}

func (s *retryingStateStore) RemoveConnection(ctx context.Context, connectionID string) error {
	return s.retry(ctx, func() error { return s.delegate.RemoveConnection(ctx, connectionID) })
}

func (s *retryingStateStore) Append(ctx context.Context, message wsstore.OfflineMessage) error {
	return s.retry(ctx, func() error { return s.delegate.Append(ctx, message) })
}

func (s *retryingStateStore) ListPending(ctx context.Context, sessionID string, limit int) ([]wsstore.OfflineMessage, error) {
	return retryStoreValue(ctx, s, func() ([]wsstore.OfflineMessage, error) {
		return s.delegate.ListPending(ctx, sessionID, limit)
	})
}

func (s *retryingStateStore) MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.retry(ctx, func() error { return s.delegate.MarkSent(ctx, sessionID, messageID, generation) })
}

func (s *retryingStateStore) MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.retry(ctx, func() error {
		if marker, ok := s.delegate.(replaySentMarker); ok {
			return marker.MarkSentForReplay(ctx, sessionID, messageID, generation)
		}
		return s.delegate.MarkSent(ctx, sessionID, messageID, generation)
	})
}

func (s *retryingStateStore) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.retry(ctx, func() error { return s.delegate.Ack(ctx, sessionID, messageID, generation) })
}

func (s *retryingStateStore) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	return s.retry(ctx, func() error { return s.delegate.DeleteExpired(ctx, before, limit) })
}

func (s *retryingStateStore) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	type reservation struct {
		record   wsstore.IdempotencyRecord
		acquired bool
	}
	result, err := retryStoreValue(ctx, s, func() (reservation, error) {
		current, acquired, err := s.delegate.BeginIdempotency(ctx, record)
		return reservation{record: current, acquired: acquired}, err
	})
	return result.record, result.acquired, err
}

func (s *retryingStateStore) CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error {
	return s.retry(ctx, func() error { return s.delegate.CompleteIdempotency(ctx, sessionID, messageID, response) })
}

func (s *retryingStateStore) GetIdempotency(ctx context.Context, sessionID, messageID string) (wsstore.IdempotencyRecord, error) {
	return retryStoreValue(ctx, s, func() (wsstore.IdempotencyRecord, error) {
		return s.delegate.GetIdempotency(ctx, sessionID, messageID)
	})
}

func (s *retryingStateStore) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	return s.retry(ctx, func() error { return s.delegate.DeleteExpiredIdempotency(ctx, before, limit) })
}
