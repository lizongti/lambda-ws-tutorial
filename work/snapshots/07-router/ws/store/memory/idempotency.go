package memory

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

func (s *Store) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	const op = "BeginIdempotency"
	if err := checkContext(ctx); err != nil {
		return wsstore.IdempotencyRecord{}, false, err
	}
	if record.SessionID == "" || record.MessageID == "" {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "session ID and message ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	byMessage := s.idempotency[record.SessionID]
	if current, ok := byMessage[record.MessageID]; ok && !expired(current.ExpiresAt, now) {
		return cloneIdempotencyRecord(current), false, nil
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.ExpiresAt.IsZero() {
		if s.idempotencyWindow <= 0 {
			return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency window must be positive")
		}
		record.ExpiresAt = now.Add(s.idempotencyWindow)
	}
	if !record.ExpiresAt.After(now) {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency record is already expired")
	}
	record.State = wsstore.IdempotencyStateProcessing
	record.Response = nil
	if byMessage == nil {
		byMessage = make(map[string]wsstore.IdempotencyRecord)
		s.idempotency[record.SessionID] = byMessage
	}
	byMessage[record.MessageID] = cloneIdempotencyRecord(record)
	return cloneIdempotencyRecord(record), true, nil
}

func (s *Store) CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error {
	const op = "CompleteIdempotency"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.idempotency[sessionID][messageID]
	if !ok || expired(record.ExpiresAt, s.now()) {
		return notFound(op, "idempotency record not found")
	}
	record.State = wsstore.IdempotencyStateCompleted
	record.Response = append([]byte(nil), response...)
	s.idempotency[sessionID][messageID] = cloneIdempotencyRecord(record)
	return nil
}

func (s *Store) GetIdempotency(ctx context.Context, sessionID, messageID string) (wsstore.IdempotencyRecord, error) {
	const op = "GetIdempotency"
	if err := checkContext(ctx); err != nil {
		return wsstore.IdempotencyRecord{}, err
	}
	if sessionID == "" || messageID == "" {
		return wsstore.IdempotencyRecord{}, invalid(op, "session ID and message ID are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.idempotency[sessionID][messageID]
	if !ok || expired(record.ExpiresAt, s.now()) {
		return wsstore.IdempotencyRecord{}, notFound(op, "idempotency record not found")
	}
	return cloneIdempotencyRecord(record), nil
}

func (s *Store) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredIdempotency"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	type candidate struct {
		sessionID string
		messageID string
		expiresAt time.Time
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidates := make([]candidate, 0)
	for sessionID, byMessage := range s.idempotency {
		for messageID, record := range byMessage {
			if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(before) {
				candidates = append(candidates, candidate{
					sessionID: sessionID,
					messageID: messageID,
					expiresAt: record.ExpiresAt,
				})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].expiresAt.Equal(candidates[j].expiresAt) {
			return candidates[i].expiresAt.Before(candidates[j].expiresAt)
		}
		if candidates[i].sessionID != candidates[j].sessionID {
			return candidates[i].sessionID < candidates[j].sessionID
		}
		return candidates[i].messageID < candidates[j].messageID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, candidate := range candidates {
		delete(s.idempotency[candidate.sessionID], candidate.messageID)
		if len(s.idempotency[candidate.sessionID]) == 0 {
			delete(s.idempotency, candidate.sessionID)
		}
	}
	return nil
}
