package memory

import (
	"context"
	"sort"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

func (s *Store) Append(ctx context.Context, msg wsstore.OfflineMessage) error {
	const op = "Append"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if msg.SessionID == "" || msg.MessageID == "" {
		return invalid(op, "session ID and message ID are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[msg.SessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return notFound(op, "session not found")
	}
	if msg.Generation != session.Generation {
		return conflict(op, "offline message generation does not match session")
	}
	if s.offlineQueueLimit <= 0 {
		return invalid(op, "offline queue limit must be positive")
	}
	if s.offlineOverflowPolicy != wsstore.OfflineOverflowRejectNew &&
		s.offlineOverflowPolicy != wsstore.OfflineOverflowEvictOldest {
		return invalid(op, "unknown offline overflow policy")
	}
	if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(now) {
		return invalid(op, "offline message is already expired")
	}
	queue := s.offline[msg.SessionID]
	if queue == nil {
		queue = make(map[string]wsstore.OfflineMessage)
		s.offline[msg.SessionID] = queue
	}
	if _, exists := queue[msg.MessageID]; exists {
		return conflict(op, "offline message ID already exists")
	}

	// Expired entries no longer consume queue capacity, while their sequence
	// remains consumed to preserve monotonicity for the live session.
	for id, queued := range queue {
		if expired(queued.ExpiresAt, now) {
			delete(queue, id)
		}
	}
	if len(queue) >= s.offlineQueueLimit {
		switch s.offlineOverflowPolicy {
		case wsstore.OfflineOverflowRejectNew:
			return quotaExceeded(op, "offline queue is full")
		case wsstore.OfflineOverflowEvictOldest:
			oldestID := ""
			var oldestSequence uint64
			for id, queued := range queue {
				if oldestID == "" || queued.Sequence < oldestSequence {
					oldestID = id
					oldestSequence = queued.Sequence
				}
			}
			delete(queue, oldestID)
		}
	}

	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	s.sequences[msg.SessionID]++
	msg.Sequence = s.sequences[msg.SessionID]
	msg.State = wsstore.OfflineMessageStatePending
	msg.SentAt = nil
	queue[msg.MessageID] = cloneOfflineMessage(msg)
	return nil
}

func (s *Store) ListPending(ctx context.Context, sessionID string, limit int) ([]wsstore.OfflineMessage, error) {
	const op = "ListPending"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if sessionID == "" || !validLimit(limit) {
		return nil, invalid(op, "session ID and a limit between 1 and MaxBatchSize are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	session, ok := s.sessions[sessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return []wsstore.OfflineMessage{}, nil
	}
	messages := make([]wsstore.OfflineMessage, 0, len(s.offline[sessionID]))
	for _, msg := range s.offline[sessionID] {
		if !expired(msg.ExpiresAt, now) {
			messages = append(messages, cloneOfflineMessage(msg))
		}
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Sequence == messages[j].Sequence {
			return messages[i].MessageID < messages[j].MessageID
		}
		return messages[i].Sequence < messages[j].Sequence
	})
	if len(messages) > limit {
		messages = messages[:limit]
	}
	return messages, nil
}

func (s *Store) MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.markSent(ctx, sessionID, messageID, generation, false)
}

// MarkSentForReplay records a new actual delivery attempt, including its SentAt
// timestamp. MarkSent retains its idempotent store-contract behavior.
func (s *Store) MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.markSent(ctx, sessionID, messageID, generation, true)
}

func (s *Store) markSent(ctx context.Context, sessionID, messageID string, generation uint64, refresh bool) error {
	const op = "MarkSent"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[sessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return notFound(op, "session not found")
	}
	if session.Generation != generation || session.CurrentConnectionID == "" {
		return conflict(op, "session generation does not match an active binding")
	}
	msg, ok := s.offline[sessionID][messageID]
	if !ok || expired(msg.ExpiresAt, now) {
		return notFound(op, "offline message not found")
	}
	if msg.Generation > generation {
		return conflict(op, "offline message generation is newer than the binding")
	}
	if !refresh && msg.State == wsstore.OfflineMessageStateSent && msg.Generation == generation {
		return nil
	}
	msg.Generation = generation
	msg.State = wsstore.OfflineMessageStateSent
	sentAt := now
	msg.SentAt = &sentAt
	s.offline[sessionID][messageID] = cloneOfflineMessage(msg)
	return nil
}

func (s *Store) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	const op = "Ack"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[sessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return notFound(op, "session not found")
	}
	if session.Generation != generation {
		return conflict(op, "session generation does not match")
	}
	queue := s.offline[sessionID]
	msg, ok := queue[messageID]
	if !ok {
		// A repeated ack for an already removed message is idempotent, but the
		// generation check above still rejects an ack from an old connection.
		return nil
	}
	if session.CurrentConnectionID == "" || msg.Generation != generation {
		return conflict(op, "offline message was not sent at the current generation")
	}
	if expired(msg.ExpiresAt, now) {
		delete(queue, messageID)
		if len(queue) == 0 {
			delete(s.offline, sessionID)
		}
		return nil
	}
	delete(queue, messageID)
	if len(queue) == 0 {
		delete(s.offline, sessionID)
	}
	return nil
}

func (s *Store) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpired"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	type candidate struct {
		sessionID string
		messageID string
		sequence  uint64
		expiresAt time.Time
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidates := make([]candidate, 0)
	for sessionID, queue := range s.offline {
		for messageID, msg := range queue {
			if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(before) {
				candidates = append(candidates, candidate{
					sessionID: sessionID,
					messageID: messageID,
					sequence:  msg.Sequence,
					expiresAt: msg.ExpiresAt,
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
		if candidates[i].sequence != candidates[j].sequence {
			return candidates[i].sequence < candidates[j].sequence
		}
		return candidates[i].messageID < candidates[j].messageID
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, candidate := range candidates {
		delete(s.offline[candidate.sessionID], candidate.messageID)
		if len(s.offline[candidate.sessionID]) == 0 {
			delete(s.offline, candidate.sessionID)
		}
	}
	return nil
}
