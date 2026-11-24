package memory

import (
	"context"
	"sort"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

func (s *Store) PutConnection(ctx context.Context, c wsstore.Connection) error {
	const op = "PutConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if c.ConnectionID == "" {
		return invalid(op, "connection ID is required")
	}
	if c.State == "" {
		c.State = wsstore.ConnectionStateActive
	}
	switch c.State {
	case wsstore.ConnectionStateActive, wsstore.ConnectionStateStale, wsstore.ConnectionStateClosed:
	default:
		return invalid(op, "unknown connection state")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if current, ok := s.connections[c.ConnectionID]; ok && !expired(current.ExpiresAt, now) {
		if c.Generation < current.Generation {
			return conflict(op, "connection generation is stale")
		}
	}
	if c.ConnectedAt.IsZero() {
		c.ConnectedAt = now
	}
	if c.LastSeenAt.IsZero() {
		c.LastSeenAt = c.ConnectedAt
	}
	s.connections[c.ConnectionID] = c
	return nil
}

func (s *Store) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	const op = "GetConnection"
	if err := checkContext(ctx); err != nil {
		return wsstore.Connection{}, err
	}
	if id == "" {
		return wsstore.Connection{}, invalid(op, "connection ID is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.connections[id]
	if !ok || expired(c.ExpiresAt, s.now()) {
		return wsstore.Connection{}, notFound(op, "connection not found")
	}
	return c, nil
}

func (s *Store) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	const op = "DeleteConnectionIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if id == "" {
		return invalid(op, "connection ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok {
		return nil
	}
	if c.Generation != generation {
		return conflict(op, "connection generation does not match")
	}
	delete(s.connections, id)
	return nil
}

func (s *Store) DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error {
	const op = "DeleteConnectionIfMatchAndLastSeen"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if id == "" || lastSeen.IsZero() {
		return invalid(op, "connection ID and last-seen timestamp are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok {
		return nil
	}
	if c.Generation != generation || !c.LastSeenAt.Equal(lastSeen) {
		return conflict(op, "connection generation or last-seen timestamp does not match")
	}
	delete(s.connections, id)
	return nil
}

func (s *Store) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	const op = "TouchConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if id == "" || now.IsZero() {
		return invalid(op, "connection ID and timestamp are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok || expired(c.ExpiresAt, now) {
		return notFound(op, "connection not found")
	}
	if c.Generation != generation {
		return conflict(op, "connection generation does not match")
	}
	if c.State == wsstore.ConnectionStateClosed {
		return conflict(op, "connection is closed")
	}
	if now.After(c.LastSeenAt) {
		c.LastSeenAt = now
		s.connections[id] = c
	}
	return nil
}

func (s *Store) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListStaleConnections"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	connections := make([]wsstore.Connection, 0)
	for _, c := range s.connections {
		if c.State != wsstore.ConnectionStateClosed && c.LastSeenAt.Before(before) {
			connections = append(connections, c)
		}
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].LastSeenAt.Equal(connections[j].LastSeenAt) {
			return connections[i].ConnectionID < connections[j].ConnectionID
		}
		return connections[i].LastSeenAt.Before(connections[j].LastSeenAt)
	})
	if len(connections) > limit {
		connections = connections[:limit]
	}
	return connections, nil
}

func (s *Store) ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListExpiredConnections"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	connections := make([]wsstore.Connection, 0)
	for _, connection := range s.connections {
		if expired(connection.ExpiresAt, before) {
			connections = append(connections, connection)
		}
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].ExpiresAt.Equal(connections[j].ExpiresAt) {
			return connections[i].ConnectionID < connections[j].ConnectionID
		}
		return connections[i].ExpiresAt.Before(connections[j].ExpiresAt)
	})
	if len(connections) > limit {
		connections = connections[:limit]
	}
	return connections, nil
}

func (s *Store) CreateSession(ctx context.Context, session wsstore.Session) error {
	const op = "CreateSession"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if session.SessionID == "" {
		return invalid(op, "session ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if current, ok := s.sessions[session.SessionID]; ok && !expired(current.ExpiresAt, now) {
		return conflict(op, "session already exists")
	}
	if current, ok := s.sessions[session.SessionID]; ok {
		s.deleteSessionLocked(current.SessionID)
	}
	if session.LastActiveAt.IsZero() {
		session.LastActiveAt = now
	}
	s.sessions[session.SessionID] = session
	return nil
}

func (s *Store) GetSession(ctx context.Context, id string) (wsstore.Session, error) {
	const op = "GetSession"
	if err := checkContext(ctx); err != nil {
		return wsstore.Session{}, err
	}
	if id == "" {
		return wsstore.Session{}, invalid(op, "session ID is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[id]
	if !ok || expired(session.ExpiresAt, s.now()) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	return session, nil
}

func (s *Store) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	const op = "BindConnection"
	if err := checkContext(ctx); err != nil {
		return wsstore.Session{}, err
	}
	if sessionID == "" || connectionID == "" {
		return wsstore.Session{}, invalid(op, "session ID and connection ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[sessionID]
	now := s.now()
	if !ok || expired(session.ExpiresAt, now) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	if session.Generation != expectedGeneration {
		return wsstore.Session{}, conflict(op, "session generation does not match")
	}
	retention := session.ExpiresAt.Sub(session.LastActiveAt)
	session.CurrentConnectionID = connectionID
	session.Generation++
	session.LastActiveAt = now
	if !session.ExpiresAt.IsZero() && retention > 0 {
		session.ExpiresAt = now.Add(retention)
	}
	s.sessions[sessionID] = session
	return session, nil
}

func (s *Store) UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error {
	const op = "UnbindConnectionIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if sessionID == "" || connectionID == "" {
		return invalid(op, "session ID and connection ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[sessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return nil
	}
	if session.Generation != generation {
		return conflict(op, "session binding does not match")
	}
	if session.CurrentConnectionID == "" {
		return nil
	}
	if session.CurrentConnectionID != connectionID {
		return conflict(op, "session binding does not match")
	}
	retention := session.ExpiresAt.Sub(session.LastActiveAt)
	session.CurrentConnectionID = ""
	session.LastActiveAt = now
	if !session.ExpiresAt.IsZero() && retention > 0 {
		session.ExpiresAt = now.Add(retention)
	}
	s.sessions[sessionID] = session
	return nil
}

func (s *Store) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredSessions"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	type candidate struct {
		id        string
		expiresAt time.Time
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidates := make([]candidate, 0)
	for id, session := range s.sessions {
		if !session.ExpiresAt.IsZero() && !session.ExpiresAt.After(before) {
			candidates = append(candidates, candidate{id: id, expiresAt: session.ExpiresAt})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].expiresAt.Equal(candidates[j].expiresAt) {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].expiresAt.Before(candidates[j].expiresAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	for _, candidate := range candidates {
		s.deleteSessionLocked(candidate.id)
	}
	return nil
}
