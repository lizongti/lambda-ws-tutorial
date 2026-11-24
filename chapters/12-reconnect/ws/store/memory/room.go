package memory

import (
	"context"
	"sort"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

func (s *Store) Join(ctx context.Context, member wsstore.RoomMember) error {
	const op = "Join"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinLocked(member)
	return nil
}

func (s *Store) JoinIfMatch(ctx context.Context, member wsstore.RoomMember) error {
	const op = "JoinIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[member.SessionID]
	if !ok || session.CurrentConnectionID != member.ConnectionID || session.Generation != member.Generation {
		return conflict(op, "session binding does not match")
	}
	s.joinLocked(member)
	return nil
}

func (s *Store) joinLocked(member wsstore.RoomMember) {
	members := s.rooms[member.RoomID]
	if members == nil {
		members = make(map[string]wsstore.RoomMember)
		s.rooms[member.RoomID] = members
	}
	if current, ok := members[member.SessionID]; ok {
		member.JoinedAt = current.JoinedAt
	}
	if member.JoinedAt.IsZero() {
		member.JoinedAt = s.now()
	}
	members[member.SessionID] = member
}

func (s *Store) Leave(ctx context.Context, roomID, sessionID string) error {
	const op = "Leave"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" {
		return invalid(op, "room ID and session ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	members := s.rooms[roomID]
	delete(members, sessionID)
	if len(members) == 0 {
		delete(s.rooms, roomID)
	}
	return nil
}

func (s *Store) LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error {
	const op = "LeaveIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" || connectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[sessionID]
	if !ok || session.CurrentConnectionID != connectionID || session.Generation != generation {
		return conflict(op, "session binding does not match")
	}
	members := s.rooms[roomID]
	delete(members, sessionID)
	if len(members) == 0 {
		delete(s.rooms, roomID)
	}
	return nil
}

func (s *Store) Members(ctx context.Context, roomID string, limit int, cursor string) ([]wsstore.RoomMember, string, error) {
	const op = "Members"
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if roomID == "" || !validLimit(limit) {
		return nil, "", invalid(op, "room ID and a limit between 1 and MaxBatchSize are required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	members := make([]wsstore.RoomMember, 0, len(s.rooms[roomID]))
	for sessionID, member := range s.rooms[roomID] {
		if sessionID <= cursor {
			continue
		}
		session, ok := s.sessions[sessionID]
		if !ok || expired(session.ExpiresAt, now) ||
			session.CurrentConnectionID != member.ConnectionID || session.Generation != member.Generation {
			continue
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].SessionID < members[j].SessionID
	})
	if len(members) > limit {
		page := append([]wsstore.RoomMember(nil), members[:limit]...)
		return page, page[len(page)-1].SessionID, nil
	}
	return members, "", nil
}

func (s *Store) RemoveConnection(ctx context.Context, connectionID string) error {
	const op = "RemoveConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if connectionID == "" {
		return invalid(op, "connection ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for roomID, members := range s.rooms {
		for sessionID, member := range members {
			if member.ConnectionID == connectionID {
				delete(members, sessionID)
			}
		}
		if len(members) == 0 {
			delete(s.rooms, roomID)
		}
	}
	return nil
}
