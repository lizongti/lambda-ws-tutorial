package redis

import (
	"context"
	"encoding/json"
	"fmt"

	wsstore "github.com/aura-studio/lambda/ws/store"
	goredis "github.com/redis/go-redis/v9"
)

var joinRoomScript = goredis.NewScript(`
local previous = redis.call('HGET', KEYS[1], ARGV[1])
local member = cjson.decode(ARGV[3])
if previous then
  local old = cjson.decode(previous)
  if old['joinedAt'] then
    member['joinedAt'] = old['joinedAt']
  end
  if old['connectionId'] and old['connectionId'] ~= '' and old['connectionId'] ~= ARGV[2] then
    redis.call('SREM', ARGV[5] .. 'connection:' .. old['connectionId'] .. ':rooms', ARGV[6])
  end
end

-- A member lives no longer than its session or connection. Shared room keys
-- retain the longest finite member TTL so one Join cannot expire other members.
local sessionTTL = redis.call('PTTL', KEYS[5])
local connectionTTL = redis.call('PTTL', KEYS[6])
local membersExisted = redis.call('EXISTS', KEYS[1]) == 1
local indexExisted = redis.call('EXISTS', KEYS[2]) == 1
local memberTTL = 0
if sessionTTL > 0 and connectionTTL > 0 then
  memberTTL = math.min(sessionTTL, connectionTTL)
elseif sessionTTL > 0 then
  memberTTL = sessionTTL
elseif connectionTTL > 0 then
  memberTTL = connectionTTL
end

redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(member))
redis.call('ZADD', KEYS[2], 0, ARGV[1])
redis.call('SADD', KEYS[3], ARGV[6])
redis.call('SADD', KEYS[4], ARGV[4])

local function refreshSharedTTL(key, ttl, existed)
  if ttl > 0 then
    local current = redis.call('PTTL', key)
    if (current == -1 and not existed) or (current >= 0 and current < ttl) then
      redis.call('PEXPIRE', key, ttl)
    end
  end
end
refreshSharedTTL(KEYS[1], memberTTL, membersExisted)
refreshSharedTTL(KEYS[2], memberTTL, indexExisted)
if memberTTL == 0 then
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', KEYS[2])
end
if memberTTL > 0 then
  redis.call('PEXPIRE', KEYS[3], memberTTL)
  redis.call('PEXPIRE', KEYS[4], memberTTL)
elseif sessionTTL == -1 and connectionTTL == -1 then
  redis.call('PERSIST', KEYS[3])
  redis.call('PERSIST', KEYS[4])
end
return 1
`)

// joinRoomIfMatchScript is the conditional counterpart of joinRoomScript. Keep
// its membership and TTL writes in sync with joinRoomScript; the only intended
// behavioral difference is the session-binding fence before any write.
var joinRoomIfMatchScript = goredis.NewScript(`
local rawSession = redis.call('GET', KEYS[5])
if not rawSession then
  return 2
end
local session = cjson.decode(rawSession)
if session['currentConnectionId'] ~= ARGV[2] or tonumber(session['generation'] or 0) ~= tonumber(ARGV[7]) then
  return 2
end

local previous = redis.call('HGET', KEYS[1], ARGV[1])
local member = cjson.decode(ARGV[3])
if previous then
  local old = cjson.decode(previous)
  if old['joinedAt'] then
    member['joinedAt'] = old['joinedAt']
  end
  if old['connectionId'] and old['connectionId'] ~= '' and old['connectionId'] ~= ARGV[2] then
    redis.call('SREM', ARGV[5] .. 'connection:' .. old['connectionId'] .. ':rooms', ARGV[6])
  end
end

-- A member lives no longer than its session or connection. Shared room keys
-- retain the longest finite member TTL so one Join cannot expire other members.
local sessionTTL = redis.call('PTTL', KEYS[5])
local connectionTTL = redis.call('PTTL', KEYS[6])
local membersExisted = redis.call('EXISTS', KEYS[1]) == 1
local indexExisted = redis.call('EXISTS', KEYS[2]) == 1
local memberTTL = 0
if sessionTTL > 0 and connectionTTL > 0 then
  memberTTL = math.min(sessionTTL, connectionTTL)
elseif sessionTTL > 0 then
  memberTTL = sessionTTL
elseif connectionTTL > 0 then
  memberTTL = connectionTTL
end

redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(member))
redis.call('ZADD', KEYS[2], 0, ARGV[1])
redis.call('SADD', KEYS[3], ARGV[6])
redis.call('SADD', KEYS[4], ARGV[4])

local function refreshSharedTTL(key, ttl, existed)
  if ttl > 0 then
    local current = redis.call('PTTL', key)
    if (current == -1 and not existed) or (current >= 0 and current < ttl) then
      redis.call('PEXPIRE', key, ttl)
    end
  end
end
refreshSharedTTL(KEYS[1], memberTTL, membersExisted)
refreshSharedTTL(KEYS[2], memberTTL, indexExisted)
if memberTTL == 0 then
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', KEYS[2])
end
if memberTTL > 0 then
  redis.call('PEXPIRE', KEYS[3], memberTTL)
  redis.call('PEXPIRE', KEYS[4], memberTTL)
elseif sessionTTL == -1 and connectionTTL == -1 then
  redis.call('PERSIST', KEYS[3])
  redis.call('PERSIST', KEYS[4])
end
return 1
`)

var leaveRoomScript = goredis.NewScript(`
local previous = redis.call('HGET', KEYS[1], ARGV[1])
if previous then
  local old = cjson.decode(previous)
  if old['connectionId'] and old['connectionId'] ~= '' then
    redis.call('SREM', ARGV[4] .. 'connection:' .. old['connectionId'] .. ':rooms', ARGV[3])
  end
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[2])
return 1
`)

var leaveRoomIfMatchScript = goredis.NewScript(`
local rawSession = redis.call('GET', KEYS[4])
if not rawSession then
  return 2
end
local session = cjson.decode(rawSession)
if session['currentConnectionId'] ~= ARGV[5] or tonumber(session['generation'] or 0) ~= tonumber(ARGV[6]) then
  return 2
end

local previous = redis.call('HGET', KEYS[1], ARGV[1])
if previous then
  local old = cjson.decode(previous)
  if old['connectionId'] and old['connectionId'] ~= '' then
    redis.call('SREM', ARGV[4] .. 'connection:' .. old['connectionId'] .. ':rooms', ARGV[3])
  end
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[2])
return 1
`)

var removeConnectionScript = goredis.NewScript(`
local refs = redis.call('SMEMBERS', KEYS[1])
for _, ref in ipairs(refs) do
  local decoded = cjson.decode(ref)
  local roomID = decoded[1]
  local sessionID = decoded[2]
  local membersKey = ARGV[1] .. 'room:' .. roomID .. ':members'
  local rawMember = redis.call('HGET', membersKey, sessionID)
  if rawMember then
    local member = cjson.decode(rawMember)
    if member['connectionId'] == ARGV[2] then
      redis.call('HDEL', membersKey, sessionID)
      redis.call('ZREM', ARGV[1] .. 'room:' .. roomID .. ':index', sessionID)
      redis.call('SREM', ARGV[1] .. 'session:' .. sessionID .. ':rooms', roomID)
    end
  else
    redis.call('ZREM', ARGV[1] .. 'room:' .. roomID .. ':index', sessionID)
  end
end
redis.call('DEL', KEYS[1])
return #refs
`)

func (s *Store) Join(ctx context.Context, member wsstore.RoomMember) error {
	const op = "Join"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	if member.JoinedAt.IsZero() {
		member.JoinedAt = s.now()
	}
	encoded, err := json.Marshal(member)
	if err != nil {
		return dataError(op, err)
	}
	ref, err := encodeRef(member.RoomID, member.SessionID)
	if err != nil {
		return dataError(op, err)
	}
	if err := joinRoomScript.Run(ctx, s.client, []string{
		s.roomMembersKey(member.RoomID), s.roomIndexKey(member.RoomID),
		s.connectionRoomsKey(member.ConnectionID), s.sessionRoomsKey(member.SessionID),
		s.sessionKey(member.SessionID), s.connectionKey(member.ConnectionID),
	}, member.SessionID, member.ConnectionID, encoded, member.RoomID, s.prefix, ref).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}

func (s *Store) JoinIfMatch(ctx context.Context, member wsstore.RoomMember) error {
	const op = "JoinIfMatch"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	if member.JoinedAt.IsZero() {
		member.JoinedAt = s.now()
	}
	encoded, err := json.Marshal(member)
	if err != nil {
		return dataError(op, err)
	}
	ref, err := encodeRef(member.RoomID, member.SessionID)
	if err != nil {
		return dataError(op, err)
	}
	result, err := joinRoomIfMatchScript.Run(ctx, s.client, []string{
		s.roomMembersKey(member.RoomID), s.roomIndexKey(member.RoomID),
		s.connectionRoomsKey(member.ConnectionID), s.sessionRoomsKey(member.SessionID),
		s.sessionKey(member.SessionID), s.connectionKey(member.ConnectionID),
	}, member.SessionID, member.ConnectionID, encoded, member.RoomID, s.prefix, ref, member.Generation).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "session binding does not match")
	}
	return nil
}

func (s *Store) Leave(ctx context.Context, roomID, sessionID string) error {
	const op = "Leave"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" {
		return invalid(op, "room ID and session ID are required")
	}
	ref, err := encodeRef(roomID, sessionID)
	if err != nil {
		return dataError(op, err)
	}
	if err := leaveRoomScript.Run(ctx, s.client, []string{
		s.roomMembersKey(roomID), s.roomIndexKey(roomID), s.sessionRoomsKey(sessionID),
	}, sessionID, roomID, ref, s.prefix).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}

func (s *Store) LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error {
	const op = "LeaveIfMatch"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" || connectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	ref, err := encodeRef(roomID, sessionID)
	if err != nil {
		return dataError(op, err)
	}
	result, err := leaveRoomIfMatchScript.Run(ctx, s.client, []string{
		s.roomMembersKey(roomID), s.roomIndexKey(roomID), s.sessionRoomsKey(sessionID), s.sessionKey(sessionID),
	}, sessionID, roomID, ref, s.prefix, connectionID, generation).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "session binding does not match")
	}
	return nil
}

func (s *Store) Members(ctx context.Context, roomID string, limit int, cursor string) ([]wsstore.RoomMember, string, error) {
	const op = "Members"
	if err := checkContext(ctx, op); err != nil {
		return nil, "", err
	}
	if roomID == "" || !validLimit(limit) {
		return nil, "", invalid(op, "room ID and a limit between 1 and MaxBatchSize are required")
	}
	min := "-"
	if cursor != "" {
		min = "(" + cursor
	}
	now := s.now()
	members := make([]wsstore.RoomMember, 0, limit+1)
	var offset int64
	for len(members) <= limit {
		ids, err := s.client.ZRangeByLex(ctx, s.roomIndexKey(roomID), &goredis.ZRangeBy{
			Min: min, Max: "+", Offset: offset, Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, "", backendError(op, err)
		}
		if len(ids) == 0 {
			break
		}
		offset += int64(len(ids))
		rawMembers, err := s.client.HMGet(ctx, s.roomMembersKey(roomID), ids...).Result()
		if err != nil {
			return nil, "", backendError(op, err)
		}
		sessionKeys := make([]string, len(ids))
		for i, id := range ids {
			sessionKeys[i] = s.sessionKey(id)
		}
		rawSessions, err := s.client.MGet(ctx, sessionKeys...).Result()
		if err != nil {
			return nil, "", backendError(op, err)
		}
		for i, id := range ids {
			if rawMembers[i] == nil || rawSessions[i] == nil {
				continue
			}
			memberText, ok := rawMembers[i].(string)
			if !ok {
				return nil, "", dataError(op, fmt.Errorf("unexpected room member value %T", rawMembers[i]))
			}
			var member wsstore.RoomMember
			if err := json.Unmarshal([]byte(memberText), &member); err != nil {
				return nil, "", dataError(op, err)
			}
			sessionText, ok := rawSessions[i].(string)
			if !ok {
				return nil, "", dataError(op, fmt.Errorf("unexpected session value %T", rawSessions[i]))
			}
			session, err := decodeSession(sessionText)
			if err != nil {
				return nil, "", dataError(op, err)
			}
			if (!session.ExpiresAt.IsZero() && !session.ExpiresAt.After(now)) ||
				session.CurrentConnectionID != member.ConnectionID || session.Generation != member.Generation {
				continue
			}
			member.SessionID = id
			members = append(members, member)
			if len(members) > limit {
				break
			}
		}
	}
	if len(members) > limit {
		page := members[:limit]
		return page, page[len(page)-1].SessionID, nil
	}
	return members, "", nil
}

func (s *Store) RemoveConnection(ctx context.Context, connectionID string) error {
	const op = "RemoveConnection"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if connectionID == "" {
		return invalid(op, "connection ID is required")
	}
	if err := removeConnectionScript.Run(ctx, s.client,
		[]string{s.connectionRoomsKey(connectionID)}, s.prefix, connectionID).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}
