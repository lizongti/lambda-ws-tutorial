package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	goredis "github.com/redis/go-redis/v9"
)

var putConnectionScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current then
  local decoded = cjson.decode(current)
  local expires = decoded['_expiresAtNano'] or 0
  if (expires == 0 or expires > tonumber(ARGV[4])) and tonumber(ARGV[3]) < tonumber(decoded['generation'] or 0) then
    return 2
  end
end
local ttl = tonumber(ARGV[5])
if ttl < 0 then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  return 1
end
if ttl > 0 then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ttl)
else
  redis.call('SET', KEYS[1], ARGV[2])
end
if ARGV[7] == 'closed' then
  redis.call('ZREM', KEYS[2], ARGV[1])
else
  redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
end
if tonumber(ARGV[8]) ~= 0 then
  redis.call('ZADD', KEYS[3], ARGV[8], ARGV[1])
else
  redis.call('ZREM', KEYS[3], ARGV[1])
end
return 1
`)

var deleteConnectionScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  return 1
end
local decoded = cjson.decode(current)
if tonumber(decoded['generation'] or 0) ~= tonumber(ARGV[2]) then
  return 2
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

var deleteConnectionFreshnessScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  return 1
end
local decoded = cjson.decode(current)
if tonumber(decoded['generation'] or 0) ~= tonumber(ARGV[2]) or
   tonumber(decoded['_lastSeenAtNano'] or 0) ~= tonumber(ARGV[3]) then
  return 2
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

var removeConnectionExpiryIndexScript = goredis.NewScript(`
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  return 0
end
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) == tonumber(ARGV[2]) then
  redis.call('ZREM', KEYS[1], ARGV[1])
end
return 1
`)

var touchConnectionScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 1
end
local decoded = cjson.decode(current)
local expires = decoded['_expiresAtNano'] or 0
if expires ~= 0 and expires <= tonumber(ARGV[3]) then
  return 1
end
if tonumber(decoded['generation'] or 0) ~= tonumber(ARGV[2]) then
  return 2
end
if decoded['state'] == 'closed' then
  return 3
end
local previous = decoded['_lastSeenAtNano'] or 0
if tonumber(ARGV[3]) > previous then
  decoded['lastSeenAt'] = ARGV[4]
  decoded['_lastSeenAtNano'] = tonumber(ARGV[3])
  local encoded = cjson.encode(decoded)
  local ttl = 0
  if expires ~= 0 then
    ttl = math.ceil((expires - tonumber(ARGV[3])) / 1000000)
  end
  if ttl > 0 then
    redis.call('SET', KEYS[1], encoded, 'PX', ttl)
  else
    redis.call('SET', KEYS[1], encoded)
  end
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
end
return 0
`)

func (s *Store) PutConnection(ctx context.Context, c wsstore.Connection) error {
	const op = "PutConnection"
	if err := checkContext(ctx, op); err != nil {
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
	now := s.now()
	if c.ConnectedAt.IsZero() {
		c.ConnectedAt = now
	}
	if c.LastSeenAt.IsZero() {
		c.LastSeenAt = c.ConnectedAt
	}
	encoded, err := encodeConnection(c)
	if err != nil {
		return dataError(op, err)
	}
	result, err := putConnectionScript.Run(ctx, s.client,
		[]string{s.connectionKey(c.ConnectionID), s.staleConnectionsKey(), s.connectionExpiryKey()},
		c.ConnectionID, encoded, c.Generation, now.UnixNano(), ttlMilliseconds(c.ExpiresAt, now),
		c.LastSeenAt.UnixNano(), string(c.State), expirationNano(c.ExpiresAt),
	).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "connection generation is stale")
	}
	return nil
}

func (s *Store) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	const op = "GetConnection"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.Connection{}, err
	}
	if id == "" {
		return wsstore.Connection{}, invalid(op, "connection ID is required")
	}
	encoded, err := s.client.Get(ctx, s.connectionKey(id)).Result()
	if err != nil {
		return wsstore.Connection{}, backendError(op, err)
	}
	connection, err := decodeConnection(encoded)
	if err != nil {
		return wsstore.Connection{}, dataError(op, err)
	}
	if !connection.ExpiresAt.IsZero() && !connection.ExpiresAt.After(s.now()) {
		return wsstore.Connection{}, notFound(op, "connection not found")
	}
	return connection, nil
}

func (s *Store) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	const op = "DeleteConnectionIfMatch"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if id == "" {
		return invalid(op, "connection ID is required")
	}
	result, err := deleteConnectionScript.Run(ctx, s.client,
		[]string{s.connectionKey(id), s.staleConnectionsKey(), s.connectionExpiryKey()}, id, generation, s.now().UnixNano()).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "connection generation does not match")
	}
	return nil
}

func (s *Store) DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error {
	const op = "DeleteConnectionIfMatchAndLastSeen"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if id == "" || lastSeen.IsZero() {
		return invalid(op, "connection ID and last-seen timestamp are required")
	}
	result, err := deleteConnectionFreshnessScript.Run(ctx, s.client,
		[]string{s.connectionKey(id), s.staleConnectionsKey(), s.connectionExpiryKey()}, id, generation, lastSeen.UnixNano()).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "connection generation or last-seen timestamp does not match")
	}
	return nil
}

func (s *Store) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	const op = "TouchConnection"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if id == "" || now.IsZero() {
		return invalid(op, "connection ID and timestamp are required")
	}
	result, err := touchConnectionScript.Run(ctx, s.client,
		[]string{s.connectionKey(id), s.staleConnectionsKey()},
		id, generation, now.UnixNano(), timeText(now),
	).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	switch code {
	case 1:
		return notFound(op, "connection not found")
	case 2:
		return conflict(op, "connection generation does not match")
	case 3:
		return conflict(op, "connection is closed")
	default:
		return nil
	}
}

func (s *Store) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListStaleConnections"
	if err := checkContext(ctx, op); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	connections := make([]wsstore.Connection, 0, limit)
	var offset int64
	for len(connections) < limit {
		entries, err := s.client.ZRangeByScoreWithScores(ctx, s.staleConnectionsKey(), &goredis.ZRangeBy{
			Min: "-inf", Max: strconv.FormatInt(before.UnixNano(), 10), Offset: offset, Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, backendError(op, err)
		}
		if len(entries) == 0 {
			break
		}
		offset += int64(len(entries))
		for _, entry := range entries {
			id, ok := entry.Member.(string)
			if !ok {
				return nil, dataError(op, fmt.Errorf("unexpected stale index member %T", entry.Member))
			}
			encoded, err := s.client.Get(ctx, s.connectionKey(id)).Result()
			if err == goredis.Nil {
				if err := s.client.ZRem(ctx, s.staleConnectionsKey(), id).Err(); err != nil {
					return nil, backendError(op, err)
				}
				// Removing an index entry shifts the offset of the next range.
				offset--
				continue
			}
			if err != nil {
				return nil, backendError(op, err)
			}
			connection, err := decodeConnection(encoded)
			if err != nil {
				return nil, dataError(op, err)
			}
			if connection.State != wsstore.ConnectionStateClosed && connection.LastSeenAt.Before(before) {
				connections = append(connections, connection)
				if len(connections) == limit {
					break
				}
			}
		}
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].LastSeenAt.Equal(connections[j].LastSeenAt) {
			return connections[i].ConnectionID < connections[j].ConnectionID
		}
		return connections[i].LastSeenAt.Before(connections[j].LastSeenAt)
	})
	return connections, nil
}

func (s *Store) ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListExpiredConnections"
	if err := checkContext(ctx, op); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	entries, err := s.client.ZRangeByScoreWithScores(ctx, s.connectionExpiryKey(), &goredis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(before.UnixNano(), 10), Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, backendError(op, err)
	}
	connections := make([]wsstore.Connection, 0, len(entries))
	for _, entry := range entries {
		id, ok := entry.Member.(string)
		if !ok {
			return nil, dataError(op, fmt.Errorf("unexpected connection expiry index member %T", entry.Member))
		}
		removeStaleIndex := func() error {
			return removeConnectionExpiryIndexScript.Run(ctx, s.client, []string{s.connectionExpiryKey(), s.connectionKey(id)}, id, int64(entry.Score)).Err()
		}
		encoded, err := s.client.Get(ctx, s.connectionKey(id)).Result()
		if err == goredis.Nil {
			if err := removeStaleIndex(); err != nil {
				return nil, backendError(op, err)
			}
			continue
		}
		if err != nil {
			return nil, backendError(op, err)
		}
		connection, err := decodeConnection(encoded)
		if err != nil {
			return nil, dataError(op, err)
		}
		if !connection.ExpiresAt.IsZero() && !connection.ExpiresAt.After(before) {
			connections = append(connections, connection)
		}
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].ExpiresAt.Equal(connections[j].ExpiresAt) {
			return connections[i].ConnectionID < connections[j].ConnectionID
		}
		return connections[i].ExpiresAt.Before(connections[j].ExpiresAt)
	})
	return connections, nil
}

const sessionCleanupLua = `
local function cleanupSession(prefix, sid)
  local sessionRoomsKey = prefix .. 'session:' .. sid .. ':rooms'
  local rooms = redis.call('SMEMBERS', sessionRoomsKey)
  for _, roomID in ipairs(rooms) do
    local membersKey = prefix .. 'room:' .. roomID .. ':members'
    local rawMember = redis.call('HGET', membersKey, sid)
    if rawMember then
      local member = cjson.decode(rawMember)
      if member['connectionId'] and member['connectionId'] ~= '' then
        redis.call('SREM', prefix .. 'connection:' .. member['connectionId'] .. ':rooms', cjson.encode({roomID, sid}))
      end
    end
    redis.call('HDEL', membersKey, sid)
    redis.call('ZREM', prefix .. 'room:' .. roomID .. ':index', sid)
  end
  redis.call('DEL', sessionRoomsKey)

  local queueKey = prefix .. 'offline:' .. sid .. ':queue'
  local messages = redis.call('ZRANGE', queueKey, 0, -1)
  for _, messageID in ipairs(messages) do
    redis.call('DEL', prefix .. 'offline:message:' .. #sid .. ':' .. sid .. ':' .. messageID)
    redis.call('ZREM', prefix .. 'offline:expiry', cjson.encode({sid, messageID}))
  end
  redis.call('DEL', queueKey, prefix .. 'offline:' .. sid .. ':sequence')

  local idempotencyIndex = prefix .. 'idempotency:' .. sid .. ':messages'
  local ids = redis.call('SMEMBERS', idempotencyIndex)
  for _, messageID in ipairs(ids) do
    redis.call('DEL', prefix .. 'idempotency:' .. #sid .. ':' .. sid .. ':' .. messageID)
    redis.call('ZREM', prefix .. 'idempotency:expiry', cjson.encode({sid, messageID}))
  end
  redis.call('DEL', idempotencyIndex)
  redis.call('DEL', prefix .. 'session:' .. sid)
  redis.call('ZREM', prefix .. 'sessions:expiry', sid)
end
`

var createSessionScript = goredis.NewScript(sessionCleanupLua + `
local current = redis.call('GET', KEYS[1])
if current then
  local decoded = cjson.decode(current)
  local expires = decoded['_expiresAtNano'] or 0
  if expires == 0 or expires > tonumber(ARGV[2]) then
    return 2
  end
  cleanupSession(ARGV[5], ARGV[1])
end
local ttl = tonumber(ARGV[3])
if ttl > 0 then
  redis.call('SET', KEYS[1], ARGV[4], 'PX', ttl)
else
  redis.call('SET', KEYS[1], ARGV[4])
end
if tonumber(ARGV[6]) ~= 0 then
  redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
else
  redis.call('ZREM', KEYS[2], ARGV[1])
end
return 1
`)

var bindConnectionScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 1
end
local decoded = cjson.decode(current)
local expires = decoded['_expiresAtNano'] or 0
if expires ~= 0 and expires <= tonumber(ARGV[2]) then
  return 1
end
if tonumber(decoded['generation'] or 0) ~= tonumber(ARGV[3]) then
  return 2
end
decoded['currentConnectionId'] = ARGV[1]
decoded['generation'] = tonumber(ARGV[3]) + 1
decoded['lastActiveAt'] = ARGV[4]
local newExpires = tonumber(ARGV[6])
if newExpires ~= 0 then
  decoded['expiresAt'] = ARGV[5]
  decoded['_expiresAtNano'] = newExpires
end
local encoded = cjson.encode(decoded)
local ttl = tonumber(ARGV[7])
if ttl > 0 then
  redis.call('SET', KEYS[1], encoded, 'PX', ttl)
else
  redis.call('SET', KEYS[1], encoded)
end
if newExpires ~= 0 then
  redis.call('ZADD', KEYS[2], newExpires, decoded['sessionId'])
else
  redis.call('ZREM', KEYS[2], decoded['sessionId'])
end
return encoded
`)

var unbindConnectionScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 0
end
local decoded = cjson.decode(current)
local expires = decoded['_expiresAtNano'] or 0
if expires ~= 0 and expires <= tonumber(ARGV[3]) then
  return 0
end
if tonumber(decoded['generation'] or 0) ~= tonumber(ARGV[4]) then
  return 2
end
if not decoded['currentConnectionId'] or decoded['currentConnectionId'] == '' then
  return 0
end
if decoded['currentConnectionId'] ~= ARGV[1] then
  return 2
end
decoded['currentConnectionId'] = ''
decoded['lastActiveAt'] = ARGV[2]
local newExpires = tonumber(ARGV[5])
if newExpires ~= 0 then
  decoded['expiresAt'] = ARGV[6]
  decoded['_expiresAtNano'] = newExpires
end
local encoded = cjson.encode(decoded)
local ttl = tonumber(ARGV[7])
if ttl > 0 then
  redis.call('SET', KEYS[1], encoded, 'PX', ttl)
else
  redis.call('SET', KEYS[1], encoded)
end
if newExpires ~= 0 then
  redis.call('ZADD', KEYS[2], newExpires, decoded['sessionId'])
else
  redis.call('ZREM', KEYS[2], decoded['sessionId'])
end
return 0
`)

var deleteExpiredSessionsScript = goredis.NewScript(sessionCleanupLua + `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, sid in ipairs(ids) do
  local raw = redis.call('GET', KEYS[2] .. sid)
  if raw then
    local session = cjson.decode(raw)
    local expires = session['_expiresAtNano'] or 0
    if expires ~= 0 and expires <= tonumber(ARGV[1]) then
      cleanupSession(ARGV[3], sid)
    else
      redis.call('ZREM', KEYS[1], sid)
    end
  else
    cleanupSession(ARGV[3], sid)
  end
end
return #ids
`)

func (s *Store) CreateSession(ctx context.Context, session wsstore.Session) error {
	const op = "CreateSession"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if session.SessionID == "" {
		return invalid(op, "session ID is required")
	}
	now := s.now()
	if session.LastActiveAt.IsZero() {
		session.LastActiveAt = now
	}
	encoded, err := encodeSession(session)
	if err != nil {
		return dataError(op, err)
	}
	result, err := createSessionScript.Run(ctx, s.client,
		[]string{s.sessionKey(session.SessionID), s.sessionExpiryKey()},
		session.SessionID, now.UnixNano(), ttlMilliseconds(session.ExpiresAt, now), encoded, s.prefix, expirationNano(session.ExpiresAt),
	).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "session already exists")
	}
	return nil
}

func (s *Store) GetSession(ctx context.Context, id string) (wsstore.Session, error) {
	const op = "GetSession"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.Session{}, err
	}
	if id == "" {
		return wsstore.Session{}, invalid(op, "session ID is required")
	}
	encoded, err := s.client.Get(ctx, s.sessionKey(id)).Result()
	if err != nil {
		return wsstore.Session{}, backendError(op, err)
	}
	session, err := decodeSession(encoded)
	if err != nil {
		return wsstore.Session{}, dataError(op, err)
	}
	if !session.ExpiresAt.IsZero() && !session.ExpiresAt.After(s.now()) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	return session, nil
}

func (s *Store) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	const op = "BindConnection"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.Session{}, err
	}
	if sessionID == "" || connectionID == "" {
		return wsstore.Session{}, invalid(op, "session ID and connection ID are required")
	}
	now := s.now()
	current, err := s.GetSession(ctx, sessionID)
	if err != nil {
		return wsstore.Session{}, err
	}
	if current.Generation != expectedGeneration {
		return wsstore.Session{}, conflict(op, "session generation does not match")
	}
	expiresAt := current.ExpiresAt
	if retention := current.ExpiresAt.Sub(current.LastActiveAt); !current.ExpiresAt.IsZero() && retention > 0 {
		expiresAt = now.Add(retention)
	}
	result, err := bindConnectionScript.Run(ctx, s.client,
		[]string{s.sessionKey(sessionID), s.sessionExpiryKey()},
		connectionID, now.UnixNano(), expectedGeneration, timeText(now), timeText(expiresAt),
		expirationNano(expiresAt), ttlMilliseconds(expiresAt, now),
	).Result()
	if err != nil {
		return wsstore.Session{}, backendError(op, err)
	}
	if code, ok := result.(int64); ok {
		switch code {
		case 1:
			return wsstore.Session{}, notFound(op, "session not found")
		case 2:
			return wsstore.Session{}, conflict(op, "session generation does not match")
		}
	}
	encoded, ok := result.(string)
	if !ok {
		return wsstore.Session{}, dataError(op, fmt.Errorf("unexpected bind result %T", result))
	}
	session, err := decodeSession(encoded)
	if err != nil {
		return wsstore.Session{}, dataError(op, err)
	}
	return session, nil
}

func (s *Store) UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error {
	const op = "UnbindConnectionIfMatch"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if sessionID == "" || connectionID == "" {
		return invalid(op, "session ID and connection ID are required")
	}
	now := s.now()
	current, err := s.GetSession(ctx, sessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return nil
		}
		return err
	}
	if current.Generation != generation {
		return conflict(op, "session binding does not match")
	}
	if current.CurrentConnectionID == "" {
		return nil
	}
	if current.CurrentConnectionID != connectionID {
		return conflict(op, "session binding does not match")
	}
	expiresAt := current.ExpiresAt
	if retention := current.ExpiresAt.Sub(current.LastActiveAt); !current.ExpiresAt.IsZero() && retention > 0 {
		expiresAt = now.Add(retention)
	}
	result, err := unbindConnectionScript.Run(ctx, s.client,
		[]string{s.sessionKey(sessionID), s.sessionExpiryKey()},
		connectionID, timeText(now), now.UnixNano(), generation, expirationNano(expiresAt),
		timeText(expiresAt), ttlMilliseconds(expiresAt, now),
	).Result()
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

func (s *Store) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredSessions"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	if err := deleteExpiredSessionsScript.Run(ctx, s.client,
		[]string{s.sessionExpiryKey(), s.prefix + "session:"}, before.UnixNano(), limit, s.prefix).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}
