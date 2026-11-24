package redis

import (
	"context"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	goredis "github.com/redis/go-redis/v9"
)

const offlineMessageKeyLua = `local function messageKey(prefix, sid, mid)
  return prefix .. 'offline:message:' .. #sid .. ':' .. sid .. ':' .. mid
end
`

var appendOfflineScript = goredis.NewScript(offlineMessageKeyLua + `
local rawSession = redis.call('GET', KEYS[1])
if not rawSession then
  return 1
end
local session = cjson.decode(rawSession)
local sessionExpires = session['_expiresAtNano'] or 0
if sessionExpires ~= 0 and sessionExpires <= tonumber(ARGV[3]) then
  return 1
end
if tonumber(session['generation'] or 0) ~= tonumber(ARGV[4]) then
  return 2
end

local sid = ARGV[1]
local mid = ARGV[2]
local now = tonumber(ARGV[3])
local existing = redis.call('ZRANGE', KEYS[3], 0, -1)
for _, queuedID in ipairs(existing) do
  local queuedKey = messageKey(ARGV[10], sid, queuedID)
  local raw = redis.call('GET', queuedKey)
  local expired = not raw
  if raw then
    local queued = cjson.decode(raw)
    local expires = queued['_expiresAtNano'] or 0
    expired = expires ~= 0 and expires <= now
  end
  if expired then
    redis.call('DEL', queuedKey)
    redis.call('ZREM', KEYS[3], queuedID)
    redis.call('ZREM', KEYS[4], cjson.encode({sid, queuedID}))
  end
end
if redis.call('ZSCORE', KEYS[3], mid) then
  return 2
end
local count = redis.call('ZCARD', KEYS[3])
if count >= tonumber(ARGV[7]) then
  if ARGV[8] == 'reject-new' then
    return 3
  end
  local oldest = redis.call('ZRANGE', KEYS[3], 0, 0)
  if #oldest > 0 then
    redis.call('DEL', messageKey(ARGV[10], sid, oldest[1]))
    redis.call('ZREM', KEYS[3], oldest[1])
    redis.call('ZREM', KEYS[4], cjson.encode({sid, oldest[1]}))
  end
end
local sequence = redis.call('INCR', KEYS[2])
-- Patch only the top-level sequence field. Re-encoding the whole object would
-- normalize the raw JSON envelope and change its bytes.
local encoded = string.gsub(ARGV[5], '"sequence":%d+', '"sequence":' .. sequence, 1)
if tonumber(ARGV[6]) > 0 then
  redis.call('SET', KEYS[5], encoded, 'PX', tonumber(ARGV[6]))
  redis.call('ZADD', KEYS[4], ARGV[9], cjson.encode({sid, mid}))
else
  redis.call('SET', KEYS[5], encoded)
  redis.call('ZREM', KEYS[4], cjson.encode({sid, mid}))
end
redis.call('ZADD', KEYS[3], sequence, mid)
return 0
`)

var listPendingScript = goredis.NewScript(offlineMessageKeyLua + `
local rawSession = redis.call('GET', KEYS[1])
if not rawSession then
  return {}
end
local session = cjson.decode(rawSession)
local sessionExpires = session['_expiresAtNano'] or 0
if sessionExpires ~= 0 and sessionExpires <= tonumber(ARGV[2]) then
  return {}
end
local result = {}
local ids = redis.call('ZRANGE', KEYS[2], 0, -1)
for _, mid in ipairs(ids) do
  local key = messageKey(ARGV[3], ARGV[1], mid)
  local raw = redis.call('GET', key)
  local expired = not raw
  if raw then
    local message = cjson.decode(raw)
    local expires = message['_expiresAtNano'] or 0
    expired = expires ~= 0 and expires <= tonumber(ARGV[2])
    if not expired then
      table.insert(result, raw)
    end
  end
  if expired then
    redis.call('DEL', key)
    redis.call('ZREM', KEYS[2], mid)
    redis.call('ZREM', KEYS[3], cjson.encode({ARGV[1], mid}))
  end
  if #result >= tonumber(ARGV[4]) then
    break
  end
end
return result
`)

var markSentScript = goredis.NewScript(`
local rawSession = redis.call('GET', KEYS[1])
if not rawSession then
  return 1
end
local session = cjson.decode(rawSession)
local sessionExpires = session['_expiresAtNano'] or 0
if sessionExpires ~= 0 and sessionExpires <= tonumber(ARGV[3]) then
  return 1
end
if tonumber(session['generation'] or 0) ~= tonumber(ARGV[4]) or not session['currentConnectionId'] or session['currentConnectionId'] == '' then
  return 2
end
local raw = redis.call('GET', KEYS[2])
if not raw then
  redis.call('ZREM', KEYS[3], ARGV[2])
  redis.call('ZREM', KEYS[4], ARGV[6])
  return 1
end
local message = cjson.decode(raw)
local expires = message['_expiresAtNano'] or 0
if expires ~= 0 and expires <= tonumber(ARGV[3]) then
  redis.call('DEL', KEYS[2])
  redis.call('ZREM', KEYS[3], ARGV[2])
  redis.call('ZREM', KEYS[4], ARGV[6])
  return 1
end
if tonumber(message['generation'] or 0) > tonumber(ARGV[4]) then
  return 2
end
if ARGV[7] ~= '1' and message['state'] == 'sent' and tonumber(message['generation'] or 0) == tonumber(ARGV[4]) then
  return 0
end
local encoded = string.gsub(raw, '"generation":%d+', '"generation":' .. ARGV[4], 1)
encoded = string.gsub(encoded, '"state":"[^"]*"', '"state":"sent"', 1)
local sentAt = cjson.encode(ARGV[5])
-- Appending makes the new top-level value win during JSON decoding without
-- accidentally replacing a sentAt field inside the raw envelope.
encoded = string.sub(encoded, 1, -2) .. ',"sentAt":' .. sentAt .. '}'
if expires ~= 0 then
  local ttl = math.ceil((expires - tonumber(ARGV[3])) / 1000000)
  redis.call('SET', KEYS[2], encoded, 'PX', ttl)
else
  redis.call('SET', KEYS[2], encoded)
end
return 0
`)

var ackOfflineScript = goredis.NewScript(`
local rawSession = redis.call('GET', KEYS[1])
if not rawSession then
  return 1
end
local session = cjson.decode(rawSession)
local sessionExpires = session['_expiresAtNano'] or 0
if sessionExpires ~= 0 and sessionExpires <= tonumber(ARGV[2]) then
  return 1
end
if tonumber(session['generation'] or 0) ~= tonumber(ARGV[3]) then
  return 2
end
local raw = redis.call('GET', KEYS[2])
if not raw then
  redis.call('ZREM', KEYS[3], ARGV[1])
  redis.call('ZREM', KEYS[4], ARGV[4])
  return 0
end
local message = cjson.decode(raw)
if not session['currentConnectionId'] or session['currentConnectionId'] == '' or tonumber(message['generation'] or 0) ~= tonumber(ARGV[3]) then
  return 2
end
redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[4])
return 0
`)

var deleteExpiredOfflineScript = goredis.NewScript(`
local refs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, ref in ipairs(refs) do
  local decoded = cjson.decode(ref)
  local sid = decoded[1]
  local mid = decoded[2]
  local key = ARGV[3] .. 'offline:message:' .. #sid .. ':' .. sid .. ':' .. mid
  local raw = redis.call('GET', key)
  if raw then
    local message = cjson.decode(raw)
    local expires = message['_expiresAtNano'] or 0
    if expires ~= 0 and expires <= tonumber(ARGV[1]) then
      redis.call('DEL', key)
      redis.call('ZREM', ARGV[3] .. 'offline:' .. sid .. ':queue', mid)
    end
  else
    redis.call('ZREM', ARGV[3] .. 'offline:' .. sid .. ':queue', mid)
  end
  redis.call('ZREM', KEYS[1], ref)
end
return #refs
`)

func (s *Store) Append(ctx context.Context, msg wsstore.OfflineMessage) error {
	const op = "Append"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if msg.SessionID == "" || msg.MessageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	if s.offlineQueueLimit <= 0 {
		return invalid(op, "offline queue limit must be positive")
	}
	if s.offlineOverflowPolicy != wsstore.OfflineOverflowRejectNew &&
		s.offlineOverflowPolicy != wsstore.OfflineOverflowEvictOldest {
		return invalid(op, "unknown offline overflow policy")
	}
	now := s.now()
	if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(now) {
		return invalid(op, "offline message is already expired")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	msg.State = wsstore.OfflineMessageStatePending
	msg.SentAt = nil
	encoded, err := encodeOfflineMessage(msg)
	if err != nil {
		return dataError(op, err)
	}
	result, err := appendOfflineScript.Run(ctx, s.client, []string{
		s.sessionKey(msg.SessionID), s.offlineSequenceKey(msg.SessionID), s.offlineQueueKey(msg.SessionID),
		s.offlineExpiryKey(), s.offlineMessageKey(msg.SessionID, msg.MessageID),
	}, msg.SessionID, msg.MessageID, now.UnixNano(), msg.Generation, encoded,
		ttlMilliseconds(msg.ExpiresAt, now), s.offlineQueueLimit, string(s.offlineOverflowPolicy),
		expirationNano(msg.ExpiresAt), s.prefix,
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
		return notFound(op, "session not found")
	case 2:
		return conflict(op, "offline message generation or ID conflicts with current state")
	case 3:
		return quotaExceeded(op, "offline queue is full")
	default:
		return nil
	}
}

func (s *Store) ListPending(ctx context.Context, sessionID string, limit int) ([]wsstore.OfflineMessage, error) {
	const op = "ListPending"
	if err := checkContext(ctx, op); err != nil {
		return nil, err
	}
	if sessionID == "" || !validLimit(limit) {
		return nil, invalid(op, "session ID and a limit between 1 and MaxBatchSize are required")
	}
	result, err := listPendingScript.Run(ctx, s.client, []string{
		s.sessionKey(sessionID), s.offlineQueueKey(sessionID), s.offlineExpiryKey(),
	}, sessionID, s.now().UnixNano(), s.prefix, limit).Result()
	if err != nil {
		return nil, backendError(op, err)
	}
	encoded, err := scriptStrings(result)
	if err != nil {
		return nil, dataError(op, err)
	}
	messages := make([]wsstore.OfflineMessage, 0, len(encoded))
	for _, item := range encoded {
		message, err := decodeOfflineMessage(item)
		if err != nil {
			return nil, dataError(op, err)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (s *Store) MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.markSent(ctx, sessionID, messageID, generation, false)
}

// MarkSentForReplay refreshes SentAt after each successful replay delivery.
func (s *Store) MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.markSent(ctx, sessionID, messageID, generation, true)
}

func (s *Store) markSent(ctx context.Context, sessionID, messageID string, generation uint64, refresh bool) error {
	const op = "MarkSent"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	now := s.now()
	ref, err := encodeRef(sessionID, messageID)
	if err != nil {
		return dataError(op, err)
	}
	refreshValue := "0"
	if refresh {
		refreshValue = "1"
	}
	result, err := markSentScript.Run(ctx, s.client, []string{
		s.sessionKey(sessionID), s.offlineMessageKey(sessionID, messageID),
		s.offlineQueueKey(sessionID), s.offlineExpiryKey(),
	}, sessionID, messageID, now.UnixNano(), generation, timeText(now), ref, refreshValue).Result()
	if err != nil {
		return backendError(op, err)
	}
	return offlineMutationError(op, result, err)
}

func (s *Store) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	const op = "Ack"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	ref, err := encodeRef(sessionID, messageID)
	if err != nil {
		return dataError(op, err)
	}
	result, err := ackOfflineScript.Run(ctx, s.client, []string{
		s.sessionKey(sessionID), s.offlineMessageKey(sessionID, messageID),
		s.offlineQueueKey(sessionID), s.offlineExpiryKey(),
	}, messageID, s.now().UnixNano(), generation, ref).Result()
	if err != nil {
		return backendError(op, err)
	}
	return offlineMutationError(op, result, err)
}

func offlineMutationError(op string, result interface{}, err error) error {
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	switch code {
	case 1:
		return notFound(op, "session or offline message not found")
	case 2:
		return conflict(op, "offline message generation does not match an active binding")
	default:
		return nil
	}
}

func (s *Store) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpired"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	if err := deleteExpiredOfflineScript.Run(ctx, s.client, []string{s.offlineExpiryKey()},
		before.UnixNano(), limit, s.prefix).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}
