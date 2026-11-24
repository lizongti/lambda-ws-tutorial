package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	goredis "github.com/redis/go-redis/v9"
)

var beginIdempotencyScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current then
  local existing = cjson.decode(current)
  local expires = existing['_expiresAtNano'] or 0
  if expires == 0 or expires > tonumber(ARGV[2]) then
    return {current, 0}
  end
  redis.call('DEL', KEYS[1])
end
local reserved = redis.call('SET', KEYS[1], ARGV[3], 'NX', 'PX', tonumber(ARGV[4]))
if not reserved then
  return {redis.call('GET', KEYS[1]), 0}
end
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[6])
return {ARGV[3], 1}
`)

var completeIdempotencyScript = goredis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  return 1
end
local record = cjson.decode(current)
local expires = record['_expiresAtNano'] or 0
if expires ~= 0 and expires <= tonumber(ARGV[1]) then
  return 1
end
record['state'] = 'completed'
if ARGV[2] == '' then
  record['response'] = nil
else
  record['response'] = cjson.decode(ARGV[2])
end
local ttl = 0
if expires ~= 0 then
  ttl = math.ceil((expires - tonumber(ARGV[1])) / 1000000)
end
if ttl > 0 then
  redis.call('SET', KEYS[1], cjson.encode(record), 'PX', ttl)
else
  redis.call('SET', KEYS[1], cjson.encode(record))
end
return 0
`)

var deleteExpiredIdempotencyScript = goredis.NewScript(`
local refs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, ref in ipairs(refs) do
  local decoded = cjson.decode(ref)
  local sid = decoded[1]
  local mid = decoded[2]
  local key = ARGV[3] .. 'idempotency:' .. #sid .. ':' .. sid .. ':' .. mid
  local raw = redis.call('GET', key)
  if raw then
    local record = cjson.decode(raw)
    local expires = record['_expiresAtNano'] or 0
    if expires ~= 0 and expires <= tonumber(ARGV[1]) then
      redis.call('DEL', key)
      redis.call('SREM', ARGV[3] .. 'idempotency:' .. sid .. ':messages', mid)
    end
  else
    redis.call('SREM', ARGV[3] .. 'idempotency:' .. sid .. ':messages', mid)
  end
  redis.call('ZREM', KEYS[1], ref)
end
return #refs
`)

func (s *Store) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	const op = "BeginIdempotency"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.IdempotencyRecord{}, false, err
	}
	if record.SessionID == "" || record.MessageID == "" {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "session ID and message ID are required")
	}
	now := s.now()
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
	encoded, err := encodeIdempotency(record)
	if err != nil {
		return wsstore.IdempotencyRecord{}, false, dataError(op, err)
	}
	ref, err := encodeRef(record.SessionID, record.MessageID)
	if err != nil {
		return wsstore.IdempotencyRecord{}, false, dataError(op, err)
	}
	result, err := beginIdempotencyScript.Run(ctx, s.client, []string{
		s.idempotencyKey(record.SessionID, record.MessageID),
		s.sessionIdempotencyKey(record.SessionID), s.idempotencyExpiryKey(),
	}, record.MessageID, now.UnixNano(), encoded, ttlMilliseconds(record.ExpiresAt, now),
		expirationNano(record.ExpiresAt), ref,
	).Result()
	if err != nil {
		return wsstore.IdempotencyRecord{}, false, backendError(op, err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return wsstore.IdempotencyRecord{}, false, dataError(op, fmt.Errorf("unexpected idempotency result %T", result))
	}
	raw, ok := values[0].(string)
	if !ok {
		return wsstore.IdempotencyRecord{}, false, dataError(op, fmt.Errorf("unexpected idempotency value %T", values[0]))
	}
	acquired, err := scriptInt(values[1])
	if err != nil {
		return wsstore.IdempotencyRecord{}, false, dataError(op, err)
	}
	current, err := decodeIdempotency(raw)
	if err != nil {
		return wsstore.IdempotencyRecord{}, false, dataError(op, err)
	}
	return current, acquired == 1, nil
}

func (s *Store) CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error {
	const op = "CompleteIdempotency"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	result, err := completeIdempotencyScript.Run(ctx, s.client,
		[]string{s.idempotencyKey(sessionID, messageID)}, s.now().UnixNano(), string(response)).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 1 {
		return notFound(op, "idempotency record not found")
	}
	return nil
}

func (s *Store) GetIdempotency(ctx context.Context, sessionID, messageID string) (wsstore.IdempotencyRecord, error) {
	const op = "GetIdempotency"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.IdempotencyRecord{}, err
	}
	if sessionID == "" || messageID == "" {
		return wsstore.IdempotencyRecord{}, invalid(op, "session ID and message ID are required")
	}
	encoded, err := s.client.Get(ctx, s.idempotencyKey(sessionID, messageID)).Result()
	if err != nil {
		return wsstore.IdempotencyRecord{}, backendError(op, err)
	}
	record, err := decodeIdempotency(encoded)
	if err != nil {
		return wsstore.IdempotencyRecord{}, dataError(op, err)
	}
	if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(s.now()) {
		return wsstore.IdempotencyRecord{}, notFound(op, "idempotency record not found")
	}
	return record, nil
}

func (s *Store) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredIdempotency"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	if err := deleteExpiredIdempotencyScript.Run(ctx, s.client, []string{s.idempotencyExpiryKey()},
		before.UnixNano(), limit, s.prefix).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}
