// Package redis provides a Redis-backed store.StateStore. Conditional state
// transitions are implemented with Lua so callers never need read/write races.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultKeyPrefix         = "lambda:ws:"
	DefaultOfflineQueueLimit = 1000
	DefaultIdempotencyWindow = 5 * time.Minute
)

// Client is implemented by redis.Client, redis.ClusterClient and redis.Ring.
type Client = redis.UniversalClient

// Option configures a Store.
type Option func(*Store)

// WithKeyPrefix sets the prefix used by every key owned by the store.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// WithPrefix is an alias for WithKeyPrefix.
func WithPrefix(prefix string) Option { return WithKeyPrefix(prefix) }

// WithClock injects the clock used for logical expiry and store-assigned times.
func WithClock(clock func() time.Time) Option {
	return func(s *Store) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithOfflineQueueLimit sets the maximum number of live messages per session.
func WithOfflineQueueLimit(limit int) Option {
	return func(s *Store) { s.offlineQueueLimit = limit }
}

// WithOfflineOverflowPolicy selects reject-new or evict-oldest behavior.
func WithOfflineOverflowPolicy(policy wsstore.OfflineOverflowPolicy) Option {
	return func(s *Store) { s.offlineOverflowPolicy = policy }
}

// WithIdempotencyWindow sets the default reservation lifetime.
func WithIdempotencyWindow(window time.Duration) Option {
	return func(s *Store) { s.idempotencyWindow = window }
}

// Store implements all StateStore contracts against one Redis client.
type Store struct {
	client Client
	prefix string
	now    func() time.Time

	offlineQueueLimit     int
	offlineOverflowPolicy wsstore.OfflineOverflowPolicy
	idempotencyWindow     time.Duration
}

var _ wsstore.StateStore = (*Store)(nil)
var _ wsstore.ConnectionExpirationStore = (*Store)(nil)
var _ wsstore.ConnectionFreshnessStore = (*Store)(nil)

// New constructs a Redis StateStore. The caller retains ownership of client.
func New(client Client, options ...Option) *Store {
	s := &Store{
		client:                client,
		prefix:                DefaultKeyPrefix,
		now:                   time.Now,
		offlineQueueLimit:     DefaultOfflineQueueLimit,
		offlineOverflowPolicy: wsstore.OfflineOverflowRejectNew,
		idempotencyWindow:     DefaultIdempotencyWindow,
	}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	return s
}

func (s *Store) connectionKey(id string) string { return s.prefix + "connection:" + id }
func (s *Store) staleConnectionsKey() string    { return s.prefix + "connections:stale" }
func (s *Store) connectionExpiryKey() string    { return s.prefix + "connections:expiry" }
func (s *Store) sessionKey(id string) string    { return s.prefix + "session:" + id }
func (s *Store) sessionExpiryKey() string       { return s.prefix + "sessions:expiry" }
func (s *Store) sessionRoomsKey(id string) string {
	return s.prefix + "session:" + id + ":rooms"
}
func (s *Store) roomMembersKey(roomID string) string {
	return s.prefix + "room:" + roomID + ":members"
}
func (s *Store) roomIndexKey(roomID string) string {
	return s.prefix + "room:" + roomID + ":index"
}
func (s *Store) connectionRoomsKey(connectionID string) string {
	return s.prefix + "connection:" + connectionID + ":rooms"
}
func (s *Store) offlineQueueKey(sessionID string) string {
	return s.prefix + "offline:" + sessionID + ":queue"
}
func (s *Store) offlineSequenceKey(sessionID string) string {
	return s.prefix + "offline:" + sessionID + ":sequence"
}
func (s *Store) offlineMessageKey(sessionID, messageID string) string {
	return s.prefix + "offline:message:" + strconv.Itoa(len(sessionID)) + ":" + sessionID + ":" + messageID
}
func (s *Store) offlineExpiryKey() string { return s.prefix + "offline:expiry" }
func (s *Store) idempotencyKey(sessionID, messageID string) string {
	return s.prefix + "idempotency:" + strconv.Itoa(len(sessionID)) + ":" + sessionID + ":" + messageID
}
func (s *Store) sessionIdempotencyKey(sessionID string) string {
	return s.prefix + "idempotency:" + sessionID + ":messages"
}
func (s *Store) idempotencyExpiryKey() string { return s.prefix + "idempotency:expiry" }

func checkContext(ctx context.Context, op string) error {
	select {
	case <-ctx.Done():
		return wsstore.NewError(wsstore.ErrorCodeUnavailable, op, ctx.Err())
	default:
		return nil
	}
}

func backendError(op string, err error) error {
	if err == nil {
		return nil
	}
	var storeErr *wsstore.Error
	if errors.As(err, &storeErr) {
		return err
	}
	if errors.Is(err, redis.Nil) {
		return wsstore.NewError(wsstore.ErrorCodeNotFound, op, err)
	}
	return wsstore.NewError(wsstore.ErrorCodeUnavailable, op, err)
}

func dataError(op string, err error) error {
	return wsstore.NewError(wsstore.ErrorCodeUnknown, op, err)
}

func invalid(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeInvalid, op, errors.New(message))
}

func conflict(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeConditionConflict, op, errors.New(message))
}

func notFound(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeNotFound, op, errors.New(message))
}

func quotaExceeded(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeQuotaExceeded, op, errors.New(message))
}

func validLimit(limit int) bool { return limit > 0 && limit <= wsstore.MaxBatchSize }

func expiredAt(expiresAt int64, now time.Time) bool {
	return expiresAt != 0 && expiresAt <= now.UnixNano()
}

func expirationNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func ttlMilliseconds(expiresAt, now time.Time) int64 {
	if expiresAt.IsZero() {
		return 0
	}
	d := expiresAt.Sub(now)
	if d <= 0 {
		return -1
	}
	milliseconds := d / time.Millisecond
	if d%time.Millisecond != 0 {
		milliseconds++
	}
	return int64(milliseconds)
}

func timeText(t time.Time) string { return t.Format(time.RFC3339Nano) }

func nanoString(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func scriptInt(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer result %T", value)
	}
}

func scriptStrings(value interface{}) ([]string, error) {
	switch value := value.(type) {
	case []interface{}:
		result := make([]string, 0, len(value))
		for _, item := range value {
			switch item := item.(type) {
			case string:
				result = append(result, item)
			case []byte:
				result = append(result, string(item))
			default:
				return nil, fmt.Errorf("unexpected Redis array item %T", item)
			}
		}
		return result, nil
	case []string:
		return value, nil
	case nil:
		return []string{}, nil
	default:
		return nil, fmt.Errorf("unexpected Redis array result %T", value)
	}
}

type connectionJSON struct {
	wsstore.Connection
	ExpiresAtNano  int64 `json:"_expiresAtNano,omitempty"`
	LastSeenAtNano int64 `json:"_lastSeenAtNano,omitempty"`
}

func encodeConnection(c wsstore.Connection) ([]byte, error) {
	return json.Marshal(connectionJSON{
		Connection:     c,
		ExpiresAtNano:  expirationNano(c.ExpiresAt),
		LastSeenAtNano: expirationNano(c.LastSeenAt),
	})
}

func decodeConnection(data string) (wsstore.Connection, error) {
	var stored connectionJSON
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return wsstore.Connection{}, err
	}
	return stored.Connection, nil
}

type sessionJSON struct {
	wsstore.Session
	ExpiresAtNano int64 `json:"_expiresAtNano,omitempty"`
}

func encodeSession(session wsstore.Session) ([]byte, error) {
	return json.Marshal(sessionJSON{Session: session, ExpiresAtNano: expirationNano(session.ExpiresAt)})
}

func decodeSession(data string) (wsstore.Session, error) {
	var stored sessionJSON
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return wsstore.Session{}, err
	}
	return stored.Session, nil
}

type offlineMessageJSON struct {
	wsstore.OfflineMessage
	ExpiresAtNano int64 `json:"_expiresAtNano,omitempty"`
}

func encodeOfflineMessage(msg wsstore.OfflineMessage) ([]byte, error) {
	return json.Marshal(offlineMessageJSON{OfflineMessage: msg, ExpiresAtNano: expirationNano(msg.ExpiresAt)})
}

func decodeOfflineMessage(data string) (wsstore.OfflineMessage, error) {
	var stored offlineMessageJSON
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return wsstore.OfflineMessage{}, err
	}
	return stored.OfflineMessage, nil
}

type idempotencyJSON struct {
	wsstore.IdempotencyRecord
	ExpiresAtNano int64 `json:"_expiresAtNano,omitempty"`
}

func encodeIdempotency(record wsstore.IdempotencyRecord) ([]byte, error) {
	return json.Marshal(idempotencyJSON{IdempotencyRecord: record, ExpiresAtNano: expirationNano(record.ExpiresAt)})
}

func decodeIdempotency(data string) (wsstore.IdempotencyRecord, error) {
	var stored idempotencyJSON
	if err := json.Unmarshal([]byte(data), &stored); err != nil {
		return wsstore.IdempotencyRecord{}, err
	}
	return stored.IdempotencyRecord, nil
}

func encodeRef(first, second string) (string, error) {
	encoded, err := json.Marshal([2]string{first, second})
	return string(encoded), err
}
