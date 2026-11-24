// Package memory provides a concurrency-safe, single-process StateStore.
package memory

import (
	"context"
	"sync"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

const (
	DefaultOfflineQueueLimit = 1000
	DefaultIdempotencyWindow = 5 * time.Minute
)

// Option configures a Store. Invalid numeric options are reported as invalid
// errors by operations that depend on them.
type Option func(*Store)

// WithClock injects the clock used for timestamps assigned by the store.
func WithClock(clock func() time.Time) Option {
	return func(s *Store) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithOfflineQueueLimit sets the maximum number of messages per session.
func WithOfflineQueueLimit(limit int) Option {
	return func(s *Store) { s.offlineQueueLimit = limit }
}

// WithOfflineOverflowPolicy selects reject-new or evict-oldest behavior.
func WithOfflineOverflowPolicy(policy wsstore.OfflineOverflowPolicy) Option {
	return func(s *Store) { s.offlineOverflowPolicy = policy }
}

// WithIdempotencyWindow sets the default lifetime of message-id reservations.
func WithIdempotencyWindow(window time.Duration) Option {
	return func(s *Store) { s.idempotencyWindow = window }
}

// Store implements all state-store contracts with one mutex. Keeping the maps
// under the same lock also makes cross-model generation checks atomic.
type Store struct {
	mu sync.RWMutex

	connections map[string]wsstore.Connection
	sessions    map[string]wsstore.Session
	rooms       map[string]map[string]wsstore.RoomMember
	offline     map[string]map[string]wsstore.OfflineMessage
	sequences   map[string]uint64
	idempotency map[string]map[string]wsstore.IdempotencyRecord

	now                   func() time.Time
	offlineQueueLimit     int
	offlineOverflowPolicy wsstore.OfflineOverflowPolicy
	idempotencyWindow     time.Duration
}

var _ wsstore.StateStore = (*Store)(nil)
var _ wsstore.ConnectionExpirationStore = (*Store)(nil)
var _ wsstore.ConnectionFreshnessStore = (*Store)(nil)

func New(options ...Option) *Store {
	s := &Store{
		connections:           make(map[string]wsstore.Connection),
		sessions:              make(map[string]wsstore.Session),
		rooms:                 make(map[string]map[string]wsstore.RoomMember),
		offline:               make(map[string]map[string]wsstore.OfflineMessage),
		sequences:             make(map[string]uint64),
		idempotency:           make(map[string]map[string]wsstore.IdempotencyRecord),
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

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func expired(expiresAt, now time.Time) bool {
	return !expiresAt.IsZero() && !expiresAt.After(now)
}

func invalid(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeInvalid, op, errString(message))
}

func conflict(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeConditionConflict, op, errString(message))
}

func notFound(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeNotFound, op, errString(message))
}

func quotaExceeded(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeQuotaExceeded, op, errString(message))
}

type errString string

func (e errString) Error() string { return string(e) }

func validLimit(limit int) bool { return limit > 0 && limit <= wsstore.MaxBatchSize }

func cloneOfflineMessage(msg wsstore.OfflineMessage) wsstore.OfflineMessage {
	if msg.Envelope != nil {
		msg.Envelope = append([]byte(nil), msg.Envelope...)
	}
	if msg.SentAt != nil {
		sentAt := *msg.SentAt
		msg.SentAt = &sentAt
	}
	return msg
}

func cloneIdempotencyRecord(record wsstore.IdempotencyRecord) wsstore.IdempotencyRecord {
	if record.Response != nil {
		record.Response = append([]byte(nil), record.Response...)
	}
	return record
}

// deleteSessionLocked also removes secondary state owned by the session.
// Callers must hold s.mu for writing.
func (s *Store) deleteSessionLocked(sessionID string) {
	delete(s.sessions, sessionID)
	delete(s.offline, sessionID)
	delete(s.sequences, sessionID)
	delete(s.idempotency, sessionID)
	for roomID, members := range s.rooms {
		delete(members, sessionID)
		if len(members) == 0 {
			delete(s.rooms, roomID)
		}
	}
}
