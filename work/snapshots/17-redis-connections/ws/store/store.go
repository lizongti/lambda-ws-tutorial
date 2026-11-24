// Package store defines the state transitions required by the WebSocket engine.
// Implementations must make conditional operations atomic; callers must not have
// to emulate them with a read followed by a write.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MaxBatchSize is the largest page or maintenance batch a backend must accept.
const MaxBatchSize = 1000

// ErrorCode is a stable, backend-independent storage error classification.
type ErrorCode string

const (
	ErrorCodeUnknown           ErrorCode = "unknown"
	ErrorCodeNotFound          ErrorCode = "not_found"
	ErrorCodeConditionConflict ErrorCode = "condition_conflict"
	ErrorCodeUnavailable       ErrorCode = "unavailable"
	ErrorCodeInvalid           ErrorCode = "invalid"
	ErrorCodeQuotaExceeded     ErrorCode = "quota_exceeded"
)

// Error wraps a backend failure without requiring callers to inspect backend
// specific error strings. Op identifies the StateStore operation.
type Error struct {
	Code ErrorCode
	Op   string
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Op == "" {
		return fmt.Sprintf("store: %s", e.Code)
	}
	return fmt.Sprintf("store %s: %s", e.Op, e.Code)
}

func (e *Error) Unwrap() error { return e.Err }

// Is lets errors.Is match errors by category, including ErrNotFound and the
// other sentinel errors below.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && e.Code == t.Code
}

var (
	ErrNotFound          = &Error{Code: ErrorCodeNotFound}
	ErrConditionConflict = &Error{Code: ErrorCodeConditionConflict}
	ErrUnavailable       = &Error{Code: ErrorCodeUnavailable}
	ErrInvalid           = &Error{Code: ErrorCodeInvalid}
	ErrQuotaExceeded     = &Error{Code: ErrorCodeQuotaExceeded}
)

// NewError classifies a backend error. The returned Error's text deliberately
// excludes Err so backend details are not accidentally exposed to clients.
func NewError(code ErrorCode, op string, err error) error {
	return &Error{Code: code, Op: op, Err: err}
}

// ErrorCodeOf returns ErrorCodeUnknown for an unclassified error.
func ErrorCodeOf(err error) ErrorCode {
	var storeErr *Error
	if errors.As(err, &storeErr) {
		return storeErr.Code
	}
	return ErrorCodeUnknown
}

func IsNotFound(err error) bool          { return ErrorCodeOf(err) == ErrorCodeNotFound }
func IsConditionConflict(err error) bool { return ErrorCodeOf(err) == ErrorCodeConditionConflict }
func IsUnavailable(err error) bool       { return ErrorCodeOf(err) == ErrorCodeUnavailable }
func IsInvalid(err error) bool           { return ErrorCodeOf(err) == ErrorCodeInvalid }
func IsQuotaExceeded(err error) bool     { return ErrorCodeOf(err) == ErrorCodeQuotaExceeded }

// IsRetryable reports whether the engine may apply bounded transient retry.
func IsRetryable(err error) bool { return IsUnavailable(err) }

// ConnectionState describes the lifecycle state of an API Gateway connection.
type ConnectionState string

const (
	ConnectionStateActive ConnectionState = "active"
	ConnectionStateStale  ConnectionState = "stale"
	ConnectionStateClosed ConnectionState = "closed"
)

// Connection is the current API Gateway connection binding.
type Connection struct {
	ConnectionID     string          `json:"connectionId"`
	SessionID        string          `json:"sessionId"`
	PrincipalSubject string          `json:"principalSubject"`
	Generation       uint64          `json:"generation"`
	ConnectedAt      time.Time       `json:"connectedAt"`
	LastSeenAt       time.Time       `json:"lastSeenAt"`
	ExpiresAt        time.Time       `json:"expiresAt"`
	State            ConnectionState `json:"state"`
}

// Session is a logical session retained across connection generations.
// ResumeCredentialHash must never contain a plaintext resume credential.
type Session struct {
	SessionID            string    `json:"sessionId"`
	PrincipalSubject     string    `json:"principalSubject"`
	CurrentConnectionID  string    `json:"currentConnectionId"`
	Generation           uint64    `json:"generation"`
	ResumeCredentialHash string    `json:"resumeCredentialHash"`
	LastActiveAt         time.Time `json:"lastActiveAt"`
	ExpiresAt            time.Time `json:"expiresAt"`
}

// RoomMember associates a logical session with its current connection binding.
type RoomMember struct {
	RoomID       string    `json:"roomId"`
	SessionID    string    `json:"sessionId"`
	ConnectionID string    `json:"connectionId"`
	Generation   uint64    `json:"generation"`
	JoinedAt     time.Time `json:"joinedAt"`
}

// OfflineMessageState is the delivery state of an unacknowledged message.
type OfflineMessageState string

const (
	OfflineMessageStatePending OfflineMessageState = "pending"
	OfflineMessageStateSent    OfflineMessageState = "sent"
)

// OfflineMessage is retained until it is acknowledged or expires. Envelope is
// kept as raw JSON so this package does not depend on the WS protocol package.
type OfflineMessage struct {
	SessionID  string              `json:"sessionId"`
	MessageID  string              `json:"messageId"`
	Sequence   uint64              `json:"sequence"`
	Generation uint64              `json:"generation"`
	Envelope   json.RawMessage     `json:"envelope"`
	CreatedAt  time.Time           `json:"createdAt"`
	ExpiresAt  time.Time           `json:"expiresAt"`
	State      OfflineMessageState `json:"state"`
	SentAt     *time.Time          `json:"sentAt,omitempty"`
}

// OfflineOverflowPolicy controls what happens at a session's queue limit.
type OfflineOverflowPolicy string

const (
	OfflineOverflowRejectNew   OfflineOverflowPolicy = "reject-new"
	OfflineOverflowEvictOldest OfflineOverflowPolicy = "evict-oldest"
)

// BroadcastResult summarizes independent per-member delivery outcomes.
type BroadcastResult struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Gone      int `json:"gone"`
	Offline   int `json:"offline"`
	Failed    int `json:"failed"`
}

// IdempotencyState describes an inbound message-id reservation.
type IdempotencyState string

const (
	IdempotencyStateProcessing IdempotencyState = "processing"
	IdempotencyStateCompleted  IdempotencyState = "completed"
)

// IdempotencyRecord reserves a message ID for a session during ExpiresAt.
// Response is optional and can contain the completed handler response.
type IdempotencyRecord struct {
	SessionID string           `json:"sessionId"`
	MessageID string           `json:"messageId"`
	State     IdempotencyState `json:"state"`
	Response  json.RawMessage  `json:"response,omitempty"`
	CreatedAt time.Time        `json:"createdAt"`
	ExpiresAt time.Time        `json:"expiresAt"`
}

// ConnectionStore stores physical connection bindings.
//
// DeleteConnectionIfMatch is idempotent when id is absent, but returns a
// condition conflict when the stored generation differs. ListStaleConnections
// returns connections with LastSeenAt strictly before before, ordered by
// LastSeenAt and ConnectionID. Limits must be in [1, MaxBatchSize].
type ConnectionStore interface {
	PutConnection(ctx context.Context, c Connection) error
	GetConnection(ctx context.Context, id string) (Connection, error)
	DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error
	TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error
	ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]Connection, error)
}

// ConnectionExpirationStore is an optional extension implemented by backends
// which can physically discover expired connections, including connections
// hidden by GetConnection's logical-expiry check. Results include connections
// whose ExpiresAt is at or before before, ordered by ExpiresAt and ConnectionID.
// Limits must be in [1, MaxBatchSize]. It is intentionally separate from
// StateStore so existing custom backends remain source compatible.
type ConnectionExpirationStore interface {
	ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]Connection, error)
}

// ConnectionFreshnessStore is an optional extension for stale cleanup. The
// conditional delete is idempotent when id is absent, but returns a condition
// conflict when either the stored generation or LastSeenAt differs. It prevents
// a stale snapshot from deleting a connection touched after that snapshot.
type ConnectionFreshnessStore interface {
	DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error
}

// SessionStore stores logical sessions. BindConnection atomically checks
// expectedGeneration, increments it exactly once, and replaces the binding.
// UnbindConnectionIfMatch is idempotent for an absent session or an already
// unbound session at the same generation.
type SessionStore interface {
	CreateSession(ctx context.Context, s Session) error
	GetSession(ctx context.Context, id string) (Session, error)
	BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (Session, error)
	UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error
	DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error
}

// RoomStore stores only generic membership, not application room state.
// Join and Leave are idempotent. Members uses an opaque, exclusive cursor and
// returns only members whose session binding still matches connection and
// generation. Results are stably ordered by SessionID.
type RoomStore interface {
	Join(ctx context.Context, member RoomMember) error
	Leave(ctx context.Context, roomID, sessionID string) error
	Members(ctx context.Context, roomID string, limit int, cursor string) ([]RoomMember, string, error)
	RemoveConnection(ctx context.Context, connectionID string) error
}

// RoomBindingStore is an optional extension which fences room writes to the
// session binding captured by the caller. Implementations must check the session
// and write the membership atomically. A missing or mismatched session binding
// returns ErrConditionConflict. LeaveIfMatch remains idempotent when the current
// binding matches but the member is already absent.
//
// It is intentionally separate from StateStore so existing custom backends
// remain source compatible; callers can fall back to Join and Leave when this
// interface is not implemented.
type RoomBindingStore interface {
	JoinIfMatch(ctx context.Context, member RoomMember) error
	LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error
}

// OfflineStore stores a per-session, monotonically sequenced bounded queue.
// ListPending includes pending and sent-but-unacknowledged messages in sequence
// order. MarkSent fences delivery to the supplied current session generation;
// Ack deletes only a message carrying that same generation. Repeating Ack for
// an already deleted message is successful at the current generation.
type OfflineStore interface {
	Append(ctx context.Context, msg OfflineMessage) error
	ListPending(ctx context.Context, sessionID string, limit int) ([]OfflineMessage, error)
	MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error
	Ack(ctx context.Context, sessionID, messageID string, generation uint64) error
	DeleteExpired(ctx context.Context, before time.Time, limit int) error
}

// IdempotencyStore atomically reserves inbound message IDs. BeginIdempotency
// returns acquired=true only for the caller that created the reservation. For
// an existing live reservation it returns a copy of that record and false.
type IdempotencyStore interface {
	BeginIdempotency(ctx context.Context, record IdempotencyRecord) (current IdempotencyRecord, acquired bool, err error)
	CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error
	GetIdempotency(ctx context.Context, sessionID, messageID string) (IdempotencyRecord, error)
	DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error
}

// StateStore is one coherent backend. Implementations must not mix backends
// implicitly across these interfaces.
type StateStore interface {
	ConnectionStore
	SessionStore
	RoomStore
	OfflineStore
	IdempotencyStore
}
