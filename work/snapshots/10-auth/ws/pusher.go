package ws

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
)

// DefaultMaxPushBytes is API Gateway's maximum WebSocket frame payload size.
const DefaultMaxPushBytes = 32 * 1024

// Pusher sends a serialized message to an existing API Gateway connection.
// Implementations must not open another WebSocket or retain the body after return.
type Pusher interface {
	PostToConnection(ctx context.Context, connectionID string, body []byte) error
}

// PushInput describes one logical delivery. MessageID is optional at this
// transport layer and is included in PushResult for correlation when supplied.
type PushInput struct {
	ConnectionID string
	MessageID    string
	Body         []byte
}

// PushResult describes a delivery attempt without retaining message payload.
// Attempts includes the initial request and is zero when validation fails.
type PushResult struct {
	ConnectionID string
	MessageID    string
	Attempts     int
}

// PushErrorKind is a stable classification for push failures.
type PushErrorKind string

const (
	PushErrorGone        PushErrorKind = "gone"
	PushErrorThrottled   PushErrorKind = "throttled"
	PushErrorUnavailable PushErrorKind = "unavailable"
	PushErrorInvalid     PushErrorKind = "invalid"
	PushErrorUnknown     PushErrorKind = "unknown"
)

// PushError wraps an implementation error with safe, stable classification.
// Error deliberately omits Cause so AWS or network details are not exposed to
// clients accidentally. Unwrap retains Cause for server-side errors.Is/As.
type PushError struct {
	Kind      PushErrorKind
	Retryable bool
	Cause     error
}

// NewPushError constructs a consistently classified error. Only throttled and
// unavailable failures are retryable.
func NewPushError(kind PushErrorKind, cause error) *PushError {
	switch kind {
	case PushErrorGone, PushErrorInvalid:
		return &PushError{Kind: kind, Cause: cause}
	case PushErrorThrottled, PushErrorUnavailable:
		return &PushError{Kind: kind, Retryable: true, Cause: cause}
	default:
		return &PushError{Kind: PushErrorUnknown, Cause: cause}
	}
}

func (e *PushError) Error() string {
	if e == nil {
		return ""
	}
	if e.Kind == "" {
		return "ws push failed"
	}
	return "ws push failed: " + string(e.Kind)
}

func (e *PushError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// IsPushErrorKind reports whether err contains a PushError of the given kind.
func IsPushErrorKind(err error, kind PushErrorKind) bool {
	var pushErr *PushError
	return errors.As(err, &pushErr) && pushErr.Kind == kind
}

// IsPushGone identifies permanent connection loss for Engine cleanup.
func IsPushGone(err error) bool {
	return IsPushErrorKind(err, PushErrorGone)
}

// IsRetryablePushError reports whether err is explicitly safe to retry.
func IsRetryablePushError(err error) bool {
	var pushErr *PushError
	if !errors.As(err, &pushErr) || !pushErr.Retryable {
		return false
	}
	return pushErr.Kind == PushErrorThrottled || pushErr.Kind == PushErrorUnavailable
}

func validatePushInput(input PushInput, maxMessageBytes int) error {
	if strings.TrimSpace(input.ConnectionID) == "" {
		return NewPushError(PushErrorInvalid, errors.New("connection ID is required"))
	}
	if len(input.Body) == 0 {
		return NewPushError(PushErrorInvalid, errors.New("message body is required"))
	}
	if maxMessageBytes > 0 && len(input.Body) > maxMessageBytes {
		return NewPushError(PushErrorInvalid, errors.New("message body exceeds the configured limit"))
	}
	return nil
}

func validatePushContext(ctx context.Context) error {
	if ctx == nil {
		return NewPushError(PushErrorInvalid, errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// FakePushHook runs after a call is recorded and before its scripted result.
// It can block on a channel or ctx.Done to exercise concurrency and cancellation.
type FakePushHook func(ctx context.Context, input PushInput) error

// FakePusher is a deterministic, concurrency-safe Pusher. Script entries are
// consumed one per call; nil entries and calls beyond the script succeed.
type FakePusher struct {
	mu      sync.Mutex
	calls   []PushInput
	errors  []error
	hook    FakePushHook
	maxBody int
}

// NewFakePusher creates a fake with an optional per-call error script.
func NewFakePusher(script ...error) *FakePusher {
	f := &FakePusher{maxBody: DefaultMaxPushBytes}
	f.SetErrors(script...)
	return f
}

// SetErrors replaces the per-call error script. It is safe to call while other
// goroutines are pushing; already recorded calls keep their selected result.
func (f *FakePusher) SetErrors(script ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors[:0], script...)
}

// SetHook replaces the optional blocking/cancellation hook.
func (f *FakePusher) SetHook(hook FakePushHook) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = hook
}

// SetMaxMessageBytes changes the fake's input limit. Non-positive values select
// DefaultMaxPushBytes.
func (f *FakePusher) SetMaxMessageBytes(max int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if max <= 0 {
		max = DefaultMaxPushBytes
	}
	f.maxBody = max
}

// Push records input and returns the scripted result. The record, hook input,
// and returned Calls snapshots do not alias the caller's body.
func (f *FakePusher) Push(ctx context.Context, input PushInput) (PushResult, error) {
	result := PushResult{ConnectionID: input.ConnectionID, MessageID: input.MessageID}

	f.mu.Lock()
	maxBody := f.maxBody
	f.mu.Unlock()
	if maxBody <= 0 {
		maxBody = DefaultMaxPushBytes
	}
	if err := validatePushContext(ctx); err != nil {
		return result, err
	}
	if err := validatePushInput(input, maxBody); err != nil {
		return result, err
	}

	privateInput := clonePushInput(input)
	f.mu.Lock()
	index := len(f.calls)
	f.calls = append(f.calls, privateInput)
	var err error
	if index < len(f.errors) {
		err = f.errors[index]
	}
	hook := f.hook
	f.mu.Unlock()

	if hook != nil {
		if hookErr := hook(ctx, clonePushInput(privateInput)); hookErr != nil {
			result.Attempts = 1
			return result, hookErr
		}
	}
	if err := ctx.Err(); err != nil {
		result.Attempts = 1
		return result, err
	}
	result.Attempts = 1
	return result, err
}

// PostToConnection implements Pusher using the structured Push path.
func (f *FakePusher) PostToConnection(ctx context.Context, connectionID string, body []byte) error {
	_, err := f.Push(ctx, PushInput{ConnectionID: connectionID, Body: body})
	return err
}

// Calls returns an isolated snapshot in invocation order.
func (f *FakePusher) Calls() []PushInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := make([]PushInput, len(f.calls))
	for i, call := range f.calls {
		calls[i] = clonePushInput(call)
	}
	return calls
}

// CallCount returns the number of valid calls recorded so far.
func (f *FakePusher) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func clonePushInput(input PushInput) PushInput {
	input.Body = bytes.Clone(input.Body)
	return input
}
