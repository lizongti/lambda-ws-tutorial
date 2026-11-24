package ws

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
)

// Backend identifies the single coherent StateStore backend used by an Engine.
type Backend string

const (
	BackendMemory   Backend = "memory"
	BackendDynamoDB Backend = "dynamodb"
	BackendRedis    Backend = "redis"
)

// AckPolicy controls when an offline message is removed from its queue.
type AckPolicy string

const (
	AckPolicyPostSuccess AckPolicy = "postSuccess"
	AckPolicyClientAck   AckPolicy = "clientAck"
)

// StateConfig selects and configures the state backend. RedisKeyPrefix is
// optional; backend-specific fields are required only by their own backend.
type StateConfig struct {
	Backend        Backend
	TableName      string
	RedisAddress   string
	RedisKeyPrefix string
}

// String avoids including Redis addresses, which can contain credentials.
func (c StateConfig) String() string {
	return fmt.Sprintf("ws.StateConfig{backend:%q}", c.Backend)
}

// GoString keeps formatted diagnostic output on the same safe representation.
func (c StateConfig) GoString() string { return c.String() }

// APIGatewayConfig contains trusted deployment settings used to construct a
// management endpoint. ManagementEndpoint takes precedence when it is set.
type APIGatewayConfig struct {
	APIID              string
	Region             string
	Stage              string
	ManagementEndpoint string
}

// String reports only whether an endpoint was configured, never its value.
func (c APIGatewayConfig) String() string {
	return fmt.Sprintf("ws.APIGatewayConfig{managementEndpointConfigured:%t}", c.ManagementEndpoint != "")
}

// GoString keeps formatted diagnostic output on the same safe representation.
func (c APIGatewayConfig) GoString() string { return c.String() }

// HeartbeatConfig controls last-seen expiry and bounded maintenance scans.
type HeartbeatConfig struct {
	Interval             time.Duration
	Timeout              time.Duration
	MaintenanceBatchSize int
}

// ReconnectConfig controls logical session retention and replacement policy.
type ReconnectConfig struct {
	Enabled          bool
	SessionRetention time.Duration
	ReplaceExisting  bool
}

// OfflineConfig controls bounded per-session queues, replay, and retention.
type OfflineConfig struct {
	Enabled               bool
	AckPolicy             AckPolicy
	AckTimeout            time.Duration
	MaxMessagesPerSession int
	Retention             time.Duration
	Overflow              wsstore.OfflineOverflowPolicy
	ReplayBatchSize       int
}

// RoomsConfig controls room membership and bounded broadcast fan-out.
type RoomsConfig struct {
	Enabled            bool
	MaxMembers         int
	BroadcastBatchSize int
}

// PushConfig controls concurrent pushes and bounded exponential retries.
type PushConfig struct {
	Concurrency    int
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// MessageConfig controls encoded Envelope size and inbound message-id reuse.
type MessageConfig struct {
	MaxBytes          int
	IdempotencyWindow time.Duration
}

// Clock supplies time to the Engine without hard-coding the system clock.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now invokes f. A nil ClockFunc returns the zero time and is rejected by
// Options.Validate when supplied as a dependency.
func (f ClockFunc) Now() time.Time {
	if f == nil {
		return time.Time{}
	}
	return f()
}

// IDGenerator supplies prefixed session and message identifiers.
type IDGenerator interface {
	ID(prefix string) string
}

// IDGeneratorFunc adapts a function to IDGenerator.
type IDGeneratorFunc func(prefix string) string

// ID invokes f. A nil IDGeneratorFunc returns an empty string and is rejected
// by Options.Validate when supplied as a dependency.
func (f IDGeneratorFunc) ID(prefix string) string {
	if f == nil {
		return ""
	}
	return f(prefix)
}

// Option configures Options.
type Option interface {
	Apply(*Options)
}

// OptionFunc adapts a function to Option.
type OptionFunc func(*Options)

// Apply invokes f when it is non-nil.
func (f OptionFunc) Apply(options *Options) {
	if f != nil {
		f(options)
	}
}

// Options is the complete configuration and dependency set consumed by Engine.
// Configuration fields are grouped by their YAML names. Dependencies are
// explicit so tests never need AWS, Redis, DynamoDB, or wall-clock time.
type Options struct {
	State      StateConfig
	APIGateway APIGatewayConfig
	Auth       AuthConfig
	Heartbeat  HeartbeatConfig
	Reconnect  ReconnectConfig
	Offline    OfflineConfig
	Rooms      RoomsConfig
	Push       PushConfig
	Message    MessageConfig

	// SharedDeployment rejects the process-local memory backend. Production is
	// the default; Auth.Development must be explicitly enabled for anonymous use.
	SharedDeployment bool

	StateStore    wsstore.StateStore
	Pusher        Pusher
	Authenticator Authenticator
	Authorizer    Authorizer
	Clock         Clock
	IDGenerator   IDGenerator
	RetrySleeper  SleepFunc
}

// String deliberately omits endpoints, backend addresses, and dependency values.
func (o Options) String() string {
	return fmt.Sprintf("ws.Options{backend:%q,sharedDeployment:%t}", o.State.Backend, o.SharedDeployment)
}

// GoString keeps formatted diagnostic output on the same safe representation.
func (o Options) GoString() string { return o.String() }

// NewOptions copies the safe defaults and applies opts in argument order.
// Configuration parsing failures panic when the corresponding Option is
// applied, matching the other lambda modules. Call Validate at Engine startup.
func NewOptions(opts ...Option) *Options {
	options := *defaultOptions
	options.init(opts...)
	return &options
}

func (o *Options) init(opts ...Option) {
	for _, opt := range opts {
		if opt != nil {
			opt.Apply(o)
		}
	}
}

// WithStateBackend selects a backend. Its complete configuration is checked by
// Validate so options can be composed in any order.
func WithStateBackend(backend Backend) Option {
	return OptionFunc(func(o *Options) { o.State.Backend = backend })
}

// WithDevelopment explicitly selects development authentication semantics.
func WithDevelopment(development bool) Option {
	return OptionFunc(func(o *Options) { o.Auth.Development = development })
}

// WithSharedDeployment declares that multiple instances share connection state.
func WithSharedDeployment(shared bool) Option {
	return OptionFunc(func(o *Options) { o.SharedDeployment = shared })
}

// WithStateStore injects a coherent StateStore implementation.
func WithStateStore(stateStore wsstore.StateStore) Option {
	return OptionFunc(func(o *Options) { o.StateStore = stateStore })
}

// WithPusher injects the API Gateway-compatible Pusher used by the Engine.
func WithPusher(pusher Pusher) Option {
	return OptionFunc(func(o *Options) { o.Pusher = pusher })
}

// WithAuthenticator injects connection authentication.
func WithAuthenticator(authenticator Authenticator) Option {
	return OptionFunc(func(o *Options) { o.Authenticator = authenticator })
}

// WithAuthorizer injects path, room, and session authorization.
func WithAuthorizer(authorizer Authorizer) Option {
	return OptionFunc(func(o *Options) { o.Authorizer = authorizer })
}

// WithClock injects the Engine clock.
func WithClock(clock Clock) Option {
	return OptionFunc(func(o *Options) { o.Clock = clock })
}

// WithIDGenerator injects the Engine identifier generator.
func WithIDGenerator(generator IDGenerator) Option {
	return OptionFunc(func(o *Options) { o.IDGenerator = generator })
}

// WithRetrySleeper injects bounded retry sleeping for deterministic tests.
func WithRetrySleeper(sleeper SleepFunc) Option {
	return OptionFunc(func(o *Options) { o.RetrySleeper = sleeper })
}

// ErrInvalidConfig classifies safe, field-oriented configuration errors.
var ErrInvalidConfig = errors.New("ws: invalid configuration")

type configError struct {
	field  string
	reason string
}

func (e *configError) Error() string {
	return "ws: invalid configuration: " + e.field + ": " + e.reason
}

func (e *configError) Unwrap() error { return ErrInvalidConfig }

func invalidConfig(field, reason string) error {
	return &configError{field: field, reason: reason}
}

// Validate checks options without including configuration values in errors.
func Validate(options *Options) error { return options.Validate() }

// Validate checks backend completeness, production safety, durations, numeric
// limits, enums, API Gateway endpoint composition, and required dependencies.
func (o *Options) Validate() error {
	if o == nil {
		return invalidConfig("options", "must not be nil")
	}
	if err := o.validateState(); err != nil {
		return err
	}

	authenticator := o.Authenticator
	if nilDependency(authenticator) {
		authenticator = nil
	}
	if err := ValidateAuthConfig(o.Auth, authenticator); err != nil {
		return fmt.Errorf("ws: auth: %w", err)
	}

	if err := validatePositiveDuration("heartbeat.interval", o.Heartbeat.Interval); err != nil {
		return err
	}
	if err := validatePositiveDuration("heartbeat.timeout", o.Heartbeat.Timeout); err != nil {
		return err
	}
	if o.Heartbeat.Timeout <= o.Heartbeat.Interval {
		return invalidConfig("heartbeat.timeout", "must be greater than heartbeat.interval")
	}
	if err := validateBatchSize("heartbeat.maintenanceBatchSize", o.Heartbeat.MaintenanceBatchSize); err != nil {
		return err
	}
	if err := validatePositiveDuration("reconnect.sessionRetention", o.Reconnect.SessionRetention); err != nil {
		return err
	}
	if err := o.validateOffline(); err != nil {
		return err
	}
	if o.Rooms.MaxMembers <= 0 {
		return invalidConfig("rooms.maxMembers", "must be a positive integer")
	}
	if err := validateBatchSize("rooms.broadcastBatchSize", o.Rooms.BroadcastBatchSize); err != nil {
		return err
	}
	if o.Push.Concurrency <= 0 {
		return invalidConfig("push.concurrency", "must be a positive integer")
	}
	if o.Push.MaxRetries < 0 {
		return invalidConfig("push.maxRetries", "must be zero or a positive integer")
	}
	if err := validatePositiveDuration("push.initialBackoff", o.Push.InitialBackoff); err != nil {
		return err
	}
	if err := validatePositiveDuration("push.maxBackoff", o.Push.MaxBackoff); err != nil {
		return err
	}
	if o.Push.InitialBackoff > o.Push.MaxBackoff {
		return invalidConfig("push.initialBackoff", "must not exceed push.maxBackoff")
	}
	if o.Message.MaxBytes <= 0 {
		return invalidConfig("message.maxBytes", "must be a positive integer")
	}
	if err := validatePositiveDuration("message.idempotencyWindow", o.Message.IdempotencyWindow); err != nil {
		return err
	}
	if nilDependency(o.Clock) {
		return invalidConfig("clock", "must not be nil")
	}
	if nilDependency(o.IDGenerator) {
		return invalidConfig("idGenerator", "must not be nil")
	}
	return o.validateAPIGateway()
}

func (o *Options) validateState() error {
	switch o.State.Backend {
	case BackendMemory:
		if o.SharedDeployment {
			return invalidConfig("state.backend", "memory cannot be used for a shared deployment")
		}
	case BackendDynamoDB:
		if strings.TrimSpace(o.State.TableName) == "" {
			return invalidConfig("state.tableName", "is required for the dynamodb backend")
		}
	case BackendRedis:
		if strings.TrimSpace(o.State.RedisAddress) == "" {
			return invalidConfig("state.redisAddress", "is required for the redis backend")
		}
	default:
		return invalidConfig("state.backend", "must be one of memory, dynamodb, or redis")
	}
	return nil
}

func (o *Options) validateOffline() error {
	switch o.Offline.AckPolicy {
	case AckPolicyPostSuccess, AckPolicyClientAck:
	default:
		return invalidConfig("offline.ackPolicy", "must be postSuccess or clientAck")
	}
	if err := validatePositiveDuration("offline.ackTimeout", o.Offline.AckTimeout); err != nil {
		return err
	}
	if o.Offline.MaxMessagesPerSession <= 0 {
		return invalidConfig("offline.maxMessagesPerSession", "must be a positive integer")
	}
	if err := validatePositiveDuration("offline.retention", o.Offline.Retention); err != nil {
		return err
	}
	switch o.Offline.Overflow {
	case wsstore.OfflineOverflowRejectNew, wsstore.OfflineOverflowEvictOldest:
	default:
		return invalidConfig("offline.overflow", "must be reject-new or evict-oldest")
	}
	return validateBatchSize("offline.replayBatchSize", o.Offline.ReplayBatchSize)
}

func (o *Options) validateAPIGateway() error {
	configured := o.APIGateway.APIID != "" || o.APIGateway.Region != "" ||
		o.APIGateway.Stage != "" || o.APIGateway.ManagementEndpoint != ""
	if !nilDependency(o.Pusher) && !configured {
		return nil
	}
	if o.APIGateway.ManagementEndpoint != "" {
		if _, err := ValidateAPIGatewayManagementEndpoint(o.APIGateway.ManagementEndpoint); err != nil {
			return invalidConfig("apiGateway.managementEndpoint", "must be an absolute HTTPS URL with a stage or base path and no credentials, query, or fragment")
		}
		return nil
	}
	if _, err := BuildAPIGatewayManagementEndpoint(o.APIGateway.APIID, o.APIGateway.Region, o.APIGateway.Stage); err != nil {
		return invalidConfig("apiGateway", "apiId, region, and stage are required and must be valid when managementEndpoint is empty")
	}
	return nil
}

func validatePositiveDuration(field string, value time.Duration) error {
	if value <= 0 {
		return invalidConfig(field, "must be a positive duration")
	}
	return nil
}

func validateBatchSize(field string, value int) error {
	if value <= 0 || value > wsstore.MaxBatchSize {
		return invalidConfig(field, fmt.Sprintf("must be between 1 and %d", wsstore.MaxBatchSize))
	}
	return nil
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
