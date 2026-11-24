// Package dynamodb implements the WebSocket StateStore using one DynamoDB table.
//
// The table must have string PK/SK keys and four string-keyed GSIs. By default
// their names are GSI1 (time-based stale/expiry indexes), GSI2 (room members by
// connection), GSI3 (room members by session), and GSI4 (offline messages by
// sequence). All GSIs must project all attributes. The optional ttl attribute
// can be enabled as the table's DynamoDB TTL attribute; reads still enforce
// logical expiry because DynamoDB TTL deletion is deliberately asynchronous.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const (
	DefaultOfflineQueueLimit = 1000
	DefaultIdempotencyWindow = 5 * time.Minute

	DefaultTimeIndexName            = "GSI1"
	DefaultConnectionRoomIndexName  = "GSI2"
	DefaultSessionRoomIndexName     = "GSI3"
	DefaultOfflineSequenceIndexName = "GSI4"

	attrPK = "PK"
	attrSK = "SK"

	partitionConnection           = "CONN#"
	partitionConnectionExpiry     = "CONNEXPIRY#"
	partitionSession              = "SESSION#"
	partitionRoom                 = "ROOM#"
	sortMetadata                  = "METADATA"
	sortOfflineMessage            = "OFFLINE#MSG#"
	sortOfflineMetadata           = "OFFLINE#STATE"
	sortIdempotency               = "IDEMPOTENCY#"
	timePartitionStale            = "CONNECTION#STALE"
	timePartitionConnectionExpiry = "CONNECTION#EXPIRY"
	timePartitionSession          = "SESSION#EXPIRY"
	timePartitionOffline          = "OFFLINE#EXPIRY"
	timePartitionIdempotency      = "IDEMPOTENCY#EXPIRY"
)

// Client is the narrow DynamoDB API used by Store. *dynamodb.Client implements
// it; tests can provide an in-memory implementation without network access.
type Client interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// Option configures a Store.
type Option func(*Store)

// WithClock injects the clock used for logical expiry and store timestamps.
func WithClock(clock func() time.Time) Option {
	return func(s *Store) {
		if clock != nil {
			s.now = clock
		}
	}
}

// WithOfflineQueueLimit sets the maximum live messages retained per session.
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

// WithIndexNames overrides the four GSI names in time, connection-room,
// session-room, and offline-sequence order. Empty names retain their defaults.
func WithIndexNames(timeIndex, connectionRoomIndex, sessionRoomIndex, offlineSequenceIndex string) Option {
	return func(s *Store) {
		if timeIndex != "" {
			s.timeIndex = timeIndex
		}
		if connectionRoomIndex != "" {
			s.connectionRoomIndex = connectionRoomIndex
		}
		if sessionRoomIndex != "" {
			s.sessionRoomIndex = sessionRoomIndex
		}
		if offlineSequenceIndex != "" {
			s.offlineSequenceIndex = offlineSequenceIndex
		}
	}
}

// Store is a complete, coherent DynamoDB-backed StateStore.
type Store struct {
	client    Client
	tableName string

	now                   func() time.Time
	offlineQueueLimit     int
	offlineOverflowPolicy wsstore.OfflineOverflowPolicy
	idempotencyWindow     time.Duration

	timeIndex            string
	connectionRoomIndex  string
	sessionRoomIndex     string
	offlineSequenceIndex string
}

var _ wsstore.StateStore = (*Store)(nil)
var _ wsstore.ConnectionExpirationStore = (*Store)(nil)

// New constructs a store. Configuration errors dependent on a method's options
// are returned as store invalid errors by that method.
func New(client Client, tableName string, options ...Option) *Store {
	s := &Store{
		client:                client,
		tableName:             tableName,
		now:                   time.Now,
		offlineQueueLimit:     DefaultOfflineQueueLimit,
		offlineOverflowPolicy: wsstore.OfflineOverflowRejectNew,
		idempotencyWindow:     DefaultIdempotencyWindow,
		timeIndex:             DefaultTimeIndexName,
		connectionRoomIndex:   DefaultConnectionRoomIndexName,
		sessionRoomIndex:      DefaultSessionRoomIndexName,
		offlineSequenceIndex:  DefaultOfflineSequenceIndexName,
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

func (s *Store) check(op string) error {
	if s.client == nil {
		return invalid(op, "DynamoDB client is required")
	}
	if s.tableName == "" {
		return invalid(op, "DynamoDB table name is required")
	}
	return nil
}

func expired(expiresAt, now time.Time) bool {
	return !expiresAt.IsZero() && !expiresAt.After(now)
}

func validLimit(limit int) bool { return limit > 0 && limit <= wsstore.MaxBatchSize }

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

func unavailable(op, message string) error {
	return wsstore.NewError(wsstore.ErrorCodeUnavailable, op, errors.New(message))
}

func backendError(op string, err error) error {
	if err == nil {
		return nil
	}
	var storeErr *wsstore.Error
	if errors.As(err, &storeErr) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	code := wsstore.ErrorCodeUnknown
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ConditionalCheckFailedException":
			code = wsstore.ErrorCodeConditionConflict
		case "ThrottlingException", "Throttling", "ThrottlingError",
			"ProvisionedThroughputExceededException", "ProvisionedThroughputExceeded",
			"RequestLimitExceeded", "LimitExceededException", "InternalServerError",
			"ServiceUnavailable", "ServiceUnavailableException", "TransactionConflictException",
			"TransactionInProgressException":
			code = wsstore.ErrorCodeUnavailable
		case "TransactionCanceledException":
			code = transactionErrorCode(err)
		case "ValidationException":
			code = wsstore.ErrorCodeInvalid
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		code = wsstore.ErrorCodeUnavailable
	}
	return wsstore.NewError(code, op, err)
}

func transactionErrorCode(err error) wsstore.ErrorCode {
	var canceled *types.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return wsstore.ErrorCodeUnknown
	}
	code := wsstore.ErrorCodeUnknown
	for _, reason := range canceled.CancellationReasons {
		if reason.Code == nil {
			continue
		}
		switch *reason.Code {
		case "ConditionalCheckFailed":
			if code == wsstore.ErrorCodeUnknown {
				code = wsstore.ErrorCodeConditionConflict
			}
		case "ThrottlingError", "ProvisionedThroughputExceeded", "TransactionConflict":
			return wsstore.ErrorCodeUnavailable
		case "ValidationError", "ItemCollectionSizeLimitExceeded":
			return wsstore.ErrorCodeInvalid
		}
	}
	return code
}

func transactionConditionFailed(err error) bool {
	if !wsstore.IsConditionConflict(backendError("Transaction", err)) {
		return false
	}
	var canceled *types.TransactionCanceledException
	return errors.As(err, &canceled)
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func tableName(table string) *string { return &table }

func debugConfig(s *Store) string {
	return fmt.Sprintf("table=%s", s.tableName)
}
