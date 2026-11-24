package ws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	goredis "github.com/redis/go-redis/v9"

	wsstore "github.com/aura-studio/lambda/ws/store"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	memorystore "github.com/aura-studio/lambda/ws/store/memory"
	redisstore "github.com/aura-studio/lambda/ws/store/redis"
)

// Factory constructs one Engine and owns only the resources it creates. Injected
// StateStore and Pusher dependencies always take precedence and remain owned by
// the caller. A Factory must be closed when its Engine is no longer used.
type Factory struct {
	mu sync.Mutex

	options *Options
	closers []io.Closer
	built   bool
	closed  bool

	awsConfig *aws.Config
}

// NewFactory copies options so initialization does not mutate the caller's
// configuration. Construction and validation are performed by Build.
func NewFactory(options *Options) *Factory {
	if options == nil {
		return &Factory{}
	}
	copyOptions := *options
	return &Factory{options: &copyOptions}
}

// Build validates the complete configuration, constructs any dependencies that
// were not explicitly injected, and returns a ready-to-use Engine. Constructors
// do not ping Redis or call AWS services. Build may be called only once.
func (f *Factory) Build() (*Engine, error) {
	if f == nil {
		return nil, invalidConfig("factory", "must not be nil")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, invalidConfig("factory", "is closed")
	}
	if f.built {
		return nil, invalidConfig("factory", "can build only one engine")
	}
	f.built = true
	if f.options == nil {
		return nil, invalidConfig("options", "must not be nil")
	}

	options := *f.options
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if options.SharedDeployment && !nilDependency(options.StateStore) {
		if _, ok := options.StateStore.(*memorystore.Store); ok {
			return nil, invalidConfig("stateStore", "memory cannot be used for a shared deployment")
		}
	}

	stateStore, closer, err := f.buildStateStore(&options)
	if err != nil {
		return nil, err
	}
	if closer != nil {
		f.closers = append(f.closers, closer)
	}
	options.StateStore = stateStore

	if nilDependency(options.Pusher) {
		pusher, err := f.buildPusher(&options)
		if err != nil {
			return nil, errors.Join(err, f.closeLocked())
		}
		options.Pusher = pusher
	}

	engine, err := NewEngine(&options)
	if err != nil {
		return nil, errors.Join(err, f.closeLocked())
	}
	return engine, nil
}

// Close releases factory-owned resources in reverse construction order. It is
// safe to call Close more than once. Injected dependencies are never closed.
func (f *Factory) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeLocked()
}

func (f *Factory) closeLocked() error {
	var err error
	for index := len(f.closers) - 1; index >= 0; index-- {
		if closeErr := f.closers[index].Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	f.closers = nil
	return err
}

func (f *Factory) buildStateStore(options *Options) (wsstore.StateStore, io.Closer, error) {
	if !nilDependency(options.StateStore) {
		return options.StateStore, nil, nil
	}

	switch options.State.Backend {
	case BackendMemory:
		return memorystore.New(
			memorystore.WithClock(options.Clock.Now),
			memorystore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			memorystore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			memorystore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		), nil, nil
	case BackendDynamoDB:
		cfg, err := f.loadAWSConfig(options)
		if err != nil {
			return nil, nil, err
		}
		client := awsdynamodb.NewFromConfig(cfg)
		return dynamodbstore.New(
			client,
			options.State.TableName,
			dynamodbstore.WithClock(options.Clock.Now),
			dynamodbstore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			dynamodbstore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			dynamodbstore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		), nil, nil
	case BackendRedis:
		redisOptions, err := parseRedisOptions(options.State.RedisAddress)
		if err != nil {
			return nil, nil, err
		}
		client := goredis.NewClient(redisOptions)
		storeOptions := []redisstore.Option{
			redisstore.WithClock(options.Clock.Now),
			redisstore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			redisstore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			redisstore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		}
		if options.State.RedisKeyPrefix != "" {
			storeOptions = append(storeOptions, redisstore.WithKeyPrefix(options.State.RedisKeyPrefix))
		}
		return redisstore.New(client, storeOptions...), client, nil
	default:
		return nil, nil, invalidConfig("state.backend", "must be one of memory, dynamodb, or redis")
	}
}

func (f *Factory) buildPusher(options *Options) (Pusher, error) {
	cfg, err := f.loadAWSConfig(options)
	if err != nil {
		return nil, err
	}
	return NewAPIGatewayPusherFromConfig(cfg, APIGatewayPusherConfig{
		APIID:              options.APIGateway.APIID,
		Region:             options.APIGateway.Region,
		Stage:              options.APIGateway.Stage,
		ManagementEndpoint: options.APIGateway.ManagementEndpoint,
		MaxMessageBytes:    options.Message.MaxBytes,
		MaxRetries:         options.Push.MaxRetries,
		InitialBackoff:     options.Push.InitialBackoff,
		MaxBackoff:         options.Push.MaxBackoff,
		Sleep:              options.RetrySleeper,
	})
}

func (f *Factory) loadAWSConfig(options *Options) (aws.Config, error) {
	if f.awsConfig != nil {
		return *f.awsConfig, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return aws.Config{}, fmt.Errorf("ws: load AWS configuration: %w", err)
	}
	if options.APIGateway.Region != "" {
		cfg.Region = options.APIGateway.Region
	}
	f.awsConfig = &cfg
	return cfg, nil
}

func parseRedisOptions(address string) (*goredis.Options, error) {
	if address == "" || strings.TrimSpace(address) != address {
		return nil, invalidConfig("state.redisAddress", "must be a valid Redis address or URL")
	}
	if strings.Contains(address, "://") {
		options, err := goredis.ParseURL(address)
		if err != nil || options == nil || options.Addr == "" {
			return nil, invalidConfig("state.redisAddress", "must be a valid Redis address or URL")
		}
		return options, nil
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return nil, invalidConfig("state.redisAddress", "must be a host:port address or Redis URL")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, invalidConfig("state.redisAddress", "must contain a valid TCP port")
	}
	return &goredis.Options{Addr: address}, nil
}
