package ws

import (
	"errors"
	"fmt"
	"os"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"gopkg.in/yaml.v2"
)

var errInvalidWSYAML = errors.New("invalid or unknown YAML configuration")

type yamlWSConfig struct {
	Lambda           string          `yaml:"lambda"`
	WS               *yamlWSConfig   `yaml:"ws"`
	SharedDeployment *bool           `yaml:"sharedDeployment"`
	State            yamlStateConfig `yaml:"state"`
	APIGateway       yamlAPIGateway  `yaml:"apiGateway"`
	Auth             yamlAuthConfig  `yaml:"auth"`
	Heartbeat        yamlHeartbeat   `yaml:"heartbeat"`
	Reconnect        yamlReconnect   `yaml:"reconnect"`
	Offline          yamlOffline     `yaml:"offline"`
	Rooms            yamlRooms       `yaml:"rooms"`
	Push             yamlPush        `yaml:"push"`
	Message          yamlMessage     `yaml:"message"`
}

type yamlStateConfig struct {
	Backend          *string `yaml:"backend"`
	TableName        *string `yaml:"tableName"`
	RedisAddress     *string `yaml:"redisAddress"`
	RedisKeyPrefix   *string `yaml:"redisKeyPrefix"`
	RedisPrefix      *string `yaml:"redisPrefix"`
	Shared           *bool   `yaml:"shared"`
	SharedDeployment *bool   `yaml:"sharedDeployment"`
}

type yamlAPIGateway struct {
	APIID              *string `yaml:"apiId"`
	Region             *string `yaml:"region"`
	Stage              *string `yaml:"stage"`
	ManagementEndpoint *string `yaml:"managementEndpoint"`
}

type yamlAuthConfig struct {
	Development *bool `yaml:"development"`
	Required    *bool `yaml:"required"`
	Protected   *bool `yaml:"protected"`
	Anonymous   *bool `yaml:"anonymous"`
}

type yamlHeartbeat struct {
	Interval             *string `yaml:"interval"`
	Timeout              *string `yaml:"timeout"`
	MaintenanceBatchSize *int    `yaml:"maintenanceBatchSize"`
}

type yamlReconnect struct {
	Enabled          *bool   `yaml:"enabled"`
	SessionRetention *string `yaml:"sessionRetention"`
	ReplaceExisting  *bool   `yaml:"replaceExisting"`
}

type yamlOffline struct {
	Enabled               *bool   `yaml:"enabled"`
	AckPolicy             *string `yaml:"ackPolicy"`
	AckTimeout            *string `yaml:"ackTimeout"`
	MaxMessagesPerSession *int    `yaml:"maxMessagesPerSession"`
	Retention             *string `yaml:"retention"`
	Overflow              *string `yaml:"overflow"`
	ReplayBatchSize       *int    `yaml:"replayBatchSize"`
}

type yamlRooms struct {
	Enabled            *bool `yaml:"enabled"`
	MaxMembers         *int  `yaml:"maxMembers"`
	BroadcastBatchSize *int  `yaml:"broadcastBatchSize"`
}

type yamlPush struct {
	Concurrency    *int    `yaml:"concurrency"`
	MaxRetries     *int    `yaml:"maxRetries"`
	InitialBackoff *string `yaml:"initialBackoff"`
	MaxBackoff     *string `yaml:"maxBackoff"`
}

type yamlMessage struct {
	MaxBytes          *int    `yaml:"maxBytes"`
	IdempotencyWindow *string `yaml:"idempotencyWindow"`
}

// WithConfig parses either a ws configuration subtree or a complete document
// containing a top-level ws node. Only fields present in YAML are overridden.
// Invalid YAML or scalar values panic when the returned Option is applied.
func WithConfig(yamlBytes []byte) Option {
	option, err := optionFromConfigBytes(yamlBytes)
	if err != nil {
		return OptionFunc(func(*Options) {
			panic(fmt.Errorf("ws.WithConfig: %w", err))
		})
	}
	return option
}

func optionFromConfigBytes(yamlBytes []byte) (Option, error) {
	var document yamlWSConfig
	// Do not expose yaml parser errors: type errors can echo configuration
	// values, and configuration may contain credentials in backend addresses.
	if err := yaml.UnmarshalStrict(yamlBytes, &document); err != nil {
		return nil, errInvalidWSYAML
	}
	config := document
	if document.WS != nil {
		config = *document.WS
		config.SharedDeployment = firstBoolPointer(config.SharedDeployment, document.SharedDeployment)
	}
	return optionFromWSConfig(config), nil
}

// WithConfigFile loads a YAML file and applies it to Options.
func WithConfigFile(path string) Option {
	contents, err := os.ReadFile(path)
	if err != nil {
		return OptionFunc(func(*Options) {
			panic(fmt.Errorf("ws.WithConfigFile(%s): %w", path, err))
		})
	}
	return WithConfig(contents)
}

func optionFromWSConfig(config yamlWSConfig) Option {
	return OptionFunc(func(options *Options) {
		if err := applyWSConfig(options, config); err != nil {
			panic(fmt.Errorf("ws.WithConfig: %w", err))
		}
	})
}

func applyWSConfig(options *Options, config yamlWSConfig) error {
	setBool(config.SharedDeployment, &options.SharedDeployment)
	setBool(config.State.Shared, &options.SharedDeployment)
	setBool(config.State.SharedDeployment, &options.SharedDeployment)

	if config.State.Backend != nil {
		backend := Backend(*config.State.Backend)
		switch backend {
		case BackendMemory, BackendDynamoDB, BackendRedis:
			options.State.Backend = backend
		default:
			return invalidConfig("state.backend", "must be one of memory, dynamodb, or redis")
		}
	}
	setString(config.State.TableName, &options.State.TableName)
	setString(config.State.RedisAddress, &options.State.RedisAddress)
	setString(config.State.RedisPrefix, &options.State.RedisKeyPrefix)
	setString(config.State.RedisKeyPrefix, &options.State.RedisKeyPrefix)

	setString(config.APIGateway.APIID, &options.APIGateway.APIID)
	setString(config.APIGateway.Region, &options.APIGateway.Region)
	setString(config.APIGateway.Stage, &options.APIGateway.Stage)
	setString(config.APIGateway.ManagementEndpoint, &options.APIGateway.ManagementEndpoint)

	setBool(config.Auth.Development, &options.Auth.Development)
	setBool(config.Auth.Required, &options.Auth.Required)
	setBool(config.Auth.Protected, &options.Auth.Protected)
	setBool(config.Auth.Anonymous, &options.Auth.Anonymous)

	if err := setDuration(config.Heartbeat.Interval, "heartbeat.interval", &options.Heartbeat.Interval); err != nil {
		return err
	}
	if err := setDuration(config.Heartbeat.Timeout, "heartbeat.timeout", &options.Heartbeat.Timeout); err != nil {
		return err
	}
	if err := setPositiveInt(config.Heartbeat.MaintenanceBatchSize, "heartbeat.maintenanceBatchSize", &options.Heartbeat.MaintenanceBatchSize, false); err != nil {
		return err
	}
	if options.Heartbeat.MaintenanceBatchSize > wsstore.MaxBatchSize {
		return invalidConfig("heartbeat.maintenanceBatchSize", fmt.Sprintf("must be between 1 and %d", wsstore.MaxBatchSize))
	}

	setBool(config.Reconnect.Enabled, &options.Reconnect.Enabled)
	if err := setDuration(config.Reconnect.SessionRetention, "reconnect.sessionRetention", &options.Reconnect.SessionRetention); err != nil {
		return err
	}
	setBool(config.Reconnect.ReplaceExisting, &options.Reconnect.ReplaceExisting)

	setBool(config.Offline.Enabled, &options.Offline.Enabled)
	if config.Offline.AckPolicy != nil {
		policy := AckPolicy(*config.Offline.AckPolicy)
		switch policy {
		case AckPolicyPostSuccess, AckPolicyClientAck:
			options.Offline.AckPolicy = policy
		default:
			return invalidConfig("offline.ackPolicy", "must be postSuccess or clientAck")
		}
	}
	if err := setDuration(config.Offline.AckTimeout, "offline.ackTimeout", &options.Offline.AckTimeout); err != nil {
		return err
	}
	if err := setPositiveInt(config.Offline.MaxMessagesPerSession, "offline.maxMessagesPerSession", &options.Offline.MaxMessagesPerSession, false); err != nil {
		return err
	}
	if err := setDuration(config.Offline.Retention, "offline.retention", &options.Offline.Retention); err != nil {
		return err
	}
	if config.Offline.Overflow != nil {
		overflow := wsstore.OfflineOverflowPolicy(*config.Offline.Overflow)
		switch overflow {
		case wsstore.OfflineOverflowRejectNew, wsstore.OfflineOverflowEvictOldest:
			options.Offline.Overflow = overflow
		default:
			return invalidConfig("offline.overflow", "must be reject-new or evict-oldest")
		}
	}
	if err := setPositiveInt(config.Offline.ReplayBatchSize, "offline.replayBatchSize", &options.Offline.ReplayBatchSize, false); err != nil {
		return err
	}
	if options.Offline.ReplayBatchSize > wsstore.MaxBatchSize {
		return invalidConfig("offline.replayBatchSize", fmt.Sprintf("must be between 1 and %d", wsstore.MaxBatchSize))
	}

	setBool(config.Rooms.Enabled, &options.Rooms.Enabled)
	if err := setPositiveInt(config.Rooms.MaxMembers, "rooms.maxMembers", &options.Rooms.MaxMembers, false); err != nil {
		return err
	}
	if err := setPositiveInt(config.Rooms.BroadcastBatchSize, "rooms.broadcastBatchSize", &options.Rooms.BroadcastBatchSize, false); err != nil {
		return err
	}
	if options.Rooms.BroadcastBatchSize > wsstore.MaxBatchSize {
		return invalidConfig("rooms.broadcastBatchSize", fmt.Sprintf("must be between 1 and %d", wsstore.MaxBatchSize))
	}

	if err := setPositiveInt(config.Push.Concurrency, "push.concurrency", &options.Push.Concurrency, false); err != nil {
		return err
	}
	if err := setPositiveInt(config.Push.MaxRetries, "push.maxRetries", &options.Push.MaxRetries, true); err != nil {
		return err
	}
	if err := setDuration(config.Push.InitialBackoff, "push.initialBackoff", &options.Push.InitialBackoff); err != nil {
		return err
	}
	if err := setDuration(config.Push.MaxBackoff, "push.maxBackoff", &options.Push.MaxBackoff); err != nil {
		return err
	}

	if err := setPositiveInt(config.Message.MaxBytes, "message.maxBytes", &options.Message.MaxBytes, false); err != nil {
		return err
	}
	return setDuration(config.Message.IdempotencyWindow, "message.idempotencyWindow", &options.Message.IdempotencyWindow)
}

func setString(source *string, target *string) {
	if source != nil {
		*target = *source
	}
}

func setBool(source *bool, target *bool) {
	if source != nil {
		*target = *source
	}
}

func setDuration(source *string, field string, target *time.Duration) error {
	if source == nil {
		return nil
	}
	value, err := time.ParseDuration(*source)
	if err != nil || value <= 0 {
		return invalidConfig(field, "must be a positive duration")
	}
	*target = value
	return nil
}

func setPositiveInt(source *int, field string, target *int, allowZero bool) error {
	if source == nil {
		return nil
	}
	if *source < 0 || (*source == 0 && !allowZero) {
		if allowZero {
			return invalidConfig(field, "must be zero or a positive integer")
		}
		return invalidConfig(field, "must be a positive integer")
	}
	*target = *source
	return nil
}

func firstBoolPointer(preferred, fallback *bool) *bool {
	if preferred != nil {
		return preferred
	}
	return fallback
}
