package ws

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultHeartbeatInterval             = 30 * time.Second
	DefaultHeartbeatTimeout              = 90 * time.Second
	DefaultHeartbeatMaintenanceBatchSize = 100
	DefaultReconnectSessionRetention     = 24 * time.Hour
	DefaultOfflineAckTimeout             = 30 * time.Second
	DefaultOfflineMaxMessagesPerSession  = 1000
	DefaultOfflineRetention              = 24 * time.Hour
	DefaultOfflineReplayBatchSize        = 100
	DefaultRoomsMaxMembers               = 10000
	DefaultRoomsBroadcastBatchSize       = 100
	DefaultPushConcurrency               = 16
	DefaultPushMaxRetries                = 2
	DefaultMessageMaxBytes               = MaxMessageBytes
	DefaultMessageIdempotencyWindow      = 5 * time.Minute
)

var defaultOptions = &Options{
	State: StateConfig{Backend: BackendMemory},
	Auth: AuthConfig{
		Development: false,
		Required:    false,
		Protected:   false,
		Anonymous:   false,
	},
	Heartbeat: HeartbeatConfig{
		Interval:             DefaultHeartbeatInterval,
		Timeout:              DefaultHeartbeatTimeout,
		MaintenanceBatchSize: DefaultHeartbeatMaintenanceBatchSize,
	},
	Reconnect: ReconnectConfig{
		Enabled:          true,
		SessionRetention: DefaultReconnectSessionRetention,
		ReplaceExisting:  true,
	},
	Offline: OfflineConfig{
		Enabled:               true,
		AckPolicy:             AckPolicyClientAck,
		AckTimeout:            DefaultOfflineAckTimeout,
		MaxMessagesPerSession: DefaultOfflineMaxMessagesPerSession,
		Retention:             DefaultOfflineRetention,
		Overflow:              "reject-new",
		ReplayBatchSize:       DefaultOfflineReplayBatchSize,
	},
	Rooms: RoomsConfig{
		Enabled:            true,
		MaxMembers:         DefaultRoomsMaxMembers,
		BroadcastBatchSize: DefaultRoomsBroadcastBatchSize,
	},
	Push: PushConfig{
		Concurrency:    DefaultPushConcurrency,
		MaxRetries:     DefaultPushMaxRetries,
		InitialBackoff: DefaultPushInitialBackoff,
		MaxBackoff:     DefaultPushMaxBackoff,
	},
	Message: MessageConfig{
		MaxBytes:          DefaultMessageMaxBytes,
		IdempotencyWindow: DefaultMessageIdempotencyWindow,
	},
	Clock:        ClockFunc(time.Now),
	IDGenerator:  IDGeneratorFunc(newDefaultID),
	RetrySleeper: sleepWithContext,
}

func newDefaultID(prefix string) string {
	id := uuid.NewString()
	if prefix == "" {
		return id
	}
	return prefix + "-" + id
}

// DefaultConfigCandidates returns relative paths checked in order when
// searching for a default WebSocket config.
func DefaultConfigCandidates() []string {
	return []string{
		"ws.yaml",
		"ws.yml",
		filepath.FromSlash("ws/ws.yaml"),
		filepath.FromSlash("ws/ws.yml"),
	}
}

// FindDefaultConfigFile searches the working directory and executable directory
// for a default WebSocket config.
func FindDefaultConfigFile() (string, error) {
	candidates := DefaultConfigCandidates()
	dirs := []string{"."}
	if executable, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(executable))
	}
	for _, dir := range dirs {
		for _, relative := range candidates {
			path := relative
			if dir != "." {
				path = filepath.Join(dir, relative)
			}
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				return path, nil
			}
		}
	}
	return "", fmt.Errorf("ws config not found (expected %v)", candidates)
}

// WithDefaultConfigFile finds and loads the default WebSocket config. It
// panics when no candidate can be found or read.
func WithDefaultConfigFile() Option {
	path, err := FindDefaultConfigFile()
	if err != nil {
		return OptionFunc(func(*Options) {
			panic(fmt.Errorf("ws.WithDefaultConfigFile: %w", err))
		})
	}
	return WithConfigFile(path)
}
