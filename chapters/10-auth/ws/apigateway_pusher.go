package ws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi/types"
	"github.com/aws/smithy-go"
)

const (
	// DefaultPushInitialBackoff is used when InitialBackoff is omitted.
	DefaultPushInitialBackoff = 50 * time.Millisecond
	// DefaultPushMaxBackoff bounds every injected or default sleep.
	DefaultPushMaxBackoff = time.Second
)

// SleepFunc is injectable for deterministic retry tests. Implementations should
// return promptly when ctx is canceled.
type SleepFunc func(ctx context.Context, delay time.Duration) error

// PostToConnectionClient is the subset of the AWS SDK client used by Pusher.
type PostToConnectionClient interface {
	PostToConnection(ctx context.Context, params *apigatewaymanagementapi.PostToConnectionInput, optFns ...func(*apigatewaymanagementapi.Options)) (*apigatewaymanagementapi.PostToConnectionOutput, error)
}

// APIGatewayPusherConfig contains only trusted deployment configuration. Target
// connection IDs are supplied separately to Push and never read from an endpoint.
type APIGatewayPusherConfig struct {
	APIID              string
	Region             string
	Stage              string
	ManagementEndpoint string

	MaxMessageBytes int
	// MaxRetries is the number of retries after the initial request. Zero
	// disables adapter retries; negative values are invalid.
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Sleep          SleepFunc
}

// APIGatewayPusher implements Pusher with API Gateway Management API.
type APIGatewayPusher struct {
	client          PostToConnectionClient
	endpoint        string
	maxMessageBytes int
	maxRetries      int
	initialBackoff  time.Duration
	maxBackoff      time.Duration
	sleep           SleepFunc
}

// BuildAPIGatewayManagementEndpoint constructs the standard stage-specific AWS
// endpoint from validated deployment fields.
func BuildAPIGatewayManagementEndpoint(apiID, region, stage string) (string, error) {
	for name, value := range map[string]string{"API ID": apiID, "region": region, "stage": stage} {
		if !validEndpointComponent(value) {
			return "", NewPushError(PushErrorInvalid, errors.New(name+" is empty or contains invalid characters"))
		}
	}
	domain := "amazonaws.com"
	if strings.HasPrefix(strings.ToLower(region), "cn-") {
		domain = "amazonaws.com.cn"
	}
	return "https://" + apiID + ".execute-api." + region + "." + domain + "/" + stage, nil
}

// ValidateAPIGatewayManagementEndpoint validates and normalizes an explicit
// trusted endpoint. HTTPS and a stage/base path are required; credentials,
// query parameters, and fragments are rejected.
func ValidateAPIGatewayManagementEndpoint(endpoint string) (string, error) {
	invalid := func(message string) (string, error) {
		return "", NewPushError(PushErrorInvalid, errors.New(message))
	}
	if endpoint == "" || strings.TrimSpace(endpoint) != endpoint {
		return invalid("management endpoint is empty or has surrounding whitespace")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Opaque != "" || u.Scheme != "https" || u.Hostname() == "" {
		return invalid("management endpoint must be an absolute HTTPS URL")
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return invalid("management endpoint must not contain credentials, query, or fragment")
	}
	if strings.Contains(strings.ToLower(u.EscapedPath()), "%2f") || strings.Contains(strings.ToLower(u.EscapedPath()), "%5c") {
		return invalid("management endpoint must not contain encoded path separators")
	}
	for _, segment := range strings.Split(u.Path, "/") {
		if segment == "." || segment == ".." {
			return invalid("management endpoint must not contain relative path segments")
		}
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	if u.Path == "" {
		return invalid("management endpoint must include a stage or base path")
	}
	return u.String(), nil
}

// NewAPIGatewayPusher constructs an adapter around an injectable SDK client.
func NewAPIGatewayPusher(client PostToConnectionClient, cfg APIGatewayPusherConfig) (*APIGatewayPusher, error) {
	if client == nil {
		return nil, NewPushError(PushErrorInvalid, errors.New("PostToConnection client is required"))
	}
	endpoint, err := apiGatewayManagementEndpoint(cfg)
	if err != nil {
		return nil, err
	}
	return newAPIGatewayPusher(client, cfg, endpoint)
}

// NewAPIGatewayPusherFromConfig constructs a real SDK client. SDK retries are
// disabled so MaxRetries bounds the total number of PostToConnection requests.
func NewAPIGatewayPusherFromConfig(awsCfg aws.Config, cfg APIGatewayPusherConfig) (*APIGatewayPusher, error) {
	endpoint, err := apiGatewayManagementEndpoint(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Region != "" {
		awsCfg.Region = cfg.Region
	}
	client := apigatewaymanagementapi.NewFromConfig(awsCfg, func(options *apigatewaymanagementapi.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.RetryMaxAttempts = 1
	})
	return newAPIGatewayPusher(client, cfg, endpoint)
}

func newAPIGatewayPusher(client PostToConnectionClient, cfg APIGatewayPusherConfig, endpoint string) (*APIGatewayPusher, error) {
	if cfg.MaxMessageBytes < 0 || cfg.MaxRetries < 0 || cfg.InitialBackoff < 0 || cfg.MaxBackoff < 0 {
		return nil, NewPushError(PushErrorInvalid, errors.New("push limits and backoff durations must not be negative"))
	}
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = DefaultMaxPushBytes
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = DefaultPushInitialBackoff
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = DefaultPushMaxBackoff
	}
	if cfg.InitialBackoff > cfg.MaxBackoff {
		return nil, NewPushError(PushErrorInvalid, errors.New("initial backoff exceeds maximum backoff"))
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepWithContext
	}
	return &APIGatewayPusher{
		client:          client,
		endpoint:        endpoint,
		maxMessageBytes: cfg.MaxMessageBytes,
		maxRetries:      cfg.MaxRetries,
		initialBackoff:  cfg.InitialBackoff,
		maxBackoff:      cfg.MaxBackoff,
		sleep:           cfg.Sleep,
	}, nil
}

func apiGatewayManagementEndpoint(cfg APIGatewayPusherConfig) (string, error) {
	if cfg.ManagementEndpoint != "" {
		return ValidateAPIGatewayManagementEndpoint(cfg.ManagementEndpoint)
	}
	return BuildAPIGatewayManagementEndpoint(cfg.APIID, cfg.Region, cfg.Stage)
}

// Endpoint returns the configured, validated management endpoint.
func (p *APIGatewayPusher) Endpoint() string {
	return p.endpoint
}

// PostToConnection implements the transport-level Pusher contract.
func (p *APIGatewayPusher) PostToConnection(ctx context.Context, connectionID string, body []byte) error {
	_, err := p.Push(ctx, PushInput{ConnectionID: connectionID, Body: body})
	return err
}

// Push posts an immutable snapshot of Body with bounded exponential retries for
// throttled and temporarily unavailable failures.
func (p *APIGatewayPusher) Push(ctx context.Context, input PushInput) (PushResult, error) {
	result := PushResult{ConnectionID: input.ConnectionID, MessageID: input.MessageID}
	if err := validatePushContext(ctx); err != nil {
		return result, err
	}
	if err := validatePushInput(input, p.maxMessageBytes); err != nil {
		return result, err
	}

	privateBody := bytes.Clone(input.Body)
	for {
		result.Attempts++
		_, err := p.client.PostToConnection(ctx, &apigatewaymanagementapi.PostToConnectionInput{
			ConnectionId: aws.String(input.ConnectionID),
			Data:         bytes.Clone(privateBody),
		})
		if err == nil {
			return result, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return result, err
		}
		pushErr := ClassifyPushError(err)
		if !IsRetryablePushError(pushErr) || result.Attempts > p.maxRetries {
			return result, pushErr
		}
		if err := p.sleep(ctx, p.retryDelay(result.Attempts)); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return result, ctxErr
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return result, err
			}
			return result, NewPushError(PushErrorUnknown, err)
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
	}
}

func (p *APIGatewayPusher) retryDelay(attempts int) time.Duration {
	delay := p.initialBackoff
	for retry := 1; retry < attempts; retry++ {
		if delay >= p.maxBackoff/2 {
			return p.maxBackoff
		}
		delay *= 2
		if delay >= p.maxBackoff {
			return p.maxBackoff
		}
	}
	if delay > p.maxBackoff {
		return p.maxBackoff
	}
	return delay
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ClassifyPushError maps modeled AWS errors, Smithy errors, HTTP status codes,
// and network failures to the stable Pusher classification.
func ClassifyPushError(err error) *PushError {
	if err == nil {
		return nil
	}
	var existing *PushError
	if errors.As(err, &existing) {
		return existing
	}
	var gone *types.GoneException
	if errors.As(err, &gone) {
		return NewPushError(PushErrorGone, err)
	}
	var limited *types.LimitExceededException
	if errors.As(err, &limited) {
		return NewPushError(PushErrorThrottled, err)
	}
	var tooLarge *types.PayloadTooLargeException
	if errors.As(err, &tooLarge) {
		return NewPushError(PushErrorInvalid, err)
	}
	var forbidden *types.ForbiddenException
	if errors.As(err, &forbidden) {
		return NewPushError(PushErrorInvalid, err)
	}

	var apiErr smithy.APIError
	code := ""
	if errors.As(err, &apiErr) {
		code = strings.ToLower(apiErr.ErrorCode())
	}
	switch code {
	case "goneexception":
		return NewPushError(PushErrorGone, err)
	case "limitexceededexception", "throttling", "throttlingexception", "throttledexception", "toomanyrequestsexception", "requestlimitexceeded":
		return NewPushError(PushErrorThrottled, err)
	case "internalerror", "internalservererror", "internalfailure", "serviceunavailable", "serviceunavailableexception", "requesttimeout", "requesttimeoutexception", "timeouterror":
		return NewPushError(PushErrorUnavailable, err)
	case "badrequestexception", "validationexception", "validationerror", "payloadtoolargeexception", "forbiddenexception", "accessdeniedexception", "unauthorizedexception", "missingauthenticationtokenexception":
		return NewPushError(PushErrorInvalid, err)
	}

	status := 0
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		status = statusErr.HTTPStatusCode()
	}
	switch status {
	case http.StatusGone:
		return NewPushError(PushErrorGone, err)
	case http.StatusTooManyRequests:
		return NewPushError(PushErrorThrottled, err)
	case http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return NewPushError(PushErrorUnavailable, err)
	}
	if status >= 500 {
		return NewPushError(PushErrorUnavailable, err)
	}
	if status >= 400 && status < 500 {
		return NewPushError(PushErrorInvalid, err)
	}
	if errors.As(err, &apiErr) && apiErr.ErrorFault() == smithy.FaultServer {
		return NewPushError(PushErrorUnavailable, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return NewPushError(PushErrorUnavailable, err)
	}
	return NewPushError(PushErrorUnknown, err)
}

func validEndpointComponent(value string) bool {
	if value == "" || value == "." || value == ".." || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}
