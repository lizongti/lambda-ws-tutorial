package ws

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultPushInitialBackoff = 50 * time.Millisecond
	DefaultPushMaxBackoff     = time.Second
)

type Pusher interface {
	PostToConnection(context.Context, string, []byte) error
}
type SleepFunc func(context.Context, time.Duration) error

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

func BuildAPIGatewayManagementEndpoint(apiID, region, stage string) (string, error) {
	if apiID == "" || region == "" || stage == "" || strings.ContainsAny(apiID+region+stage, "/ ?#") {
		return "", errors.New("invalid API Gateway endpoint components")
	}
	domain := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		domain = "amazonaws.com.cn"
	}
	return fmt.Sprintf("https://%s.execute-api.%s.%s/%s", apiID, region, domain, stage), nil
}

func ValidateAPIGatewayManagementEndpoint(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" || u.Path == "/" {
		return "", errors.New("invalid API Gateway management endpoint")
	}
	return strings.TrimSuffix(endpoint, "/"), nil
}
