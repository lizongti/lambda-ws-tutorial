package tests

import (
	"context"
	"github.com/aura-studio/lambda/ws"
	"testing"
	"time"
)

type chapter02Pusher struct{}

func (chapter02Pusher) PostToConnection(context.Context, string, []byte) error { return nil }

func TestChapter02OptionsAndYAML(t *testing.T) {
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithAuthenticator(ws.NewAnonymousAuthenticator()),
		ws.WithPusher(chapter02Pusher{}),
		ws.OptionFunc(func(o *ws.Options) { o.Auth.Anonymous = true }),
		ws.WithConfig([]byte("heartbeat:\n  interval: 10s\n  timeout: 30s\n")),
	)
	if err := options.Validate(); err != nil {
		t.Fatal(err)
	}
	if options.Heartbeat.Interval != 10*time.Second || options.Heartbeat.Timeout != 30*time.Second {
		t.Fatalf("YAML override failed: %#v", options.Heartbeat)
	}
	options.SharedDeployment = true
	if err := options.Validate(); err == nil {
		t.Fatal("shared memory deployment was accepted")
	}
}

func TestChapter02EndpointValidation(t *testing.T) {
	endpoint, err := ws.BuildAPIGatewayManagementEndpoint("api", "cn-north-1", "prod")
	if err != nil || endpoint != "https://api.execute-api.cn-north-1.amazonaws.com.cn/prod" {
		t.Fatalf("endpoint=%q err=%v", endpoint, err)
	}
	if _, err := ws.ValidateAPIGatewayManagementEndpoint("https://user:pass@example.com/prod"); err == nil {
		t.Fatal("credential endpoint accepted")
	}
}
