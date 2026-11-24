package tests

import (
	"context"
	"github.com/aura-studio/lambda/server"
	"github.com/aura-studio/lambda/ws"
	"testing"
)

func TestChapter19ServerOptionsFactoryAndObservability(t *testing.T) {
	options := &server.Options{}
	server.WithLambdaType("ws").Apply(options)
	server.WithWsOptions(ws.WithDevelopment(true)).Apply(options)
	if options.Lambda != "ws" || len(options.Ws) != 1 {
		t.Fatalf("server options=%#v", options)
	}
	engine, _, _ := newChapter09Engine(t, nil)
	events := []ws.LogEvent{}
	engine.SetLogger(ws.LoggerFunc(func(_ context.Context, event ws.LogEvent) { events = append(events, event) }))
	if engine.StateStore() == nil {
		t.Fatal("factory engine has no state store")
	}
}
