package ws

import (
	"context"
	"errors"
)

var (
	ErrAuthenticationFailed = errors.New("ws: authentication failed")
	ErrAuthorizationFailed  = errors.New("ws: authorization failed")
	ErrInvalidEvent         = errors.New("ws: invalid event")
	ErrConnectionNotFound   = errors.New("ws: connection not found")
	ErrSessionNotFound      = errors.New("ws: session not found")
	ErrStaleConnection      = errors.New("ws: stale connection")
	ErrInvalidResume        = errors.New("ws: invalid resume credential")
	ErrReconnectDisabled    = errors.New("ws: reconnect is disabled")
	ErrConnectionConflict   = errors.New("ws: connection binding conflict")
)

type ReplayResult struct {
	Attempted, Sent, Acknowledged, Retained, Failed, Batches int
	Truncated                                                bool
}
type Engine struct{}

func (*Engine) HandleConnect(_ context.Context, event ConnectEvent) (ConnectResult, error) {
	return ConnectResult{ConnectionID: event.ConnectionID}, nil
}
func (*Engine) HandleDisconnect(context.Context, DisconnectEvent) error { return nil }
func (*Engine) HandleMessage(context.Context, MessageEvent) error       { return nil }
