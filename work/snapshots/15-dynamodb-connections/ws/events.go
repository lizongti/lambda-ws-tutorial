package ws

import (
	"fmt"
	"time"
)

const (
	RouteConnect    = "$connect"
	RouteDisconnect = "$disconnect"
)

// ConnectEvent is the transport-neutral representation of an API Gateway
// $connect event. ResumeCredential is write-only from the framework's point of
// view: it is hashed before storage and is never included in logs or metrics.
type ConnectEvent struct {
	ConnectionID                    string
	RequestID                       string
	RouteKey                        string
	SessionID                       string
	ResumeCredential                string
	Headers                         map[string]string
	QueryParameters                 map[string]string
	MultiValueHeaders               map[string][]string
	MultiValueQueryStringParameters map[string][]string
	RequestContext                  map[string]any
	ConnectedAt                     time.Time
}

// String deliberately omits credentials and all client-controlled metadata.
func (e ConnectEvent) String() string {
	return fmt.Sprintf("connectionId=%q requestId=%q", e.ConnectionID, e.RequestID)
}

func (e ConnectEvent) GoString() string { return e.String() }

// ConnectResult describes an accepted physical connection and its logical
// session. ResumeCredential is returned only to the connecting caller. API
// Gateway responses never serialize this result into their return body.
type ConnectResult struct {
	ConnectionID     string
	SessionID        string
	Generation       uint64
	ResumeCredential string
	Reconnected      bool
	Replay           ReplayResult
}

// String prevents accidental credential disclosure through formatted logs.
func (r ConnectResult) String() string {
	return fmt.Sprintf("connectionId=%q sessionId=%q generation=%d reconnected=%t", r.ConnectionID, r.SessionID, r.Generation, r.Reconnected)
}

func (r ConnectResult) GoString() string { return r.String() }

// DisconnectEvent represents $disconnect. Generation is optional for API
// Gateway events; when supplied by a direct caller it provides an additional
// fence in addition to the stored connection/session binding checks.
type DisconnectEvent struct {
	ConnectionID   string
	RequestID      string
	RouteKey       string
	SessionID      string
	Generation     uint64
	DisconnectedAt time.Time
}

// MessageEvent represents $default or a custom API Gateway route. Body must be
// a text JSON Envelope; base64/binary messages are rejected by Engine.
type MessageEvent struct {
	ConnectionID    string
	RequestID       string
	RouteKey        string
	SessionID       string
	Generation      uint64
	Body            string
	IsBase64Encoded bool
}

// EventResponse is the small response model used by the API Gateway adapter.
// Application messages are always sent through Pusher, never through Body.
type EventResponse struct {
	StatusCode int
	Body       string
	Headers    map[string]string
}
