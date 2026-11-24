package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aura-studio/lambda/dynamic"
	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// Handler is the transport-neutral lifecycle interface implemented by Engine.
type Handler interface {
	HandleConnect(context.Context, ConnectEvent) (ConnectResult, error)
	HandleDisconnect(context.Context, DisconnectEvent) error
	HandleMessage(context.Context, MessageEvent) error
}

var _ Handler = (*Engine)(nil)

var (
	serverMu sync.Mutex
	engine   *Engine
	factory  *Factory
)

// Serve constructs the configured Engine and registers its API Gateway
// WebSocket handler with AWS Lambda. dynamicOpts are accepted for consistency
// with the other Lambda modules; the WS lifecycle does not depend on dynamic
// application packages.
func Serve(wsOpts []Option, dynamicOpts []dynamic.Option) {
	_ = dynamicOpts

	resourceFactory := NewFactory(NewOptions(wsOpts...))
	configuredEngine, err := resourceFactory.Build()
	if err != nil {
		_ = resourceFactory.Close()
		panic(fmt.Errorf("ws.Serve: %w", err))
	}

	serverMu.Lock()
	previousFactory := factory
	factory = resourceFactory
	engine = configuredEngine
	serverMu.Unlock()

	if previousFactory != nil {
		_ = previousFactory.Close()
	}
	lambda.Start(engine.Invoke)
}

// Close releases resources created by the WS factory. It is safe to call Close
// without a preceding Serve or more than once. Injected dependencies are left
// under their caller's ownership.
func Close() error {
	serverMu.Lock()
	resourceFactory := factory
	factory = nil
	engine = nil
	serverMu.Unlock()

	if resourceFactory == nil {
		return nil
	}
	return resourceFactory.Close()
}

// ConnectEventFromAPIGateway maps approved connection metadata without
// interpreting an application authentication format.
func ConnectEventFromAPIGateway(request events.APIGatewayWebsocketProxyRequest) ConnectEvent {
	requestContext := request.RequestContext
	query := request.QueryStringParameters
	return ConnectEvent{
		ConnectionID:                    requestContext.ConnectionID,
		RequestID:                       requestContext.RequestID,
		RouteKey:                        requestContext.RouteKey,
		SessionID:                       firstMetadataValue(query, request.MultiValueQueryStringParameters, "sessionId", "sessionID"),
		ResumeCredential:                firstCredential(request.Headers, query, request.MultiValueQueryStringParameters),
		Headers:                         cloneStringMap(request.Headers),
		QueryParameters:                 cloneStringMap(query),
		MultiValueHeaders:               cloneStringSliceMap(request.MultiValueHeaders),
		MultiValueQueryStringParameters: cloneStringSliceMap(request.MultiValueQueryStringParameters),
		RequestContext:                  apiGatewayRequestContext(request),
		ConnectedAt:                     time.UnixMilli(requestContext.ConnectedAt),
	}
}

// DisconnectEventFromAPIGateway retains correlation and disconnect timing while
// leaving generation fencing to the stored current binding.
func DisconnectEventFromAPIGateway(request events.APIGatewayWebsocketProxyRequest) DisconnectEvent {
	requestContext := request.RequestContext
	return DisconnectEvent{
		ConnectionID:   requestContext.ConnectionID,
		RequestID:      requestContext.RequestID,
		RouteKey:       requestContext.RouteKey,
		DisconnectedAt: time.UnixMilli(requestContext.RequestTimeEpoch),
	}
}

// MessageEventFromAPIGateway preserves the original route key for diagnostics
// and trusted route-to-path resolution. Base64 bodies are not decoded as text.
func MessageEventFromAPIGateway(request events.APIGatewayWebsocketProxyRequest) MessageEvent {
	requestContext := request.RequestContext
	return MessageEvent{
		ConnectionID:    requestContext.ConnectionID,
		RequestID:       requestContext.RequestID,
		RouteKey:        requestContext.RouteKey,
		Body:            request.Body,
		IsBase64Encoded: request.IsBase64Encoded,
	}
}

// HandleEvent adapts API Gateway's WebSocket proxy request to Engine. Expected
// lifecycle failures are represented by HTTP status codes and a nil Lambda
// error; internal details and application payloads never enter the return body.
func (e *Engine) HandleEvent(ctx context.Context, request events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	var err error
	switch request.RequestContext.RouteKey {
	case RouteConnect:
		_, err = e.HandleConnect(ctx, ConnectEventFromAPIGateway(request))
	case RouteDisconnect:
		err = e.HandleDisconnect(ctx, DisconnectEventFromAPIGateway(request))
	case DefaultRouteKey:
		err = e.HandleMessage(ctx, MessageEventFromAPIGateway(request))
	default:
		// Unknown/custom route keys follow Router.ResolvePath's $default rule.
		err = e.HandleMessage(ctx, MessageEventFromAPIGateway(request))
	}
	return events.APIGatewayProxyResponse{StatusCode: statusForError(err)}, nil
}

// HandleAPIGatewayEvent is an explicit alias useful at integration boundaries.
func (e *Engine) HandleAPIGatewayEvent(ctx context.Context, request events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	return e.HandleEvent(ctx, request)
}

// Invoke matches the Lambda handler function signature.
func (e *Engine) Invoke(ctx context.Context, request events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	return e.HandleEvent(ctx, request)
}

func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var protocolErr *ProtocolError
	switch {
	case errors.As(err, &protocolErr), errors.Is(err, ErrInvalidEvent):
		return http.StatusBadRequest
	case errors.Is(err, ErrAuthenticationFailed), errors.Is(err, ErrInvalidResume):
		return http.StatusUnauthorized
	case errors.Is(err, ErrAuthorizationFailed):
		return http.StatusForbidden
	case errors.Is(err, ErrConnectionNotFound), errors.Is(err, ErrSessionNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrConnectionConflict), errors.Is(err, ErrReconnectDisabled):
		return http.StatusConflict
	case IsRetryablePushError(err):
		return http.StatusServiceUnavailable
	default:
		if code := wsstore.ErrorCodeOf(err); code == wsstore.ErrorCodeUnavailable {
			return http.StatusServiceUnavailable
		}
		return http.StatusInternalServerError
	}
}

func firstCredential(headers, query map[string]string, multiQuery map[string][]string) string {
	for _, key := range []string{"x-ws-resume-credential", "x-resume-credential"} {
		for actual, value := range headers {
			if strings.EqualFold(actual, key) {
				return value
			}
		}
	}
	return firstMetadataValue(query, multiQuery, "resumeCredential", "resumeToken", "resume")
}

func firstMetadataValue(query map[string]string, multi map[string][]string, keys ...string) string {
	for _, key := range keys {
		if value := query[key]; value != "" {
			return value
		}
		for actual, value := range query {
			if strings.EqualFold(actual, key) && value != "" {
				return value
			}
		}
		if values := multi[key]; len(values) > 0 {
			return values[0]
		}
		for actual, values := range multi {
			if strings.EqualFold(actual, key) && len(values) > 0 {
				return values[0]
			}
		}
	}
	return ""
}

func apiGatewayRequestContext(request events.APIGatewayWebsocketProxyRequest) map[string]any {
	r := request.RequestContext
	return map[string]any{
		"accountId":            r.AccountID,
		"resourceId":           r.ResourceID,
		"stage":                r.Stage,
		"requestId":            r.RequestID,
		"identity":             r.Identity,
		"resourcePath":         r.ResourcePath,
		"authorizer":           r.Authorizer,
		"httpMethod":           r.HTTPMethod,
		"apiId":                r.APIID,
		"connectedAt":          r.ConnectedAt,
		"connectionId":         r.ConnectionID,
		"domainName":           r.DomainName,
		"eventType":            r.EventType,
		"extendedRequestId":    r.ExtendedRequestID,
		"integrationLatency":   r.IntegrationLatency,
		"messageDirection":     r.MessageDirection,
		"messageId":            r.MessageID,
		"requestTime":          r.RequestTime,
		"requestTimeEpoch":     r.RequestTimeEpoch,
		"routeKey":             r.RouteKey,
		"disconnectStatusCode": r.DisconnectStatusCode,
		"disconnectReason":     r.DisconnectReason,
	}
}
