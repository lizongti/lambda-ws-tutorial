package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
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
	ErrOfflineDisabled      = errors.New("ws: offline delivery is disabled")
	ErrRoomsDisabled        = errors.New("ws: rooms are disabled")
	ErrRoomFull             = errors.New("ws: room member limit reached")
	ErrInternal             = errors.New("ws: internal error")
)

// Engine composes the generic WebSocket transport, state and routing services.
// Router is embedded so applications can use Handle, Use, NoRoute and
// MapRouteKey directly, consistently with the other lambda modules.
type Engine struct {
	*Options
	*Router
	stateStore    wsstore.StateStore
	observability *observability
	connectMu     sync.Mutex
}

// NewEngine validates the complete configuration and all required runtime
// dependencies. It does not start goroutines or perform network calls.
func NewEngine(options *Options) (*Engine, error) {
	if options == nil {
		return nil, invalidConfig("options", "must not be nil")
	}
	if nilDependency(options.StateStore) {
		return nil, invalidConfig("stateStore", "must not be nil")
	}
	if nilDependency(options.Pusher) {
		return nil, invalidConfig("pusher", "must not be nil")
	}
	copyOptions := *options
	if nilDependency(copyOptions.RetrySleeper) {
		copyOptions.RetrySleeper = sleepWithContext
	}
	if err := copyOptions.Validate(); err != nil {
		return nil, err
	}
	stateStore := copyOptions.StateStore
	copyOptions.StateStore = &retryingStateStore{delegate: stateStore, options: &copyOptions}
	return &Engine{
		Options:       &copyOptions,
		Router:        NewRouter(),
		stateStore:    stateStore,
		observability: &observability{},
	}, nil
}

// StateStore returns the single coherent backend selected at construction.
func (e *Engine) StateStore() wsstore.StateStore {
	if e == nil || e.Options == nil {
		return nil
	}
	return e.stateStore
}

// HandleMessage validates, fences, decodes and dispatches one text message.
// Protocol and application failures are sent as error Envelopes; an error is
// returned only when the framework could not complete that handling itself.
func (e *Engine) HandleMessage(ctx context.Context, event MessageEvent) (err error) {
	if e == nil || e.Options == nil || e.Router == nil {
		return ErrInternal
	}
	defer func() {
		if recover() != nil {
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return ErrInvalidEvent
	}
	if err := ValidateConnectionID(event.ConnectionID); err != nil {
		return fmt.Errorf("%w: connectionId", ErrInvalidEvent)
	}
	if event.IsBase64Encoded {
		return e.sendMessageError(ctx, event, nil, newProtocolError(ErrorCodeInvalidPayload, "body", nil))
	}

	envelope, decodeErr := decodeInboundEnvelope(event, e.Options.Message.MaxBytes)
	if decodeErr != nil {
		return e.sendMessageError(ctx, event, nil, decodeErr)
	}
	connection, session, err := e.validateActiveBinding(ctx, event.ConnectionID)
	if err != nil {
		if isFencingError(err) {
			return nil
		}
		return err
	}
	if event.Generation != 0 && event.Generation != connection.Generation {
		return nil
	}
	if event.SessionID != "" && event.SessionID != connection.SessionID {
		return nil
	}
	if envelope.SessionID != "" && envelope.SessionID != connection.SessionID {
		return e.sendMessageError(ctx, event, &connection, NewHandlerError("session_mismatch", "session does not match connection"))
	}

	requestID := envelope.RequestID
	if requestID == "" {
		requestID = event.RequestID
	}
	touch := func() (bool, error) {
		if err := e.Options.StateStore.TouchConnection(ctx, connection.ConnectionID, connection.Generation, e.Options.Clock.Now()); err != nil {
			if isFencingError(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	switch envelope.Type {
	case TypePing:
		touched, err := touch()
		if err != nil || !touched {
			return err
		}
		msg := NewPong(requestID)
		msg.SessionID = session.SessionID
		return e.pushToConnectionRecord(ctx, connection, msg, false)
	case TypePong:
		_, err := touch()
		return err
	case TypeAck:
		// A rejected or stale acknowledgment must not keep a connection alive.
		if err := e.acknowledge(ctx, connection, *envelope); err != nil {
			if isFencingError(err) {
				return nil
			}
			return e.sendMessageError(ctx, event, &connection, NewHandlerError("ack_rejected", "acknowledgment rejected"))
		}
		_, err := touch()
		return err
	case TypeRequest, TypeEvent:
		touched, err := touch()
		if err != nil || !touched {
			return err
		}
		// Application messages continue below.
	default:
		return e.sendMessageError(ctx, event, &connection, newProtocolError(ErrorCodeInvalidType, "type", nil))
	}

	var reservation *wsstore.IdempotencyRecord
	if envelope.MessageID != "" {
		record, acquired, err := e.Options.StateStore.BeginIdempotency(ctx, wsstore.IdempotencyRecord{
			SessionID: session.SessionID,
			MessageID: envelope.MessageID,
			State:     wsstore.IdempotencyStateProcessing,
			CreatedAt: e.Options.Clock.Now(),
			ExpiresAt: e.Options.Clock.Now().Add(e.Options.Message.IdempotencyWindow),
		})
		if err != nil {
			return err
		}
		if !acquired {
			if record.State == wsstore.IdempotencyStateCompleted && len(record.Response) > 0 {
				return e.postEncoded(ctx, &connection, record.MessageID, record.Response)
			}
			// A concurrent or interrupted invocation already owns the message.
			return nil
		}
		reservation = &record
	}

	path := envelope.Path
	handlerCtx := &Context{
		Context:      ctx,
		ConnectionID: event.ConnectionID,
		SessionID:    session.SessionID,
		Principal:    Principal{Subject: session.PrincipalSubject},
		Path:         path,
		RequestID:    requestID,
		MessageID:    envelope.MessageID,
		Payload:      cloneRawMessage(envelope.Payload),
	}
	handlerCtx.originatingSessionID = session.SessionID
	if e.Options.Auth.Protected {
		if nilDependency(e.Options.Authorizer) {
			handlerCtx.AbortWithError(NewHandlerError("authorization_denied", "access denied"))
		} else if err := e.Options.Authorizer.Authorize(ctx, handlerCtx.Principal.Clone(), Action{
			Name: ActionPathAccess, Path: path, Resource: path, SessionID: session.SessionID,
		}); err != nil {
			handlerCtx.AbortWithError(NewHandlerError("authorization_denied", "access denied"))
		} else {
			e.Router.DispatchRoute(handlerCtx, event.RouteKey)
		}
	} else {
		e.Router.DispatchRoute(handlerCtx, event.RouteKey)
	}

	response, responseErr := e.responseForContext(handlerCtx)
	if responseErr != nil {
		response, _ = EncodeOutboundMessageWithLimit(NewErrorFromError(requestID, responseErr), e.Options.Message.MaxBytes)
	}
	if reservation != nil {
		if err := e.Options.StateStore.CompleteIdempotency(ctx, session.SessionID, envelope.MessageID, response); err != nil {
			return err
		}
	}
	if len(response) == 0 {
		return nil
	}
	return e.postEncoded(ctx, &connection, envelope.MessageID, response)
}

// Reply sends a response to the immutable originating connection captured at
// dispatch. No payload field or handler mutation can select another target.
func (e *Engine) Reply(ctx *Context, payload any) error {
	if e == nil || ctx == nil {
		return ErrInvalidEvent
	}
	body, err := marshalPayload(payload)
	if err != nil {
		return err
	}
	msg := NewResponse(ctx.RequestID, body)
	msg.SessionID = ctx.OriginatingSessionID()
	return e.Push(ctx.StdContext(), ctx.OriginatingConnectionID(), msg)
}

// Push serializes and sends msg only to connectionID. Unknown targets are never
// replaced with a session's or request's connection.
func (e *Engine) Push(ctx context.Context, connectionID string, msg OutboundMessage) error {
	if e == nil || ctx == nil {
		return ErrInvalidEvent
	}
	if err := ValidateConnectionID(connectionID); err != nil {
		return err
	}
	connection, err := e.Options.StateStore.GetConnection(ctx, connectionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return errors.Join(ErrConnectionNotFound, err)
		}
		return err
	}
	if connection.State != wsstore.ConnectionStateActive {
		return ErrStaleConnection
	}
	return e.pushToConnectionRecord(ctx, connection, msg, true)
}

func (e *Engine) pushToConnectionRecord(ctx context.Context, connection wsstore.Connection, msg OutboundMessage, queueOnGone bool) error {
	prepared, body, err := e.prepareOutbound(msg, connection.SessionID)
	if err != nil {
		return err
	}
	err = e.postEncoded(ctx, &connection, prepared.MessageID, body)
	if IsPushGone(err) && queueOnGone && connection.SessionID != "" && e.Options.Offline.Enabled {
		if queueErr := e.enqueueOffline(ctx, connection.SessionID, prepared, body); queueErr != nil {
			return errors.Join(err, queueErr)
		}
	}
	return err
}

func (e *Engine) prepareOutbound(msg OutboundMessage, sessionID string) (OutboundMessage, []byte, error) {
	msg.SessionID = sessionID
	if msg.MessageID == "" {
		msg.MessageID = e.Options.IDGenerator.ID("msg")
	}
	if err := ValidateMessageID(msg.MessageID); err != nil {
		return OutboundMessage{}, nil, err
	}
	msg.Payload = cloneRawMessage(msg.Payload)
	body, err := EncodeOutboundMessageWithLimit(msg, e.Options.Message.MaxBytes)
	if err != nil {
		return OutboundMessage{}, nil, err
	}
	return msg, body, nil
}

func (e *Engine) postEncoded(ctx context.Context, connection *wsstore.Connection, messageID string, body []byte) error {
	if ctx == nil || len(body) == 0 {
		return ErrInvalidEvent
	}
	connectionID := ""
	if connection != nil {
		connectionID = connection.ConnectionID
	}
	if err := ValidateConnectionID(connectionID); err != nil {
		return err
	}
	privateBody := bytes.Clone(body)
	var lastErr error
	for attempt := 0; attempt <= e.Options.Push.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := e.Options.Pusher.PostToConnection(ctx, connectionID, bytes.Clone(privateBody))
		if err == nil {
			e.observability.log(ctx, LogEvent{Name: LogEventPush, Outcome: "success", ConnectionID: connectionID, MessageID: messageID})
			return nil
		}
		lastErr = err
		e.observability.log(ctx, LogEvent{Name: LogEventPush, Outcome: "failure", ConnectionID: connectionID, MessageID: messageID, ErrorCode: safeErrorCode(err)})
		e.observability.record(ctx, Metric{Name: MetricPushFailures, Kind: MetricCounter, Value: 1, ConnectionID: connectionID})
		if IsPushGone(err) {
			current := connection
			if current == nil || current.Generation == 0 || current.SessionID == "" {
				if stored, getErr := e.Options.StateStore.GetConnection(ctx, connectionID); getErr == nil {
					current = &stored
				}
			}
			if current != nil {
				if cleanupErr := e.cleanupConnection(ctx, *current, "gone"); cleanupErr != nil && !isFencingError(cleanupErr) {
					lastErr = errors.Join(err, cleanupErr)
				}
			}
			return lastErr
		}
		if !IsRetryablePushError(err) || attempt == e.Options.Push.MaxRetries {
			return err
		}
		if err := e.Options.RetrySleeper(ctx, retryDelay(e.Options.Push, attempt+1)); err != nil {
			return err
		}
	}
	return lastErr
}

func retryDelay(config PushConfig, retry int) time.Duration {
	delay := config.InitialBackoff
	for i := 1; i < retry; i++ {
		if delay >= config.MaxBackoff/2 {
			return config.MaxBackoff
		}
		delay *= 2
	}
	if delay > config.MaxBackoff {
		return config.MaxBackoff
	}
	return delay
}

func (e *Engine) responseForContext(ctx *Context) ([]byte, error) {
	if ctx.Err() != nil {
		return EncodeOutboundMessageWithLimit(outboundError(ctx.RequestID, ctx.OriginatingSessionID(), ctx.Err()), e.Options.Message.MaxBytes)
	}
	if !ctx.HasResponse() {
		return nil, nil
	}
	payload, err := marshalPayload(ctx.Response())
	if err != nil {
		return nil, err
	}
	msg := NewResponse(ctx.RequestID, payload)
	msg.SessionID = ctx.OriginatingSessionID()
	return EncodeOutboundMessageWithLimit(msg, e.Options.Message.MaxBytes)
}

func outboundError(requestID, sessionID string, err error) OutboundMessage {
	var handlerErr *HandlerError
	if errors.As(err, &handlerErr) && handlerErr != nil {
		body := ErrorBody{Code: handlerErr.Code, Message: handlerErr.Message, Retryable: handlerErr.Retryable}
		msg := NewError(requestID, body)
		msg.SessionID = sessionID
		if msg.Validate() == nil {
			return msg
		}
	}
	msg := NewErrorFromError(requestID, err)
	msg.SessionID = sessionID
	return msg
}

func (e *Engine) sendMessageError(ctx context.Context, event MessageEvent, connection *wsstore.Connection, cause error) error {
	requestID := event.RequestID
	sessionID := event.SessionID
	if connection != nil {
		sessionID = connection.SessionID
	} else {
		connection = &wsstore.Connection{ConnectionID: event.ConnectionID}
	}
	msg := outboundError(requestID, sessionID, cause)
	if msg.MessageID == "" {
		msg.MessageID = e.Options.IDGenerator.ID("msg")
	}
	body, err := EncodeOutboundMessageWithLimit(msg, e.Options.Message.MaxBytes)
	if err != nil {
		return err
	}
	return e.postEncoded(ctx, connection, msg.MessageID, body)
}

func (e *Engine) validateActiveBinding(ctx context.Context, connectionID string) (wsstore.Connection, wsstore.Session, error) {
	connection, err := e.Options.StateStore.GetConnection(ctx, connectionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return wsstore.Connection{}, wsstore.Session{}, errors.Join(ErrConnectionNotFound, err)
		}
		return wsstore.Connection{}, wsstore.Session{}, err
	}
	if connection.State != wsstore.ConnectionStateActive || connection.SessionID == "" {
		return wsstore.Connection{}, wsstore.Session{}, ErrStaleConnection
	}
	session, err := e.Options.StateStore.GetSession(ctx, connection.SessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return wsstore.Connection{}, wsstore.Session{}, errors.Join(ErrSessionNotFound, err)
		}
		return wsstore.Connection{}, wsstore.Session{}, err
	}
	if session.CurrentConnectionID != connectionID || session.Generation != connection.Generation ||
		session.PrincipalSubject != connection.PrincipalSubject ||
		(!session.ExpiresAt.IsZero() && !session.ExpiresAt.After(e.Options.Clock.Now())) {
		return wsstore.Connection{}, wsstore.Session{}, ErrStaleConnection
	}
	return connection, session, nil
}

func decodeInboundEnvelope(event MessageEvent, maxBytes int) (*Envelope, error) {
	data := []byte(event.Body)
	envelope, err := DecodeEnvelopeWithLimit(data, maxBytes)
	if err == nil {
		return envelope, nil
	}
	// A custom API Gateway route may supply the path itself, so a request body
	// need not duplicate it. All other strict protocol validation still applies.
	var protocolErr *ProtocolError
	if event.RouteKey == "" || event.RouteKey == DefaultRouteKey || !errors.As(err, &protocolErr) ||
		protocolErr.Code != ErrorCodeMissingField || protocolErr.Field != "path" {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded Envelope
	if decodeErr := decoder.Decode(&decoded); decodeErr != nil {
		return nil, err
	}
	var trailing any
	if trailingErr := decoder.Decode(&trailing); trailingErr != io.EOF {
		return nil, err
	}
	decoded.Path = "/"
	if validateErr := decoded.Validate(); validateErr != nil {
		return nil, validateErr
	}
	return &decoded, nil
}

func marshalPayload(payload any) (json.RawMessage, error) {
	if payload == nil {
		return json.RawMessage("null"), nil
	}
	if raw, ok := payload.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return nil, newProtocolError(ErrorCodeInvalidPayload, "payload", nil)
		}
		return cloneRawMessage(raw), nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(body), nil
}

func safeErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var pushErr *PushError
	if errors.As(err, &pushErr) && pushErr != nil {
		return "push_" + string(pushErr.Kind)
	}
	if code := wsstore.ErrorCodeOf(err); code != wsstore.ErrorCodeUnknown {
		return "store_" + string(code)
	}
	if code := HandlerErrorCode(err); code != "internal_error" {
		return code
	}
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) && protocolErr != nil {
		return string(protocolErr.Code)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, ErrAuthenticationFailed):
		return "authentication_failed"
	case errors.Is(err, ErrAuthorizationFailed):
		return "authorization_denied"
	default:
		return "internal_error"
	}
}

func isFencingError(err error) bool {
	return errors.Is(err, ErrStaleConnection) || errors.Is(err, ErrConnectionNotFound) ||
		errors.Is(err, ErrSessionNotFound) || wsstore.IsConditionConflict(err) || wsstore.IsNotFound(err)
}
