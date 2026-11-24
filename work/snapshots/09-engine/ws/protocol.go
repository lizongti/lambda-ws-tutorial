package ws

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// ProtocolVersion is the only Envelope version understood by this package.
	ProtocolVersion = 1

	// MaxMessageBytes is the default limit for a complete encoded Envelope.
	MaxMessageBytes = 32768
	// MaxPathBytes is the maximum UTF-8 byte length of an application path.
	MaxPathBytes = 1024
	// MaxIDBytes is the maximum UTF-8 byte length of a protocol identifier.
	MaxIDBytes = 256
	// MaxErrorCodeBytes is the maximum UTF-8 byte length of an error code.
	MaxErrorCodeBytes = 128
	// MaxErrorMessageBytes is the maximum UTF-8 byte length of a safe error message.
	MaxErrorMessageBytes = 1024

	MaxConnectionIDBytes = MaxIDBytes
	MaxSessionIDBytes    = MaxIDBytes
	MaxRoomIDBytes       = MaxIDBytes
	MaxRequestIDBytes    = MaxIDBytes
	MaxMessageIDBytes    = MaxIDBytes
)

const (
	TypeRequest  = "request"
	TypeResponse = "response"
	TypeEvent    = "event"
	TypePing     = "ping"
	TypePong     = "pong"
	TypeAck      = "ack"
	TypeError    = "error"

	// MessageType aliases make the constants self-describing at call sites.
	MessageTypeRequest  = TypeRequest
	MessageTypeResponse = TypeResponse
	MessageTypeEvent    = TypeEvent
	MessageTypePing     = TypePing
	MessageTypePong     = TypePong
	MessageTypeAck      = TypeAck
	MessageTypeError    = TypeError
)

// ErrorCode is a stable, machine-readable protocol error code.
type ErrorCode string

const (
	ErrorCodeInvalidJSON        ErrorCode = "invalid_json"
	ErrorCodeUnsupportedVersion ErrorCode = "unsupported_version"
	ErrorCodeInvalidType        ErrorCode = "invalid_message_type"
	ErrorCodeMissingField       ErrorCode = "missing_field"
	ErrorCodeInvalidField       ErrorCode = "invalid_field"
	ErrorCodeMessageTooLarge    ErrorCode = "message_too_large"
	ErrorCodeInvalidUTF8        ErrorCode = "invalid_utf8"
	ErrorCodeInvalidPath        ErrorCode = "invalid_path"
	ErrorCodeInvalidID          ErrorCode = "invalid_identifier"
	ErrorCodeInvalidPayload     ErrorCode = "invalid_payload"
	ErrorCodeRouteNotFound      ErrorCode = "route_not_found"
	ErrorCodeInternal           ErrorCode = "internal_error"
)

// Envelope is the transport-neutral JSON message envelope. Payload remains raw
// JSON so applications can choose their own DTOs.
type Envelope struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	Path      string          `json:"path,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
	MessageID string          `json:"messageId,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     *ErrorBody      `json:"error,omitempty"`
}

// ErrorBody is safe to send to a peer. Message must not contain internal error
// text, credentials, stack traces, or dependency details.
type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// OutboundMessage describes an application or control message without requiring
// callers to set the protocol version. Encoding always emits ProtocolVersion.
type OutboundMessage struct {
	Type      string
	Path      string
	RequestID string
	MessageID string
	SessionID string
	Payload   json.RawMessage
	Error     *ErrorBody
}

// ProtocolError classifies a protocol failure without including input data in
// its external message. Cause is retained only for local errors.Is/errors.As.
type ProtocolError struct {
	Code      ErrorCode
	Message   string
	Retryable bool
	Field     string
	cause     error
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Message
}

// Unwrap exposes the local cause for programmatic classification. Error and
// SafeErrorBody never expose the cause's text to a remote peer.
func (e *ProtocolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Envelope converts an outbound message to a versioned wire Envelope.
func (m OutboundMessage) Envelope() Envelope {
	return Envelope{
		Version:   ProtocolVersion,
		Type:      m.Type,
		Path:      m.Path,
		RequestID: m.RequestID,
		MessageID: m.MessageID,
		SessionID: m.SessionID,
		Payload:   m.Payload,
		Error:     m.Error,
	}
}

// Validate checks all outbound fields and the raw payload.
func (m OutboundMessage) Validate() error {
	return m.Envelope().Validate()
}

// Encode serializes an outbound message using MaxMessageBytes.
func (m OutboundMessage) Encode() ([]byte, error) {
	return EncodeOutboundMessage(m)
}

// Validate checks version, type-specific required fields, identifiers, path,
// payload JSON, UTF-8, and error-body bounds. It does not impose a wire-size
// limit; use EncodeEnvelopeWithLimit for that check.
func (e Envelope) Validate() error {
	return ValidateEnvelope(e)
}

// NewResponse constructs a response correlated to a valid request ID.
func NewResponse(requestID string, payload json.RawMessage) OutboundMessage {
	return OutboundMessage{
		Type:      TypeResponse,
		RequestID: inheritRequestID(requestID),
		Payload:   payload,
	}
}

// NewEvent constructs an event at path, optionally correlated to a request.
func NewEvent(path, requestID string, payload json.RawMessage) OutboundMessage {
	return OutboundMessage{
		Type:      TypeEvent,
		Path:      path,
		RequestID: inheritRequestID(requestID),
		Payload:   payload,
	}
}

// NewPing constructs a heartbeat ping.
func NewPing(requestID string) OutboundMessage {
	return OutboundMessage{Type: TypePing, RequestID: inheritRequestID(requestID)}
}

// NewPong constructs a heartbeat pong correlated to a valid request ID.
func NewPong(requestID string) OutboundMessage {
	return OutboundMessage{Type: TypePong, RequestID: inheritRequestID(requestID)}
}

// NewAck constructs an acknowledgment for messageID.
func NewAck(requestID, messageID string) OutboundMessage {
	return OutboundMessage{
		Type:      TypeAck,
		RequestID: inheritRequestID(requestID),
		MessageID: messageID,
	}
}

// NewError constructs an error message from an explicitly safe ErrorBody.
func NewError(requestID string, body ErrorBody) OutboundMessage {
	return OutboundMessage{
		Type:      TypeError,
		RequestID: inheritRequestID(requestID),
		Error:     &body,
	}
}

// NewErrorFromError converts err to a safe error message. Protocol failures use
// canonical public messages; every other error becomes a generic internal error.
func NewErrorFromError(requestID string, err error) OutboundMessage {
	return NewError(requestID, SafeErrorBody(err))
}

// SafeErrorBody converts an error without leaking its concrete error text.
func SafeErrorBody(err error) ErrorBody {
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) && protocolErr != nil {
		if message, ok := publicErrorMessage(protocolErr.Code); ok {
			return ErrorBody{
				Code:      string(protocolErr.Code),
				Message:   message,
				Retryable: false,
			}
		}
	}
	return ErrorBody{
		Code:      string(ErrorCodeInternal),
		Message:   "internal error",
		Retryable: false,
	}
}

// DecodeEnvelope strictly decodes and validates one Envelope using
// MaxMessageBytes. Unknown fields and trailing JSON values are rejected.
func DecodeEnvelope(data []byte) (*Envelope, error) {
	return DecodeEnvelopeWithLimit(data, MaxMessageBytes)
}

// DecodeEnvelopeWithLimit strictly decodes and validates one Envelope. A
// non-positive maxBytes selects MaxMessageBytes.
func DecodeEnvelopeWithLimit(data []byte, maxBytes int) (*Envelope, error) {
	maxBytes = effectiveMessageLimit(maxBytes)
	if len(data) > maxBytes {
		return nil, newProtocolError(ErrorCodeMessageTooLarge, "", nil)
	}
	if !utf8.Valid(data) {
		return nil, newProtocolError(ErrorCodeInvalidUTF8, "", nil)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, newProtocolError(ErrorCodeInvalidJSON, "", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, newProtocolError(ErrorCodeInvalidJSON, "", err)
	}
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	return &envelope, nil
}

// EncodeEnvelope validates and serializes an Envelope using MaxMessageBytes.
func EncodeEnvelope(envelope Envelope) ([]byte, error) {
	return EncodeEnvelopeWithLimit(envelope, MaxMessageBytes)
}

// EncodeEnvelopeWithLimit validates and serializes an Envelope without
// modifying the caller's payload. A non-positive maxBytes selects
// MaxMessageBytes.
func EncodeEnvelopeWithLimit(envelope Envelope, maxBytes int) ([]byte, error) {
	maxBytes = effectiveMessageLimit(maxBytes)
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, err
	}

	// RawMessage is copied before marshaling so neither encoding nor subsequent
	// caller writes can create an aliasing surprise at the transport boundary.
	wireEnvelope := envelope
	wireEnvelope.Payload = cloneRawMessage(envelope.Payload)
	data, err := json.Marshal(wireEnvelope)
	if err != nil {
		return nil, newProtocolError(ErrorCodeInvalidPayload, "payload", err)
	}
	if len(data) > maxBytes {
		return nil, newProtocolError(ErrorCodeMessageTooLarge, "", nil)
	}
	return data, nil
}

// EncodeOutboundMessage validates and serializes msg using MaxMessageBytes.
func EncodeOutboundMessage(msg OutboundMessage) ([]byte, error) {
	return EncodeEnvelope(msg.Envelope())
}

// EncodeOutboundMessageWithLimit validates and serializes msg with maxBytes.
func EncodeOutboundMessageWithLimit(msg OutboundMessage, maxBytes int) ([]byte, error) {
	return EncodeEnvelopeWithLimit(msg.Envelope(), maxBytes)
}

// ValidateEnvelope validates protocol semantics and field boundaries.
func ValidateEnvelope(envelope Envelope) error {
	if envelope.Version == 0 {
		return newProtocolError(ErrorCodeMissingField, "version", nil)
	}
	if envelope.Version != ProtocolVersion {
		return newProtocolError(ErrorCodeUnsupportedVersion, "version", nil)
	}
	if envelope.Type == "" {
		return newProtocolError(ErrorCodeMissingField, "type", nil)
	}

	switch envelope.Type {
	case TypeRequest:
		if envelope.Path == "" {
			return newProtocolError(ErrorCodeMissingField, "path", nil)
		}
	case TypeResponse, TypeEvent, TypePing, TypePong, TypeError:
		// No additional common required fields.
	case TypeAck:
		if envelope.MessageID == "" {
			return newProtocolError(ErrorCodeMissingField, "messageId", nil)
		}
	default:
		return newProtocolError(ErrorCodeInvalidType, "type", nil)
	}

	if envelope.Type == TypeError && envelope.Error == nil {
		return newProtocolError(ErrorCodeMissingField, "error", nil)
	}
	if envelope.Path != "" {
		if err := ValidatePath(envelope.Path); err != nil {
			return err
		}
	}
	if envelope.RequestID != "" {
		if err := ValidateRequestID(envelope.RequestID); err != nil {
			return err
		}
	}
	if envelope.MessageID != "" {
		if err := ValidateMessageID(envelope.MessageID); err != nil {
			return err
		}
	}
	if envelope.SessionID != "" {
		if err := ValidateSessionID(envelope.SessionID); err != nil {
			return err
		}
	}
	if len(envelope.Payload) != 0 {
		if !utf8.Valid(envelope.Payload) {
			return newProtocolError(ErrorCodeInvalidUTF8, "payload", nil)
		}
		if !json.Valid(envelope.Payload) {
			return newProtocolError(ErrorCodeInvalidPayload, "payload", nil)
		}
	}
	if envelope.Error != nil {
		if err := validateErrorBody(*envelope.Error); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePath validates an absolute application route path.
func ValidatePath(path string) error {
	if path == "" {
		return newProtocolError(ErrorCodeInvalidPath, "path", nil)
	}
	if !utf8.ValidString(path) {
		return newProtocolError(ErrorCodeInvalidUTF8, "path", nil)
	}
	if len(path) > MaxPathBytes || path[0] != '/' || strings.ContainsAny(path, "?#\\") {
		return newProtocolError(ErrorCodeInvalidPath, "path", nil)
	}
	for _, r := range path {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return newProtocolError(ErrorCodeInvalidPath, "path", nil)
		}
	}
	return nil
}

// ValidateConnectionID validates an API Gateway connection identifier.
func ValidateConnectionID(id string) error {
	return validateID(id, "connectionId", MaxConnectionIDBytes)
}

// ValidateSessionID validates a logical session identifier.
func ValidateSessionID(id string) error {
	return validateID(id, "sessionId", MaxSessionIDBytes)
}

// ValidateRoomID validates a room identifier.
func ValidateRoomID(id string) error {
	return validateID(id, "roomId", MaxRoomIDBytes)
}

// ValidateRequestID validates a request correlation identifier.
func ValidateRequestID(id string) error {
	return validateID(id, "requestId", MaxRequestIDBytes)
}

// ValidateMessageID validates a message correlation/deduplication identifier.
func ValidateMessageID(id string) error {
	return validateID(id, "messageId", MaxMessageIDBytes)
}

func validateID(id, field string, maxBytes int) error {
	if id == "" {
		return newProtocolError(ErrorCodeInvalidID, field, nil)
	}
	if !utf8.ValidString(id) {
		return newProtocolError(ErrorCodeInvalidUTF8, field, nil)
	}
	if len(id) > maxBytes {
		return newProtocolError(ErrorCodeInvalidID, field, nil)
	}
	for _, r := range id {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return newProtocolError(ErrorCodeInvalidID, field, nil)
		}
	}
	return nil
}

func validateErrorBody(body ErrorBody) error {
	if body.Code == "" {
		return newProtocolError(ErrorCodeMissingField, "error.code", nil)
	}
	if !utf8.ValidString(body.Code) || !utf8.ValidString(body.Message) {
		return newProtocolError(ErrorCodeInvalidUTF8, "error", nil)
	}
	if len(body.Code) > MaxErrorCodeBytes {
		return newProtocolError(ErrorCodeInvalidField, "error.code", nil)
	}
	for _, r := range body.Code {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return newProtocolError(ErrorCodeInvalidField, "error.code", nil)
		}
	}
	if strings.TrimSpace(body.Message) == "" || len(body.Message) > MaxErrorMessageBytes {
		return newProtocolError(ErrorCodeInvalidField, "error.message", nil)
	}
	for _, r := range body.Message {
		if unicode.IsControl(r) {
			return newProtocolError(ErrorCodeInvalidField, "error.message", nil)
		}
	}
	return nil
}

func inheritRequestID(requestID string) string {
	if requestID != "" && ValidateRequestID(requestID) != nil {
		return ""
	}
	return requestID
}

func effectiveMessageLimit(maxBytes int) int {
	if maxBytes <= 0 {
		return MaxMessageBytes
	}
	return maxBytes
}

func cloneRawMessage(payload json.RawMessage) json.RawMessage {
	if payload == nil {
		return nil
	}
	clone := make(json.RawMessage, len(payload))
	copy(clone, payload)
	return clone
}

func newProtocolError(code ErrorCode, field string, cause error) *ProtocolError {
	message, ok := publicErrorMessage(code)
	if !ok {
		code = ErrorCodeInternal
		message = "internal error"
	}
	return &ProtocolError{
		Code:      code,
		Message:   message,
		Retryable: false,
		Field:     field,
		cause:     cause,
	}
}

func publicErrorMessage(code ErrorCode) (string, bool) {
	switch code {
	case ErrorCodeInvalidJSON:
		return "invalid JSON message", true
	case ErrorCodeUnsupportedVersion:
		return "unsupported protocol version", true
	case ErrorCodeInvalidType:
		return "unsupported message type", true
	case ErrorCodeMissingField:
		return "required field is missing", true
	case ErrorCodeInvalidField:
		return "invalid field", true
	case ErrorCodeMessageTooLarge:
		return "message exceeds size limit", true
	case ErrorCodeInvalidUTF8:
		return "message contains invalid UTF-8", true
	case ErrorCodeInvalidPath:
		return "invalid path", true
	case ErrorCodeInvalidID:
		return "invalid identifier", true
	case ErrorCodeInvalidPayload:
		return "invalid JSON payload", true
	case ErrorCodeRouteNotFound:
		return "no route for path", true
	case ErrorCodeInternal:
		return "internal error", true
	default:
		return "", false
	}
}
