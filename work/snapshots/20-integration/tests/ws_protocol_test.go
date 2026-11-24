package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aura-studio/lambda/ws"
)

func TestWSProtocolEnvelopeRoundTrip(t *testing.T) {
	payload := json.RawMessage(`{ "roomId": "lobby", "count": 2, "nested": {"ok": true}, "text": "世界" }`)
	originalPayload := bytes.Clone(payload)
	envelope := ws.Envelope{
		Version:   ws.ProtocolVersion,
		Type:      ws.TypeRequest,
		Path:      "/room/join",
		RequestID: "req-7f3",
		MessageID: "msg-91a",
		SessionID: "session-1",
		Payload:   payload,
	}

	encoded, err := ws.EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("EncodeEnvelope() error = %v", err)
	}
	if !json.Valid(encoded) {
		t.Fatalf("EncodeEnvelope() produced invalid JSON: %q", encoded)
	}
	decoded, err := ws.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatalf("DecodeEnvelope() error = %v", err)
	}

	if decoded.Version != envelope.Version || decoded.Type != envelope.Type ||
		decoded.Path != envelope.Path || decoded.RequestID != envelope.RequestID ||
		decoded.MessageID != envelope.MessageID || decoded.SessionID != envelope.SessionID {
		t.Fatalf("round-trip fields mismatch: got %+v, want %+v", decoded, envelope)
	}
	assertWSProtocolJSONEqual(t, decoded.Payload, payload)
	if !bytes.Equal(payload, originalPayload) {
		t.Fatalf("EncodeEnvelope mutated payload: got %q, want %q", payload, originalPayload)
	}
}

func TestWSProtocolControlMessageConstructors(t *testing.T) {
	payload := json.RawMessage(`{"value":1}`)
	tests := []struct {
		name          string
		message       ws.OutboundMessage
		wantType      string
		wantPath      string
		wantRequestID string
		wantMessageID string
		wantPayload   bool
		wantError     bool
	}{
		{
			name:          "response",
			message:       ws.NewResponse("req-1", payload),
			wantType:      ws.TypeResponse,
			wantRequestID: "req-1",
			wantPayload:   true,
		},
		{
			name:          "event",
			message:       ws.NewEvent("/room/updated", "req-2", payload),
			wantType:      ws.TypeEvent,
			wantPath:      "/room/updated",
			wantRequestID: "req-2",
			wantPayload:   true,
		},
		{
			name:          "ping",
			message:       ws.NewPing("req-3"),
			wantType:      ws.TypePing,
			wantRequestID: "req-3",
		},
		{
			name:          "pong",
			message:       ws.NewPong("req-3"),
			wantType:      ws.TypePong,
			wantRequestID: "req-3",
		},
		{
			name:          "ack",
			message:       ws.NewAck("req-4", "msg-4"),
			wantType:      ws.TypeAck,
			wantRequestID: "req-4",
			wantMessageID: "msg-4",
		},
		{
			name: "error",
			message: ws.NewError("req-5", ws.ErrorBody{
				Code:      "route_not_found",
				Message:   "no route for path",
				Retryable: false,
			}),
			wantType:      ws.TypeError,
			wantRequestID: "req-5",
			wantError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := ws.EncodeOutboundMessage(tt.message)
			if err != nil {
				t.Fatalf("EncodeOutboundMessage() error = %v", err)
			}
			decoded, err := ws.DecodeEnvelope(encoded)
			if err != nil {
				t.Fatalf("DecodeEnvelope() error = %v", err)
			}
			if decoded.Version != ws.ProtocolVersion {
				t.Errorf("Version = %d, want %d", decoded.Version, ws.ProtocolVersion)
			}
			if decoded.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", decoded.Type, tt.wantType)
			}
			if decoded.Path != tt.wantPath {
				t.Errorf("Path = %q, want %q", decoded.Path, tt.wantPath)
			}
			if decoded.RequestID != tt.wantRequestID {
				t.Errorf("RequestID = %q, want %q", decoded.RequestID, tt.wantRequestID)
			}
			if decoded.MessageID != tt.wantMessageID {
				t.Errorf("MessageID = %q, want %q", decoded.MessageID, tt.wantMessageID)
			}
			if (len(decoded.Payload) != 0) != tt.wantPayload {
				t.Errorf("Payload = %s, wantPayload = %v", decoded.Payload, tt.wantPayload)
			}
			if (decoded.Error != nil) != tt.wantError {
				t.Errorf("Error = %+v, wantError = %v", decoded.Error, tt.wantError)
			}
			if tt.wantPayload {
				assertWSProtocolJSONEqual(t, decoded.Payload, payload)
			}
		})
	}
}

func TestWSProtocolConstructorsOnlyInheritValidRequestID(t *testing.T) {
	for _, message := range []ws.OutboundMessage{
		ws.NewResponse("bad request", nil),
		ws.NewEvent("/event", "bad request", nil),
		ws.NewPing("bad request"),
		ws.NewPong("bad request"),
		ws.NewAck("bad request", "msg-1"),
		ws.NewError("bad request", ws.ErrorBody{Code: "invalid", Message: "invalid"}),
	} {
		if message.RequestID != "" {
			t.Errorf("%s RequestID = %q, want empty", message.Type, message.RequestID)
		}
	}
}

func TestWSProtocolDecodeRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantCode ws.ErrorCode
	}{
		{name: "malformed", input: `{"version":1,`, wantCode: ws.ErrorCodeInvalidJSON},
		{name: "multiple top level values", input: `{"version":1,"type":"ping"} {"version":1,"type":"pong"}`, wantCode: ws.ErrorCodeInvalidJSON},
		{name: "trailing scalar", input: `{"version":1,"type":"ping"} null`, wantCode: ws.ErrorCodeInvalidJSON},
		{name: "unknown field", input: `{"version":1,"type":"ping","connectionId":"attacker-controlled"}`, wantCode: ws.ErrorCodeInvalidJSON},
		{name: "null envelope", input: `null`, wantCode: ws.ErrorCodeMissingField},
		{name: "missing version", input: `{"type":"ping"}`, wantCode: ws.ErrorCodeMissingField},
		{name: "unsupported version", input: `{"version":2,"type":"ping"}`, wantCode: ws.ErrorCodeUnsupportedVersion},
		{name: "missing type", input: `{"version":1}`, wantCode: ws.ErrorCodeMissingField},
		{name: "unknown type", input: `{"version":1,"type":"command"}`, wantCode: ws.ErrorCodeInvalidType},
		{name: "request missing path", input: `{"version":1,"type":"request","requestId":"req-1"}`, wantCode: ws.ErrorCodeMissingField},
		{name: "relative path", input: `{"version":1,"type":"request","path":"room/join"}`, wantCode: ws.ErrorCodeInvalidPath},
		{name: "path with query", input: `{"version":1,"type":"request","path":"/room/join?x=1"}`, wantCode: ws.ErrorCodeInvalidPath},
		{name: "ack missing message ID", input: `{"version":1,"type":"ack","requestId":"req-1"}`, wantCode: ws.ErrorCodeMissingField},
		{name: "error missing body", input: `{"version":1,"type":"error","requestId":"req-1"}`, wantCode: ws.ErrorCodeMissingField},
		{name: "error missing code", input: `{"version":1,"type":"error","error":{"message":"bad","retryable":false}}`, wantCode: ws.ErrorCodeMissingField},
		{name: "error missing message", input: `{"version":1,"type":"error","error":{"code":"bad","retryable":false}}`, wantCode: ws.ErrorCodeInvalidField},
		{name: "invalid request ID", input: `{"version":1,"type":"ping","requestId":"bad request"}`, wantCode: ws.ErrorCodeInvalidID},
		{name: "invalid session ID", input: `{"version":1,"type":"ack","messageId":"msg-1","sessionId":"bad session"}`, wantCode: ws.ErrorCodeInvalidID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ws.DecodeEnvelope([]byte(tt.input))
			assertWSProtocolErrorCode(t, err, tt.wantCode)
		})
	}
}

func TestWSProtocolEncodeRejectsInvalidEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		envelope ws.Envelope
		wantCode ws.ErrorCode
	}{
		{
			name:     "unsupported version",
			envelope: ws.Envelope{Version: 99, Type: ws.TypePing},
			wantCode: ws.ErrorCodeUnsupportedVersion,
		},
		{
			name:     "request without path",
			envelope: ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypeRequest},
			wantCode: ws.ErrorCodeMissingField,
		},
		{
			name:     "invalid raw payload",
			envelope: ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypeEvent, Payload: json.RawMessage(`{"broken":}`)},
			wantCode: ws.ErrorCodeInvalidPayload,
		},
		{
			name: "invalid error message",
			envelope: ws.Envelope{
				Version: ws.ProtocolVersion,
				Type:    ws.TypeError,
				Error:   &ws.ErrorBody{Code: "bad", Message: "line one\nline two"},
			},
			wantCode: ws.ErrorCodeInvalidField,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ws.EncodeEnvelope(tt.envelope)
			assertWSProtocolErrorCode(t, err, tt.wantCode)
		})
	}
}

func TestWSProtocolMessageSizeLimit(t *testing.T) {
	if ws.MaxMessageBytes != 32768 {
		t.Fatalf("MaxMessageBytes = %d, want 32768", ws.MaxMessageBytes)
	}
	_, err := ws.DecodeEnvelope(make([]byte, ws.MaxMessageBytes+1))
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeMessageTooLarge)

	encoded, err := ws.EncodeEnvelope(ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypePing})
	if err != nil {
		t.Fatalf("EncodeEnvelope() error = %v", err)
	}
	if _, err := ws.DecodeEnvelopeWithLimit(encoded, len(encoded)); err != nil {
		t.Fatalf("DecodeEnvelopeWithLimit(exact size) error = %v", err)
	}
	_, err = ws.DecodeEnvelopeWithLimit(encoded, len(encoded)-1)
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeMessageTooLarge)
	_, err = ws.EncodeEnvelopeWithLimit(ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypePing}, len(encoded)-1)
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeMessageTooLarge)

	large := ws.NewEvent("/large", "", bytes.Repeat([]byte{' '}, 256))
	large.Payload = json.RawMessage(`"` + strings.Repeat("x", 256) + `"`)
	_, err = ws.EncodeOutboundMessageWithLimit(large, 128)
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeMessageTooLarge)
}

func TestWSProtocolRejectsInvalidUTF8(t *testing.T) {
	invalidInput := append([]byte(`{"version":1,"type":"ping","requestId":"`), 0xff)
	invalidInput = append(invalidInput, []byte(`"}`)...)
	_, err := ws.DecodeEnvelope(invalidInput)
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeInvalidUTF8)

	_, err = ws.EncodeEnvelope(ws.Envelope{
		Version: ws.ProtocolVersion,
		Type:    ws.TypeRequest,
		Path:    string([]byte{'/', 0xff}),
	})
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeInvalidUTF8)

	_, err = ws.EncodeEnvelope(ws.Envelope{
		Version: ws.ProtocolVersion,
		Type:    ws.TypeEvent,
		Payload: json.RawMessage{'"', 0xff, '"'},
	})
	assertWSProtocolErrorCode(t, err, ws.ErrorCodeInvalidUTF8)
}

func TestWSProtocolPathAndIdentifierLimits(t *testing.T) {
	if err := ws.ValidatePath("/" + strings.Repeat("a", ws.MaxPathBytes-1)); err != nil {
		t.Errorf("ValidatePath(max length) error = %v", err)
	}
	for _, path := range []string{
		"",
		"relative",
		"/" + strings.Repeat("a", ws.MaxPathBytes),
		"/contains space",
		"/contains\nnewline",
		"/query?value=1",
		"/fragment#value",
		`C:\not-a-route`,
	} {
		if err := ws.ValidatePath(path); err == nil {
			t.Errorf("ValidatePath(%q) unexpectedly succeeded", path)
		}
	}

	validators := map[string]func(string) error{
		"connection": ws.ValidateConnectionID,
		"session":    ws.ValidateSessionID,
		"room":       ws.ValidateRoomID,
		"request":    ws.ValidateRequestID,
		"message":    ws.ValidateMessageID,
	}
	for name, validate := range validators {
		t.Run(name, func(t *testing.T) {
			if err := validate(strings.Repeat("i", ws.MaxIDBytes)); err != nil {
				t.Errorf("max-length ID error = %v", err)
			}
			for _, id := range []string{
				"",
				strings.Repeat("i", ws.MaxIDBytes+1),
				"contains space",
				"contains\nnewline",
				string([]byte{0xff}),
			} {
				if err := validate(id); err == nil {
					t.Errorf("validator accepted invalid ID %q", id)
				}
			}
		})
	}
}

func TestWSProtocolEncodingDoesNotMutatePayload(t *testing.T) {
	payload := json.RawMessage(" \n { \"html\": \"<tag>\", \"items\": [1, 2, 3], \"text\": \"世界\" } \t")
	before := bytes.Clone(payload)
	message := ws.NewEvent("/payload", "req-payload", payload)

	first, err := message.Encode()
	if err != nil {
		t.Fatalf("first Encode() error = %v", err)
	}
	second, err := message.Encode()
	if err != nil {
		t.Fatalf("second Encode() error = %v", err)
	}
	if !bytes.Equal(payload, before) {
		t.Fatalf("payload mutated: got %q, want %q", payload, before)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("repeated encoding differs:\nfirst:  %s\nsecond: %s", first, second)
	}

	decoded, err := ws.DecodeEnvelope(first)
	if err != nil {
		t.Fatalf("DecodeEnvelope() error = %v", err)
	}
	assertWSProtocolJSONEqual(t, decoded.Payload, before)
}

func TestWSProtocolSafeErrorConversion(t *testing.T) {
	const secret = "redis password=do-not-send"
	message := ws.NewErrorFromError("req-safe", errors.New(secret))
	if message.Error == nil {
		t.Fatal("NewErrorFromError() returned nil ErrorBody")
	}
	if message.Error.Code != string(ws.ErrorCodeInternal) || message.Error.Message != "internal error" {
		t.Fatalf("generic error body = %+v, want safe internal error", message.Error)
	}
	encoded, err := message.Encode()
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if bytes.Contains(encoded, []byte(secret)) || bytes.Contains(encoded, []byte("redis")) {
		t.Fatalf("safe error leaked internal details: %s", encoded)
	}

	_, protocolErr := ws.DecodeEnvelope([]byte(`{"version":999,"type":"ping"}`))
	protocolMessage := ws.NewErrorFromError("req-version", protocolErr)
	if protocolMessage.Error.Code != string(ws.ErrorCodeUnsupportedVersion) {
		t.Fatalf("protocol error code = %q, want %q", protocolMessage.Error.Code, ws.ErrorCodeUnsupportedVersion)
	}

	custom := &ws.ProtocolError{Code: ws.ErrorCodeInvalidJSON, Message: secret}
	if body := ws.SafeErrorBody(custom); body.Message == secret || strings.Contains(body.Message, "do-not-send") {
		t.Fatalf("SafeErrorBody used untrusted ProtocolError.Message: %+v", body)
	}
}

func assertWSProtocolErrorCode(t *testing.T, err error, want ws.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want protocol code %q", want)
	}
	var protocolErr *ws.ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error type = %T, want *ws.ProtocolError (error = %v)", err, err)
	}
	if protocolErr.Code != want {
		t.Fatalf("protocol code = %q, want %q (error = %v)", protocolErr.Code, want, err)
	}
	if protocolErr.Retryable {
		t.Fatalf("protocol error %q is retryable, want false", want)
	}
}

func assertWSProtocolJSONEqual(t *testing.T, got, want json.RawMessage) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got payload: %v", err)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want payload: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("payload semantics mismatch: got %#v, want %#v", gotValue, wantValue)
	}
}
