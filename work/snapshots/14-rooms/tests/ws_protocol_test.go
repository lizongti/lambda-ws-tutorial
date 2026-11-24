package tests

import (
	"encoding/json"
	"github.com/aura-studio/lambda/ws"
	"strings"
	"testing"
)

func TestChapter01EnvelopeRoundTripAndValidation(t *testing.T) {
	original := ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/echo", RequestID: "req-1", MessageID: "msg-1", Payload: json.RawMessage(`{"text":"你好"}`)}
	body, err := ws.EncodeEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ws.DecodeEnvelopeWithLimit(body, ws.MaxMessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Path != original.Path || string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("round trip mismatch: %#v", decoded)
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(`{"version":1,"type":"request"}`), ws.MaxMessageBytes); err == nil {
		t.Fatal("missing path was accepted")
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(`{"version":2,"type":"request","path":"/"}`), ws.MaxMessageBytes); err == nil {
		t.Fatal("unsupported version was accepted")
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(strings.Repeat(" ", ws.MaxMessageBytes+1)), ws.MaxMessageBytes); err == nil {
		t.Fatal("oversized input was accepted")
	}
}
