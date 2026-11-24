package tests

import (
	"github.com/aura-studio/lambda/ws"
	"github.com/aws/aws-lambda-go/events"
	"testing"
)

func TestChapter08APIGatewayAdapters(t *testing.T) {
	request := events.APIGatewayWebsocketProxyRequest{Body: `{"version":1,"type":"ping"}`, Headers: map[string]string{"X-WS-Resume-Credential": "secret"}, QueryStringParameters: map[string]string{"sessionId": "s1"}}
	request.RequestContext.ConnectionID = "c1"
	request.RequestContext.RequestID = "r1"
	request.RequestContext.RouteKey = ws.RouteConnect
	request.RequestContext.ConnectedAt = 1000
	connected := ws.ConnectEventFromAPIGateway(request)
	if connected.ConnectionID != "c1" || connected.SessionID != "s1" || connected.ResumeCredential != "secret" {
		t.Fatalf("connect=%#v", connected)
	}
	if connected.String() == "" || containsCredential(connected.String(), "secret") {
		t.Fatalf("credential leaked: %q", connected.String())
	}
	request.RequestContext.RouteKey = ws.DefaultRouteKey
	message := ws.MessageEventFromAPIGateway(request)
	if message.Body != request.Body || message.ConnectionID != "c1" {
		t.Fatalf("message=%#v", message)
	}
}

func containsCredential(text, credential string) bool {
	for i := 0; i+len(credential) <= len(text); i++ {
		if text[i:i+len(credential)] == credential {
			return true
		}
	}
	return false
}
