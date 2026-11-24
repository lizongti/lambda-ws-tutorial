package dynamodb_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
	dynamodbstore "github.com/aura-studio/lambda/ws/store/dynamodb"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
)

func TestWSEngineDynamoDBMaintenanceDeletesRecentlySeenExpiredConnection(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	store := dynamodbstore.New(dynamodbtest.New(), "ws-maintenance-expiration", dynamodbstore.WithClock(func() time.Time { return base }))
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "session-expired", PrincipalSubject: "alice", ExpiresAt: base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "session-expired", "connection-expired", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{
		ConnectionID: "connection-expired", SessionID: "session-expired", PrincipalSubject: "alice", Generation: bound.Generation,
		ConnectedAt: base, LastSeenAt: base.Add(59*time.Minute + 45*time.Second), ExpiresAt: base.Add(time.Hour), State: wsstore.ConnectionStateActive,
	}); err != nil {
		t.Fatal(err)
	}
	maintenanceTime := base.Add(61 * time.Minute)
	options := ws.NewOptions(
		ws.WithStateStore(store),
		ws.WithPusher(ws.NewFakePusher()),
		ws.WithAuthenticator(ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
			return ws.Principal{Subject: "alice"}, nil
		})),
		ws.WithClock(ws.ClockFunc(func() time.Time { return maintenanceTime })),
	)
	options.Push.MaxRetries = 0
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunMaintenanceWithResult(ctx, maintenanceTime)
	if err != nil || result.StaleScanned != 0 || result.ExpiredScanned != 1 || result.ExpiredCleaned != 1 || result.Failed != 0 {
		t.Fatalf("DynamoDB expired maintenance = %+v, %v", result, err)
	}
	remaining, err := store.ListExpiredConnections(ctx, maintenanceTime, 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("DynamoDB expired connection remains physically indexed: %+v, %v", remaining, err)
	}
}

func TestWSEngineWithDynamoDBFakeEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := dynamodbstore.New(dynamodbtest.New(), "ws-engine-integration")
	pusher := ws.NewFakePusher()
	options := ws.NewOptions(
		ws.WithStateStore(store),
		ws.WithPusher(pusher),
		ws.WithAuthenticator(ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
			return ws.Principal{Subject: "alice"}, nil
		})),
	)
	options.Push.MaxRetries = 0
	engine, err := ws.NewEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	first, err := engine.HandleConnect(ctx, ws.ConnectEvent{ConnectionID: "connection-ddb-old", RequestID: "request-connect-old"})
	if err != nil {
		t.Fatal(err)
	}
	engine.Handle("/echo", func(handlerCtx *ws.Context) { handlerCtx.SetResponse(handlerCtx.Payload) })
	body, err := ws.EncodeEnvelope(ws.Envelope{
		Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/echo", RequestID: "request-echo",
		MessageID: "message-echo", SessionID: first.SessionID, Payload: json.RawMessage(`{"value":"same-connection"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation, Body: string(body),
	}); err != nil {
		t.Fatal(err)
	}
	calls := pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != first.ConnectionID {
		t.Fatalf("same-connection calls = %+v", calls)
	}
	if err := engine.HandleDisconnect(ctx, ws.DisconnectEvent{
		ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
	}); err != nil {
		t.Fatal(err)
	}
	offline := ws.NewEvent("/offline", "request-offline", json.RawMessage(`{"value":"durable"}`))
	offline.MessageID = "message-offline"
	delivery, err := engine.PushToSessionWithResult(ctx, first.SessionID, offline)
	if err != nil || delivery.Status != ws.DeliveryOffline {
		t.Fatalf("offline delivery = %+v, %v", delivery, err)
	}
	second, err := engine.HandleConnect(ctx, ws.ConnectEvent{
		ConnectionID: "connection-ddb-new", RequestID: "request-connect-new",
		SessionID: first.SessionID, ResumeCredential: first.ResumeCredential,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Replay.Sent != 1 || second.Replay.Retained != 1 {
		t.Fatalf("DynamoDB replay = %+v", second.Replay)
	}
	ack, err := ws.EncodeOutboundMessage(ws.NewAck("request-ack", offline.MessageID))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleMessage(ctx, ws.MessageEvent{
		ConnectionID: second.ConnectionID, SessionID: second.SessionID, Generation: second.Generation, Body: string(ack),
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after ack = %+v, %v", pending, err)
	}
	if _, err := store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(err) {
		t.Fatalf("old connection after replacement = %v", err)
	}
}
