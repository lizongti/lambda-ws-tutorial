package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aura-studio/lambda/ws"
	wsstore "github.com/aura-studio/lambda/ws/store"
)

func newWSOfflineTestFixture(t *testing.T, configure func(*ws.Options)) *wsSessionTestFixture {
	t.Helper()
	return newWSSessionTestFixture(t, func(options *ws.Options) {
		options.Offline.Enabled = true
		options.Offline.AckPolicy = ws.AckPolicyClientAck
		options.Offline.AckTimeout = time.Minute
		options.Offline.MaxMessagesPerSession = 8
		options.Offline.Retention = 30 * time.Minute
		options.Offline.Overflow = wsstore.OfflineOverflowRejectNew
		options.Offline.ReplayBatchSize = 2
		if configure != nil {
			configure(options)
		}
	})
}

func wsOfflineTestMessage(messageID string, position int) ws.OutboundMessage {
	message := ws.NewEvent("/offline/notice", "request-"+messageID, json.RawMessage(fmt.Sprintf(`{"position":%d}`, position)))
	message.MessageID = messageID
	return message
}

func mustWSOfflineTestDisconnect(t *testing.T, f *wsSessionTestFixture, connected ws.ConnectResult) {
	t.Helper()
	if err := f.engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{
		ConnectionID: connected.ConnectionID,
		SessionID:    connected.SessionID,
		Generation:   connected.Generation,
	}); err != nil {
		t.Fatalf("HandleDisconnect() error = %v", err)
	}
}

func mustWSOfflineTestPush(t *testing.T, engine *ws.Engine, sessionID string, message ws.OutboundMessage) ws.DeliveryResult {
	t.Helper()
	result, err := engine.PushToSessionWithResult(context.Background(), sessionID, message)
	if err != nil {
		t.Fatalf("PushToSessionWithResult(%s) error = %v", message.MessageID, err)
	}
	return result
}

func mustWSOfflineTestAck(t *testing.T, engine *ws.Engine, connected ws.ConnectResult, messageID string) {
	t.Helper()
	ack := ws.NewAck("ack-"+messageID, messageID)
	ack.SessionID = connected.SessionID
	body, err := ack.Encode()
	if err != nil {
		t.Fatalf("Encode(ack) error = %v", err)
	}
	if err := engine.HandleMessage(context.Background(), ws.MessageEvent{
		ConnectionID: connected.ConnectionID,
		SessionID:    connected.SessionID,
		Generation:   connected.Generation,
		Body:         string(body),
	}); err != nil {
		t.Fatalf("HandleMessage(ack %s) error = %v", messageID, err)
	}
}

func wsOfflineTestMessageIDs(messages []wsstore.OfflineMessage) []string {
	ids := make([]string, len(messages))
	for i, message := range messages {
		ids[i] = message.MessageID
	}
	return ids
}

func wsOfflineTestEnvelopePosition(t *testing.T, body []byte) (ws.Envelope, int) {
	t.Helper()
	envelope, err := ws.DecodeEnvelope(body)
	if err != nil {
		t.Fatalf("DecodeEnvelope(push) error = %v", err)
	}
	var payload struct {
		Position int `json:"position"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal(push payload) error = %v", err)
	}
	return *envelope, payload.Position
}

func TestWSOfflineQueuesInOrderAndPreservesOriginalMessageID(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-offline", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)

	wantIDs := []string{"message-original-a", "message-original-b", "message-original-c"}
	for i, messageID := range wantIDs {
		result := mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage(messageID, i+1))
		if result.Status != ws.DeliveryOffline || !result.Offline || result.Gone ||
			result.SessionID != first.SessionID || result.ConnectionID != "" || result.MessageID != messageID {
			t.Fatalf("offline delivery result = %+v", result)
		}
	}
	queued, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := wsOfflineTestMessageIDs(queued); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("queued IDs = %v, want %v", got, wantIDs)
	}
	for i, message := range queued {
		if message.Sequence != uint64(i+1) || message.Generation != first.Generation ||
			message.State != wsstore.OfflineMessageStatePending || !message.CreatedAt.Equal(f.base) ||
			!message.ExpiresAt.Equal(f.base.Add(30*time.Minute)) {
			t.Fatalf("queued message %d = %+v", i, message)
		}
		envelope, position := wsOfflineTestEnvelopePosition(t, message.Envelope)
		if envelope.MessageID != wantIDs[i] || envelope.SessionID != first.SessionID || position != i+1 {
			t.Fatalf("stored envelope %d = %+v position=%d", i, envelope, position)
		}
	}

	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-replayed", "alice", first.SessionID, first.ResumeCredential,
	))
	if resumed.Replay.Attempted != 3 || resumed.Replay.Sent != 3 || resumed.Replay.Acknowledged != 0 ||
		resumed.Replay.Retained != 3 || resumed.Replay.Failed != 0 || resumed.Replay.Batches != 2 || resumed.Replay.Truncated {
		t.Fatalf("replay result = %+v, want three ordered sends in two batches", resumed.Replay)
	}
	calls := f.pusher.Calls()
	if len(calls) != len(wantIDs) {
		t.Fatalf("replay push count = %d, want %d", len(calls), len(wantIDs))
	}
	for i, call := range calls {
		if call.ConnectionID != resumed.ConnectionID {
			t.Fatalf("replay call %d targeted %q, want %q", i, call.ConnectionID, resumed.ConnectionID)
		}
		envelope, position := wsOfflineTestEnvelopePosition(t, call.Body)
		if envelope.MessageID != wantIDs[i] || envelope.SessionID != first.SessionID || position != i+1 {
			t.Fatalf("replayed envelope %d = %+v position=%d", i, envelope, position)
		}
	}
	queued, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(queued) != 3 {
		t.Fatalf("clientAck queue after replay = %+v, %v", queued, err)
	}
	for _, message := range queued {
		if message.State != wsstore.OfflineMessageStateSent || message.Generation != resumed.Generation || message.SentAt == nil {
			t.Fatalf("clientAck message was not retained as sent at current generation: %+v", message)
		}
	}
}

func TestWSOfflinePostSuccessAckPolicyDeletesAfterReplay(t *testing.T) {
	f := newWSOfflineTestFixture(t, func(options *ws.Options) {
		options.Offline.AckPolicy = ws.AckPolicyPostSuccess
	})
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-post-1", 1))
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-post-2", 2))

	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	if resumed.Replay.Attempted != 2 || resumed.Replay.Sent != 2 || resumed.Replay.Acknowledged != 2 ||
		resumed.Replay.Retained != 0 || resumed.Replay.Failed != 0 {
		t.Fatalf("postSuccess replay = %+v", resumed.Replay)
	}
	pending, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("postSuccess messages remain pending: %+v, %v", pending, err)
	}
	callsBefore := f.pusher.CallCount()
	replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || replay.Sent != 0 || f.pusher.CallCount() != callsBefore {
		t.Fatalf("acknowledged postSuccess messages replayed again: %+v, %v", replay, err)
	}
}

func TestWSOfflineClientAckRedeliveryDeduplicatesByOriginalID(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-ack-1", 1))
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-ack-2", 2))
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))

	mustWSOfflineTestAck(t, f.engine, resumed, "message-ack-1")
	f.clock.Advance(time.Minute)
	pending, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"message-ack-2"}) {
		t.Fatalf("queue after first client ack = %v, %v", wsOfflineTestMessageIDs(pending), err)
	}

	// The unacknowledged message is intentionally delivered again, with the
	// exact ID the client uses for at-least-once deduplication. The acknowledged
	// message is not needlessly replayed.
	replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 1 || replay.Sent != 1 || replay.Retained != 1 {
		t.Fatalf("second clientAck replay = %+v, %v", replay, err)
	}
	calls := f.pusher.Calls()
	if len(calls) != 3 {
		t.Fatalf("push count after redelivery = %d, want 3", len(calls))
	}
	envelope, position := wsOfflineTestEnvelopePosition(t, calls[2].Body)
	if calls[2].ConnectionID != resumed.ConnectionID || envelope.MessageID != "message-ack-2" || position != 2 {
		t.Fatalf("redelivered call = %+v envelope=%+v position=%d", calls[2], envelope, position)
	}

	mustWSOfflineTestAck(t, f.engine, resumed, "message-ack-2")
	mustWSOfflineTestAck(t, f.engine, resumed, "message-ack-2") // duplicate ack is idempotent
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("queue after client acks = %+v, %v", pending, err)
	}
	replay, err = f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != 3 {
		t.Fatalf("acknowledged clientAck message replayed: %+v, %v", replay, err)
	}
}

func TestWSOfflineClientAckRedeliveryWaitsForAckTimeout(t *testing.T) {
	f := newWSOfflineTestFixture(t, func(options *ws.Options) {
		options.Offline.AckTimeout = 10 * time.Minute
	})
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-timeout", 1))
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	if resumed.Replay.Sent != 1 || f.pusher.CallCount() != 1 {
		t.Fatalf("initial pending replay = %+v, pushes=%d", resumed.Replay, f.pusher.CallCount())
	}

	f.clock.Advance(9 * time.Minute)
	replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || replay.Sent != 0 || replay.Retained != 1 || f.pusher.CallCount() != 1 {
		t.Fatalf("message redelivered before ack timeout: replay=%+v pushes=%d err=%v", replay, f.pusher.CallCount(), err)
	}

	f.clock.Advance(time.Minute)
	replay, err = f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 1 || replay.Sent != 1 || replay.Retained != 1 || f.pusher.CallCount() != 2 {
		t.Fatalf("message was not redelivered at ack timeout: replay=%+v pushes=%d err=%v", replay, f.pusher.CallCount(), err)
	}
	pending, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 1 || pending[0].SentAt == nil || !pending[0].SentAt.Equal(f.clock.Now()) {
		t.Fatalf("redelivery did not refresh SentAt: %+v, %v", pending, err)
	}
	replay, err = f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != 2 {
		t.Fatalf("redelivery was not bounded by refreshed SentAt: replay=%+v pushes=%d err=%v", replay, f.pusher.CallCount(), err)
	}

	mustWSOfflineTestAck(t, f.engine, resumed, "message-timeout")
	f.clock.Advance(10 * time.Minute)
	replay, err = f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != 2 {
		t.Fatalf("acknowledged message was redelivered: replay=%+v pushes=%d err=%v", replay, f.pusher.CallCount(), err)
	}
}

func TestWSOfflineStaleAckCannotDeleteCurrentGenerationMessage(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-original", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-fenced-ack", 1))

	oldGeneration := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-generation-2", "alice", first.SessionID, first.ResumeCredential,
	))
	f.clock.Advance(time.Minute)
	current := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-generation-3", "alice", first.SessionID, first.ResumeCredential,
	))
	pending, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 1 || pending[0].Generation != current.Generation {
		t.Fatalf("queue before stale acks = %+v, %v", pending, err)
	}

	// Both an event from the replaced physical connection and a stale generation
	// attached to the current connection must be harmless no-ops.
	mustWSOfflineTestAck(t, f.engine, oldGeneration, "message-fenced-ack")
	staleCurrent := current
	staleCurrent.Generation = oldGeneration.Generation
	mustWSOfflineTestAck(t, f.engine, staleCurrent, "message-fenced-ack")
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 1 || pending[0].Generation != current.Generation ||
		pending[0].State != wsstore.OfflineMessageStateSent {
		t.Fatalf("stale ack deleted or changed current message: %+v, %v", pending, err)
	}
	stored, err := f.store.GetSession(ctx, first.SessionID)
	if err != nil || stored.CurrentConnectionID != current.ConnectionID || stored.Generation != current.Generation {
		t.Fatalf("stale ack changed current binding: %+v, %v", stored, err)
	}

	mustWSOfflineTestAck(t, f.engine, current, "message-fenced-ack")
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("current-generation ack did not delete message: %+v, %v", pending, err)
	}
}

func TestWSOfflineOverflowPolicies(t *testing.T) {
	for _, tt := range []struct {
		name     string
		policy   wsstore.OfflineOverflowPolicy
		wantIDs  []string
		wantErr  bool
		wantSeqs []uint64
	}{
		{
			name: "reject new", policy: wsstore.OfflineOverflowRejectNew,
			wantIDs: []string{"message-1", "message-2"}, wantSeqs: []uint64{1, 2}, wantErr: true,
		},
		{
			name: "evict oldest", policy: wsstore.OfflineOverflowEvictOldest,
			wantIDs: []string{"message-2", "message-3"}, wantSeqs: []uint64{2, 3},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWSOfflineTestFixture(t, func(options *ws.Options) {
				options.Offline.MaxMessagesPerSession = 2
				options.Offline.Overflow = tt.policy
			})
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
			mustWSOfflineTestDisconnect(t, f, first)
			mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-1", 1))
			mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-2", 2))

			result, err := f.engine.PushToSessionWithResult(context.Background(), first.SessionID, wsOfflineTestMessage("message-3", 3))
			if tt.wantErr {
				if !wsstore.IsQuotaExceeded(err) {
					t.Fatalf("overflow error = %v, want quota exceeded", err)
				}
			} else if err != nil || result.Status != ws.DeliveryOffline || !result.Offline {
				t.Fatalf("evict-oldest result = %+v, error = %v", result, err)
			}
			pending, listErr := f.store.ListPending(context.Background(), first.SessionID, 10)
			if listErr != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), tt.wantIDs) {
				t.Fatalf("queue after overflow = %v, %v; want %v", wsOfflineTestMessageIDs(pending), listErr, tt.wantIDs)
			}
			for i, message := range pending {
				if message.Sequence != tt.wantSeqs[i] {
					t.Fatalf("queue sequence after overflow = %+v, want %v", pending, tt.wantSeqs)
				}
			}
			if f.pusher.CallCount() != 0 {
				t.Fatalf("offline overflow unexpectedly pushed %d messages", f.pusher.CallCount())
			}
		})
	}
}

func TestWSOfflineExpirySkipsReplayAndMaintenanceRetainsLiveMessages(t *testing.T) {
	f := newWSOfflineTestFixture(t, func(options *ws.Options) {
		options.Offline.Retention = 10 * time.Minute
	})
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-expired", 1))
	f.clock.Advance(5 * time.Minute)
	mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage("message-live", 2))
	f.clock.Advance(6 * time.Minute) // first message expired; second has four minutes left

	pending, err := f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"message-live"}) {
		t.Fatalf("live pending view before reconnect = %v, %v", wsOfflineTestMessageIDs(pending), err)
	}
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-new", "alice", first.SessionID, first.ResumeCredential,
	))
	if resumed.Replay.Attempted != 1 || resumed.Replay.Sent != 1 || resumed.Replay.Retained != 1 {
		t.Fatalf("replay with expired entry = %+v", resumed.Replay)
	}
	calls := f.pusher.Calls()
	if len(calls) != 1 {
		t.Fatalf("expired message was replayed: %d calls", len(calls))
	}
	envelope, _ := wsOfflineTestEnvelopePosition(t, calls[0].Body)
	if envelope.MessageID != "message-live" {
		t.Fatalf("replayed message ID = %q, want message-live", envelope.MessageID)
	}

	maintenance, err := f.engine.RunMaintenanceWithResult(ctx, f.clock.Now())
	if err != nil || maintenance.ExpiredOfflineBatches != 1 {
		t.Fatalf("maintenance = %+v, %v", maintenance, err)
	}
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"message-live"}) ||
		pending[0].State != wsstore.OfflineMessageStateSent {
		t.Fatalf("maintenance removed or changed live message: %+v, %v", pending, err)
	}

	f.clock.Advance(9 * time.Minute) // the second message is now expired too
	replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != 1 {
		t.Fatalf("expired clientAck message replayed: %+v, %v", replay, err)
	}
	if _, err := f.engine.RunMaintenanceWithResult(ctx, f.clock.Now()); err != nil {
		t.Fatalf("second maintenance error = %v", err)
	}
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("expired queue is not empty: %+v, %v", pending, err)
	}
}

func TestWSOfflineReplayInterruptionRetainsAndResumesUnacknowledged(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-old", "alice", "", ""))
	mustWSOfflineTestDisconnect(t, f, first)
	wantIDs := []string{"message-replay-1", "message-replay-2", "message-replay-3"}
	for i, messageID := range wantIDs {
		mustWSOfflineTestPush(t, f.engine, first.SessionID, wsOfflineTestMessage(messageID, i+1))
	}

	gone := ws.NewPushError(ws.PushErrorGone, errors.New("connection disappeared"))
	f.pusher.SetErrors(nil, gone)
	interrupted, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
		"connection-interrupted", "alice", first.SessionID, first.ResumeCredential,
	))
	if !ws.IsPushGone(err) {
		t.Fatalf("interrupted reconnect error = %v, want gone", err)
	}
	if interrupted.SessionID != first.SessionID || interrupted.Generation != 2 || interrupted.Replay.Attempted != 2 ||
		interrupted.Replay.Sent != 1 || interrupted.Replay.Failed != 1 || interrupted.Replay.Retained != 1 {
		t.Fatalf("interrupted reconnect result = %+v", interrupted)
	}
	calls := f.pusher.Calls()
	if len(calls) != 2 {
		t.Fatalf("replay continued after failure: %d calls, want 2", len(calls))
	}
	for i, call := range calls {
		envelope, _ := wsOfflineTestEnvelopePosition(t, call.Body)
		if call.ConnectionID != interrupted.ConnectionID || envelope.MessageID != wantIDs[i] {
			t.Fatalf("interrupted replay call %d = %+v envelope=%+v", i, call, envelope)
		}
	}
	if _, getErr := f.store.GetConnection(ctx, interrupted.ConnectionID); !wsstore.IsNotFound(getErr) {
		t.Fatalf("gone replay connection was not cleaned: %v", getErr)
	}
	stored, getErr := f.store.GetSession(ctx, first.SessionID)
	if getErr != nil || stored.CurrentConnectionID != "" || stored.Generation != interrupted.Generation {
		t.Fatalf("session after interrupted replay = %+v, %v", stored, getErr)
	}
	pending, listErr := f.store.ListPending(ctx, first.SessionID, 10)
	if listErr != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), wantIDs) {
		t.Fatalf("unacknowledged messages lost after interruption: %v, %v", wsOfflineTestMessageIDs(pending), listErr)
	}
	if pending[0].State != wsstore.OfflineMessageStateSent || pending[0].Generation != interrupted.Generation ||
		pending[1].State != wsstore.OfflineMessageStatePending || pending[2].State != wsstore.OfflineMessageStatePending {
		t.Fatalf("unexpected states after interrupted replay: %+v", pending)
	}

	f.pusher.SetErrors()
	f.clock.Advance(time.Minute)
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-resumed", "alice", first.SessionID, first.ResumeCredential,
	))
	if resumed.Generation != 3 || resumed.Replay.Attempted != 3 || resumed.Replay.Sent != 3 || resumed.Replay.Failed != 0 {
		t.Fatalf("resumed replay = %+v", resumed)
	}
	calls = f.pusher.Calls()
	if len(calls) != 5 {
		t.Fatalf("push count after resumed replay = %d, want 5", len(calls))
	}
	for i, call := range calls[2:] {
		envelope, position := wsOfflineTestEnvelopePosition(t, call.Body)
		if call.ConnectionID != resumed.ConnectionID || envelope.MessageID != wantIDs[i] || position != i+1 {
			t.Fatalf("resumed call %d = %+v envelope=%+v position=%d", i, call, envelope, position)
		}
	}
	for _, messageID := range wantIDs {
		mustWSOfflineTestAck(t, f.engine, resumed, messageID)
	}
	pending, err = f.store.ListPending(ctx, first.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("queue after resumed client acks = %+v, %v", pending, err)
	}
	replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
	if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != 5 {
		t.Fatalf("acknowledged interrupted messages replayed again: %+v, %v", replay, err)
	}
}

func TestWSOfflinePushGoneQueuesInsteadOfReportingLiveSuccess(t *testing.T) {
	f := newWSOfflineTestFixture(t, nil)
	ctx := context.Background()
	first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-gone", "alice", "", ""))
	f.pusher.SetErrors(ws.NewPushError(ws.PushErrorGone, errors.New("stale API Gateway connection")))

	result, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage("message-after-gone", 1))
	if err != nil {
		t.Fatalf("PushToSessionWithResult(gone) error = %v, want durable offline acceptance", err)
	}
	if result.Status != ws.DeliveryOffline || !result.Offline || !result.Gone ||
		result.ConnectionID != first.ConnectionID || result.MessageID != "message-after-gone" {
		t.Fatalf("gone delivery result = %+v", result)
	}
	if _, getErr := f.store.GetConnection(ctx, first.ConnectionID); !wsstore.IsNotFound(getErr) {
		t.Fatalf("gone live connection remains stored: %v", getErr)
	}
	stored, getErr := f.store.GetSession(ctx, first.SessionID)
	if getErr != nil || stored.CurrentConnectionID != "" {
		t.Fatalf("gone connection remains bound: %+v, %v", stored, getErr)
	}
	pending, listErr := f.store.ListPending(ctx, first.SessionID, 10)
	if listErr != nil || !reflect.DeepEqual(wsOfflineTestMessageIDs(pending), []string{"message-after-gone"}) {
		t.Fatalf("queue after live gone = %v, %v", wsOfflineTestMessageIDs(pending), listErr)
	}

	f.pusher.SetErrors()
	resumed := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
		"connection-replacement", "alice", first.SessionID, first.ResumeCredential,
	))
	calls := f.pusher.Calls()
	if len(calls) != 2 {
		t.Fatalf("push count after reconnect = %d, want live attempt plus replay", len(calls))
	}
	envelope, _ := wsOfflineTestEnvelopePosition(t, calls[1].Body)
	if calls[1].ConnectionID != resumed.ConnectionID || envelope.MessageID != "message-after-gone" {
		t.Fatalf("replayed gone message = %+v envelope=%+v", calls[1], envelope)
	}
}
