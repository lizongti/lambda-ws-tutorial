package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aura-studio/lambda/ws/store/dynamodb/dynamodbtest"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type faultFixture struct {
	store *Store
	fake  *dynamodbtest.Fake
	base  time.Time
}

func newFaultFixture() *faultFixture {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := dynamodbtest.New()
	return &faultFixture{
		store: New(fake, "ws-faults", WithClock(func() time.Time { return base }), WithOfflineQueueLimit(10)),
		fake:  fake,
		base:  base,
	}
}

func (f *faultFixture) createBoundSession(t *testing.T, sessionID, connectionID string) {
	t.Helper()
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: sessionID, ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
		t.Fatalf("CreateSession(%s) error = %v", sessionID, err)
	}
	if _, err := f.store.BindConnection(ctx, sessionID, connectionID, 0); err != nil {
		t.Fatalf("BindConnection(%s, %s) error = %v", sessionID, connectionID, err)
	}
}

func (f *faultFixture) roomItemExists(t *testing.T, roomID, sessionID string) bool {
	t.Helper()
	output, err := f.fake.GetItem(context.Background(), &awsdynamodb.GetItemInput{Key: roomKey(roomID, sessionID)})
	if err != nil {
		t.Fatalf("GetItem(%s/%s) error = %v", roomID, sessionID, err)
	}
	return len(output.Item) != 0
}

func transactionConditionFault(items int, failed int) error {
	reasons := make([]types.CancellationReason, items)
	for index := range reasons {
		code := "None"
		if index == failed {
			code = "ConditionalCheckFailed"
		}
		reasons[index].Code = &code
	}
	return &types.TransactionCanceledException{Message: awsString("injected transaction condition failure"), CancellationReasons: reasons}
}

func TestDynamoDBRoomFaultInjectionPaginationAndPartialCleanup(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()

	joinCondition := &types.ConditionalCheckFailedException{Message: awsString("injected join condition failure")}
	f.fake.Fail("UpdateItem", joinCondition)
	err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "room-condition", SessionID: "s-condition", ConnectionID: "c-condition"})
	if !wsstore.IsConditionConflict(err) || !errors.Is(err, joinCondition) {
		t.Fatalf("Join injected condition error = %v, want wrapped condition conflict", err)
	}
	if f.roomItemExists(t, "room-condition", "s-condition") {
		t.Fatal("failed conditional Join created a room member")
	}

	f.createBoundSession(t, "s1", "c-shared")
	f.createBoundSession(t, "s2", "c2")
	f.createBoundSession(t, "s3", "c3")
	for _, member := range []wsstore.RoomMember{
		{RoomID: "room-a", SessionID: "s3", ConnectionID: "c3", Generation: 1},
		{RoomID: "room-a", SessionID: "s2", ConnectionID: "c2", Generation: 1},
		{RoomID: "room-a", SessionID: "s1", ConnectionID: "c-shared", Generation: 1},
		{RoomID: "room-b", SessionID: "s1", ConnectionID: "c-shared", Generation: 1},
		{RoomID: "room-c", SessionID: "s1", ConnectionID: "c-shared", Generation: 1},
	} {
		if err := f.store.Join(ctx, member); err != nil {
			t.Fatalf("Join(%s/%s) error = %v", member.RoomID, member.SessionID, err)
		}
	}

	f.fake.SetMaxQueryItems(1)
	first, cursor, err := f.store.Members(ctx, "room-a", 2, "")
	if err != nil || len(first) != 2 || first[0].SessionID != "s1" || first[1].SessionID != "s2" || cursor != "s2" {
		t.Fatalf("first paginated Members = %+v, cursor %q, error %v", first, cursor, err)
	}
	second, next, err := f.store.Members(ctx, "room-a", 2, cursor)
	if err != nil || len(second) != 1 || second[0].SessionID != "s3" || next != "" {
		t.Fatalf("second paginated Members = %+v, cursor %q, error %v", second, next, err)
	}

	queryFault := errors.New("injected room query failure")
	f.fake.Fail("Query", queryFault)
	if _, _, err := f.store.Members(ctx, "room-a", 2, ""); !errors.Is(err, queryFault) {
		t.Fatalf("Members injected query error = %v, want wrapped query fault", err)
	}

	deleteFault := errors.New("injected partial room delete failure")
	f.fake.Fail("DeleteItem", deleteFault)
	if err := f.store.RemoveConnection(ctx, "c-shared"); !errors.Is(err, deleteFault) {
		t.Fatalf("RemoveConnection partial failure = %v, want wrapped delete fault", err)
	}
	if !f.roomItemExists(t, "room-a", "s1") {
		t.Fatal("member with injected delete failure was unexpectedly removed")
	}
	for _, roomID := range []string{"room-b", "room-c"} {
		if f.roomItemExists(t, roomID, "s1") {
			t.Fatalf("RemoveConnection stopped after partial failure; %s member remains", roomID)
		}
	}

	if err := f.store.Join(ctx, wsstore.RoomMember{RoomID: "room-b", SessionID: "s1", ConnectionID: "c-shared", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	conditionalDelete := &types.ConditionalCheckFailedException{Message: awsString("injected stale room cleanup")}
	f.fake.Fail("DeleteItem", conditionalDelete)
	if err := f.store.RemoveConnection(ctx, "c-shared"); err != nil {
		t.Fatalf("RemoveConnection returned conditional cleanup conflict = %v, want idempotent success", err)
	}
	if !f.roomItemExists(t, "room-a", "s1") || f.roomItemExists(t, "room-b", "s1") {
		t.Fatal("conditional cleanup did not continue across paginated reverse-index results")
	}
}

func TestDynamoDBOfflineTransactionConflictAndConditionFaults(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()
	f.createBoundSession(t, "s1", "c1")
	message := wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1, ExpiresAt: f.base.Add(time.Hour)}

	transactionConflict := &types.TransactionConflictException{Message: awsString("injected offline transaction conflict")}
	f.fake.Fail("TransactWriteItems", transactionConflict)
	if err := f.store.Append(ctx, message); !wsstore.IsUnavailable(err) || !errors.Is(err, transactionConflict) {
		t.Fatalf("Append transaction conflict = %v, want wrapped unavailable error", err)
	}
	if pending, err := f.store.ListPending(ctx, "s1", 10); err != nil || len(pending) != 0 {
		t.Fatalf("aborted Append left offline state: %+v, %v", pending, err)
	}

	// A conditional metadata CAS conflict is retryable. The next transaction must
	// succeed without duplicating the message or advancing its sequence twice.
	f.fake.Fail("TransactWriteItems", transactionConditionFault(3, 1))
	if err := f.store.Append(ctx, message); err != nil {
		t.Fatalf("Append did not retry injected metadata condition conflict: %v", err)
	}
	pending, err := f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].MessageID != "m1" || pending[0].Sequence != 1 || pending[0].State != wsstore.OfflineMessageStatePending {
		t.Fatalf("pending after transaction retry = %+v, %v", pending, err)
	}

	if err := f.store.MarkSent(ctx, "s1", "m1", 0); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale MarkSent condition error = %v", err)
	}
	pending, err = f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].State != wsstore.OfflineMessageStatePending {
		t.Fatalf("failed MarkSent changed offline state: %+v, %v", pending, err)
	}
	if err := f.store.MarkSent(ctx, "s1", "m1", 1); err != nil {
		t.Fatalf("valid MarkSent error = %v", err)
	}

	f.fake.Fail("TransactWriteItems", transactionConditionFault(3, 0))
	if err := f.store.Ack(ctx, "s1", "m1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("Ack injected condition error = %v, want condition conflict", err)
	}
	pending, err = f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].State != wsstore.OfflineMessageStateSent {
		t.Fatalf("failed Ack removed or changed message: %+v, %v", pending, err)
	}
	if err := f.store.Ack(ctx, "s1", "m1", 1); err != nil {
		t.Fatalf("Ack after injected condition failure error = %v", err)
	}
}

func TestDynamoDBDeleteConnectionIfMatchAndLastSeen(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()
	connection := wsstore.Connection{
		ConnectionID: "c1", Generation: 3, LastSeenAt: f.base, ExpiresAt: f.base.Add(time.Hour),
	}
	if err := f.store.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 2, f.base); !wsstore.IsConditionConflict(err) {
		t.Fatalf("mismatched generation delete = %v, want condition conflict", err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 3, f.base.Add(-time.Second)); !wsstore.IsConditionConflict(err) {
		t.Fatalf("mismatched LastSeen delete = %v, want condition conflict", err)
	}
	seenAt := f.base.Add(time.Minute)
	if err := f.store.TouchConnection(ctx, "c1", 3, seenAt); err != nil {
		t.Fatal(err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 3, f.base); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale LastSeen delete = %v, want condition conflict", err)
	}
	stored, err := f.store.GetConnection(ctx, "c1")
	if err != nil || !stored.LastSeenAt.Equal(seenAt) {
		t.Fatalf("connection after fenced deletes = %+v, %v", stored, err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 3, seenAt); err != nil {
		t.Fatalf("matching delete error = %v", err)
	}
	if _, err := f.store.GetConnection(ctx, "c1"); !wsstore.IsNotFound(err) {
		t.Fatalf("deleted connection = %v, want not found", err)
	}
	expired, err := f.store.ListExpiredConnections(ctx, f.base.Add(2*time.Hour), 10)
	if err != nil || len(expired) != 0 {
		t.Fatalf("deleted connection expiry index remains = %+v, %v", expired, err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 3, seenAt); err != nil {
		t.Fatalf("duplicate delete = %v", err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "", 3, seenAt); !wsstore.IsInvalid(err) {
		t.Fatalf("empty connection ID = %v, want invalid", err)
	}
	if err := f.store.DeleteConnectionIfMatchAndLastSeen(ctx, "c1", 3, time.Time{}); !wsstore.IsInvalid(err) {
		t.Fatalf("zero LastSeen = %v, want invalid", err)
	}
}

func TestDynamoDBExpiredSessionCleanupFailureIsRetryable(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()
	oldSession := wsstore.Session{SessionID: "s1", PrincipalSubject: "old", ExpiresAt: f.base.Add(time.Hour)}
	if err := f.store.CreateSession(ctx, oldSession); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old-1", "old-2"} {
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}

	cleanupFault := errors.New("injected session secondary delete failure")
	f.fake.Fail("TransactWriteItems", cleanupFault)
	if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(2*time.Hour), 10); !errors.Is(err, cleanupFault) {
		t.Fatalf("DeleteExpiredSessions cleanup failure = %v, want wrapped injected fault", err)
	}
	main, err := f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: sessionKey("s1")})
	if err != nil || len(main.Item) == 0 {
		t.Fatalf("old main record was not retained after cleanup failure: %+v, %v", main, err)
	}
	indexed, err := f.fake.Query(ctx, &awsdynamodb.QueryInput{
		IndexName:                awsString(f.store.timeIndex),
		KeyConditionExpression:   awsString("#gsi1pk = :partition AND #gsi1sk < :after"),
		ExpressionAttributeNames: map[string]string{"#gsi1pk": attrGSI1PK, "#gsi1sk": attrGSI1SK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition": stringValue(timePartitionSession), ":after": stringValue(timeKey(f.base.Add(2 * time.Hour).Add(time.Nanosecond))),
		},
	})
	if err != nil || len(indexed.Items) != 1 {
		t.Fatalf("expired session is not discoverable for retry = %+v, %v", indexed, err)
	}

	if err := f.store.DeleteExpiredSessions(ctx, f.base.Add(2*time.Hour), 10); err != nil {
		t.Fatalf("DeleteExpiredSessions retry error = %v", err)
	}
	main, err = f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: sessionKey("s1")})
	if err != nil || len(main.Item) != 0 {
		t.Fatalf("main record remains after successful retry: %+v, %v", main, err)
	}
	for _, id := range []string{"old-1", "old-2"} {
		secondary, getErr := f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: offlineKey("s1", id)})
		if getErr != nil || len(secondary.Item) != 0 {
			t.Fatalf("secondary message %s remains after retry: %+v, %v", id, secondary, getErr)
		}
	}
}

func TestDynamoDBCreateSessionCleansExpiredSecondaryBeforeReplacement(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", PrincipalSubject: "old", ExpiresAt: f.base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "old-message", ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	replacementTime := f.base.Add(2 * time.Hour)
	replacementStore := New(f.fake, "ws-faults", WithClock(func() time.Time { return replacementTime }), WithOfflineQueueLimit(10))
	newSession := wsstore.Session{SessionID: "s1", PrincipalSubject: "new", ExpiresAt: f.base.Add(24 * time.Hour)}

	cleanupFault := errors.New("injected replacement cleanup failure")
	f.fake.Fail("TransactWriteItems", cleanupFault)
	if err := replacementStore.CreateSession(ctx, newSession); !errors.Is(err, cleanupFault) {
		t.Fatalf("CreateSession cleanup failure = %v, want wrapped injected fault", err)
	}
	main, err := f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: sessionKey("s1")})
	if err != nil || len(main.Item) == 0 {
		t.Fatalf("old main record was not retained after replacement cleanup failure: %+v, %v", main, err)
	}
	stored, err := decodeSession(main.Item)
	if err != nil || stored.PrincipalSubject != "old" || !stored.ExpiresAt.Equal(f.base.Add(time.Hour)) {
		t.Fatalf("failed replacement changed main record = %+v, %v", stored, err)
	}
	oldMessage, err := f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: offlineKey("s1", "old-message")})
	if err != nil || len(oldMessage.Item) == 0 {
		t.Fatalf("injected cleanup unexpectedly removed old message: %+v, %v", oldMessage, err)
	}

	if err := replacementStore.CreateSession(ctx, newSession); err != nil {
		t.Fatalf("CreateSession retry error = %v", err)
	}
	stored, err = replacementStore.GetSession(ctx, "s1")
	if err != nil || stored.PrincipalSubject != "new" || !stored.ExpiresAt.Equal(newSession.ExpiresAt) {
		t.Fatalf("replacement session = %+v, %v", stored, err)
	}
	oldMessage, err = f.fake.GetItem(ctx, &awsdynamodb.GetItemInput{Key: offlineKey("s1", "old-message")})
	if err != nil || len(oldMessage.Item) != 0 {
		t.Fatalf("old message remains after successful replacement: %+v, %v", oldMessage, err)
	}
	if err := replacementStore.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "new-message"}); err != nil {
		t.Fatal(err)
	}
	pending, err := replacementStore.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].MessageID != "new-message" {
		t.Fatalf("old and new messages mixed after replacement = %+v, %v", pending, err)
	}
}

func TestDynamoDBOfflinePaginationAndPartialExpiryFaults(t *testing.T) {
	f := newFaultFixture()
	ctx := context.Background()
	if err := f.store.CreateSession(ctx, wsstore.Session{SessionID: "s1", ExpiresAt: f.base.Add(24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if err := f.store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: id, ExpiresAt: f.base.Add(time.Hour)}); err != nil {
			t.Fatalf("Append(%s) error = %v", id, err)
		}
	}

	f.fake.SetMaxQueryItems(1)
	pending, err := f.store.ListPending(ctx, "s1", 2)
	if err != nil || len(pending) != 2 || pending[0].MessageID != "m1" || pending[1].MessageID != "m2" {
		t.Fatalf("first paginated pending page = %+v, %v", pending, err)
	}
	pending, err = f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 3 || pending[2].MessageID != "m3" || pending[2].Sequence != 3 {
		t.Fatalf("fully paginated pending result = %+v, %v", pending, err)
	}
	queryFault := errors.New("injected offline query failure")
	f.fake.Fail("Query", queryFault)
	if _, err := f.store.ListPending(ctx, "s1", 10); !errors.Is(err, queryFault) {
		t.Fatalf("ListPending injected query error = %v, want wrapped query fault", err)
	}

	f.fake.SetMaxQueryItems(0)
	deleteFault := errors.New("injected partial offline transaction failure")
	f.fake.Fail("TransactWriteItems", deleteFault)
	if err := f.store.DeleteExpired(ctx, f.base.Add(2*time.Hour), 10); !errors.Is(err, deleteFault) {
		t.Fatalf("DeleteExpired partial failure = %v, want wrapped transaction fault", err)
	}
	pending, err = f.store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].MessageID != "m1" {
		t.Fatalf("DeleteExpired stopped after partial failure: %+v, %v", pending, err)
	}

	f.fake.Fail("TransactWriteItems", transactionConditionFault(2, 0))
	if err := f.store.DeleteExpired(ctx, f.base.Add(2*time.Hour), 10); !wsstore.IsConditionConflict(err) {
		t.Fatalf("DeleteExpired injected condition error = %v, want condition conflict", err)
	}
	if err := f.store.DeleteExpired(ctx, f.base.Add(2*time.Hour), 10); err != nil {
		t.Fatalf("DeleteExpired retry after injected faults error = %v", err)
	}
	if pending, err := f.store.ListPending(ctx, "s1", 10); err != nil || len(pending) != 0 {
		t.Fatalf("expired messages remain after successful retry: %+v, %v", pending, err)
	}
}
