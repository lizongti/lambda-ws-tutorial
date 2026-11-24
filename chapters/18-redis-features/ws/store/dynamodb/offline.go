package dynamodb

import (
	"context"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxAppendAttempts = 128

type offlineMetadata struct {
	sequence uint64
	count    uint64
	exists   bool
}

func decodeOfflineMetadata(attributes map[string]types.AttributeValue) (offlineMetadata, error) {
	sequence, err := uint64Attribute(attributes, "sequence")
	if err != nil {
		return offlineMetadata{}, err
	}
	count, err := uint64Attribute(attributes, "count")
	if err != nil {
		return offlineMetadata{}, err
	}
	return offlineMetadata{sequence: sequence, count: count, exists: true}, nil
}

func (s *Store) getOfflineMetadata(ctx context.Context, sessionID string) (offlineMetadata, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: tableName(s.tableName), Key: offlineMetadataKey(sessionID), ConsistentRead: awsBool(true),
	})
	if err != nil {
		return offlineMetadata{}, err
	}
	if len(output.Item) == 0 {
		return offlineMetadata{}, nil
	}
	return decodeOfflineMetadata(output.Item)
}

func (s *Store) getOfflineMessageItem(ctx context.Context, sessionID, messageID string) (item, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: tableName(s.tableName), Key: offlineKey(sessionID, messageID), ConsistentRead: awsBool(true),
	})
	if err != nil {
		return nil, false, err
	}
	if len(output.Item) == 0 {
		return nil, false, nil
	}
	return item(output.Item), true, nil
}

func (s *Store) Append(ctx context.Context, msg wsstore.OfflineMessage) error {
	const op = "Append"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if msg.SessionID == "" || msg.MessageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	if s.offlineQueueLimit <= 0 {
		return invalid(op, "offline queue limit must be positive")
	}
	if s.offlineOverflowPolicy != wsstore.OfflineOverflowRejectNew &&
		s.offlineOverflowPolicy != wsstore.OfflineOverflowEvictOldest {
		return invalid(op, "unknown offline overflow policy")
	}
	now := s.now()
	if msg.ExpiresAt.IsZero() == false && !msg.ExpiresAt.After(now) {
		return invalid(op, "offline message is already expired")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	msg.State = wsstore.OfflineMessageStatePending
	msg.SentAt = nil
	msg.Envelope = cloneBytes(msg.Envelope)

	if err := s.purgeExpiredSessionMessages(ctx, msg.SessionID, now); err != nil {
		return backendError(op, err)
	}

	for attempt := 0; attempt < maxAppendAttempts; attempt++ {
		if err := checkContext(ctx); err != nil {
			return err
		}
		now = s.now()
		if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(now) {
			return invalid(op, "offline message is already expired")
		}
		attributes, ok, err := s.getSessionItem(ctx, msg.SessionID)
		if err != nil {
			return backendError(op, err)
		}
		if !ok {
			return notFound(op, "session not found")
		}
		session, err := decodeSession(attributes)
		if err != nil {
			return wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
		}
		if expired(session.ExpiresAt, now) {
			return notFound(op, "session not found")
		}
		if session.Generation != msg.Generation {
			return conflict(op, "offline message generation does not match session")
		}
		if existing, ok, err := s.getOfflineMessageItem(ctx, msg.SessionID, msg.MessageID); err != nil {
			return backendError(op, err)
		} else if ok {
			current, decodeErr := decodeOfflineMessage(existing)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if !expired(current.ExpiresAt, now) {
				return conflict(op, "offline message ID already exists")
			}
			if err := s.deleteOfflineMessage(ctx, op, current); err != nil {
				return err
			}
			continue
		}

		metadata, err := s.getOfflineMetadata(ctx, msg.SessionID)
		if err != nil {
			return backendError(op, err)
		}
		var oldest *wsstore.OfflineMessage
		if metadata.count >= uint64(s.offlineQueueLimit) {
			switch s.offlineOverflowPolicy {
			case wsstore.OfflineOverflowRejectNew:
				return quotaExceeded(op, "offline queue is full")
			case wsstore.OfflineOverflowEvictOldest:
				oldest, err = s.oldestOfflineMessage(ctx, msg.SessionID)
				if err != nil {
					return backendError(op, err)
				}
				if oldest == nil {
					return unavailable(op, "offline queue metadata has no oldest message")
				}
			}
		}

		nextSequence := metadata.sequence + 1
		nextCount := metadata.count + 1
		if oldest != nil {
			nextCount = metadata.count
		}
		msg.Sequence = nextSequence
		metadataCondition := "attribute_not_exists(#pk)"
		if metadata.exists {
			metadataCondition = "#sequence = :oldSequence AND #count = :oldCount"
		}
		metadataValues := map[string]types.AttributeValue{
			":sequence": unsignedValue(nextSequence), ":count": unsignedValue(nextCount),
			":oldSequence": unsignedValue(metadata.sequence), ":oldCount": unsignedValue(metadata.count),
		}
		transaction := []types.TransactWriteItem{
			{ConditionCheck: &types.ConditionCheck{
				TableName: tableName(s.tableName), Key: sessionKey(msg.SessionID),
				ConditionExpression:       awsString("#generation = :generation AND " + liveExpiryCondition),
				ExpressionAttributeNames:  map[string]string{"#generation": "generation", "#expiresAt": "expiresAt"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":generation": unsignedValue(msg.Generation), ":now": numberValue(now.UnixNano())},
			}},
			{Update: &types.Update{
				TableName: tableName(s.tableName), Key: offlineMetadataKey(msg.SessionID),
				UpdateExpression:          awsString("SET #sequence = :sequence, #count = :count"),
				ConditionExpression:       awsString(metadataCondition),
				ExpressionAttributeNames:  map[string]string{"#pk": attrPK, "#sequence": "sequence", "#count": "count"},
				ExpressionAttributeValues: metadataValues,
			}},
			{Put: &types.Put{
				TableName: tableName(s.tableName), Item: encodeOfflineMessage(msg),
				ConditionExpression:      awsString("attribute_not_exists(#pk)"),
				ExpressionAttributeNames: map[string]string{"#pk": attrPK},
			}},
		}
		if oldest != nil {
			transaction = append(transaction, types.TransactWriteItem{Delete: &types.Delete{
				TableName: tableName(s.tableName), Key: offlineKey(oldest.SessionID, oldest.MessageID),
				ConditionExpression:      awsString("#messageId = :messageID AND #sequence = :oldMessageSequence"),
				ExpressionAttributeNames: map[string]string{"#messageId": "messageId", "#sequence": "sequence"},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":messageID": stringValue(oldest.MessageID), ":oldMessageSequence": unsignedValue(oldest.Sequence),
				},
			}})
		}
		_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transaction})
		if err == nil {
			return nil
		}
		if !wsstore.IsConditionConflict(backendError(op, err)) {
			return backendError(op, err)
		}
		// A metadata or oldest-message condition is an optimistic CAS retry.
		// Session and duplicate-ID failures are re-read at the start of the next
		// attempt, which preserves their stable not-found/conflict classification.
	}
	return unavailable(op, "offline queue changed concurrently; append did not converge")
}

func (s *Store) oldestOfflineMessage(ctx context.Context, sessionID string) (*wsstore.OfflineMessage, error) {
	limit := int32(1)
	output, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName: tableName(s.tableName), IndexName: awsString(s.offlineSequenceIndex),
		KeyConditionExpression:    awsString("#gsi4pk = :session"),
		ExpressionAttributeNames:  map[string]string{"#gsi4pk": attrGSI4PK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":session": stringValue(partitionSession + sessionID)},
		Limit:                     &limit, ScanIndexForward: awsBool(true),
	})
	if err != nil || len(output.Items) == 0 {
		return nil, err
	}
	msg, err := decodeOfflineMessage(output.Items[0])
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (s *Store) ListPending(ctx context.Context, sessionID string, limit int) ([]wsstore.OfflineMessage, error) {
	const op = "ListPending"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := s.check(op); err != nil {
		return nil, err
	}
	if sessionID == "" || !validLimit(limit) {
		return nil, invalid(op, "session ID and a limit between 1 and MaxBatchSize are required")
	}
	if _, err := s.GetSession(ctx, sessionID); err != nil {
		if wsstore.IsNotFound(err) {
			return []wsstore.OfflineMessage{}, nil
		}
		return nil, err
	}
	messages := make([]wsstore.OfflineMessage, 0, limit)
	var startKey map[string]types.AttributeValue
	now := s.now()
	for len(messages) < limit {
		pageLimit := int32(limit - len(messages))
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), IndexName: awsString(s.offlineSequenceIndex),
			KeyConditionExpression:    awsString("#gsi4pk = :session"),
			ExpressionAttributeNames:  map[string]string{"#gsi4pk": attrGSI4PK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":session": stringValue(partitionSession + sessionID)},
			ExclusiveStartKey:         startKey, Limit: &pageLimit, ScanIndexForward: awsBool(true),
		})
		if err != nil {
			return nil, backendError(op, err)
		}
		for _, attributes := range output.Items {
			msg, decodeErr := decodeOfflineMessage(attributes)
			if decodeErr != nil {
				return nil, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if !expired(msg.ExpiresAt, now) {
				messages = append(messages, cloneOfflineMessage(msg))
				if len(messages) == limit {
					break
				}
			}
		}
		if len(output.LastEvaluatedKey) == 0 || len(messages) == limit {
			break
		}
		startKey = output.LastEvaluatedKey
	}
	return messages, nil
}

func (s *Store) MarkSent(ctx context.Context, sessionID, messageID string, generation uint64) error {
	const op = "MarkSent"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	now := s.now()
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: tableName(s.tableName), Key: sessionKey(sessionID),
			ConditionExpression:       awsString("#generation = :generation AND #currentConnectionId <> :empty AND " + liveExpiryCondition),
			ExpressionAttributeNames:  map[string]string{"#generation": "generation", "#currentConnectionId": "currentConnectionId", "#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":generation": unsignedValue(generation), ":empty": stringValue(""), ":now": numberValue(now.UnixNano())},
		}},
		{Update: &types.Update{
			TableName: tableName(s.tableName), Key: offlineKey(sessionID, messageID),
			UpdateExpression:         awsString("SET #generation = :generation, #state = :sent, #sentAt = :sentAt"),
			ConditionExpression:      awsString("attribute_exists(#pk) AND #generation <= :generation AND " + liveExpiryCondition),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#generation": "generation", "#state": "state", "#sentAt": "sentAt", "#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":generation": unsignedValue(generation), ":sent": stringValue(string(wsstore.OfflineMessageStateSent)),
				":sentAt": numberValue(now.UnixNano()), ":now": numberValue(now.UnixNano()),
			},
		}},
	}})
	if err == nil {
		return nil
	}
	if !wsstore.IsConditionConflict(backendError(op, err)) {
		return backendError(op, err)
	}
	return s.classifyOfflineCondition(ctx, op, sessionID, messageID, generation, now, false)
}

// MarkSentForReplay refreshes SentAt after each successful replay delivery.
func (s *Store) MarkSentForReplay(ctx context.Context, sessionID, messageID string, generation uint64) error {
	return s.MarkSent(ctx, sessionID, messageID, generation)
}

func (s *Store) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	const op = "Ack"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	now := s.now()
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: tableName(s.tableName), Key: sessionKey(sessionID),
			ConditionExpression:       awsString("#generation = :generation AND #currentConnectionId <> :empty AND " + liveExpiryCondition),
			ExpressionAttributeNames:  map[string]string{"#generation": "generation", "#currentConnectionId": "currentConnectionId", "#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":generation": unsignedValue(generation), ":empty": stringValue(""), ":now": numberValue(now.UnixNano())},
		}},
		{Delete: &types.Delete{
			TableName: tableName(s.tableName), Key: offlineKey(sessionID, messageID),
			ConditionExpression:       awsString("attribute_exists(#pk) AND #generation = :generation"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK, "#generation": "generation"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":generation": unsignedValue(generation)},
		}},
		{Update: &types.Update{
			TableName: tableName(s.tableName), Key: offlineMetadataKey(sessionID),
			UpdateExpression:          awsString("ADD #count :minusOne"),
			ConditionExpression:       awsString("#count > :zero"),
			ExpressionAttributeNames:  map[string]string{"#count": "count"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":minusOne": numberValue(-1), ":zero": numberValue(0)},
		}},
	}})
	if err == nil {
		return nil
	}
	if !wsstore.IsConditionConflict(backendError(op, err)) {
		return backendError(op, err)
	}
	return s.classifyOfflineCondition(ctx, op, sessionID, messageID, generation, now, true)
}

func (s *Store) classifyOfflineCondition(ctx context.Context, op, sessionID, messageID string, generation uint64, now time.Time, missingAckSucceeds bool) error {
	attributes, ok, err := s.getSessionItem(ctx, sessionID)
	if err != nil {
		return backendError(op, err)
	}
	if !ok {
		return notFound(op, "session not found")
	}
	session, err := decodeSession(attributes)
	if err != nil {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(session.ExpiresAt, now) {
		return notFound(op, "session not found")
	}
	if session.Generation != generation {
		return conflict(op, "session generation does not match")
	}
	messageAttributes, ok, err := s.getOfflineMessageItem(ctx, sessionID, messageID)
	if err != nil {
		return backendError(op, err)
	}
	if !ok {
		if missingAckSucceeds {
			return nil
		}
		return notFound(op, "offline message not found")
	}
	msg, err := decodeOfflineMessage(messageAttributes)
	if err != nil {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(msg.ExpiresAt, now) && !missingAckSucceeds {
		return notFound(op, "offline message not found")
	}
	if session.CurrentConnectionID == "" || msg.Generation > generation || (missingAckSucceeds && msg.Generation != generation) {
		return conflict(op, "offline message was not sent at the current generation")
	}
	return conflict(op, "offline message or queue condition does not match")
}

func (s *Store) DeleteExpired(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpired"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if before.IsZero() || !validLimit(limit) {
		return invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	output, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName: tableName(s.tableName), IndexName: awsString(s.timeIndex),
		KeyConditionExpression:   awsString("#gsi1pk = :partition AND #gsi1sk < :after"),
		ExpressionAttributeNames: map[string]string{"#gsi1pk": attrGSI1PK, "#gsi1sk": attrGSI1SK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition": stringValue(timePartitionOffline), ":after": stringValue(timeKey(before.Add(time.Nanosecond))),
		},
		Limit: awsInt32(int32(limit)),
	})
	if err != nil {
		return backendError(op, err)
	}
	var firstErr error
	for _, attributes := range output.Items {
		msg, decodeErr := decodeOfflineMessage(attributes)
		if decodeErr != nil {
			return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		if deleteErr := s.deleteOfflineMessage(ctx, op, msg); deleteErr != nil && firstErr == nil {
			firstErr = deleteErr
		}
	}
	return firstErr
}

func (s *Store) deleteOfflineMessage(ctx context.Context, op string, msg wsstore.OfflineMessage) error {
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Delete: &types.Delete{
			TableName: tableName(s.tableName), Key: offlineKey(msg.SessionID, msg.MessageID),
			ConditionExpression:       awsString("attribute_exists(#pk) AND #expiresAt = :expiresAt"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK, "#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":expiresAt": numberValue(msg.ExpiresAt.UnixNano())},
		}},
		{Update: &types.Update{
			TableName: tableName(s.tableName), Key: offlineMetadataKey(msg.SessionID),
			UpdateExpression: awsString("ADD #count :minusOne"), ConditionExpression: awsString("#count > :zero"),
			ExpressionAttributeNames:  map[string]string{"#count": "count"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":minusOne": numberValue(-1), ":zero": numberValue(0)},
		}},
	}})
	if err == nil || !wsstore.IsConditionConflict(backendError(op, err)) {
		return backendError(op, err)
	}
	_, ok, getErr := s.getOfflineMessageItem(ctx, msg.SessionID, msg.MessageID)
	if getErr != nil {
		return backendError(op, getErr)
	}
	if !ok {
		return nil
	}
	return conflict(op, "offline message or queue count changed during expiry cleanup")
}

func (s *Store) purgeExpiredSessionMessages(ctx context.Context, sessionID string, now time.Time) error {
	var startKey map[string]types.AttributeValue
	for {
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), ConsistentRead: awsBool(true),
			KeyConditionExpression:   awsString("#pk = :pk AND begins_with(#sk, :prefix)"),
			ExpressionAttributeNames: map[string]string{"#pk": attrPK, "#sk": attrSK},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": stringValue(partitionSession + sessionID), ":prefix": stringValue(sortOfflineMessage),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return err
		}
		for _, attributes := range output.Items {
			msg, decodeErr := decodeOfflineMessage(attributes)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, "Append", decodeErr)
			}
			if expired(msg.ExpiresAt, now) {
				if err := s.deleteOfflineMessage(ctx, "Append", msg); err != nil {
					return err
				}
			}
		}
		if len(output.LastEvaluatedKey) == 0 {
			return nil
		}
		startKey = output.LastEvaluatedKey
	}
}
