package dynamodb

import (
	"context"
	"encoding/json"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (s *Store) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	const op = "BeginIdempotency"
	if err := checkContext(ctx); err != nil {
		return wsstore.IdempotencyRecord{}, false, err
	}
	if err := s.check(op); err != nil {
		return wsstore.IdempotencyRecord{}, false, err
	}
	if record.SessionID == "" || record.MessageID == "" {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "session ID and message ID are required")
	}
	for attempt := 0; attempt < 3; attempt++ {
		now := s.now()
		candidate := cloneIdempotencyRecord(record)
		if candidate.CreatedAt.IsZero() {
			candidate.CreatedAt = now
		}
		if candidate.ExpiresAt.IsZero() {
			if s.idempotencyWindow <= 0 {
				return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency window must be positive")
			}
			candidate.ExpiresAt = now.Add(s.idempotencyWindow)
		}
		if !candidate.ExpiresAt.After(now) {
			return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency record is already expired")
		}
		candidate.State = wsstore.IdempotencyStateProcessing
		candidate.Response = nil
		_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: tableName(s.tableName), Item: encodeIdempotencyRecord(candidate),
			ConditionExpression:       awsString("attribute_not_exists(#pk) OR (attribute_exists(#expiresAt) AND #expiresAt <= :now)"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK, "#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":now": numberValue(now.UnixNano())},
		})
		if err == nil {
			return candidate, true, nil
		}
		if !wsstore.IsConditionConflict(backendError(op, err)) {
			return wsstore.IdempotencyRecord{}, false, backendError(op, err)
		}
		output, getErr := s.client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: tableName(s.tableName), Key: idempotencyKey(record.SessionID, record.MessageID), ConsistentRead: awsBool(true),
		})
		if getErr != nil {
			return wsstore.IdempotencyRecord{}, false, backendError(op, getErr)
		}
		if len(output.Item) == 0 {
			continue
		}
		current, decodeErr := decodeIdempotencyRecord(output.Item)
		if decodeErr != nil {
			return wsstore.IdempotencyRecord{}, false, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		if expired(current.ExpiresAt, s.now()) {
			continue
		}
		return cloneIdempotencyRecord(current), false, nil
	}
	return wsstore.IdempotencyRecord{}, false, unavailable(op, "idempotency reservation changed concurrently")
}

func (s *Store) CompleteIdempotency(ctx context.Context, sessionID, messageID string, response json.RawMessage) error {
	const op = "CompleteIdempotency"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: tableName(s.tableName), Key: idempotencyKey(sessionID, messageID),
		UpdateExpression:    awsString("SET #state = :state, #response = :response"),
		ConditionExpression: awsString("attribute_exists(#pk) AND " + liveExpiryCondition),
		ExpressionAttributeNames: map[string]string{
			"#state": "state", "#response": "response", "#pk": attrPK, "#expiresAt": "expiresAt",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":state":    stringValue(string(wsstore.IdempotencyStateCompleted)),
			":response": binaryValue(response), ":now": numberValue(s.now().UnixNano()),
		},
	})
	if err == nil {
		return nil
	}
	if wsstore.IsConditionConflict(backendError(op, err)) {
		return notFound(op, "idempotency record not found")
	}
	return backendError(op, err)
}

func (s *Store) GetIdempotency(ctx context.Context, sessionID, messageID string) (wsstore.IdempotencyRecord, error) {
	const op = "GetIdempotency"
	if err := checkContext(ctx); err != nil {
		return wsstore.IdempotencyRecord{}, err
	}
	if err := s.check(op); err != nil {
		return wsstore.IdempotencyRecord{}, err
	}
	if sessionID == "" || messageID == "" {
		return wsstore.IdempotencyRecord{}, invalid(op, "session ID and message ID are required")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: tableName(s.tableName), Key: idempotencyKey(sessionID, messageID), ConsistentRead: awsBool(true),
	})
	if err != nil {
		return wsstore.IdempotencyRecord{}, backendError(op, err)
	}
	if len(output.Item) == 0 {
		return wsstore.IdempotencyRecord{}, notFound(op, "idempotency record not found")
	}
	record, err := decodeIdempotencyRecord(output.Item)
	if err != nil {
		return wsstore.IdempotencyRecord{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(record.ExpiresAt, s.now()) {
		return wsstore.IdempotencyRecord{}, notFound(op, "idempotency record not found")
	}
	return cloneIdempotencyRecord(record), nil
}

func (s *Store) DeleteExpiredIdempotency(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredIdempotency"
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
			":partition": stringValue(timePartitionIdempotency), ":after": stringValue(timeKey(before.Add(time.Nanosecond))),
		},
		Limit: awsInt32(int32(limit)),
	})
	if err != nil {
		return backendError(op, err)
	}
	var firstErr error
	for _, attributes := range output.Items {
		record, decodeErr := decodeIdempotencyRecord(attributes)
		if decodeErr != nil {
			return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		_, deleteErr := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: tableName(s.tableName), Key: idempotencyKey(record.SessionID, record.MessageID),
			ConditionExpression:       awsString("#expiresAt = :expiresAt"),
			ExpressionAttributeNames:  map[string]string{"#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":expiresAt": numberValue(record.ExpiresAt.UnixNano())},
		})
		if deleteErr != nil && !isConditionalFailure(deleteErr) && firstErr == nil {
			firstErr = backendError(op, deleteErr)
		}
	}
	return firstErr
}
