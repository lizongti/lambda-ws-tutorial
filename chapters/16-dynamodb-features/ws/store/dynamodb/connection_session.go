package dynamodb

import (
	"context"
	"errors"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var liveExpiryCondition = "(attribute_not_exists(#expiresAt) OR #expiresAt > :now)"

const maxSessionCreateAttempts = 128

func (s *Store) PutConnection(ctx context.Context, c wsstore.Connection) error {
	const op = "PutConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if c.ConnectionID == "" {
		return invalid(op, "connection ID is required")
	}
	if c.State == "" {
		c.State = wsstore.ConnectionStateActive
	}
	switch c.State {
	case wsstore.ConnectionStateActive, wsstore.ConnectionStateStale, wsstore.ConnectionStateClosed:
	default:
		return invalid(op, "unknown connection state")
	}
	now := s.now()
	if c.ConnectedAt.IsZero() {
		c.ConnectedAt = now
	}
	if c.LastSeenAt.IsZero() {
		c.LastSeenAt = c.ConnectedAt
	}
	mainPut := &types.Put{
		TableName:           tableName(s.tableName),
		Item:                encodeConnection(c),
		ConditionExpression: awsString("attribute_not_exists(#pk) OR #generation <= :generation OR (attribute_exists(#expiresAt) AND #expiresAt <= :now)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK, "#generation": "generation", "#expiresAt": "expiresAt",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":generation": unsignedValue(c.Generation),
			":now":        numberValue(now.UnixNano()),
		},
	}
	operations := []types.TransactWriteItem{{Put: mainPut}}
	if c.ExpiresAt.IsZero() {
		operations = append(operations, types.TransactWriteItem{Delete: &types.Delete{
			TableName: tableName(s.tableName), Key: connectionExpiryKey(c.ConnectionID),
		}})
	} else {
		operations = append(operations, types.TransactWriteItem{Put: &types.Put{
			TableName: tableName(s.tableName), Item: encodeConnectionExpiry(c),
		}})
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: operations})
	return backendError(op, err)
}

func (s *Store) getConnectionItem(ctx context.Context, id string) (item, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: tableName(s.tableName), Key: connectionKey(id), ConsistentRead: awsBool(true),
	})
	if err != nil {
		return nil, false, err
	}
	if len(output.Item) == 0 {
		return nil, false, nil
	}
	return item(output.Item), true, nil
}

func (s *Store) GetConnection(ctx context.Context, id string) (wsstore.Connection, error) {
	const op = "GetConnection"
	if err := checkContext(ctx); err != nil {
		return wsstore.Connection{}, err
	}
	if err := s.check(op); err != nil {
		return wsstore.Connection{}, err
	}
	if id == "" {
		return wsstore.Connection{}, invalid(op, "connection ID is required")
	}
	attributes, ok, err := s.getConnectionItem(ctx, id)
	if err != nil {
		return wsstore.Connection{}, backendError(op, err)
	}
	if !ok {
		return wsstore.Connection{}, notFound(op, "connection not found")
	}
	c, err := decodeConnection(attributes)
	if err != nil {
		return wsstore.Connection{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(c.ExpiresAt, s.now()) {
		return wsstore.Connection{}, notFound(op, "connection not found")
	}
	return c, nil
}

func (s *Store) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	const op = "DeleteConnectionIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if id == "" {
		return invalid(op, "connection ID is required")
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Delete: &types.Delete{
			TableName: tableName(s.tableName), Key: connectionKey(id),
			ConditionExpression: awsString("attribute_not_exists(#pk) OR #generation = :generation"),
			ExpressionAttributeNames: map[string]string{
				"#pk": attrPK, "#generation": "generation",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":generation": unsignedValue(generation),
			},
		}},
		{Delete: &types.Delete{TableName: tableName(s.tableName), Key: connectionExpiryKey(id)}},
	}})
	return backendError(op, err)
}

func (s *Store) DeleteConnectionIfMatchAndLastSeen(ctx context.Context, id string, generation uint64, lastSeen time.Time) error {
	const op = "DeleteConnectionIfMatchAndLastSeen"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if id == "" || lastSeen.IsZero() {
		return invalid(op, "connection ID and timestamp are required")
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Delete: &types.Delete{
			TableName: tableName(s.tableName), Key: connectionKey(id),
			ConditionExpression: awsString("attribute_not_exists(#pk) OR (#generation = :generation AND #lastSeenAt = :lastSeenAt)"),
			ExpressionAttributeNames: map[string]string{
				"#pk": attrPK, "#generation": "generation", "#lastSeenAt": "lastSeenAt",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":generation": unsignedValue(generation), ":lastSeenAt": numberValue(lastSeen.UnixNano()),
			},
		}},
		{Delete: &types.Delete{TableName: tableName(s.tableName), Key: connectionExpiryKey(id)}},
	}})
	return backendError(op, err)
}

func (s *Store) TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error {
	const op = "TouchConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if id == "" || now.IsZero() {
		return invalid(op, "connection ID and timestamp are required")
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: tableName(s.tableName), Key: connectionKey(id),
		UpdateExpression:    awsString("SET #lastSeenAt = :lastSeenAt, #gsi1pk = :gsi1pk, #gsi1sk = :gsi1sk"),
		ConditionExpression: awsString("#generation = :generation AND #state <> :closed AND (attribute_not_exists(#lastSeenAt) OR #lastSeenAt < :lastSeenAt) AND " + liveExpiryCondition),
		ExpressionAttributeNames: map[string]string{
			"#generation": "generation", "#state": "state", "#lastSeenAt": "lastSeenAt",
			"#expiresAt": "expiresAt", "#gsi1pk": attrGSI1PK, "#gsi1sk": attrGSI1SK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":generation": unsignedValue(generation), ":closed": stringValue(string(wsstore.ConnectionStateClosed)),
			":lastSeenAt": numberValue(now.UnixNano()), ":now": numberValue(now.UnixNano()),
			":gsi1pk": stringValue(timePartitionStale),
			":gsi1sk": stringValue(timeKey(now) + "#" + id),
		},
	})
	if err == nil {
		return nil
	}
	if !wsstore.IsConditionConflict(backendError(op, err)) {
		return backendError(op, err)
	}
	attributes, ok, getErr := s.getConnectionItem(ctx, id)
	if getErr != nil {
		return backendError(op, getErr)
	}
	if !ok {
		return notFound(op, "connection not found")
	}
	c, decodeErr := decodeConnection(attributes)
	if decodeErr != nil {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
	}
	if expired(c.ExpiresAt, now) {
		return notFound(op, "connection not found")
	}
	if c.Generation == generation && c.State != wsstore.ConnectionStateClosed && !now.After(c.LastSeenAt) {
		return nil
	}
	return conflict(op, "connection generation or state does not match")
}

func (s *Store) ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListStaleConnections"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := s.check(op); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	connections := make([]wsstore.Connection, 0, limit)
	var startKey map[string]types.AttributeValue
	for len(connections) < limit {
		remaining := int32(limit - len(connections))
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), IndexName: awsString(s.timeIndex),
			KeyConditionExpression:    awsString("#gsi1pk = :partition AND #gsi1sk < :before"),
			ExpressionAttributeNames:  map[string]string{"#gsi1pk": attrGSI1PK, "#gsi1sk": attrGSI1SK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":partition": stringValue(timePartitionStale), ":before": stringValue(timeKey(before))},
			ExclusiveStartKey:         startKey, Limit: &remaining,
		})
		if err != nil {
			return nil, backendError(op, err)
		}
		for _, attributes := range output.Items {
			c, decodeErr := decodeConnection(attributes)
			if decodeErr != nil {
				return nil, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if c.State != wsstore.ConnectionStateClosed {
				connections = append(connections, c)
				if len(connections) == limit {
					break
				}
			}
		}
		if len(output.LastEvaluatedKey) == 0 || len(connections) == limit {
			break
		}
		startKey = output.LastEvaluatedKey
	}
	return connections, nil
}

func (s *Store) ListExpiredConnections(ctx context.Context, before time.Time, limit int) ([]wsstore.Connection, error) {
	const op = "ListExpiredConnections"
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := s.check(op); err != nil {
		return nil, err
	}
	if before.IsZero() || !validLimit(limit) {
		return nil, invalid(op, "a cutoff and a limit between 1 and MaxBatchSize are required")
	}
	output, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName: tableName(s.tableName), IndexName: awsString(s.timeIndex),
		KeyConditionExpression:   awsString("#gsi1pk = :partition AND #gsi1sk < :after"),
		ExpressionAttributeNames: map[string]string{"#gsi1pk": attrGSI1PK, "#gsi1sk": attrGSI1SK},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition": stringValue(timePartitionConnectionExpiry), ":after": stringValue(timeKey(before.Add(time.Nanosecond))),
		},
		Limit: awsInt32(int32(limit)),
	})
	if err != nil {
		return nil, backendError(op, err)
	}
	connections := make([]wsstore.Connection, 0, len(output.Items))
	for _, attributes := range output.Items {
		id, generation, expiresAt, decodeErr := decodeConnectionExpiry(attributes)
		if decodeErr != nil {
			return nil, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		removeOrphanIndex := func() error {
			_, deleteErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
				{ConditionCheck: &types.ConditionCheck{
					TableName: tableName(s.tableName), Key: connectionKey(id),
					ConditionExpression: awsString("attribute_not_exists(#pk)"), ExpressionAttributeNames: map[string]string{"#pk": attrPK},
				}},
				{Delete: &types.Delete{
					TableName: tableName(s.tableName), Key: connectionExpiryKey(id),
					ConditionExpression:       awsString("#generation = :generation AND #expiresAt = :expiresAt"),
					ExpressionAttributeNames:  map[string]string{"#generation": "generation", "#expiresAt": "expiresAt"},
					ExpressionAttributeValues: map[string]types.AttributeValue{":generation": unsignedValue(generation), ":expiresAt": numberValue(expiresAt.UnixNano())},
				}},
			}})
			if wsstore.IsConditionConflict(backendError(op, deleteErr)) {
				return nil
			}
			return backendError(op, deleteErr)
		}
		stored, ok, getErr := s.getConnectionItem(ctx, id)
		if getErr != nil {
			return nil, backendError(op, getErr)
		}
		if !ok {
			if err := removeOrphanIndex(); err != nil {
				return nil, err
			}
			continue
		}
		connection, decodeErr := decodeConnection(stored)
		if decodeErr != nil {
			return nil, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		if connection.Generation != generation || !connection.ExpiresAt.Equal(expiresAt) {
			continue
		}
		if expired(connection.ExpiresAt, before) {
			connections = append(connections, connection)
		}
	}
	return connections, nil
}

func (s *Store) CreateSession(ctx context.Context, session wsstore.Session) error {
	const op = "CreateSession"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if session.SessionID == "" {
		return invalid(op, "session ID is required")
	}
	if session.LastActiveAt.IsZero() {
		session.LastActiveAt = s.now()
	}
	for attempt := 0; attempt < maxSessionCreateAttempts; attempt++ {
		if err := checkContext(ctx); err != nil {
			return err
		}
		now := s.now()
		attributes, ok, err := s.getSessionItem(ctx, session.SessionID)
		if err != nil {
			return backendError(op, err)
		}
		condition := "attribute_not_exists(#pk) OR (attribute_exists(#expiresAt) AND #expiresAt <= :now)"
		names := map[string]string{"#pk": attrPK, "#expiresAt": "expiresAt"}
		values := map[string]types.AttributeValue{":now": numberValue(now.UnixNano())}
		if ok {
			previous, decodeErr := decodeSession(attributes)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if expired(previous.ExpiresAt, now) {
				if cleanupErr := s.cleanupSessionSecondary(ctx, op, session.SessionID, previous.ExpiresAt); cleanupErr != nil {
					if wsstore.IsConditionConflict(cleanupErr) {
						continue
					}
					return cleanupErr
				}
				condition = "#expiresAt = :previousExpiresAt AND #expiresAt <= :now"
				values[":previousExpiresAt"] = numberValue(previous.ExpiresAt.UnixNano())
			}
		}
		_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: tableName(s.tableName), Item: encodeSession(session),
			ConditionExpression:       awsString(condition),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		})
		if err == nil {
			return nil
		}
		if !wsstore.IsConditionConflict(backendError(op, err)) {
			return backendError(op, err)
		}
		if ok {
			previous, decodeErr := decodeSession(attributes)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if !expired(previous.ExpiresAt, now) {
				return backendError(op, err)
			}
		}
	}
	return unavailable(op, "session changed concurrently; create did not converge")
}

func (s *Store) getSessionItem(ctx context.Context, id string) (item, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: tableName(s.tableName), Key: sessionKey(id), ConsistentRead: awsBool(true),
	})
	if err != nil {
		return nil, false, err
	}
	if len(output.Item) == 0 {
		return nil, false, nil
	}
	return item(output.Item), true, nil
}

func (s *Store) GetSession(ctx context.Context, id string) (wsstore.Session, error) {
	const op = "GetSession"
	if err := checkContext(ctx); err != nil {
		return wsstore.Session{}, err
	}
	if err := s.check(op); err != nil {
		return wsstore.Session{}, err
	}
	if id == "" {
		return wsstore.Session{}, invalid(op, "session ID is required")
	}
	attributes, ok, err := s.getSessionItem(ctx, id)
	if err != nil {
		return wsstore.Session{}, backendError(op, err)
	}
	if !ok {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	session, err := decodeSession(attributes)
	if err != nil {
		return wsstore.Session{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(session.ExpiresAt, s.now()) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	return session, nil
}

func (s *Store) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	const op = "BindConnection"
	if err := checkContext(ctx); err != nil {
		return wsstore.Session{}, err
	}
	if err := s.check(op); err != nil {
		return wsstore.Session{}, err
	}
	if sessionID == "" || connectionID == "" {
		return wsstore.Session{}, invalid(op, "session ID and connection ID are required")
	}
	now := s.now()
	attributes, ok, err := s.getSessionItem(ctx, sessionID)
	if err != nil {
		return wsstore.Session{}, backendError(op, err)
	}
	if !ok {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	current, err := decodeSession(attributes)
	if err != nil {
		return wsstore.Session{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(current.ExpiresAt, now) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	if current.Generation != expectedGeneration {
		return wsstore.Session{}, conflict(op, "session generation does not match")
	}
	expiresAt := current.ExpiresAt
	if retention := current.ExpiresAt.Sub(current.LastActiveAt); !current.ExpiresAt.IsZero() && retention > 0 {
		expiresAt = now.Add(retention)
	}
	updateExpression := "SET #currentConnectionId = :connectionID, #generation = #generation + :one, #lastActiveAt = :lastActiveAt"
	names := map[string]string{
		"#currentConnectionId": "currentConnectionId", "#generation": "generation",
		"#lastActiveAt": "lastActiveAt", "#expiresAt": "expiresAt",
	}
	values := map[string]types.AttributeValue{
		":connectionID": stringValue(connectionID), ":expected": unsignedValue(expectedGeneration),
		":one": unsignedValue(1), ":lastActiveAt": numberValue(now.UnixNano()), ":now": numberValue(now.UnixNano()),
	}
	if !expiresAt.IsZero() {
		updateExpression += ", #expiresAt = :newExpiresAt, #ttl = :ttl, #gsi1pk = :gsi1pk, #gsi1sk = :gsi1sk"
		names["#ttl"] = attrTTL
		names["#gsi1pk"] = attrGSI1PK
		names["#gsi1sk"] = attrGSI1SK
		values[":newExpiresAt"] = numberValue(expiresAt.UnixNano())
		values[":ttl"] = numberValue(expiresAt.Unix())
		values[":gsi1pk"] = stringValue(timePartitionSession)
		values[":gsi1sk"] = stringValue(timeKey(expiresAt) + "#" + sessionID)
	}
	output, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: tableName(s.tableName), Key: sessionKey(sessionID), ReturnValues: types.ReturnValueAllNew,
		UpdateExpression:          awsString(updateExpression),
		ConditionExpression:       awsString("#generation = :expected AND " + liveExpiryCondition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err == nil {
		session, decodeErr := decodeSession(output.Attributes)
		if decodeErr != nil {
			return wsstore.Session{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		return session, nil
	}
	if !wsstore.IsConditionConflict(backendError(op, err)) {
		return wsstore.Session{}, backendError(op, err)
	}
	attributes, ok, getErr := s.getSessionItem(ctx, sessionID)
	if getErr != nil {
		return wsstore.Session{}, backendError(op, getErr)
	}
	if !ok {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	current, decodeErr := decodeSession(attributes)
	if decodeErr != nil {
		return wsstore.Session{}, wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
	}
	if expired(current.ExpiresAt, now) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	return wsstore.Session{}, conflict(op, "session generation does not match")
}

func (s *Store) UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error {
	const op = "UnbindConnectionIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if sessionID == "" || connectionID == "" {
		return invalid(op, "session ID and connection ID are required")
	}
	now := s.now()
	attributes, ok, err := s.getSessionItem(ctx, sessionID)
	if err != nil {
		return backendError(op, err)
	}
	if !ok {
		return nil
	}
	current, err := decodeSession(attributes)
	if err != nil {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, op, err)
	}
	if expired(current.ExpiresAt, now) {
		return nil
	}
	if current.Generation != generation {
		return conflict(op, "session binding does not match")
	}
	if current.CurrentConnectionID == "" {
		return nil
	}
	if current.CurrentConnectionID != connectionID {
		return conflict(op, "session binding does not match")
	}
	expiresAt := current.ExpiresAt
	if retention := current.ExpiresAt.Sub(current.LastActiveAt); !current.ExpiresAt.IsZero() && retention > 0 {
		expiresAt = now.Add(retention)
	}
	updateExpression := "SET #currentConnectionId = :empty, #lastActiveAt = :lastActiveAt"
	names := map[string]string{
		"#currentConnectionId": "currentConnectionId", "#generation": "generation",
		"#lastActiveAt": "lastActiveAt", "#expiresAt": "expiresAt",
	}
	values := map[string]types.AttributeValue{
		":generation": unsignedValue(generation), ":connectionID": stringValue(connectionID),
		":empty": stringValue(""), ":lastActiveAt": numberValue(now.UnixNano()), ":now": numberValue(now.UnixNano()),
	}
	if !expiresAt.IsZero() {
		updateExpression += ", #expiresAt = :newExpiresAt, #ttl = :ttl, #gsi1pk = :gsi1pk, #gsi1sk = :gsi1sk"
		names["#ttl"] = attrTTL
		names["#gsi1pk"] = attrGSI1PK
		names["#gsi1sk"] = attrGSI1SK
		values[":newExpiresAt"] = numberValue(expiresAt.UnixNano())
		values[":ttl"] = numberValue(expiresAt.Unix())
		values[":gsi1pk"] = stringValue(timePartitionSession)
		values[":gsi1sk"] = stringValue(timeKey(expiresAt) + "#" + sessionID)
	}
	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: tableName(s.tableName), Key: sessionKey(sessionID),
		UpdateExpression:          awsString(updateExpression),
		ConditionExpression:       awsString("#generation = :generation AND #currentConnectionId = :connectionID AND " + liveExpiryCondition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err == nil {
		return nil
	}
	if !wsstore.IsConditionConflict(backendError(op, err)) {
		return backendError(op, err)
	}
	attributes, ok, getErr := s.getSessionItem(ctx, sessionID)
	if getErr != nil {
		return backendError(op, getErr)
	}
	if !ok {
		return nil
	}
	current, decodeErr := decodeSession(attributes)
	if decodeErr != nil {
		return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
	}
	if expired(current.ExpiresAt, now) {
		return nil
	}
	if current.Generation == generation && current.CurrentConnectionID == "" {
		return nil
	}
	return conflict(op, "session binding does not match")
}

func (s *Store) DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error {
	const op = "DeleteExpiredSessions"
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
			":partition": stringValue(timePartitionSession), ":after": stringValue(timeKey(before.Add(time.Nanosecond))),
		},
		Limit: awsInt32(int32(limit)),
	})
	if err != nil {
		return backendError(op, err)
	}
	var firstErr error
	for _, attributes := range output.Items {
		session, decodeErr := decodeSession(attributes)
		if decodeErr != nil {
			return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
		}
		if cleanupErr := s.cleanupSessionSecondary(ctx, op, session.SessionID, session.ExpiresAt); cleanupErr != nil {
			if !wsstore.IsConditionConflict(cleanupErr) && firstErr == nil {
				firstErr = cleanupErr
			}
			continue
		}
		_, deleteErr := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: tableName(s.tableName), Key: sessionKey(session.SessionID),
			ConditionExpression:       awsString("#expiresAt = :expiresAt"),
			ExpressionAttributeNames:  map[string]string{"#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":expiresAt": numberValue(session.ExpiresAt.UnixNano())},
		})
		if deleteErr != nil {
			if wsstore.IsConditionConflict(backendError(op, deleteErr)) {
				continue
			}
			if firstErr == nil {
				firstErr = backendError(op, deleteErr)
			}
		}
	}
	return firstErr
}

func (s *Store) deleteSessionSecondary(ctx context.Context, op, sessionID string, expiresAt time.Time, secondaryKey map[string]types.AttributeValue) error {
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: tableName(s.tableName), Key: sessionKey(sessionID),
			ConditionExpression:       awsString("#expiresAt = :expiresAt"),
			ExpressionAttributeNames:  map[string]string{"#expiresAt": "expiresAt"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":expiresAt": numberValue(expiresAt.UnixNano())},
		}},
		{Delete: &types.Delete{TableName: tableName(s.tableName), Key: secondaryKey}},
	}})
	return backendError(op, err)
}

func (s *Store) cleanupSessionSecondary(ctx context.Context, op, sessionID string, expiresAt time.Time) error {
	var firstErr error
	var startKey map[string]types.AttributeValue
	for {
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), ConsistentRead: awsBool(true),
			KeyConditionExpression:    awsString("#pk = :pk"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":pk": stringValue(partitionSession + sessionID)},
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return backendError(op, err)
		}
		for _, attributes := range output.Items {
			sk, decodeErr := stringAttribute(attributes, attrSK)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			if sk == sortMetadata {
				continue
			}
			deleteErr := s.deleteSessionSecondary(ctx, op, sessionID, expiresAt, key(partitionSession+sessionID, sk))
			if deleteErr != nil {
				if wsstore.IsConditionConflict(deleteErr) {
					return deleteErr
				}
				if firstErr == nil {
					firstErr = deleteErr
				}
			}
		}
		if len(output.LastEvaluatedKey) == 0 {
			break
		}
		startKey = output.LastEvaluatedKey
	}

	startKey = nil
	for {
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), IndexName: awsString(s.sessionRoomIndex),
			KeyConditionExpression:    awsString("#gsi3pk = :session"),
			ExpressionAttributeNames:  map[string]string{"#gsi3pk": attrGSI3PK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":session": stringValue(partitionSession + sessionID)},
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = backendError(op, err)
			}
			break
		}
		for _, attributes := range output.Items {
			member, decodeErr := decodeRoomMember(attributes)
			if decodeErr != nil {
				return wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			deleteErr := s.deleteSessionSecondary(ctx, op, sessionID, expiresAt, roomKey(member.RoomID, member.SessionID))
			if deleteErr != nil {
				if wsstore.IsConditionConflict(deleteErr) {
					return deleteErr
				}
				if firstErr == nil {
					firstErr = deleteErr
				}
			}
		}
		if len(output.LastEvaluatedKey) == 0 {
			break
		}
		startKey = output.LastEvaluatedKey
	}
	return firstErr
}

func awsString(value string) *string { return &value }
func awsBool(value bool) *bool       { return &value }
func awsInt32(value int32) *int32    { return &value }

func isConditionalFailure(err error) bool {
	var conditional *types.ConditionalCheckFailedException
	return errors.As(err, &conditional) || transactionConditionFailed(err)
}
