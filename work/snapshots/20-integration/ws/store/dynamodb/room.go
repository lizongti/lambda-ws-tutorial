package dynamodb

import (
	"context"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (s *Store) Join(ctx context.Context, member wsstore.RoomMember) error {
	const op = "Join"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	if member.JoinedAt.IsZero() {
		member.JoinedAt = s.now()
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: tableName(s.tableName), Key: roomKey(member.RoomID, member.SessionID),
		UpdateExpression: awsString("SET #roomId = :roomId, #sessionId = :sessionId, #connectionId = :connectionId, #generation = :generation, #joinedAt = if_not_exists(#joinedAt, :joinedAt), #gsi2pk = :gsi2pk, #gsi2sk = :gsi2sk, #gsi3pk = :gsi3pk, #gsi3sk = :gsi3sk"),
		ExpressionAttributeNames: map[string]string{
			"#roomId": "roomId", "#sessionId": "sessionId", "#connectionId": "connectionId",
			"#generation": "generation", "#joinedAt": "joinedAt", "#gsi2pk": attrGSI2PK,
			"#gsi2sk": attrGSI2SK, "#gsi3pk": attrGSI3PK, "#gsi3sk": attrGSI3SK,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":roomId": stringValue(member.RoomID), ":sessionId": stringValue(member.SessionID),
			":connectionId": stringValue(member.ConnectionID), ":generation": unsignedValue(member.Generation),
			":joinedAt": numberValue(member.JoinedAt.UnixNano()),
			":gsi2pk":   stringValue(partitionConnection + member.ConnectionID),
			":gsi2sk":   stringValue(partitionRoom + member.RoomID + "#" + partitionSession + member.SessionID),
			":gsi3pk":   stringValue(partitionSession + member.SessionID),
			":gsi3sk":   stringValue(partitionRoom + member.RoomID),
		},
	})
	return backendError(op, err)
}

func (s *Store) JoinIfMatch(ctx context.Context, member wsstore.RoomMember) error {
	const op = "JoinIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	if member.JoinedAt.IsZero() {
		member.JoinedAt = s.now()
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: tableName(s.tableName), Key: sessionKey(member.SessionID),
			ConditionExpression: awsString("#currentConnectionId = :connectionID AND #generation = :generation"),
			ExpressionAttributeNames: map[string]string{
				"#currentConnectionId": "currentConnectionId", "#generation": "generation",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":connectionID": stringValue(member.ConnectionID), ":generation": unsignedValue(member.Generation),
			},
		}},
		{Put: &types.Put{TableName: tableName(s.tableName), Item: encodeRoomMember(member)}},
	}})
	return backendError(op, err)
}

func (s *Store) Leave(ctx context.Context, roomID, sessionID string) error {
	const op = "Leave"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" {
		return invalid(op, "room ID and session ID are required")
	}
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: tableName(s.tableName), Key: roomKey(roomID, sessionID),
	})
	return backendError(op, err)
}

func (s *Store) LeaveIfMatch(ctx context.Context, roomID, sessionID, connectionID string, generation uint64) error {
	const op = "LeaveIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if roomID == "" || sessionID == "" || connectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{ConditionCheck: &types.ConditionCheck{
			TableName: tableName(s.tableName), Key: sessionKey(sessionID),
			ConditionExpression: awsString("#currentConnectionId = :connectionID AND #generation = :generation"),
			ExpressionAttributeNames: map[string]string{
				"#currentConnectionId": "currentConnectionId", "#generation": "generation",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":connectionID": stringValue(connectionID), ":generation": unsignedValue(generation),
			},
		}},
		{Delete: &types.Delete{TableName: tableName(s.tableName), Key: roomKey(roomID, sessionID)}},
	}})
	return backendError(op, err)
}

func (s *Store) Members(ctx context.Context, roomID string, limit int, cursor string) ([]wsstore.RoomMember, string, error) {
	const op = "Members"
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if err := s.check(op); err != nil {
		return nil, "", err
	}
	if roomID == "" || !validLimit(limit) {
		return nil, "", invalid(op, "room ID and a limit between 1 and MaxBatchSize are required")
	}
	members := make([]wsstore.RoomMember, 0, limit)
	var startKey map[string]types.AttributeValue
	for {
		pageLimit := int32(limit)
		input := &dynamodb.QueryInput{
			TableName:                 tableName(s.tableName),
			KeyConditionExpression:    awsString("#pk = :room"),
			ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":room": stringValue(partitionRoom + roomID)},
			ExclusiveStartKey:         startKey, Limit: &pageLimit,
		}
		if cursor != "" && startKey == nil {
			input.KeyConditionExpression = awsString("#pk = :room AND #sk > :cursor")
			input.ExpressionAttributeNames["#sk"] = attrSK
			input.ExpressionAttributeValues[":cursor"] = stringValue(partitionSession + cursor)
		}
		output, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, "", backendError(op, err)
		}
		for index, attributes := range output.Items {
			member, decodeErr := decodeRoomMember(attributes)
			if decodeErr != nil {
				return nil, "", wsstore.NewError(wsstore.ErrorCodeInvalid, op, decodeErr)
			}
			session, getErr := s.GetSession(ctx, member.SessionID)
			if getErr != nil {
				if wsstore.IsNotFound(getErr) {
					continue
				}
				return nil, "", getErr
			}
			if session.CurrentConnectionID != member.ConnectionID || session.Generation != member.Generation {
				continue
			}
			members = append(members, member)
			if len(members) == limit {
				next := ""
				if index < len(output.Items)-1 || len(output.LastEvaluatedKey) != 0 {
					next = member.SessionID
				}
				return members, next, nil
			}
		}
		if len(output.LastEvaluatedKey) == 0 {
			return members, "", nil
		}
		startKey = output.LastEvaluatedKey
	}
}

func (s *Store) RemoveConnection(ctx context.Context, connectionID string) error {
	const op = "RemoveConnection"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := s.check(op); err != nil {
		return err
	}
	if connectionID == "" {
		return invalid(op, "connection ID is required")
	}
	var startKey map[string]types.AttributeValue
	var firstErr error
	for {
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName: tableName(s.tableName), IndexName: awsString(s.connectionRoomIndex),
			KeyConditionExpression:    awsString("#gsi2pk = :connection"),
			ExpressionAttributeNames:  map[string]string{"#gsi2pk": attrGSI2PK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":connection": stringValue(partitionConnection + connectionID)},
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
			_, deleteErr := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: tableName(s.tableName), Key: roomKey(member.RoomID, member.SessionID),
				ConditionExpression:       awsString("#connectionId = :connectionID"),
				ExpressionAttributeNames:  map[string]string{"#connectionId": "connectionId"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":connectionID": stringValue(connectionID)},
			})
			if deleteErr != nil && !isConditionalFailure(deleteErr) && firstErr == nil {
				firstErr = backendError(op, deleteErr)
			}
		}
		if len(output.LastEvaluatedKey) == 0 {
			break
		}
		startKey = output.LastEvaluatedKey
	}
	return firstErr
}
