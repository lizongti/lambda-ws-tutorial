package dynamodb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	wsstore "github.com/aura-studio/lambda/ws/store"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	attrGSI1PK = "GSI1PK"
	attrGSI1SK = "GSI1SK"
	attrGSI2PK = "GSI2PK"
	attrGSI2SK = "GSI2SK"
	attrGSI3PK = "GSI3PK"
	attrGSI3SK = "GSI3SK"
	attrGSI4PK = "GSI4PK"
	attrGSI4SK = "GSI4SK"
	attrTTL    = "ttl"
)

type item map[string]types.AttributeValue

func stringValue(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

func numberValue(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func unsignedValue(value uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(value, 10)}
}

func binaryValue(value []byte) types.AttributeValue {
	return &types.AttributeValueMemberB{Value: cloneBytes(value)}
}

func key(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: stringValue(pk), attrSK: stringValue(sk)}
}

func connectionKey(id string) map[string]types.AttributeValue {
	return key(partitionConnection+id, sortMetadata)
}

func connectionExpiryKey(id string) map[string]types.AttributeValue {
	return key(partitionConnectionExpiry+id, sortMetadata)
}

func sessionKey(id string) map[string]types.AttributeValue {
	return key(partitionSession+id, sortMetadata)
}

func offlineKey(sessionID, messageID string) map[string]types.AttributeValue {
	return key(partitionSession+sessionID, sortOfflineMessage+messageID)
}

func offlineMetadataKey(sessionID string) map[string]types.AttributeValue {
	return key(partitionSession+sessionID, sortOfflineMetadata)
}

func idempotencyKey(sessionID, messageID string) map[string]types.AttributeValue {
	return key(partitionSession+sessionID, sortIdempotency+messageID)
}

func roomKey(roomID, sessionID string) map[string]types.AttributeValue {
	return key(partitionRoom+roomID, partitionSession+sessionID)
}

func setString(attributes item, name, value string) {
	attributes[name] = stringValue(value)
}

func setTime(attributes item, name string, value time.Time) {
	if !value.IsZero() {
		attributes[name] = numberValue(value.UnixNano())
	}
}

func setTTL(attributes item, expiresAt time.Time) {
	if !expiresAt.IsZero() {
		attributes[attrTTL] = numberValue(expiresAt.Unix())
	}
}

func timeKey(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func setTimeIndex(attributes item, partition string, value time.Time, id string) {
	if !value.IsZero() {
		attributes[attrGSI1PK] = stringValue(partition)
		attributes[attrGSI1SK] = stringValue(timeKey(value) + "#" + id)
	}
}

func stringAttribute(attributes map[string]types.AttributeValue, name string) (string, error) {
	attribute, ok := attributes[name]
	if !ok {
		return "", nil
	}
	value, ok := attribute.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("attribute %s is not a string", name)
	}
	return value.Value, nil
}

func requiredStringAttribute(attributes map[string]types.AttributeValue, name string) (string, error) {
	value, err := stringAttribute(attributes, name)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("attribute %s is required", name)
	}
	return value, nil
}

func uint64Attribute(attributes map[string]types.AttributeValue, name string) (uint64, error) {
	attribute, ok := attributes[name]
	if !ok {
		return 0, nil
	}
	value, ok := attribute.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attribute %s is not a number", name)
	}
	parsed, err := strconv.ParseUint(value.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("attribute %s: %w", name, err)
	}
	return parsed, nil
}

func int64Attribute(attributes map[string]types.AttributeValue, name string) (int64, error) {
	attribute, ok := attributes[name]
	if !ok {
		return 0, nil
	}
	value, ok := attribute.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attribute %s is not a number", name)
	}
	parsed, err := strconv.ParseInt(value.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("attribute %s: %w", name, err)
	}
	return parsed, nil
}

func timeAttribute(attributes map[string]types.AttributeValue, name string) (time.Time, error) {
	value, err := int64Attribute(attributes, name)
	if err != nil || value == 0 {
		return time.Time{}, err
	}
	return time.Unix(0, value).UTC(), nil
}

func binaryAttribute(attributes map[string]types.AttributeValue, name string) ([]byte, error) {
	attribute, ok := attributes[name]
	if !ok {
		return nil, nil
	}
	value, ok := attribute.(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("attribute %s is not binary", name)
	}
	return cloneBytes(value.Value), nil
}

func encodeConnection(c wsstore.Connection) item {
	attributes := item{
		attrPK: stringValue(partitionConnection + c.ConnectionID),
		attrSK: stringValue(sortMetadata),
	}
	setString(attributes, "connectionId", c.ConnectionID)
	setString(attributes, "sessionId", c.SessionID)
	setString(attributes, "principalSubject", c.PrincipalSubject)
	attributes["generation"] = unsignedValue(c.Generation)
	setTime(attributes, "connectedAt", c.ConnectedAt)
	setTime(attributes, "lastSeenAt", c.LastSeenAt)
	setTime(attributes, "expiresAt", c.ExpiresAt)
	setString(attributes, "state", string(c.State))
	setTTL(attributes, c.ExpiresAt)
	setTimeIndex(attributes, timePartitionStale, c.LastSeenAt, c.ConnectionID)
	return attributes
}

func encodeConnectionExpiry(c wsstore.Connection) item {
	attributes := item{
		attrPK: stringValue(partitionConnectionExpiry + c.ConnectionID),
		attrSK: stringValue(sortMetadata),
	}
	setString(attributes, "connectionId", c.ConnectionID)
	attributes["generation"] = unsignedValue(c.Generation)
	setTime(attributes, "expiresAt", c.ExpiresAt)
	setTTL(attributes, c.ExpiresAt)
	setTimeIndex(attributes, timePartitionConnectionExpiry, c.ExpiresAt, c.ConnectionID)
	return attributes
}

func decodeConnectionExpiry(attributes map[string]types.AttributeValue) (connectionID string, generation uint64, expiresAt time.Time, err error) {
	connectionID, err = requiredStringAttribute(attributes, "connectionId")
	if err != nil {
		return "", 0, time.Time{}, err
	}
	generation, err = uint64Attribute(attributes, "generation")
	if err != nil {
		return "", 0, time.Time{}, err
	}
	expiresAt, err = timeAttribute(attributes, "expiresAt")
	if err != nil {
		return "", 0, time.Time{}, err
	}
	if connectionID == "" || expiresAt.IsZero() {
		return "", 0, time.Time{}, fmt.Errorf("connection expiry index is incomplete")
	}
	return connectionID, generation, expiresAt, nil
}

func decodeConnection(attributes map[string]types.AttributeValue) (wsstore.Connection, error) {
	var c wsstore.Connection
	var err error
	if c.ConnectionID, err = requiredStringAttribute(attributes, "connectionId"); err != nil {
		return c, err
	}
	if c.SessionID, err = stringAttribute(attributes, "sessionId"); err != nil {
		return c, err
	}
	if c.PrincipalSubject, err = stringAttribute(attributes, "principalSubject"); err != nil {
		return c, err
	}
	if c.Generation, err = uint64Attribute(attributes, "generation"); err != nil {
		return c, err
	}
	if c.ConnectedAt, err = timeAttribute(attributes, "connectedAt"); err != nil {
		return c, err
	}
	if c.LastSeenAt, err = timeAttribute(attributes, "lastSeenAt"); err != nil {
		return c, err
	}
	if c.ExpiresAt, err = timeAttribute(attributes, "expiresAt"); err != nil {
		return c, err
	}
	state, err := stringAttribute(attributes, "state")
	if err != nil {
		return c, err
	}
	c.State = wsstore.ConnectionState(state)
	return c, nil
}

func encodeSession(s wsstore.Session) item {
	attributes := item{
		attrPK: stringValue(partitionSession + s.SessionID),
		attrSK: stringValue(sortMetadata),
	}
	setString(attributes, "sessionId", s.SessionID)
	setString(attributes, "principalSubject", s.PrincipalSubject)
	setString(attributes, "currentConnectionId", s.CurrentConnectionID)
	attributes["generation"] = unsignedValue(s.Generation)
	setString(attributes, "resumeCredentialHash", s.ResumeCredentialHash)
	setTime(attributes, "lastActiveAt", s.LastActiveAt)
	setTime(attributes, "expiresAt", s.ExpiresAt)
	setTTL(attributes, s.ExpiresAt)
	setTimeIndex(attributes, timePartitionSession, s.ExpiresAt, s.SessionID)
	return attributes
}

func decodeSession(attributes map[string]types.AttributeValue) (wsstore.Session, error) {
	var s wsstore.Session
	var err error
	if s.SessionID, err = requiredStringAttribute(attributes, "sessionId"); err != nil {
		return s, err
	}
	if s.PrincipalSubject, err = stringAttribute(attributes, "principalSubject"); err != nil {
		return s, err
	}
	if s.CurrentConnectionID, err = stringAttribute(attributes, "currentConnectionId"); err != nil {
		return s, err
	}
	if s.Generation, err = uint64Attribute(attributes, "generation"); err != nil {
		return s, err
	}
	if s.ResumeCredentialHash, err = stringAttribute(attributes, "resumeCredentialHash"); err != nil {
		return s, err
	}
	if s.LastActiveAt, err = timeAttribute(attributes, "lastActiveAt"); err != nil {
		return s, err
	}
	if s.ExpiresAt, err = timeAttribute(attributes, "expiresAt"); err != nil {
		return s, err
	}
	return s, nil
}

func encodeRoomMember(member wsstore.RoomMember) item {
	attributes := item{
		attrPK:     stringValue(partitionRoom + member.RoomID),
		attrSK:     stringValue(partitionSession + member.SessionID),
		attrGSI2PK: stringValue(partitionConnection + member.ConnectionID),
		attrGSI2SK: stringValue(partitionRoom + member.RoomID + "#" + partitionSession + member.SessionID),
		attrGSI3PK: stringValue(partitionSession + member.SessionID),
		attrGSI3SK: stringValue(partitionRoom + member.RoomID),
	}
	setString(attributes, "roomId", member.RoomID)
	setString(attributes, "sessionId", member.SessionID)
	setString(attributes, "connectionId", member.ConnectionID)
	attributes["generation"] = unsignedValue(member.Generation)
	setTime(attributes, "joinedAt", member.JoinedAt)
	return attributes
}

func decodeRoomMember(attributes map[string]types.AttributeValue) (wsstore.RoomMember, error) {
	var member wsstore.RoomMember
	var err error
	if member.RoomID, err = requiredStringAttribute(attributes, "roomId"); err != nil {
		return member, err
	}
	if member.SessionID, err = requiredStringAttribute(attributes, "sessionId"); err != nil {
		return member, err
	}
	if member.ConnectionID, err = requiredStringAttribute(attributes, "connectionId"); err != nil {
		return member, err
	}
	if member.Generation, err = uint64Attribute(attributes, "generation"); err != nil {
		return member, err
	}
	if member.JoinedAt, err = timeAttribute(attributes, "joinedAt"); err != nil {
		return member, err
	}
	return member, nil
}

func encodeOfflineMessage(msg wsstore.OfflineMessage) item {
	attributes := item{
		attrPK:     stringValue(partitionSession + msg.SessionID),
		attrSK:     stringValue(sortOfflineMessage + msg.MessageID),
		attrGSI4PK: stringValue(partitionSession + msg.SessionID),
		attrGSI4SK: stringValue(fmt.Sprintf("%020d#%s", msg.Sequence, msg.MessageID)),
	}
	setString(attributes, "sessionId", msg.SessionID)
	setString(attributes, "messageId", msg.MessageID)
	attributes["sequence"] = unsignedValue(msg.Sequence)
	attributes["generation"] = unsignedValue(msg.Generation)
	if msg.Envelope != nil {
		attributes["envelope"] = binaryValue(msg.Envelope)
	}
	setTime(attributes, "createdAt", msg.CreatedAt)
	setTime(attributes, "expiresAt", msg.ExpiresAt)
	setString(attributes, "state", string(msg.State))
	if msg.SentAt != nil {
		setTime(attributes, "sentAt", *msg.SentAt)
	}
	setTTL(attributes, msg.ExpiresAt)
	setTimeIndex(attributes, timePartitionOffline, msg.ExpiresAt, msg.SessionID+"#"+msg.MessageID)
	return attributes
}

func decodeOfflineMessage(attributes map[string]types.AttributeValue) (wsstore.OfflineMessage, error) {
	var msg wsstore.OfflineMessage
	var err error
	if msg.SessionID, err = requiredStringAttribute(attributes, "sessionId"); err != nil {
		return msg, err
	}
	if msg.MessageID, err = requiredStringAttribute(attributes, "messageId"); err != nil {
		return msg, err
	}
	if msg.Sequence, err = uint64Attribute(attributes, "sequence"); err != nil {
		return msg, err
	}
	if msg.Generation, err = uint64Attribute(attributes, "generation"); err != nil {
		return msg, err
	}
	if msg.Envelope, err = binaryAttribute(attributes, "envelope"); err != nil {
		return msg, err
	}
	msg.Envelope = json.RawMessage(msg.Envelope)
	if msg.CreatedAt, err = timeAttribute(attributes, "createdAt"); err != nil {
		return msg, err
	}
	if msg.ExpiresAt, err = timeAttribute(attributes, "expiresAt"); err != nil {
		return msg, err
	}
	state, err := stringAttribute(attributes, "state")
	if err != nil {
		return msg, err
	}
	msg.State = wsstore.OfflineMessageState(state)
	sentAt, err := timeAttribute(attributes, "sentAt")
	if err != nil {
		return msg, err
	}
	if !sentAt.IsZero() {
		msg.SentAt = &sentAt
	}
	return msg, nil
}

func encodeIdempotencyRecord(record wsstore.IdempotencyRecord) item {
	attributes := item{
		attrPK: stringValue(partitionSession + record.SessionID),
		attrSK: stringValue(sortIdempotency + record.MessageID),
	}
	setString(attributes, "sessionId", record.SessionID)
	setString(attributes, "messageId", record.MessageID)
	setString(attributes, "state", string(record.State))
	if record.Response != nil {
		attributes["response"] = binaryValue(record.Response)
	}
	setTime(attributes, "createdAt", record.CreatedAt)
	setTime(attributes, "expiresAt", record.ExpiresAt)
	setTTL(attributes, record.ExpiresAt)
	setTimeIndex(attributes, timePartitionIdempotency, record.ExpiresAt, record.SessionID+"#"+record.MessageID)
	return attributes
}

func decodeIdempotencyRecord(attributes map[string]types.AttributeValue) (wsstore.IdempotencyRecord, error) {
	var record wsstore.IdempotencyRecord
	var err error
	if record.SessionID, err = requiredStringAttribute(attributes, "sessionId"); err != nil {
		return record, err
	}
	if record.MessageID, err = requiredStringAttribute(attributes, "messageId"); err != nil {
		return record, err
	}
	state, err := stringAttribute(attributes, "state")
	if err != nil {
		return record, err
	}
	record.State = wsstore.IdempotencyState(state)
	if record.Response, err = binaryAttribute(attributes, "response"); err != nil {
		return record, err
	}
	record.Response = json.RawMessage(record.Response)
	if record.CreatedAt, err = timeAttribute(attributes, "createdAt"); err != nil {
		return record, err
	}
	if record.ExpiresAt, err = timeAttribute(attributes, "expiresAt"); err != nil {
		return record, err
	}
	return record, nil
}

func cloneOfflineMessage(msg wsstore.OfflineMessage) wsstore.OfflineMessage {
	msg.Envelope = json.RawMessage(cloneBytes(msg.Envelope))
	if msg.SentAt != nil {
		sentAt := *msg.SentAt
		msg.SentAt = &sentAt
	}
	return msg
}

func cloneIdempotencyRecord(record wsstore.IdempotencyRecord) wsstore.IdempotencyRecord {
	record.Response = json.RawMessage(cloneBytes(record.Response))
	return record
}
