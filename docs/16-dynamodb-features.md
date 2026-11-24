# 第 16 节：DynamoDB 房间、离线与幂等

> 目标：完成本章代码 ZIP 中的“DynamoDB 房间、离线与幂等”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

本节把 DynamoDB 连接/会话扩展到完整 StateStore，并处理房间反向索引、离线队列计数和幂等记录的部分失败。

完成本节后，你将能够：

- 解释并应用 **反向索引**：按 Connection_ID 找到房间成员，支持断连清理。
- 解释并应用 **事务队列**：消息写入、计数与溢出策略需要一致提交。
- 解释并应用 **故障分类**：条件冲突、暂时不可用与永久错误采用不同处理。

本章与前一节的关系：保留第 15 节“DynamoDB 连接与会话”的累计代码，再加入本章能力。

### 过渡实现与后续替换

恢复 DynamoDB 的完整 StateStore 编译断言，房间、离线和幂等操作不再有过渡占位。

## 2. 关键概念与设计

### 反向索引

按 Connection_ID 找到房间成员，支持断连清理。

### 事务队列

消息写入、计数与溢出策略需要一致提交。

### 故障分类

条件冲突、暂时不可用与永久错误采用不同处理。

### 实现约束

1. Members 游标稳定且过滤当前 generation。
1. 离线 Append/ListPending/MarkSent/Ack 保留 sequence 与世代语义。
1. 清理部分失败返回错误并允许 Maintenance 重试，不虚报成功。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 清理连接房间索引

通过 Connection_ID 反向索引定位成员，逐批删除且不影响其他连接。

在 `ws/store/dynamodb/room.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/room.go
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
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 原子离线入队

队列元数据与消息记录使用条件/事务协调 sequence 和溢出策略。

在 `ws/store/dynamodb/offline.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/offline.go
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
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 条件确认消息

只有 Session、messageId 和 generation 匹配才删除，重复 Ack 保持幂等。

在 `ws/store/dynamodb/offline.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/offline.go
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
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_dynamodb_features_test.go
func TestChapter16DynamoDBRoomOfflineIdempotency(t *testing.T) {
	ctx := context.Background()
	store, now := chapter15Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-feature", CurrentConnectionID: "ddb-feature-c", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Join(ctx, wsstore.RoomMember{RoomID: "r", SessionID: "ddb-feature", ConnectionID: "ddb-feature-c", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "ddb-feature", MessageID: "m", Generation: 1, Envelope: []byte(`{"ok":true}`), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, "ddb-feature", 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	_, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "ddb-feature", MessageID: "i", ExpiresAt: now.Add(time.Minute)})
	if err != nil || !acquired {
		t.Fatalf("acquired=%t err=%v", acquired, err)
	}
	members, _, err := store.Members(ctx, "r", 10, "")
	if err != nil || len(members) != 1 {
		t.Fatalf("members=%#v err=%v", members, err)
	}
}
```

## 4. 运行与验证

将本章 ZIP 解压到空目录，进入其根目录后执行：

```sh
go version
go mod download
go test -mod=readonly -count=1 ./...
```

再执行 race 验证：

```sh
go test -mod=readonly -race -count=1 ./...
```

### 预期结果

fake 上房间、离线和幂等操作与 Memory 契约一致，故障路径可重复验证。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 删除成员后遗留 Connection_ID 反向索引。
- 把条件冲突当暂时错误无限重试。
- 部分清理失败后直接返回全部成功。

### 小结

本章完成了“DynamoDB 房间、离线与幂等”。fake 上房间、离线和幂等操作与 Memory 契约一致，故障路径可重复验证。

下一节：第 17 节“Redis 连接与会话”。
