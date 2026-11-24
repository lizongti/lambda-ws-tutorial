# 第 15 节：DynamoDB 连接与会话

> 目标：完成本章代码 ZIP 中的“DynamoDB 连接与会话”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

Memory 的语义已经明确，本节用 DynamoDB 条件写、事务、GSI 和 TTL 字段实现共享连接与会话。

完成本节后，你将能够：

- 解释并应用 **PK/SK**：单表内区分 Connection、Session 与索引记录。
- 解释并应用 **条件表达式**：在 DynamoDB 内部拒绝 generation 或绑定不匹配。
- 解释并应用 **TTL**：只提供最终异步删除，读取和维护仍需逻辑过期判断。

本章与前一节的关系：保留第 14 节“房间与有界广播”的累计代码，再加入本章能力。

### 过渡实现与后续替换

本节先实现 DynamoDB 连接与会话，暂时移除完整 StateStore 编译断言；第 16 节补齐其余方法后恢复，避免用空方法伪装完整后端。

## 2. 关键概念与设计

### PK/SK

单表内区分 Connection、Session 与索引记录。

### 条件表达式

在 DynamoDB 内部拒绝 generation 或绑定不匹配。

### TTL

只提供最终异步删除，读取和维护仍需逻辑过期判断。

### 实现约束

1. codec 统一属性编解码，时间以可排序格式保存。
1.  stale/expiry 查询使用 GSI 与 LastEvaluatedKey 分页。
1. ConditionalCheckFailed、限流和网络错误映射为统一存储分类。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 DynamoDB 客户端契约

窄接口覆盖实际需要的 SDK 操作，进程内 fake 可以实现同一行为。

在 `ws/store/dynamodb/store.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/store.go
type Client interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 条件删除连接

删除与次级索引清理共享条件，旧 generation 不会误删新记录。

在 `ws/store/dynamodb/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/connection_session.go
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
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 事务绑定会话

事务同时更新 Session 与相关连接记录，防止共享部署出现半完成绑定。

在 `ws/store/dynamodb/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/dynamodb/connection_session.go
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
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_dynamodb_connection_test.go
func TestChapter15DynamoDBConnectionSession(t *testing.T) {
	ctx := context.Background()
	store, now := chapter15Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "ddb-s1", CurrentConnectionID: "ddb-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "ddb-c1", SessionID: "ddb-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "ddb-s1", "ddb-c2", 1)
	if err != nil || bound.Generation != 2 {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "ddb-s1", "ddb-c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind=%v", err)
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

在 dynamodbtest.Fake 上完成绑定和旧解绑冲突验证；没有访问真实 DynamoDB。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把 DynamoDB TTL 当实时删除。
- 忽略分页的 LastEvaluatedKey。
- 用先读后写替代条件表达式。

### 小结

本章完成了“DynamoDB 连接与会话”。在 dynamodbtest.Fake 上完成绑定和旧解绑冲突验证；没有访问真实 DynamoDB。

下一节：第 16 节“DynamoDB 房间、离线与幂等”。
