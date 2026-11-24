# 第 18 节：Redis 房间、离线与幂等

> 目标：完成本章代码 ZIP 中的“Redis 房间、离线与幂等”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

本节完成 Redis 房间、离线和幂等实现，重点保证脚本原子性与各类索引的过期清理。

完成本节后，你将能够：

- 解释并应用 **房间反向索引**：连接失效时可找到并删除所有相关成员。
- 解释并应用 **Lua 队列**：sequence 分配、条数上限与溢出在同一原子路径完成。
- 解释并应用 **幂等窗口**：set-if-not-exists 加过期时间预留 messageId。

本章与前一节的关系：保留第 17 节“Redis 连接与会话”的累计代码，再加入本章能力。

### 过渡实现与后续替换

恢复 Redis 的完整 StateStore 编译断言，并用真实房间、离线和幂等操作完成共享后端。

## 2. 关键概念与设计

### 房间反向索引

连接失效时可找到并删除所有相关成员。

### Lua 队列

sequence 分配、条数上限与溢出在同一原子路径完成。

### 幂等窗口

set-if-not-exists 加过期时间预留 messageId。

### 实现约束

1. 离线和成员读取使用稳定顺序与有界批次。
1. Ack/MarkSent 都按 generation 做条件迁移。
1. 主体 key 过期时同步维护索引，避免无限残留。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 原子清理房间成员

Lua 同时处理成员集合与 Connection_ID 反向索引。

在 `ws/store/redis/room.go` 中加入或保留以下实现：

```go path=ws/store/redis/room.go
func (s *Store) RemoveConnection(ctx context.Context, connectionID string) error {
	const op = "RemoveConnection"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if connectionID == "" {
		return invalid(op, "connection ID is required")
	}
	if err := removeConnectionScript.Run(ctx, s.client,
		[]string{s.connectionRoomsKey(connectionID)}, s.prefix, connectionID).Err(); err != nil {
		return backendError(op, err)
	}
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 有界离线 Append

脚本协调 sequence、消息 body、队列计数与 reject/evict 行为。

在 `ws/store/redis/offline.go` 中加入或保留以下实现：

```go path=ws/store/redis/offline.go
func (s *Store) Append(ctx context.Context, msg wsstore.OfflineMessage) error {
	const op = "Append"
	if err := checkContext(ctx, op); err != nil {
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
	if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(now) {
		return invalid(op, "offline message is already expired")
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	msg.State = wsstore.OfflineMessageStatePending
	msg.SentAt = nil
	encoded, err := encodeOfflineMessage(msg)
	if err != nil {
		return dataError(op, err)
	}
	result, err := appendOfflineScript.Run(ctx, s.client, []string{
		s.sessionKey(msg.SessionID), s.offlineSequenceKey(msg.SessionID), s.offlineQueueKey(msg.SessionID),
		s.offlineExpiryKey(), s.offlineMessageKey(msg.SessionID, msg.MessageID),
	}, msg.SessionID, msg.MessageID, now.UnixNano(), msg.Generation, encoded,
		ttlMilliseconds(msg.ExpiresAt, now), s.offlineQueueLimit, string(s.offlineOverflowPolicy),
		expirationNano(msg.ExpiresAt), s.prefix,
	).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	switch code {
	case 1:
		return notFound(op, "session not found")
	case 2:
		return conflict(op, "offline message generation or ID conflicts with current state")
	case 3:
		return quotaExceeded(op, "offline queue is full")
	default:
		return nil
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 世代条件 Ack

当前 generation 不匹配时不能删除消息，重复确认保持幂等。

在 `ws/store/redis/offline.go` 中加入或保留以下实现：

```go path=ws/store/redis/offline.go
func (s *Store) Ack(ctx context.Context, sessionID, messageID string, generation uint64) error {
	const op = "Ack"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if sessionID == "" || messageID == "" {
		return invalid(op, "session ID and message ID are required")
	}
	ref, err := encodeRef(sessionID, messageID)
	if err != nil {
		return dataError(op, err)
	}
	result, err := ackOfflineScript.Run(ctx, s.client, []string{
		s.sessionKey(sessionID), s.offlineMessageKey(sessionID, messageID),
		s.offlineQueueKey(sessionID), s.offlineExpiryKey(),
	}, messageID, s.now().UnixNano(), generation, ref).Result()
	if err != nil {
		return backendError(op, err)
	}
	return offlineMutationError(op, result, err)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_redis_features_test.go
func TestChapter18RedisRoomOfflineIdempotency(t *testing.T) {
	ctx := context.Background()
	store, now := chapter17Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "redis-feature", CurrentConnectionID: "redis-feature-c", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Join(ctx, wsstore.RoomMember{RoomID: "r", SessionID: "redis-feature", ConnectionID: "redis-feature-c", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "redis-feature", MessageID: "m", Generation: 1, Envelope: []byte(`{"ok":true}`), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, "redis-feature", 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	_, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "redis-feature", MessageID: "i", ExpiresAt: now.Add(time.Minute)})
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

miniredis 上房间、队列与幂等契约通过；真实 Redis 未纳入验证声明。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把多条独立 Redis 命令误当成一个原子操作。
- 只清理消息 body 而忘记索引。
- 无界读取大房间或长离线队列。

### 小结

本章完成了“Redis 房间、离线与幂等”。miniredis 上房间、队列与幂等契约通过；真实 Redis 未纳入验证声明。

下一节：第 19 节“server、Factory 与可观测性”。
