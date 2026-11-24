# 第 05 节：Memory 房间、离线与幂等

> 目标：完成本章代码 ZIP 中的“Memory 房间、离线与幂等”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

连接和会话已经可靠，本节补齐 Memory 的完整 StateStore：通用房间、离线队列和入站幂等。

完成本节后，你将能够：

- 解释并应用 **幂等成员**：roomId + sessionId 决定唯一成员，重复 Join 不产生重复记录。
- 解释并应用 **离线 sequence**：每个 Session 内单调递增，为重连补发提供确定顺序。
- 解释并应用 **幂等预留**：messageId 首次 Begin 获得处理权，后续调用看到已有记录。

本章与前一节的关系：保留第 04 节“Memory 连接与会话”的累计代码，再加入本章能力。

### 过渡实现与后续替换

恢复第 04 节暂时移除的 Memory StateStore 编译断言；房间、离线和幂等方法现已齐备，不再需要过渡声明。

## 2. 关键概念与设计

### 幂等成员

roomId + sessionId 决定唯一成员，重复 Join 不产生重复记录。

### 离线 sequence

每个 Session 内单调递增，为重连补发提供确定顺序。

### 幂等预留

messageId 首次 Begin 获得处理权，后续调用看到已有记录。

### 实现约束

1. 队列限制和 reject-new/evict-oldest 在锁内执行。
1. Ack 校验 Session、messageId 和 generation，旧世代不能删除新世代消息。
1. 删除 Session 时同步清理房间、离线和幂等索引。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 加入通用房间

成员只保存框架绑定，不包含任何游戏房间状态。

在 `ws/store/memory/room.go` 中加入或保留以下实现：

```go path=ws/store/memory/room.go
func (s *Store) Join(ctx context.Context, member wsstore.RoomMember) error {
	const op = "Join"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if member.RoomID == "" || member.SessionID == "" || member.ConnectionID == "" {
		return invalid(op, "room, session, and connection IDs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinLocked(member)
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 有界离线入队

分配 sequence、复制 Envelope 并应用队列上限，避免重试共享底层字节。

在 `ws/store/memory/offline.go` 中加入或保留以下实现：

```go path=ws/store/memory/offline.go
func (s *Store) Append(ctx context.Context, msg wsstore.OfflineMessage) error {
	const op = "Append"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if msg.SessionID == "" || msg.MessageID == "" {
		return invalid(op, "session ID and message ID are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	session, ok := s.sessions[msg.SessionID]
	if !ok || expired(session.ExpiresAt, now) {
		return notFound(op, "session not found")
	}
	if msg.Generation != session.Generation {
		return conflict(op, "offline message generation does not match session")
	}
	if s.offlineQueueLimit <= 0 {
		return invalid(op, "offline queue limit must be positive")
	}
	if s.offlineOverflowPolicy != wsstore.OfflineOverflowRejectNew &&
		s.offlineOverflowPolicy != wsstore.OfflineOverflowEvictOldest {
		return invalid(op, "unknown offline overflow policy")
	}
	if !msg.ExpiresAt.IsZero() && !msg.ExpiresAt.After(now) {
		return invalid(op, "offline message is already expired")
	}
	queue := s.offline[msg.SessionID]
	if queue == nil {
		queue = make(map[string]wsstore.OfflineMessage)
		s.offline[msg.SessionID] = queue
	}
	if _, exists := queue[msg.MessageID]; exists {
		return conflict(op, "offline message ID already exists")
	}

	// Expired entries no longer consume queue capacity, while their sequence
	// remains consumed to preserve monotonicity for the live session.
	for id, queued := range queue {
		if expired(queued.ExpiresAt, now) {
			delete(queue, id)
		}
	}
	if len(queue) >= s.offlineQueueLimit {
		switch s.offlineOverflowPolicy {
		case wsstore.OfflineOverflowRejectNew:
			return quotaExceeded(op, "offline queue is full")
		case wsstore.OfflineOverflowEvictOldest:
			oldestID := ""
			var oldestSequence uint64
			for id, queued := range queue {
				if oldestID == "" || queued.Sequence < oldestSequence {
					oldestID = id
					oldestSequence = queued.Sequence
				}
			}
			delete(queue, oldestID)
		}
	}

	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	s.sequences[msg.SessionID]++
	msg.Sequence = s.sequences[msg.SessionID]
	msg.State = wsstore.OfflineMessageStatePending
	msg.SentAt = nil
	queue[msg.MessageID] = cloneOfflineMessage(msg)
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 预留消息处理权

同一个临界区完成查找与创建，并发调用者不能同时获得 acquired=true。

在 `ws/store/memory/idempotency.go` 中加入或保留以下实现：

```go path=ws/store/memory/idempotency.go
func (s *Store) BeginIdempotency(ctx context.Context, record wsstore.IdempotencyRecord) (wsstore.IdempotencyRecord, bool, error) {
	const op = "BeginIdempotency"
	if err := checkContext(ctx); err != nil {
		return wsstore.IdempotencyRecord{}, false, err
	}
	if record.SessionID == "" || record.MessageID == "" {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "session ID and message ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	byMessage := s.idempotency[record.SessionID]
	if current, ok := byMessage[record.MessageID]; ok && !expired(current.ExpiresAt, now) {
		return cloneIdempotencyRecord(current), false, nil
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.ExpiresAt.IsZero() {
		if s.idempotencyWindow <= 0 {
			return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency window must be positive")
		}
		record.ExpiresAt = now.Add(s.idempotencyWindow)
	}
	if !record.ExpiresAt.After(now) {
		return wsstore.IdempotencyRecord{}, false, invalid(op, "idempotency record is already expired")
	}
	record.State = wsstore.IdempotencyStateProcessing
	record.Response = nil
	if byMessage == nil {
		byMessage = make(map[string]wsstore.IdempotencyRecord)
		s.idempotency[record.SessionID] = byMessage
	}
	byMessage[record.MessageID] = cloneIdempotencyRecord(record)
	return cloneIdempotencyRecord(record), true, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_memory_features_test.go
func TestChapter05RoomOfflineAndIdempotency(t *testing.T) {
	ctx := context.Background()
	store, now := chapter04Store()
	session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
	if err := store.CreateSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := store.Join(ctx, wsstore.RoomMember{RoomID: "lobby", SessionID: "s1", ConnectionID: "c1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	members, cursor, err := store.Members(ctx, "lobby", 10, "")
	if err != nil || len(members) != 1 || cursor != "" {
		t.Fatalf("members=%v cursor=%q err=%v", members, cursor, err)
	}
	if err := store.Append(ctx, wsstore.OfflineMessage{SessionID: "s1", MessageID: "m1", Generation: 1, Envelope: []byte(`{"ok":true}`), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(ctx, "s1", 10)
	if err != nil || len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if err := store.MarkSent(ctx, "s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, "s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	record, acquired, err := store.BeginIdempotency(ctx, wsstore.IdempotencyRecord{SessionID: "s1", MessageID: "incoming-1", ExpiresAt: now.Add(time.Minute)})
	if err != nil || !acquired || record.MessageID != "incoming-1" {
		t.Fatalf("begin=%#v acquired=%t err=%v", record, acquired, err)
	}
	if _, acquired, err = store.BeginIdempotency(ctx, record); err != nil || acquired {
		t.Fatalf("duplicate acquired=%t err=%v", acquired, err)
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

房间成员、离线 sequence、MarkSent/Ack 和重复 Begin 的行为符合契约，并通过 race。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 只限制消息条数却忘记过期索引。
- 把 postSuccess 当作客户端业务确认。
- 允许旧 generation 的 Ack 删除当前消息。

### 小结

本章完成了“Memory 房间、离线与幂等”。房间成员、离线 sequence、MarkSent/Ack 和重复 Begin 的行为符合契约，并通过 race。

下一节：第 06 节“Pusher 与 API Gateway 适配器”。
