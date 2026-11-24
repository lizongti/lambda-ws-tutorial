# 第 04 节：Memory 连接与会话

> 目标：完成本章代码 ZIP 中的“Memory 连接与会话”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

先用单进程 Memory 实现连接和会话条件语义，建立可观察、可做 race 验证的参照实现。

完成本节后，你将能够：

- 解释并应用 **RWMutex**：同一锁保护连接、会话和索引，保证跨模型操作的一致性。
- 解释并应用 **副本隔离**：返回结构或 RawMessage 的副本，避免调用方绕过锁修改内部状态。
- 解释并应用 **CAS**：BindConnection 在同一个临界区检查并递增 generation。

本章与前一节的关系：保留第 03 节“StateStore 契约与测试基座”的累计代码，再加入本章能力。

## 2. 关键概念与设计

### RWMutex

同一锁保护连接、会话和索引，保证跨模型操作的一致性。

### 副本隔离

返回结构或 RawMessage 的副本，避免调用方绕过锁修改内部状态。

### CAS

BindConnection 在同一个临界区检查并递增 generation。

### 实现约束

1. Memory 只用于开发和测试，不能用于多实例共享部署。
1. stale 查询使用稳定排序和明确批次上限。
1. 旧连接解绑必须失败或成为 no-op，不能清除新连接。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 并发安全 Store

所有 map 与序列状态归属于同一 Store，后续房间和离线数据也复用这把锁。

在 `ws/store/memory/store.go` 中加入或保留以下实现：

```go path=ws/store/memory/store.go
type Store struct {
	mu sync.RWMutex

	connections map[string]wsstore.Connection
	sessions    map[string]wsstore.Session
	rooms       map[string]map[string]wsstore.RoomMember
	offline     map[string]map[string]wsstore.OfflineMessage
	sequences   map[string]uint64
	idempotency map[string]map[string]wsstore.IdempotencyRecord

	now                   func() time.Time
	offlineQueueLimit     int
	offlineOverflowPolicy wsstore.OfflineOverflowPolicy
	idempotencyWindow     time.Duration
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 条件删除连接

记录不存在时幂等成功，generation 不同则返回条件冲突。

在 `ws/store/memory/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/memory/connection_session.go
func (s *Store) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	const op = "DeleteConnectionIfMatch"
	if err := checkContext(ctx); err != nil {
		return err
	}
	if id == "" {
		return invalid(op, "connection ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.connections[id]
	if !ok {
		return nil
	}
	if c.Generation != generation {
		return conflict(op, "connection generation does not match")
	}
	delete(s.connections, id)
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 原子重绑会话

比较当前连接和 generation 后一次性更新绑定，旧事件无法覆盖新值。

在 `ws/store/memory/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/memory/connection_session.go
func (s *Store) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	const op = "BindConnection"
	if err := checkContext(ctx); err != nil {
		return wsstore.Session{}, err
	}
	if sessionID == "" || connectionID == "" {
		return wsstore.Session{}, invalid(op, "session ID and connection ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[sessionID]
	now := s.now()
	if !ok || expired(session.ExpiresAt, now) {
		return wsstore.Session{}, notFound(op, "session not found")
	}
	if session.Generation != expectedGeneration {
		return wsstore.Session{}, conflict(op, "session generation does not match")
	}
	retention := session.ExpiresAt.Sub(session.LastActiveAt)
	session.CurrentConnectionID = connectionID
	session.Generation++
	session.LastActiveAt = now
	if !session.ExpiresAt.IsZero() && retention > 0 {
		session.ExpiresAt = now.Add(retention)
	}
	s.sessions[sessionID] = session
	return session, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_memory_connection_test.go
func TestChapter04ConnectionSessionAndRace(t *testing.T) {
	ctx := context.Background()
	store, now := chapter04Store()
	session := wsstore.Session{SessionID: "s1", PrincipalSubject: "alice", CurrentConnectionID: "c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}
	if err := store.CreateSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	connection := wsstore.Connection{ConnectionID: "c1", SessionID: "s1", PrincipalSubject: "alice", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}
	if err := store.PutConnection(ctx, connection); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "s1", "c2", 1)
	if err != nil || bound.Generation != 2 || bound.CurrentConnectionID != "c2" {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "s1", "c1", 1); !wsstore.IsConditionConflict(err) {
		t.Fatalf("stale unbind err=%v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = store.TouchConnection(ctx, "c1", 1, now.Add(time.Second)) }()
	}
	wg.Wait()
	got, err := store.GetConnection(ctx, "c1")
	if err != nil || !got.LastSeenAt.Equal(now.Add(time.Second)) {
		t.Fatalf("touch=%#v err=%v", got, err)
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

重绑后 generation 为 2，旧解绑返回冲突，8 个并发 Touch 在 race 下保持正确状态。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 给每个 map 使用互不相关的锁，造成跨模型中间状态。
- 返回内部 map 或字节切片的别名。
- 忽略 go test -race，误以为加锁声明就足够。

### 小结

本章完成了“Memory 连接与会话”。重绑后 generation 为 2，旧解绑返回冲突，8 个并发 Touch 在 race 下保持正确状态。

下一节：第 05 节“Memory 房间、离线与幂等”。
