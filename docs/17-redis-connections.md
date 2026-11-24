# 第 17 节：Redis 连接与会话

> 目标：完成本章代码 ZIP 中的“Redis 连接与会话”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

Redis 共享实现使用 Lua 把条件检查和写入放在服务端一次原子执行，避免多实例读改写竞争。

完成本节后，你将能够：

- 解释并应用 **key prefix**：所有连接、会话和索引共享可配置命名空间。
- 解释并应用 **Lua CAS**：比较 JSON 中的 generation/绑定后原子更新。
- 解释并应用 **sorted-set stale 索引**：last-seen 作为分数，按截止时间有界查询。

本章与前一节的关系：保留第 16 节“DynamoDB 房间、离线与幂等”的累计代码，再加入本章能力。

### 过渡实现与后续替换

本节先实现 Redis 连接与会话，完整 StateStore 断言延到第 18 节；Lua 条件更新从第一节共享后端实现就保持真实语义。

## 2. 关键概念与设计

### key prefix

所有连接、会话和索引共享可配置命名空间。

### Lua CAS

比较 JSON 中的 generation/绑定后原子更新。

### sorted-set stale 索引

last-seen 作为分数，按截止时间有界查询。

### 实现约束

1. 逻辑对象使用 JSON 编解码并设置 TTL。
1. redis.Nil 映射 not_found，连接与超时映射 unavailable。
1. 注入 clock 使逻辑过期与测试确定，miniredis 仅作为行为模拟。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 Redis Store 与配置

client 由调用方持有，Store 保存前缀、时钟和队列策略。

在 `ws/store/redis/store.go` 中加入或保留以下实现：

```go path=ws/store/redis/store.go
type Store struct {
	client Client
	prefix string
	now    func() time.Time

	offlineQueueLimit     int
	offlineOverflowPolicy wsstore.OfflineOverflowPolicy
	idempotencyWindow     time.Duration
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 Lua 条件删除

读取、比较、删除及索引清理由脚本一次执行。

在 `ws/store/redis/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/redis/connection_session.go
func (s *Store) DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error {
	const op = "DeleteConnectionIfMatch"
	if err := checkContext(ctx, op); err != nil {
		return err
	}
	if id == "" {
		return invalid(op, "connection ID is required")
	}
	result, err := deleteConnectionScript.Run(ctx, s.client,
		[]string{s.connectionKey(id), s.staleConnectionsKey(), s.connectionExpiryKey()}, id, generation, s.now().UnixNano()).Result()
	if err != nil {
		return backendError(op, err)
	}
	code, err := scriptInt(result)
	if err != nil {
		return dataError(op, err)
	}
	if code == 2 {
		return conflict(op, "connection generation does not match")
	}
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 Lua 原子重绑

在脚本内校验 expectedGeneration、递增代数并更新当前连接。

在 `ws/store/redis/connection_session.go` 中加入或保留以下实现：

```go path=ws/store/redis/connection_session.go
func (s *Store) BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (wsstore.Session, error) {
	const op = "BindConnection"
	if err := checkContext(ctx, op); err != nil {
		return wsstore.Session{}, err
	}
	if sessionID == "" || connectionID == "" {
		return wsstore.Session{}, invalid(op, "session ID and connection ID are required")
	}
	now := s.now()
	current, err := s.GetSession(ctx, sessionID)
	if err != nil {
		return wsstore.Session{}, err
	}
	if current.Generation != expectedGeneration {
		return wsstore.Session{}, conflict(op, "session generation does not match")
	}
	expiresAt := current.ExpiresAt
	if retention := current.ExpiresAt.Sub(current.LastActiveAt); !current.ExpiresAt.IsZero() && retention > 0 {
		expiresAt = now.Add(retention)
	}
	result, err := bindConnectionScript.Run(ctx, s.client,
		[]string{s.sessionKey(sessionID), s.sessionExpiryKey()},
		connectionID, now.UnixNano(), expectedGeneration, timeText(now), timeText(expiresAt),
		expirationNano(expiresAt), ttlMilliseconds(expiresAt, now),
	).Result()
	if err != nil {
		return wsstore.Session{}, backendError(op, err)
	}
	if code, ok := result.(int64); ok {
		switch code {
		case 1:
			return wsstore.Session{}, notFound(op, "session not found")
		case 2:
			return wsstore.Session{}, conflict(op, "session generation does not match")
		}
	}
	encoded, ok := result.(string)
	if !ok {
		return wsstore.Session{}, dataError(op, fmt.Errorf("unexpected bind result %T", result))
	}
	session, err := decodeSession(encoded)
	if err != nil {
		return wsstore.Session{}, dataError(op, err)
	}
	return session, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_redis_connection_test.go
func TestChapter17RedisConnectionSession(t *testing.T) {
	ctx := context.Background()
	store, now := chapter17Store(t)
	if err := store.CreateSession(ctx, wsstore.Session{SessionID: "redis-s1", CurrentConnectionID: "redis-c1", Generation: 1, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConnection(ctx, wsstore.Connection{ConnectionID: "redis-c1", SessionID: "redis-s1", Generation: 1, LastSeenAt: now, State: wsstore.ConnectionStateActive}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.BindConnection(ctx, "redis-s1", "redis-c2", 1)
	if err != nil || bound.Generation != 2 {
		t.Fatalf("bind=%#v err=%v", bound, err)
	}
	if err := store.UnbindConnectionIfMatch(ctx, "redis-s1", "redis-c1", 1); !wsstore.IsConditionConflict(err) {
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

miniredis 上绑定与条件冲突符合契约，Lua 脚本原子路径被执行。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 在 Go 侧 GET 后再无条件 SET。
- 消息主体过期却留下 stale/房间索引。
- 把 miniredis 结果描述为真实 Redis 网络验证。

### 小结

本章完成了“Redis 连接与会话”。miniredis 上绑定与条件冲突符合契约，Lua 脚本原子路径被执行。

下一节：第 18 节“Redis 房间、离线与幂等”。
