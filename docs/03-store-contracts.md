# 第 03 节：StateStore 契约与测试基座

> 目标：完成本章代码 ZIP 中的“StateStore 契约与测试基座”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

连接、会话、房间和离线数据会跨 Lambda 实例共享。先写清存储契约，才能用同一组语义实现 Memory、DynamoDB 和 Redis。

完成本节后，你将能够：

- 解释并应用 **Connection_ID**：一次 API Gateway 物理连接。
- 解释并应用 **Session_ID**：跨重连保留的逻辑会话。
- 解释并应用 **generation**：绑定代数，用于拒绝旧连接迟到的事件。
- 解释并应用 **条件更新**：比较 generation 后在存储内部原子写入，不能由调用方先读后写。

本章与前一节的关系：保留第 02 节“Options、默认配置与 YAML”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用完整 ws/store/store.go 替换第 02 节的最小 StateStore 占位，因为后续后端需要统一模型、错误和原子操作语义。

## 2. 关键概念与设计

### Connection_ID

一次 API Gateway 物理连接。

### Session_ID

跨重连保留的逻辑会话。

### generation

绑定代数，用于拒绝旧连接迟到的事件。

### 条件更新

比较 generation 后在存储内部原子写入，不能由调用方先读后写。

### 实现约束

1. StateStore 组合五类接口，保证初始化时选择一套一致后端。
1. 可选扩展接口表达 freshness、expiration 和 fenced room 操作，避免破坏已有自定义后端。
1. 统一错误码把 not_found、condition_conflict、unavailable 等语义与具体数据库错误分离。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 统一存储错误

外部错误文本只包含操作和稳定分类，底层原因保留在 Unwrap 中供服务端诊断。

在 `ws/store/store.go` 中加入或保留以下实现：

```go path=ws/store/store.go
func NewError(code ErrorCode, op string, err error) error {
	return &Error{Code: code, Op: op, Err: err}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 连接条件接口

Delete、Touch 和 stale 查询直接表达 generation 与分页约束。

在 `ws/store/store.go` 中加入或保留以下实现：

```go path=ws/store/store.go
type ConnectionStore interface {
	PutConnection(ctx context.Context, c Connection) error
	GetConnection(ctx context.Context, id string) (Connection, error)
	DeleteConnectionIfMatch(ctx context.Context, id string, generation uint64) error
	TouchConnection(ctx context.Context, id string, generation uint64, now time.Time) error
	ListStaleConnections(ctx context.Context, before time.Time, limit int) ([]Connection, error)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 会话原子绑定

BindConnection 必须原子检查 expectedGeneration、递增代数并替换当前连接。

在 `ws/store/store.go` 中加入或保留以下实现：

```go path=ws/store/store.go
type SessionStore interface {
	CreateSession(ctx context.Context, s Session) error
	GetSession(ctx context.Context, id string) (Session, error)
	BindConnection(ctx context.Context, sessionID, connectionID string, expectedGeneration uint64) (Session, error)
	UnbindConnectionIfMatch(ctx context.Context, sessionID, connectionID string, generation uint64) error
	DeleteExpiredSessions(ctx context.Context, before time.Time, limit int) error
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_store_contract_test.go
func TestChapter03StoreModelsAndErrors(t *testing.T) {
	err := wsstore.NewError(wsstore.ErrorCodeConditionConflict, "BindConnection", errors.New("private backend detail"))
	if !wsstore.IsConditionConflict(err) || wsstore.IsRetryable(err) {
		t.Fatalf("wrong classification: %v", err)
	}
	if err.Error() == "" || err.Error() == "private backend detail" {
		t.Fatalf("unsafe error text: %q", err)
	}
	now := time.Now()
	session := wsstore.Session{SessionID: "s1", CurrentConnectionID: "c1", Generation: 2, LastActiveAt: now}
	connection := wsstore.Connection{ConnectionID: "c1", SessionID: session.SessionID, Generation: session.Generation}
	if connection.SessionID != session.SessionID || connection.Generation != 2 {
		t.Fatal("model binding mismatch")
	}
	var store wsstore.StateStore
	if store != nil {
		t.Fatal("nil interface must remain nil")
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

### 预期结果

错误分类和模型绑定测试通过，后续三个后端可以复用同一契约。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把 Connection_ID 当成可跨重连身份。
- 在 Engine 中先 Get 再 Put 并称之为原子绑定。
- 把 DynamoDB/Redis 原始错误文本暴露给客户端。

### 小结

本章完成了“StateStore 契约与测试基座”。错误分类和模型绑定测试通过，后续三个后端可以复用同一契约。

下一节：第 04 节“Memory 连接与会话”。
