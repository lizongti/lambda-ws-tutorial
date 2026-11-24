# 第 09 节：Engine 与同连接消息闭环

> 目标：完成本章代码 ZIP 中的“Engine 与同连接消息闭环”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

现在把协议、存储、路由和 Pusher 组合成 Engine，完成“入站请求从同一逻辑连接收到回复”的核心闭环。

完成本节后，你将能够：

- 解释并应用 **同连接回复**：Reply 使用 Context 捕获的 originating Connection_ID。
- 解释并应用 **活动绑定校验**：消息处理前确认 Connection、Session、Principal 和 generation 一致。
- 解释并应用 **入站幂等**：messageId 预留处理权，已完成响应可直接重放。

本章与前一节的关系：保留第 08 节“API Gateway 事件与生命周期”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用真实 engine.go 与 Factory 过渡构造器替换第 08 节的空 Engine。stage_core_support.go 暂时承接维护、重连和离线符号，第 11、12、13 节会逐一替换，第 19 节再替换 Factory。

## 2. 关键概念与设计

### 同连接回复

Reply 使用 Context 捕获的 originating Connection_ID。

### 活动绑定校验

消息处理前确认 Connection、Session、Principal 和 generation 一致。

### 入站幂等

messageId 预留处理权，已完成响应可直接重放。

### 实现约束

1. 无效消息在业务 Handler 前变成 error Envelope。
1. 显式 Push 不猜测目标，未知 Connection_ID 返回 not-found。
1. Pusher gone 进入统一清理与离线策略，暂时错误才有界重试。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 构造 Engine

校验完整依赖并包装可重试 StateStore，但不启动后台 goroutine 或网络调用。

在 `ws/engine.go` 中加入或保留以下实现：

```go path=ws/engine.go
func NewEngine(options *Options) (*Engine, error) {
	if options == nil {
		return nil, invalidConfig("options", "must not be nil")
	}
	if nilDependency(options.StateStore) {
		return nil, invalidConfig("stateStore", "must not be nil")
	}
	if nilDependency(options.Pusher) {
		return nil, invalidConfig("pusher", "must not be nil")
	}
	copyOptions := *options
	if nilDependency(copyOptions.RetrySleeper) {
		copyOptions.RetrySleeper = sleepWithContext
	}
	if err := copyOptions.Validate(); err != nil {
		return nil, err
	}
	stateStore := copyOptions.StateStore
	copyOptions.StateStore = &retryingStateStore{delegate: stateStore, options: &copyOptions}
	return &Engine{
		Options:       &copyOptions,
		Router:        NewRouter(),
		stateStore:    stateStore,
		observability: &observability{},
	}, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 从原连接回复

响应目标来自不可变 originating 字段，而不是客户端 payload。

在 `ws/engine.go` 中加入或保留以下实现：

```go path=ws/engine.go
func (e *Engine) Reply(ctx *Context, payload any) error {
	if e == nil || ctx == nil {
		return ErrInvalidEvent
	}
	body, err := marshalPayload(payload)
	if err != nil {
		return err
	}
	msg := NewResponse(ctx.RequestID, body)
	msg.SessionID = ctx.OriginatingSessionID()
	return e.Push(ctx.StdContext(), ctx.OriginatingConnectionID(), msg)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 校验活动绑定

同时校验物理连接、逻辑会话、主体和 generation，拒绝旧连接继续操作。

在 `ws/engine.go` 中加入或保留以下实现：

```go path=ws/engine.go
func (e *Engine) validateActiveBinding(ctx context.Context, connectionID string) (wsstore.Connection, wsstore.Session, error) {
	connection, err := e.Options.StateStore.GetConnection(ctx, connectionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return wsstore.Connection{}, wsstore.Session{}, errors.Join(ErrConnectionNotFound, err)
		}
		return wsstore.Connection{}, wsstore.Session{}, err
	}
	if connection.State != wsstore.ConnectionStateActive || connection.SessionID == "" {
		return wsstore.Connection{}, wsstore.Session{}, ErrStaleConnection
	}
	session, err := e.Options.StateStore.GetSession(ctx, connection.SessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return wsstore.Connection{}, wsstore.Session{}, errors.Join(ErrSessionNotFound, err)
		}
		return wsstore.Connection{}, wsstore.Session{}, err
	}
	if session.CurrentConnectionID != connectionID || session.Generation != connection.Generation ||
		session.PrincipalSubject != connection.PrincipalSubject ||
		(!session.ExpiresAt.IsZero() && !session.ExpiresAt.After(e.Options.Clock.Now())) {
		return wsstore.Connection{}, wsstore.Session{}, ErrStaleConnection
	}
	return connection, session, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_engine_test.go
func TestChapter09SameConnectionReply(t *testing.T) {
	engine, pusher, store := newChapter09Engine(t, nil)
	chapter09Seed(t, engine, store, "connection-A", "session-A")
	engine.Handle("/echo", func(ctx *ws.Context) {
		if err := engine.Reply(ctx, map[string]string{"text": "hello"}); err != nil {
			t.Fatal(err)
		}
	})
	body, _ := json.Marshal(ws.Envelope{Version: 1, Type: ws.TypeRequest, Path: "/echo", RequestID: "r1", MessageID: "m1"})
	if err := engine.HandleMessage(context.Background(), ws.MessageEvent{ConnectionID: "connection-A", RouteKey: ws.DefaultRouteKey, Body: string(body)}); err != nil {
		t.Fatal(err)
	}
	calls := pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != "connection-A" {
		t.Fatalf("calls=%#v", calls)
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

/echo 的处理只产生一次 PostToConnection，目标严格等于入站 connection-A。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 依赖 Lambda 实例亲和性保存连接。
- Reply 从 payload 读取目标 Connection_ID。
- 协议错误也进入业务重试。

### 小结

本章完成了“Engine 与同连接消息闭环”。/echo 的处理只产生一次 PostToConnection，目标严格等于入站 connection-A。

下一节：第 10 节“认证、授权与 Principal”。
