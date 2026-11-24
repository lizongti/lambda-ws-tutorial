# 第 19 节：server、Factory 与可观测性

> 目标：完成本章代码 ZIP 中的“server、Factory 与可观测性”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

核心框架和三后端已经完整，本节接入现有 server、统一依赖构造与资源所有权，并提供安全日志和指标钩子。

完成本节后，你将能够：

- 解释并应用 **Factory**：构造一套一致后端与 Pusher，只关闭自己创建的资源。
- 解释并应用 **lambda: ws**：沿用现有 server Option/YAML 分支，不改变其他 Lambda 类型。
- 解释并应用 **可观测性**：结构化 allowlist 字段记录生命周期和投递结果，不记录 payload/token。

本章与前一节的关系：保留第 18 节“Redis 房间、离线与幂等”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用 factory.go 替换 stage_factory.go 的简单构造器，新增一致后端选择、AWS/Redis 客户端构造和明确的资源所有权。

## 2. 关键概念与设计

### Factory

构造一套一致后端与 Pusher，只关闭自己创建的资源。

### lambda: ws

沿用现有 server Option/YAML 分支，不改变其他 Lambda 类型。

### 可观测性

结构化 allowlist 字段记录生命周期和投递结果，不记录 payload/token。

### 实现约束

1. 显式注入的 StateStore/Pusher 优先，所有权仍属于调用方。
1. Factory.Build 只做构造与配置加载，不主动 ping 外部服务。
1. Close 逆序关闭自建资源且可重复调用。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 构造完整 Engine

验证配置、选择 StateStore 和 Pusher 后创建 Engine，失败时联合同步清理已创建资源。

在 `ws/factory.go` 中加入或保留以下实现：

```go path=ws/factory.go
func (f *Factory) Build() (*Engine, error) {
	if f == nil {
		return nil, invalidConfig("factory", "must not be nil")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, invalidConfig("factory", "is closed")
	}
	if f.built {
		return nil, invalidConfig("factory", "can build only one engine")
	}
	f.built = true
	if f.options == nil {
		return nil, invalidConfig("options", "must not be nil")
	}

	options := *f.options
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if options.SharedDeployment && !nilDependency(options.StateStore) {
		if _, ok := options.StateStore.(*memorystore.Store); ok {
			return nil, invalidConfig("stateStore", "memory cannot be used for a shared deployment")
		}
	}

	stateStore, closer, err := f.buildStateStore(&options)
	if err != nil {
		return nil, err
	}
	if closer != nil {
		f.closers = append(f.closers, closer)
	}
	options.StateStore = stateStore

	if nilDependency(options.Pusher) {
		pusher, err := f.buildPusher(&options)
		if err != nil {
			return nil, errors.Join(err, f.closeLocked())
		}
		options.Pusher = pusher
	}

	engine, err := NewEngine(&options)
	if err != nil {
		return nil, errors.Join(err, f.closeLocked())
	}
	return engine, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 选择单一后端

memory、dynamodb、redis 只选一套完整实现，注入依赖优先。

在 `ws/factory.go` 中加入或保留以下实现：

```go path=ws/factory.go
func (f *Factory) buildStateStore(options *Options) (wsstore.StateStore, io.Closer, error) {
	if !nilDependency(options.StateStore) {
		return options.StateStore, nil, nil
	}

	switch options.State.Backend {
	case BackendMemory:
		return memorystore.New(
			memorystore.WithClock(options.Clock.Now),
			memorystore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			memorystore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			memorystore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		), nil, nil
	case BackendDynamoDB:
		cfg, err := f.loadAWSConfig(options)
		if err != nil {
			return nil, nil, err
		}
		client := awsdynamodb.NewFromConfig(cfg)
		return dynamodbstore.New(
			client,
			options.State.TableName,
			dynamodbstore.WithClock(options.Clock.Now),
			dynamodbstore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			dynamodbstore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			dynamodbstore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		), nil, nil
	case BackendRedis:
		redisOptions, err := parseRedisOptions(options.State.RedisAddress)
		if err != nil {
			return nil, nil, err
		}
		client := goredis.NewClient(redisOptions)
		storeOptions := []redisstore.Option{
			redisstore.WithClock(options.Clock.Now),
			redisstore.WithOfflineQueueLimit(options.Offline.MaxMessagesPerSession),
			redisstore.WithOfflineOverflowPolicy(options.Offline.Overflow),
			redisstore.WithIdempotencyWindow(options.Message.IdempotencyWindow),
		}
		if options.State.RedisKeyPrefix != "" {
			storeOptions = append(storeOptions, redisstore.WithKeyPrefix(options.State.RedisKeyPrefix))
		}
		return redisstore.New(client, storeOptions...), client, nil
	default:
		return nil, nil, invalidConfig("state.backend", "must be one of memory, dynamodb, or redis")
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 接入 server 选项

把 ws.Option 追加到 server.Options，与 event/http/reqresp/sqs 保持同样组合方式。

在 `server/config.go` 中加入或保留以下实现：

```go path=server/config.go
func WithWsOptions(opts ...ws.Option) Option {
	return serveOptionFunc(func(o *Options) {
		o.Ws = append(o.Ws, opts...)
	})
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_server_test.go
func TestChapter19ServerOptionsFactoryAndObservability(t *testing.T) {
	options := &server.Options{}
	server.WithLambdaType("ws").Apply(options)
	server.WithWsOptions(ws.WithDevelopment(true)).Apply(options)
	if options.Lambda != "ws" || len(options.Ws) != 1 {
		t.Fatalf("server options=%#v", options)
	}
	engine, _, _ := newChapter09Engine(t, nil)
	events := []ws.LogEvent{}
	engine.SetLogger(ws.LoggerFunc(func(_ context.Context, event ws.LogEvent) { events = append(events, event) }))
	if engine.StateStore() == nil {
		t.Fatal("factory engine has no state store")
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

server 能接收 lambda: ws 选项，Factory/Engine 依赖完整，日志字段保持安全。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 关闭调用方注入的 Redis client。
- 连接用 DynamoDB、Session 却隐式使用 Redis。
- 日志记录 payload、恢复凭据或原始 AWS 错误。

### 小结

本章完成了“server、Factory 与可观测性”。server 能接收 lambda: ws 选项，Factory/Engine 依赖完整，日志字段保持安全。

下一节：第 20 节“跨后端集成与故障注入”。
