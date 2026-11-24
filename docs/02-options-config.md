# 第 02 节：Options、默认配置与 YAML

> 目标：完成本章代码 ZIP 中的“Options、默认配置与 YAML”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

协议已经可运行，下一步需要一种可组合、可校验的启动配置，避免把默认值、YAML 和测试依赖散落在业务代码里。

完成本节后，你将能够：

- 解释并应用 **Option**：按参数顺序修改 Options，代码配置与 YAML 使用同一覆盖路径。
- 解释并应用 **Validate**：启动时集中检查后端、认证、时间、批次和 endpoint 组合。
- 解释并应用 **依赖注入**：Clock、IDGenerator、Pusher、StateStore 等由调用方提供，测试不需要真实服务。

本章与前一节的关系：保留第 01 节“JSON Envelope 与协议校验”的累计代码，再加入本章能力。

### 过渡实现与后续替换

本章用 stage_runtime_dependencies.go、stage_auth_dependencies.go 和最小 store 契约隔离未来依赖。它们只用于让配置阶段独立编译，第 03、06、10 节会分别用完整契约、Pusher 和认证实现替换。

## 2. 关键概念与设计

### Option

按参数顺序修改 Options，代码配置与 YAML 使用同一覆盖路径。

### Validate

启动时集中检查后端、认证、时间、批次和 endpoint 组合。

### 依赖注入

Clock、IDGenerator、Pusher、StateStore 等由调用方提供，测试不需要真实服务。

### 实现约束

1. 默认值先复制再应用 Option，避免全局默认对象被某次启动修改。
1. 解析错误在 Option 应用时 panic，与仓库其他 Lambda 模块一致；Engine 启动前再执行 Validate。
1. 敏感 endpoint、Redis 地址和依赖值不进入 String/GoString。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 组合配置与依赖

配置字段按 YAML 名称分组，运行时依赖集中在同一结构中，Factory 后续只消费这一份配置。

在 `ws/options.go` 中加入或保留以下实现：

```go path=ws/options.go
type Options struct {
	State      StateConfig
	APIGateway APIGatewayConfig
	Auth       AuthConfig
	Heartbeat  HeartbeatConfig
	Reconnect  ReconnectConfig
	Offline    OfflineConfig
	Rooms      RoomsConfig
	Push       PushConfig
	Message    MessageConfig

	// SharedDeployment rejects the process-local memory backend. Production is
	// the default; Auth.Development must be explicitly enabled for anonymous use.
	SharedDeployment bool

	StateStore    wsstore.StateStore
	Pusher        Pusher
	Authenticator Authenticator
	Authorizer    Authorizer
	Clock         Clock
	IDGenerator   IDGenerator
	RetrySleeper  SleepFunc
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 应用 Option

复制默认值后按顺序应用，最后一个设置生效，同时保留可预测的覆盖规则。

在 `ws/options.go` 中加入或保留以下实现：

```go path=ws/options.go
func NewOptions(opts ...Option) *Options {
	options := *defaultOptions
	options.init(opts...)
	return &options
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 解析严格 YAML

支持 ws 子树和完整文档，未知字段与类型错误不会悄悄通过。

在 `ws/config.go` 中加入或保留以下实现：

```go path=ws/config.go
func WithConfig(yamlBytes []byte) Option {
	option, err := optionFromConfigBytes(yamlBytes)
	if err != nil {
		return OptionFunc(func(*Options) {
			panic(fmt.Errorf("ws.WithConfig: %w", err))
		})
	}
	return option
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_config_test.go
func TestChapter02OptionsAndYAML(t *testing.T) {
	options := ws.NewOptions(
		ws.WithDevelopment(true),
		ws.WithAuthenticator(ws.NewAnonymousAuthenticator()),
		ws.WithPusher(chapter02Pusher{}),
		ws.OptionFunc(func(o *ws.Options) { o.Auth.Anonymous = true }),
		ws.WithConfig([]byte("heartbeat:\n  interval: 10s\n  timeout: 30s\n")),
	)
	if err := options.Validate(); err != nil {
		t.Fatal(err)
	}
	if options.Heartbeat.Interval != 10*time.Second || options.Heartbeat.Timeout != 30*time.Second {
		t.Fatalf("YAML override failed: %#v", options.Heartbeat)
	}
	options.SharedDeployment = true
	if err := options.Validate(); err == nil {
		t.Fatal("shared memory deployment was accepted")
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

10s/30s 的 YAML 覆盖生效；共享部署使用 memory 时 Validate 失败；中国区 endpoint 正确生成。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 直接修改全局 defaultOptions。
- 只在 YAML 路径校验而绕过代码 Option 路径。
- 为了打印调试信息泄露 Redis URL 中的凭据。

### 小结

本章完成了“Options、默认配置与 YAML”。10s/30s 的 YAML 覆盖生效；共享部署使用 memory 时 Validate 失败；中国区 endpoint 正确生成。

下一节：第 03 节“StateStore 契约与测试基座”。
