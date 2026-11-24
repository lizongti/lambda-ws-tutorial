# 第 06 节：Pusher 与 API Gateway 适配器

> 目标：完成本章代码 ZIP 中的“Pusher 与 API Gateway 适配器”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

Lambda 不持有客户端 socket。所有业务出站消息都必须通过 API Gateway Management API 的 PostToConnection 发回已有 Connection_ID。

完成本节后，你将能够：

- 解释并应用 **Pusher**：只接收 connectionID 和序列化字节的传输抽象。
- 解释并应用 **gone**：目标连接已失效，应清理而不是继续暂时性重试。
- 解释并应用 **有界退避**：仅 throttled/unavailable 等暂时错误按上限重试。

本章与前一节的关系：保留第 05 节“Memory 房间、离线与幂等”的累计代码，再加入本章能力。

### 过渡实现与后续替换

删除 stage_runtime_dependencies.go 中的 Pusher、SleepFunc 和 endpoint 过渡实现，改用本章真实 pusher.go 与 apigateway_pusher.go。

## 2. 关键概念与设计

### Pusher

只接收 connectionID 和序列化字节的传输抽象。

### gone

目标连接已失效，应清理而不是继续暂时性重试。

### 有界退避

仅 throttled/unavailable 等暂时错误按上限重试。

### 实现约束

1. 管理 endpoint 只来自可信部署配置，不从消息 payload 构造。
1. 请求体在调用前后复制，FakePusher 记录也避免别名。
1. Lambda 返回值只表示集成状态，不替代应用推送。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 定义推送抽象

接口足够小，生产使用 AWS 适配器，测试使用 FakePusher。

在 `ws/pusher.go` 中加入或保留以下实现：

```go path=ws/pusher.go
type Pusher interface {
	PostToConnection(ctx context.Context, connectionID string, body []byte) error
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 分类推送错误

gone/invalid 永久失败，throttled/unavailable 才标记 Retryable。

在 `ws/pusher.go` 中加入或保留以下实现：

```go path=ws/pusher.go
func NewPushError(kind PushErrorKind, cause error) *PushError {
	switch kind {
	case PushErrorGone, PushErrorInvalid:
		return &PushError{Kind: kind, Cause: cause}
	case PushErrorThrottled, PushErrorUnavailable:
		return &PushError{Kind: kind, Retryable: true, Cause: cause}
	default:
		return &PushError{Kind: PushErrorUnknown, Cause: cause}
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 构造可信 endpoint

校验 API ID、region、stage，并按 AWS 分区选择域名。

在 `ws/apigateway_pusher.go` 中加入或保留以下实现：

```go path=ws/apigateway_pusher.go
func BuildAPIGatewayManagementEndpoint(apiID, region, stage string) (string, error) {
	for name, value := range map[string]string{"API ID": apiID, "region": region, "stage": stage} {
		if !validEndpointComponent(value) {
			return "", NewPushError(PushErrorInvalid, errors.New(name+" is empty or contains invalid characters"))
		}
	}
	domain := "amazonaws.com"
	if strings.HasPrefix(strings.ToLower(region), "cn-") {
		domain = "amazonaws.com.cn"
	}
	return "https://" + apiID + ".execute-api." + region + "." + domain + "/" + stage, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_pusher_test.go
func TestChapter06FakePusherAndErrors(t *testing.T) {
	gone := ws.NewPushError(ws.PushErrorGone, errors.New("410"))
	temporary := ws.NewPushError(ws.PushErrorThrottled, errors.New("limited"))
	if ws.IsRetryablePushError(gone) || !ws.IsPushGone(gone) || !ws.IsRetryablePushError(temporary) {
		t.Fatal("push error classification mismatch")
	}
	pusher := ws.NewFakePusher(nil)
	if err := pusher.PostToConnection(context.Background(), "c1", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	calls := pusher.Calls()
	if len(calls) != 1 || calls[0].ConnectionID != "c1" || string(calls[0].Body) != `{"ok":true}` {
		t.Fatalf("calls=%#v", calls)
	}
	calls[0].Body[0] = 'X'
	if string(pusher.Calls()[0].Body)[0] == 'X' {
		t.Fatal("recorded body aliases caller mutation")
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

FakePusher 记录同一 Connection_ID 和原始 JSON，gone 与暂时错误分类正确。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 尝试在 Lambda 中保存可写 WebSocket。
- 对 410 Gone 做指数退避。
- 让客户端 Envelope 指定任意管理 endpoint。

### 小结

本章完成了“Pusher 与 API Gateway 适配器”。FakePusher 记录同一 Connection_ID 和原始 JSON，gone 与暂时错误分类正确。

下一节：第 07 节“Context、路由与中间件”。
