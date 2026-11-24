# 第 08 节：API Gateway 事件与生命周期

> 目标：完成本章代码 ZIP 中的“API Gateway 事件与生命周期”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

本节把 API Gateway 的 $connect、$disconnect、$default 事件转换为内部事件，建立 Lambda 与传输无关 Engine 之间的薄适配层。

完成本节后，你将能够：

- 解释并应用 **$connect**：认证与创建/恢复绑定的入口。
- 解释并应用 **$disconnect**：按存储中的当前绑定进行条件清理。
- 解释并应用 **$default**：文本 Envelope 的默认消息入口；自定义 route key 可映射内部 path。

本章与前一节的关系：保留第 07 节“Context、路由与中间件”的累计代码，再加入本章能力。

### 过渡实现与后续替换

本章仅用 stage08_engine.go、stage08_factory.go 和 clone 辅助函数保持事件适配器可编译。第 09 节会替换临时 Engine/Factory，第 12 节会把 clone 辅助函数收敛到 session.go。

## 2. 关键概念与设计

### $connect

认证与创建/恢复绑定的入口。

### $disconnect

按存储中的当前绑定进行条件清理。

### $default

文本 Envelope 的默认消息入口；自定义 route key 可映射内部 path。

### 实现约束

1. 只复制批准的 headers、query 和 request context，认证格式仍由应用决定。
1. ResumeCredential 不进入 String/GoString 或日志。
1. 适配器返回 HTTP 状态码，应用消息始终走 Pusher。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 内部生命周期事件

内部模型保留连接元数据与恢复凭据，但不绑定 AWS SDK 类型。

在 `ws/events.go` 中加入或保留以下实现：

```go path=ws/events.go
type ConnectEvent struct {
	ConnectionID                    string
	RequestID                       string
	RouteKey                        string
	SessionID                       string
	ResumeCredential                string
	Headers                         map[string]string
	QueryParameters                 map[string]string
	MultiValueHeaders               map[string][]string
	MultiValueQueryStringParameters map[string][]string
	RequestContext                  map[string]any
	ConnectedAt                     time.Time
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 映射连接事件

提取 Connection_ID、requestId、route key、认证元数据和连接时间。

在 `ws/server.go` 中加入或保留以下实现：

```go path=ws/server.go
func ConnectEventFromAPIGateway(request events.APIGatewayWebsocketProxyRequest) ConnectEvent {
	requestContext := request.RequestContext
	query := request.QueryStringParameters
	return ConnectEvent{
		ConnectionID:                    requestContext.ConnectionID,
		RequestID:                       requestContext.RequestID,
		RouteKey:                        requestContext.RouteKey,
		SessionID:                       firstMetadataValue(query, request.MultiValueQueryStringParameters, "sessionId", "sessionID"),
		ResumeCredential:                firstCredential(request.Headers, query, request.MultiValueQueryStringParameters),
		Headers:                         cloneStringMap(request.Headers),
		QueryParameters:                 cloneStringMap(query),
		MultiValueHeaders:               cloneStringSliceMap(request.MultiValueHeaders),
		MultiValueQueryStringParameters: cloneStringSliceMap(request.MultiValueQueryStringParameters),
		RequestContext:                  apiGatewayRequestContext(request),
		ConnectedAt:                     time.UnixMilli(requestContext.ConnectedAt),
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 统一事件入口

按 route key 选择生命周期方法，默认与未知自定义路由进入消息处理。

在 `ws/server.go` 中加入或保留以下实现：

```go path=ws/server.go
func (e *Engine) HandleEvent(ctx context.Context, request events.APIGatewayWebsocketProxyRequest) (events.APIGatewayProxyResponse, error) {
	var err error
	switch request.RequestContext.RouteKey {
	case RouteConnect:
		_, err = e.HandleConnect(ctx, ConnectEventFromAPIGateway(request))
	case RouteDisconnect:
		err = e.HandleDisconnect(ctx, DisconnectEventFromAPIGateway(request))
	case DefaultRouteKey:
		err = e.HandleMessage(ctx, MessageEventFromAPIGateway(request))
	default:
		// Unknown/custom route keys follow Router.ResolvePath's $default rule.
		err = e.HandleMessage(ctx, MessageEventFromAPIGateway(request))
	}
	return events.APIGatewayProxyResponse{StatusCode: statusForError(err)}, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_lifecycle_test.go
func TestChapter08APIGatewayAdapters(t *testing.T) {
	request := events.APIGatewayWebsocketProxyRequest{Body: `{"version":1,"type":"ping"}`, Headers: map[string]string{"X-WS-Resume-Credential": "secret"}, QueryStringParameters: map[string]string{"sessionId": "s1"}}
	request.RequestContext.ConnectionID = "c1"
	request.RequestContext.RequestID = "r1"
	request.RequestContext.RouteKey = ws.RouteConnect
	request.RequestContext.ConnectedAt = 1000
	connected := ws.ConnectEventFromAPIGateway(request)
	if connected.ConnectionID != "c1" || connected.SessionID != "s1" || connected.ResumeCredential != "secret" {
		t.Fatalf("connect=%#v", connected)
	}
	if connected.String() == "" || containsCredential(connected.String(), "secret") {
		t.Fatalf("credential leaked: %q", connected.String())
	}
	request.RequestContext.RouteKey = ws.DefaultRouteKey
	message := ws.MessageEventFromAPIGateway(request)
	if message.Body != request.Body || message.ConnectionID != "c1" {
		t.Fatalf("message=%#v", message)
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

$connect 元数据、$default body 和 Connection_ID 正确映射，格式化结果不含恢复凭据。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 在 Lambda return body 中返回业务推送。
- 把 base64 二进制数据直接当 JSON 文本。
- 日志打印整个 ConnectEvent 或请求 headers。

### 小结

本章完成了“API Gateway 事件与生命周期”。$connect 元数据、$default body 和 Connection_ID 正确映射，格式化结果不含恢复凭据。

下一节：第 09 节“Engine 与同连接消息闭环”。
