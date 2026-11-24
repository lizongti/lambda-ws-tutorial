# 第 07 节：Context、路由与中间件

> 目标：完成本章代码 ZIP 中的“Context、路由与中间件”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

消息协议和推送已经就绪，需要把 path 分发给应用 Handler，同时保留可信连接身份和可控的中间件终止语义。

完成本节后，你将能够：

- 解释并应用 **Context**：保存 Connection_ID、Session_ID、Principal、requestId 和原始 payload。
- 解释并应用 **中间件链**：Use 注册公共逻辑，Abort 阻止后续 Handler。
- 解释并应用 **通配路径**：精确匹配之外支持仓库已有的 *path 约定。

本章与前一节的关系：保留第 06 节“Pusher 与 API Gateway 适配器”的累计代码，再加入本章能力。

## 2. 关键概念与设计

### Context

保存 Connection_ID、Session_ID、Principal、requestId 和原始 payload。

### 中间件链

Use 注册公共逻辑，Abort 阻止后续 Handler。

### 通配路径

精确匹配之外支持仓库已有的 *path 约定。

### 实现约束

1. 路由 panic 在边界恢复并转为安全 internal_error。
1.  originating connection 在分发边界固定，Handler 不能通过修改 payload 改推送目标。
1. NoRoute 返回稳定 route_not_found，而不是静默成功。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 保存可信上下文

公开必要的请求信息，内部字段保存原始目标和路由匹配状态。

在 `ws/context.go` 中加入或保留以下实现：

```go path=ws/context.go
type Context struct {
	Context      context.Context
	ConnectionID string
	SessionID    string
	Principal    Principal
	Path         string
	RequestID    string
	MessageID    string
	Payload      json.RawMessage

	response                any
	responseSet             bool
	err                     error
	aborted                 bool
	routePath               string
	pathParam               string
	originatingConnectionID string
	originatingSessionID    string
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 注册路由

规范化 pattern 并保存 Handler 链，业务只关心 path 与 payload。

在 `ws/router.go` 中加入或保留以下实现：

```go path=ws/router.go
func (r *Router) Handle(pattern string, handlers ...HandlerFunc) {
	normalized, err := normalizePattern(pattern)
	if err != nil {
		panic("ws: invalid route pattern")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes = append(r.routes, route{
		pattern:  normalized,
		handlers: compactHandlers(handlers),
	})
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 执行分发

按确定顺序执行中间件与 Handler，并处理 Abort、NoRoute 和 panic。

在 `ws/router.go` 中加入或保留以下实现：

```go path=ws/router.go
func (r *Router) DispatchRoute(ctx *Context, routeKey string) {
	if ctx == nil {
		return
	}
	resolved, err := r.ResolvePath(routeKey, ctx.Path)
	if err != nil {
		ctx.AbortWithError(err)
		return
	}
	ctx.Path = resolved
	r.Dispatch(ctx)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_router_test.go
func TestChapter07RouterMiddlewareWildcardAndAbort(t *testing.T) {
	router := ws.NewRouter()
	order := []string{}
	router.Use(func(ctx *ws.Context) { order = append(order, "middleware") })
	router.Handle("/echo/*path", func(ctx *ws.Context) {
		order = append(order, "handler")
		if ctx.PathParam() != "/42" {
			t.Fatalf("path param=%q", ctx.PathParam())
		}
		ctx.SetResponse(map[string]any{"ok": true})
	})
	ctx := &ws.Context{Context: context.Background(), Path: "/echo/42"}
	router.Dispatch(ctx)
	if len(order) != 2 || order[0] != "middleware" || !ctx.HasResponse() {
		t.Fatalf("order=%v response=%t", order, ctx.HasResponse())
	}
	router.NoRoute(func(ctx *ws.Context) { ctx.AbortWithError(ws.NewHandlerError("route_not_found", "no route")) })
	missing := &ws.Context{Context: context.Background(), Path: "/missing"}
	router.Dispatch(missing)
	if !missing.IsAborted() || ws.HandlerErrorCode(missing.Err()) != "route_not_found" {
		t.Fatalf("missing state=%v err=%v", missing.IsAborted(), missing.Err())
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

中间件先于 Handler，/echo/*path 提取 /42，未知路径被安全中止。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 让业务 Handler 直接读取 AWS SDK 事件。
- Abort 后仍继续执行后续 Handler。
- 把 route key 与 Envelope path 混为一层。

### 小结

本章完成了“Context、路由与中间件”。中间件先于 Handler，/echo/*path 提取 /42，未知路径被安全中止。

下一节：第 08 节“API Gateway 事件与生命周期”。
