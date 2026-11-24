# 第 10 节：认证、授权与 Principal

> 目标：完成本章代码 ZIP 中的“认证、授权与 Principal”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

通用框架不能绑定某一种 token。通过 Authenticator/Authorizer 注入身份与策略，并保证认证失败前没有任何状态写入。

完成本节后，你将能够：

- 解释并应用 **Principal**：稳定 Subject 加应用字符串 Claims，不包含游戏对象。
- 解释并应用 **Authenticator**：从批准的连接元数据建立身份。
- 解释并应用 **Authorizer**：对 path、房间和 Session 恢复等 Action 做策略判断。

本章与前一节的关系：保留第 09 节“Engine 与同连接消息闭环”的累计代码，再加入本章能力。

### 过渡实现与后续替换

删除 stage_auth_dependencies.go，改用本章完整认证、授权和 Principal 实现；配置 API 保持不变，因此前两章测试仍应通过。

## 2. 关键概念与设计

### Principal

稳定 Subject 加应用字符串 Claims，不包含游戏对象。

### Authenticator

从批准的连接元数据建立身份。

### Authorizer

对 path、房间和 Session 恢复等 Action 做策略判断。

### 实现约束

1. 匿名认证使用独立类型，只有显式 development + anonymous 才能通过启动校验。
1. 认证失败的底层 token 错误不向外暴露。
1. 授权中间件在业务 Handler 前 Abort。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 身份模型

Subject 是框架需要的最小稳定身份，Claims 可复制且默认格式化不泄露值。

在 `ws/auth.go` 中加入或保留以下实现：

```go path=ws/auth.go
type Principal struct {
	Subject string
	Claims  map[string]string
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 生产认证校验

生产缺少认证器、匿名模式未显式开启或配置不一致都在启动时失败。

在 `ws/auth.go` 中加入或保留以下实现：

```go path=ws/auth.go
func ValidateAuthConfig(config AuthConfig, authenticator Authenticator) error {
	_, anonymousAuthenticator := authenticator.(interface{ developmentAnonymous() })

	if !config.Development && (config.Anonymous || anonymousAuthenticator) {
		return ErrAnonymousNotAllowed
	}
	if config.Anonymous != anonymousAuthenticator {
		return ErrAnonymousNotExplicit
	}
	if !config.Development && authenticator == nil {
		return ErrAuthenticatorRequired
	}
	if (config.Required || config.Protected) && authenticator == nil {
		return ErrAuthenticatorRequired
	}
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 路径授权中间件

检查 Subject、调用策略并以安全 authorization_denied 终止后续处理。

在 `ws/auth.go` 中加入或保留以下实现：

```go path=ws/auth.go
func RequireAuthorization(authorizer Authorizer) HandlerFunc {
	return func(ctx *Context) {
		if ctx == nil || ctx.IsAborted() || ctx.Err() != nil {
			return
		}
		if ctx.Principal.Subject == "" {
			ctx.AbortWithError(NewHandlerError("authentication_required", "authentication required"))
			return
		}
		if authorizer == nil {
			ctx.AbortWithError(NewHandlerError("internal_error", "authorization is not configured"))
			return
		}

		action := Action{
			Name:     ActionPathAccess,
			Resource: ctx.RoutePath(),
			Path:     ctx.RoutePath(),
		}
		if err := authorizer.Authorize(ctx.StdContext(), ctx.Principal.Clone(), action); err != nil {
			// Do not wrap or copy the application's error: it can contain policy
			// details, credentials or backend information.
			ctx.AbortWithError(NewHandlerError("authorization_denied", "access denied"))
		}
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_auth_test.go
func TestChapter10AuthenticationAndAuthorization(t *testing.T) {
	if err := ws.ValidateProductionAuth(nil); !errors.Is(err, ws.ErrAuthenticatorRequired) {
		t.Fatalf("production auth err=%v", err)
	}
	denied := ws.AuthenticatorFunc(func(context.Context, ws.ConnectRequest) (ws.Principal, error) {
		return ws.Principal{}, errors.New("private token detail")
	})
	engine, _, store := newChapter09Engine(t, func(o *ws.Options) { o.Authenticator = denied; o.Auth.Anonymous = false })
	if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "denied-connection"}); !errors.Is(err, ws.ErrAuthenticationFailed) {
		t.Fatalf("connect err=%v", err)
	}
	if _, err := store.GetConnection(context.Background(), "denied-connection"); !wsstore.IsNotFound(err) {
		t.Fatalf("failed auth created state: %v", err)
	}
	ctx := &ws.Context{Context: context.Background(), Path: "/private", Principal: ws.Principal{Subject: "alice"}}
	ws.RequireAuthorization(ws.AuthorizerFunc(func(context.Context, ws.Principal, ws.Action) error { return errors.New("denied") }))(ctx)
	if !ctx.IsAborted() || ws.HandlerErrorCode(ctx.Err()) != "authorization_denied" {
		t.Fatalf("authorization state=%v err=%v", ctx.IsAborted(), ctx.Err())
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

生产无认证器被拒绝，失败连接不留状态，授权失败转为安全错误。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把原始 token 放入日志或 Principal.String。
- 先创建 Session 再认证。
- 将游戏权限模型硬编码到 ws 框架。

### 小结

本章完成了“认证、授权与 Principal”。生产无认证器被拒绝，失败连接不留状态，授权失败转为安全错误。

下一节：第 11 节“心跳与 Maintenance”。
