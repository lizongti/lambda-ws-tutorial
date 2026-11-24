# 第 12 节：Session 与原子重连

> 目标：完成本章代码 ZIP 中的“Session 与原子重连”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

网络中断后，客户端应凭恢复凭据把同一 Session 绑定到新 Connection_ID，同时旧连接的迟到事件不能破坏新绑定。

完成本节后，你将能够：

- 解释并应用 **恢复凭据**：原始值只返回连接发起方，存储域分离 SHA-256 表示。
- 解释并应用 **原子重连**：BindConnection 保留 Session_ID 并递增 generation。
- 解释并应用 **fencing**：消息、心跳、disconnect 和 ack 都校验当前绑定。

本章与前一节的关系：保留第 11 节“心跳与 Maintenance”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用 session.go 的 HandleConnect 和 clone 辅助函数替换临时实现；恢复凭据、原子重连和离线补发入口现在由真实会话流程负责。

## 2. 关键概念与设计

### 恢复凭据

原始值只返回连接发起方，存储域分离 SHA-256 表示。

### 原子重连

BindConnection 保留 Session_ID 并递增 generation。

### fencing

消息、心跳、disconnect 和 ack 都校验当前绑定。

### 实现约束

1. 恢复时同时校验 Session、Principal、凭据和过期时间。
1. replaceExisting 决定已有活动连接是拒绝还是替换。
1. 断连只条件解绑，Session 在重连窗口内继续保留。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 保护恢复凭据

哈希输入包含协议域与 Session_ID，避免凭据哈希被移植到其他会话。

在 `ws/session.go` 中加入或保留以下实现：

```go path=ws/session.go
func HashResumeCredential(sessionID, credential string) string {
	digest := sha256.Sum256([]byte("lambda/ws/resume-v1\x00" + sessionID + "\x00" + credential))
	return hex.EncodeToString(digest[:])
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 恢复已有会话

验证授权与凭据后执行存储原子绑定，并触发离线补发。

在 `ws/session.go` 中加入或保留以下实现：

```go path=ws/session.go
func (e *Engine) resumeSession(ctx context.Context, event ConnectEvent, principal Principal) (ConnectResult, error) {
	session, err := e.Options.StateStore.GetSession(ctx, event.SessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return ConnectResult{}, errors.Join(ErrInvalidResume, err)
		}
		return ConnectResult{}, err
	}
	now := e.Options.Clock.Now()
	if session.PrincipalSubject != principal.Subject || !resumeCredentialMatches(session, event.ResumeCredential) ||
		(!session.ExpiresAt.IsZero() && !session.ExpiresAt.After(now)) {
		return ConnectResult{}, ErrInvalidResume
	}
	if !nilDependency(e.Options.Authorizer) {
		if err := e.Options.Authorizer.Authorize(ctx, principal.Clone(), Action{
			Name: ActionSessionResume, SessionID: session.SessionID, Resource: session.SessionID,
		}); err != nil {
			return ConnectResult{}, ErrAuthorizationFailed
		}
	}

	oldConnectionID := session.CurrentConnectionID
	if oldConnectionID != "" && !e.Options.Reconnect.ReplaceExisting {
		if old, getErr := e.Options.StateStore.GetConnection(ctx, oldConnectionID); getErr == nil && old.State == wsstore.ConnectionStateActive {
			return ConnectResult{}, ErrConnectionConflict
		} else if getErr != nil && !wsstore.IsNotFound(getErr) {
			return ConnectResult{}, getErr
		}
	}
	bound, err := e.Options.StateStore.BindConnection(ctx, session.SessionID, event.ConnectionID, session.Generation)
	if err != nil {
		if wsstore.IsConditionConflict(err) {
			return ConnectResult{}, errors.Join(ErrConnectionConflict, err)
		}
		return ConnectResult{}, err
	}
	connection := e.newConnection(event.ConnectionID, bound, now)
	if err := e.Options.StateStore.PutConnection(ctx, connection); err != nil {
		_ = e.Options.StateStore.UnbindConnectionIfMatch(ctx, session.SessionID, event.ConnectionID, bound.Generation)
		return ConnectResult{}, err
	}

	if oldConnectionID != "" && oldConnectionID != event.ConnectionID {
		if old, getErr := e.Options.StateStore.GetConnection(ctx, oldConnectionID); getErr == nil {
			// The session has already advanced. cleanupConnection's conditional
			// unbind therefore cannot clear the replacement binding.
			if cleanupErr := e.cleanupConnection(ctx, old, "replaced"); cleanupErr != nil && !isFencingError(cleanupErr) {
				e.observability.log(ctx, LogEvent{Name: LogEventReconnect, Outcome: "cleanup_failed", RequestID: event.RequestID, ConnectionID: old.ConnectionID, SessionID: session.SessionID, ErrorCode: safeErrorCode(cleanupErr)})
			}
		}
	}
	return ConnectResult{
		ConnectionID:     event.ConnectionID,
		SessionID:        session.SessionID,
		Generation:       bound.Generation,
		ResumeCredential: event.ResumeCredential,
		Reconnected:      true,
	}, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 创建当前连接记录

物理连接继承当前 Session、Principal 与 generation，并初始化 last-seen。

在 `ws/session.go` 中加入或保留以下实现：

```go path=ws/session.go
func (e *Engine) newConnection(connectionID string, session wsstore.Session, nowTime time.Time) wsstore.Connection {
	return wsstore.Connection{
		ConnectionID:     connectionID,
		SessionID:        session.SessionID,
		PrincipalSubject: session.PrincipalSubject,
		Generation:       session.Generation,
		ConnectedAt:      nowTime,
		LastSeenAt:       nowTime,
		ExpiresAt:        nowTime.Add(e.Options.Reconnect.SessionRetention),
		State:            wsstore.ConnectionStateActive,
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_reconnect_test.go
func TestChapter12ReconnectAndStaleDisconnect(t *testing.T) {
	engine, _, store := newChapter09Engine(t, nil)
	first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c1"})
	if err != nil || first.ResumeCredential == "" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil {
		t.Fatal(err)
	}
	second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c2", SessionID: first.SessionID, ResumeCredential: first.ResumeCredential})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Reconnected || second.SessionID != first.SessionID || second.Generation <= first.Generation {
		t.Fatalf("second=%#v first=%#v", second, first)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "c1", SessionID: first.SessionID, Generation: first.Generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetConnection(context.Background(), "c2"); err != nil {
		t.Fatalf("stale disconnect removed c2: %v", err)
	}
	if _, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "c3", SessionID: first.SessionID, ResumeCredential: "wrong"}); err == nil {
		t.Fatal("wrong credential accepted")
	}
	_ = wsstore.ConnectionStateActive
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

c1 断开后 c2 恢复同一 Session 且 generation 增加，旧 c1 disconnect 不影响 c2。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 在 Session 中保存明文恢复凭据。
- 重连时生成新的 Session_ID。
- 只校验 Connection_ID 而不校验 generation。

### 小结

本章完成了“Session 与原子重连”。c1 断开后 c2 恢复同一 Session 且 generation 增加，旧 c1 disconnect 不影响 c2。

下一节：第 13 节“离线队列、确认与补发”。
