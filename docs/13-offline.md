# 第 13 节：离线队列、确认与补发

> 目标：完成本章代码 ZIP 中的“离线队列、确认与补发”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

Session 没有活动连接时，重要事件进入有界离线队列；重连后按 sequence 补发，并用明确确认策略控制删除。

完成本节后，你将能够：

- 解释并应用 **postSuccess**：API Gateway 接受推送后删除，简单但不表示客户端业务处理成功。
- 解释并应用 **clientAck**：推送后保留 sent 状态，当前 generation 的有效 Ack 才删除。
- 解释并应用 **至少一次**：重试保留原 messageId，客户端负责去重。

本章与前一节的关系：保留第 12 节“Session 与原子重连”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用 offline.go 的 acknowledge、enqueueOffline、replaySession 和 ReplayResult 替换最后一批核心过渡符号，并删除 stage_core_support.go。

## 2. 关键概念与设计

### postSuccess

API Gateway 接受推送后删除，简单但不表示客户端业务处理成功。

### clientAck

推送后保留 sent 状态，当前 generation 的有效 Ack 才删除。

### 至少一次

重试保留原 messageId，客户端负责去重。

### 实现约束

1. 实时 gone 或绑定变化可按策略转离线，不把持久入队报告成实时发送。
1. 队列有长度、过期和溢出策略。
1. replay 中断时未确认消息保留，确认消息不重复进入 pending。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 区分投递结果

sent 与 offline 明确区分，调用方无需把 nil error 误解为客户端已收到。

在 `ws/offline.go` 中加入或保留以下实现：

```go path=ws/offline.go
type DeliveryResult struct {
	Status       DeliveryStatus
	SessionID    string
	ConnectionID string
	MessageID    string
	Gone         bool
	Offline      bool
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 按会话投递

解析当前绑定、处理竞争并在无活动连接时持久入队。

在 `ws/offline.go` 中加入或保留以下实现：

```go path=ws/offline.go
func (e *Engine) PushToSessionWithResult(ctx context.Context, sessionID string, msg OutboundMessage) (DeliveryResult, error) {
	result := DeliveryResult{SessionID: sessionID}
	if e == nil || ctx == nil {
		return result, ErrInvalidEvent
	}
	if err := ValidateSessionID(sessionID); err != nil {
		return result, err
	}
	session, err := e.Options.StateStore.GetSession(ctx, sessionID)
	if err != nil {
		if wsstore.IsNotFound(err) {
			return result, errors.Join(ErrSessionNotFound, err)
		}
		return result, err
	}
	prepared, body, err := e.prepareOutbound(msg, sessionID)
	if err != nil {
		return result, err
	}
	result.MessageID = prepared.MessageID

	for attempt := 0; attempt < 2; attempt++ {
		result.ConnectionID = session.CurrentConnectionID
		if session.CurrentConnectionID == "" {
			return e.queueSessionDelivery(ctx, result, prepared, body)
		}
		connection, getErr := e.Options.StateStore.GetConnection(ctx, session.CurrentConnectionID)
		if getErr != nil {
			if !wsstore.IsNotFound(getErr) {
				return result, getErr
			}
			_ = e.Options.StateStore.RemoveConnection(ctx, session.CurrentConnectionID)
			if unbindErr := e.Options.StateStore.UnbindConnectionIfMatch(ctx, session.SessionID, session.CurrentConnectionID, session.Generation); unbindErr != nil && !wsstore.IsConditionConflict(unbindErr) {
				return result, unbindErr
			}
			return e.queueSessionDelivery(ctx, result, prepared, body)
		}
		if connection.State != wsstore.ConnectionStateActive || connection.SessionID != session.SessionID || connection.Generation != session.Generation {
			latest, latestErr := e.Options.StateStore.GetSession(ctx, sessionID)
			if latestErr != nil {
				return result, latestErr
			}
			session = latest
			continue
		}

		err = e.postEncoded(ctx, &connection, prepared.MessageID, body)
		if err == nil {
			result.Status = DeliverySent
			return result, nil
		}
		if !IsPushGone(err) {
			return result, err
		}
		result.Gone = true
		if !e.Options.Offline.Enabled {
			return result, err
		}
		queued, queueErr := e.queueSessionDelivery(ctx, result, prepared, body)
		if queueErr != nil {
			return queued, errors.Join(err, queueErr)
		}
		return queued, nil
	}
	return e.queueSessionDelivery(ctx, result, prepared, body)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 处理客户端确认

Ack 同时校验消息所有权、Session 和当前 generation。

在 `ws/offline.go` 中加入或保留以下实现：

```go path=ws/offline.go
func (e *Engine) acknowledge(ctx context.Context, connection wsstore.Connection, envelope Envelope) error {
	if !e.Options.Offline.Enabled {
		return ErrOfflineDisabled
	}
	if envelope.SessionID != "" && envelope.SessionID != connection.SessionID {
		return ErrAuthorizationFailed
	}
	return e.Options.StateStore.Ack(ctx, connection.SessionID, envelope.MessageID, connection.Generation)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_offline_test.go
func TestChapter13OfflineReplay(t *testing.T) {
	engine, pusher, store := newChapter09Engine(t, func(o *ws.Options) { o.Offline.AckPolicy = ws.AckPolicyPostSuccess })
	connected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.HandleDisconnect(context.Background(), ws.DisconnectEvent{ConnectionID: "offline-c1", SessionID: connected.SessionID, Generation: connected.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := engine.PushToSession(context.Background(), connected.SessionID, ws.OutboundMessage{Type: ws.TypeEvent, Path: "/notice", Payload: []byte(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListPending(context.Background(), connected.SessionID, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	reconnected, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "offline-c2", SessionID: connected.SessionID, ResumeCredential: connected.ResumeCredential})
	if err != nil {
		t.Fatal(err)
	}
	if reconnected.Replay.Sent != 1 || pusher.CallCount() != 1 {
		t.Fatalf("replay=%#v calls=%d", reconnected.Replay, pusher.CallCount())
	}
	pending, err = store.ListPending(context.Background(), connected.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("postSuccess queue remains=%#v err=%v", pending, err)
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

断连期间消息入队，重连补发一次；postSuccess 模式下成功后队列清空。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 补发时生成新 messageId。
- 无限制保存离线消息。
- postSuccess 成功后仍声称具备客户端确认。

### 小结

本章完成了“离线队列、确认与补发”。断连期间消息入队，重连补发一次；postSuccess 模式下成功后队列清空。

下一节：第 14 节“房间与有界广播”。
