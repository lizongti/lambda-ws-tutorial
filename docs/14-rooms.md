# 第 14 节：房间与有界广播

> 目标：完成本章代码 ZIP 中的“房间与有界广播”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

房间只管理通用成员关系。广播需要分页、有限并发和逐目标失败隔离，不能因一个失效连接取消整个批次。

完成本节后，你将能够：

- 解释并应用 **RoomMember**：roomId、sessionId、connectionId、generation 的通用绑定。
- 解释并应用 **有界广播**：worker 数量受配置限制，逐目标收集结果。
- 解释并应用 **发送者排除**：使用可信 SenderSessionID/ConnectionID，而非 payload 字段。

本章与前一节的关系：保留第 13 节“离线队列、确认与补发”的累计代码，再加入本章能力。

## 2. 关键概念与设计

### RoomMember

roomId、sessionId、connectionId、generation 的通用绑定。

### 有界广播

worker 数量受配置限制，逐目标收集结果。

### 发送者排除

使用可信 SenderSessionID/ConnectionID，而非 payload 字段。

### 实现约束

1. Join/Leave 幂等且可经过 Authorizer。
1. 断连、stale 和 gone 都清理成员及反向索引。
1. 空房间成功返回零投递，不隐式创建游戏房间。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 广播选项与结果

发送者与 Principal 都来自可信调用上下文，结果分别统计成功、gone、离线和失败。

在 `ws/room.go` 中加入或保留以下实现：

```go path=ws/room.go
type BroadcastOptions struct {
	ExcludeSender      bool
	SenderSessionID    string
	SenderConnectionID string
	Principal          Principal
	RequestID          string
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 加入房间

先校验当前绑定与授权，再写入或更新当前 generation 的成员关系。

在 `ws/room.go` 中加入或保留以下实现：

```go path=ws/room.go
func (e *Engine) JoinRoom(ctx *Context, roomID string) error {
	if e == nil || !e.Options.Rooms.Enabled {
		return ErrRoomsDisabled
	}
	if err := ValidateRoomID(roomID); err != nil {
		return err
	}
	_, session, connection, err := e.roomContextBinding(ctx)
	if err != nil {
		return err
	}
	if err := e.authorize(ctx.StdContext(), ctx.Principal, Action{Name: ActionRoomJoin, RoomID: roomID, SessionID: session.SessionID, Resource: roomID}); err != nil {
		return err
	}
	count, alreadyMember, err := e.roomMembershipCount(ctx.StdContext(), roomID, session.SessionID)
	if err != nil {
		return err
	}
	if count >= e.Options.Rooms.MaxMembers && !alreadyMember {
		return ErrRoomFull
	}
	member := wsstore.RoomMember{
		RoomID: roomID, SessionID: session.SessionID, ConnectionID: connection.ConnectionID,
		Generation: connection.Generation, JoinedAt: e.Options.Clock.Now(),
	}
	var joinErr error
	if bindingStore, ok := e.Options.StateStore.(wsstore.RoomBindingStore); ok {
		joinErr = bindingStore.JoinIfMatch(ctx.StdContext(), member)
	} else {
		joinErr = e.Options.StateStore.Join(ctx.StdContext(), member)
	}
	if joinErr != nil {
		if wsstore.IsQuotaExceeded(joinErr) {
			return errors.Join(ErrRoomFull, joinErr)
		}
		return joinErr
	}
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 执行有界广播

分页读取成员并使用有限并发推送，单目标失败只进入自己的 DeliveryResult。

在 `ws/room.go` 中加入或保留以下实现：

```go path=ws/room.go
func (e *Engine) Broadcast(ctx context.Context, roomID string, msg OutboundMessage, opts BroadcastOptions) BroadcastResult {
	var result BroadcastResult
	if e == nil || e.Options == nil || ctx == nil {
		result.Err = ErrInvalidEvent
		return result
	}
	if !e.Options.Rooms.Enabled {
		result.Err = ErrRoomsDisabled
		return result
	}
	if err := ValidateRoomID(roomID); err != nil {
		result.Err = err
		return result
	}
	// Validate the common envelope before reading any membership. Per-target
	// session and message IDs are added later and encoded again at the boundary.
	if _, err := EncodeOutboundMessageWithLimit(msg, e.Options.Message.MaxBytes); err != nil {
		result.Err = err
		return result
	}
	principal := opts.Principal.Clone()
	if principal.Subject == "" && opts.SenderSessionID != "" {
		if session, err := e.Options.StateStore.GetSession(ctx, opts.SenderSessionID); err == nil {
			principal.Subject = session.PrincipalSubject
		}
	}
	if err := e.authorize(ctx, principal, Action{Name: ActionRoomBroadcast, RoomID: roomID, SessionID: opts.SenderSessionID, Resource: roomID}); err != nil {
		result.Err = err
		e.observability.log(ctx, LogEvent{Name: LogEventBroadcast, Outcome: "denied", RequestID: opts.RequestID, RoomID: roomID, ErrorCode: "authorization_denied"})
		return result
	}

	cursor := ""
	for {
		members, next, err := e.Options.StateStore.Members(ctx, roomID, e.Options.Rooms.BroadcastBatchSize, cursor)
		if err != nil {
			result.Err = err
			return result
		}
		selected := make([]wsstore.RoomMember, 0, len(members))
		for _, member := range members {
			if opts.ExcludeSender && ((opts.SenderSessionID != "" && member.SessionID == opts.SenderSessionID) ||
				(opts.SenderConnectionID != "" && member.ConnectionID == opts.SenderConnectionID)) {
				continue
			}
			selected = append(selected, member)
		}
		pageResult := e.broadcastPage(ctx, msg, selected)
		mergeBroadcastResult(&result, pageResult)
		if ctx.Err() != nil {
			result.Err = ctx.Err()
			return result
		}
		if next == "" || len(members) == 0 {
			break
		}
		if next == cursor {
			result.Err = ErrInternal
			return result
		}
		cursor = next
	}
	e.observability.log(ctx, LogEvent{Name: LogEventBroadcast, Outcome: "success", RequestID: opts.RequestID, RoomID: roomID, Count: result.Succeeded + result.Offline})
	if result.Failed > 0 {
		e.observability.record(ctx, Metric{Name: MetricBroadcastFailures, Kind: MetricCounter, Value: int64(result.Failed), RequestID: opts.RequestID, RoomID: roomID})
	}
	return result
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_room_test.go
func TestChapter14RoomBroadcast(t *testing.T) {
	engine, pusher, _ := newChapter09Engine(t, nil)
	first, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.HandleConnect(context.Background(), ws.ConnectEvent{ConnectionID: "room-c2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []ws.ConnectResult{first, second} {
		ctx := &ws.Context{Context: context.Background(), ConnectionID: item.ConnectionID, SessionID: item.SessionID, Principal: ws.Principal{Subject: "anonymous"}}
		if err := engine.JoinRoom(ctx, "lobby"); err != nil {
			t.Fatal(err)
		}
	}
	result := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message", Payload: []byte(`{"text":"hello"}`)}, ws.BroadcastOptions{})
	if result.Err != nil || result.Succeeded != 2 || pusher.CallCount() != 2 {
		t.Fatalf("broadcast=%#v calls=%d", result, pusher.CallCount())
	}
	excluded := engine.Broadcast(context.Background(), "lobby", ws.OutboundMessage{Type: ws.TypeEvent, Path: "/room/message"}, ws.BroadcastOptions{ExcludeSender: true, SenderSessionID: first.SessionID})
	if excluded.Err != nil || excluded.Succeeded != 1 {
		t.Fatalf("excluded=%#v", excluded)
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

两个成员都收到广播，排除发送者后只有另一成员收到，统计与调用次数一致。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把游戏战斗状态写入通用 RoomStore。
- 为每个成员无上限启动 goroutine。
- 一个连接 gone 就停止整个广播。

### 小结

本章完成了“房间与有界广播”。两个成员都收到广播，排除发送者后只有另一成员收到，统计与调用次数一致。

下一节：第 15 节“DynamoDB 连接与会话”。
