# 第 20 节：跨后端集成与故障注入

> 目标：完成本章代码 ZIP 中的“跨后端集成与故障注入”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

最后用同一组集成场景运行 Memory、DynamoDB fake 和 Redis miniredis，并执行故障注入、race、vet、重复并发和终态哈希核对。

完成本节后，你将能够：

- 解释并应用 **后端矩阵**：同一测试函数对三个后端断言相同可观察语义。
- 解释并应用 **故障注入**：脚本化 gone、暂时错误、慢目标与存储冲突，验证边界而非只测成功路径。
- 解释并应用 **终态一致**：ZIP 解包后的 ws 路径集合与 SHA-256 必须和只读源快照完全一致。

本章与前一节的关系：保留第 19 节“server、Factory 与可观测性”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用 Source_Snapshot 的完整 ws 和现有 WebSocket 测试替换前 19 节的精简教学测试；最终 ws 不做格式化或改写，并逐文件核对 SHA-256。

## 2. 关键概念与设计

### 后端矩阵

同一测试函数对三个后端断言相同可观察语义。

### 故障注入

脚本化 gone、暂时错误、慢目标与存储冲突，验证边界而非只测成功路径。

### 终态一致

ZIP 解包后的 ws 路径集合与 SHA-256 必须和只读源快照完全一致。

### 实现约束

1. 集成测试使用真实 Engine 和 StateStore 代码，外部服务全部进程内模拟。
1. 并发场景覆盖 reconnect/disconnect、replay/ack 和 broadcast/cleanup。
1. 测试退出码、日志、PDF 检查与哈希报告共同构成证据，不能只信生成器成功。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 运行三后端矩阵

每个子测试构造独立 fixture，复用同一业务场景且明确后端名称。

在 `tests/ws_integration_test.go` 中加入或保留以下实现：

```go path=tests/ws_integration_test.go
func runWSIntegrationBackendsConfigured(t *testing.T, configure func(*ws.Options), test func(t *testing.T, f *wsIntegrationFixture)) {
	t.Helper()
	for _, backend := range []wsIntegrationBackend{wsIntegrationMemory, wsIntegrationDynamoDB, wsIntegrationRedis} {
		t.Run(string(backend), func(t *testing.T) {
			test(t, newWSIntegrationFixture(t, backend, configure))
		})
	}
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 验证完整连接闭环

从 API Gateway 事件进入 Engine，检查同连接回复、推送与断连清理。

在 `tests/ws_integration_test.go` 中加入或保留以下实现：

```go path=tests/ws_integration_test.go
func TestWSIntegrationConnectRequestReplyDisconnect(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx := context.Background()
		connect := wsIntegrationGatewayEvent("connection-e2e", "request-connect", ws.RouteConnect, "")
		response, err := f.engine.HandleAPIGatewayEvent(ctx, connect)
		if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
			t.Fatalf("connect response = %+v, error = %v", response, err)
		}
		sessionID := f.onlySessionID(t)
		session, err := f.store.GetSession(ctx, sessionID)
		if err != nil || session.CurrentConnectionID != "connection-e2e" || session.Generation != 1 {
			t.Fatalf("session after API Gateway connect = %+v, %v", session, err)
		}

		f.engine.Handle("/integration/echo", func(handlerCtx *ws.Context) {
			handlerCtx.SetResponse(handlerCtx.Payload)
		})
		body, err := ws.EncodeEnvelope(ws.Envelope{
			Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/integration/echo",
			RequestID: "request-e2e", MessageID: "message-e2e", SessionID: sessionID,
			Payload: json.RawMessage(`{"echo":"through-api-gateway-adapter"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		request := wsIntegrationGatewayEvent("connection-e2e", "gateway-request-e2e", ws.DefaultRouteKey, string(body))
		response, err = f.engine.HandleAPIGatewayEvent(ctx, request)
		if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
			t.Fatalf("request response = %+v, error = %v", response, err)
		}
		calls := f.pusher.Calls()
		if len(calls) != 1 || calls[0].ConnectionID != "connection-e2e" {
			t.Fatalf("request pushes = %+v, want one reply to connection-e2e", calls)
		}
		reply := wsEngineDecodeCall(t, calls[0])
		if reply.Type != ws.TypeResponse || reply.RequestID != "request-e2e" || reply.SessionID != sessionID ||
			!bytes.Contains(reply.Payload, []byte("through-api-gateway-adapter")) {
			t.Fatalf("end-to-end reply = %+v", reply)
		}

		var explicitReplyErr error
		f.engine.Handle("/integration/reply", func(handlerCtx *ws.Context) {
			handlerCtx.ConnectionID = "connection-untrusted"
			explicitReplyErr = f.engine.Reply(handlerCtx, json.RawMessage(`{"kind":"explicit-reply"}`))
		})
		f.engine.Handle("/integration/failure", func(handlerCtx *ws.Context) {
			handlerCtx.ConnectionID = "connection-untrusted"
			handlerCtx.AbortWithError(ws.NewHandlerError("integration_denied", "safe integration failure"))
		})
		if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, ws.ConnectResult{
			ConnectionID: "connection-e2e", SessionID: sessionID, Generation: 1,
		}, "message-explicit-reply", "request-explicit-reply", "/integration/reply", nil)); err != nil || explicitReplyErr != nil {
			t.Fatalf("explicit Reply error = %v / %v", err, explicitReplyErr)
		}
		if err := f.engine.Push(ctx, "connection-e2e", ws.NewEvent(
			"/integration/push", "request-active-push", json.RawMessage(`{"kind":"active-push"}`),
		)); err != nil {
			t.Fatalf("active Push() error = %v", err)
		}
		if err := f.engine.HandleMessage(ctx, wsEngineMessageEvent(t, ws.ConnectResult{
			ConnectionID: "connection-e2e", SessionID: sessionID, Generation: 1,
		}, "message-error", "request-error", "/integration/failure", nil)); err != nil {
			t.Fatalf("error Envelope request error = %v", err)
		}
		calls = f.pusher.Calls()
		if len(calls) != 4 {
			t.Fatalf("Reply/Push/error push count = %d, want 4", len(calls))
		}
		for index, wantType := range []string{ws.TypeResponse, ws.TypeResponse, ws.TypeEvent, ws.TypeError} {
			if calls[index].ConnectionID != "connection-e2e" {
				t.Errorf("push %d targeted %q, want connection-e2e", index, calls[index].ConnectionID)
			}
			envelope := wsEngineDecodeCall(t, calls[index])
			if envelope.Type != wantType || envelope.SessionID != sessionID {
				t.Errorf("push %d Envelope = %+v, want type %s and session %s", index, envelope, wantType, sessionID)
			}
		}
		errorEnvelope := wsEngineDecodeCall(t, calls[3])
		if errorEnvelope.RequestID != "request-error" || errorEnvelope.Error == nil ||
			errorEnvelope.Error.Code != "integration_denied" || errorEnvelope.Error.Message != "safe integration failure" {
			t.Fatalf("error Envelope = %+v", errorEnvelope)
		}

		disconnect := wsIntegrationGatewayEvent("connection-e2e", "request-disconnect", ws.RouteDisconnect, "")
		for attempt := 0; attempt < 2; attempt++ {
			response, err = f.engine.HandleAPIGatewayEvent(ctx, disconnect)
			if err != nil || response.StatusCode != http.StatusOK || response.Body != "" {
				t.Fatalf("disconnect %d response = %+v, error = %v", attempt+1, response, err)
			}
		}
		if _, err := f.store.GetConnection(ctx, "connection-e2e"); !wsstore.IsNotFound(err) {
			t.Fatalf("connection after disconnect = %v, want not-found", err)
		}
		session, err = f.store.GetSession(ctx, sessionID)
		if err != nil || session.CurrentConnectionID != "" || session.Generation != 1 {
			t.Fatalf("session after disconnect = %+v, %v", session, err)
		}
	})
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 交错重连与确认

并发执行生命周期与离线操作，用 race 和重复运行暴露世代竞争。

在 `tests/ws_integration_test.go` 中加入或保留以下实现：

```go path=tests/ws_integration_test.go
func TestWSIntegrationConcurrentDisconnectReconnectReplayAck(t *testing.T) {
	runWSIntegrationBackends(t, func(t *testing.T, f *wsIntegrationFixture) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for iteration := 0; iteration < 6; iteration++ {
			suffix := fmt.Sprintf("-%02d", iteration)
			first := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent("connection-race-old"+suffix, "alice", "", ""))
			if err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
				ConnectionID: first.ConnectionID, SessionID: first.SessionID, Generation: first.Generation,
			}); err != nil {
				t.Fatalf("initial disconnect error = %v", err)
			}
			messageID := "message-race" + suffix
			if _, err := f.engine.PushToSessionWithResult(ctx, first.SessionID, wsOfflineTestMessage(messageID, iteration+1)); err != nil {
				t.Fatalf("offline push error = %v", err)
			}
			second := mustWSSessionTestConnect(t, f.engine, wsSessionTestConnectEvent(
				"connection-race-second"+suffix, "alice", first.SessionID, first.ResumeCredential,
			))
			ackEvent := wsLifecycleControlEvent(t, second, ws.NewAck("request-race-ack"+suffix, messageID))
			// Make the surviving generation-2 delivery eligible for the winning
			// reconnect to fence and resend it at generation 3.
			f.clock.Advance(f.options.Offline.AckTimeout)

			type outcome struct {
				kind      string
				connected ws.ConnectResult
				err       error
			}
			start := make(chan struct{})
			outcomes := make(chan outcome, 4)
			var wg sync.WaitGroup
			wg.Add(4)
			go func() {
				defer wg.Done()
				<-start
				err := f.engine.HandleDisconnect(ctx, ws.DisconnectEvent{
					ConnectionID: second.ConnectionID, SessionID: second.SessionID, Generation: second.Generation,
				})
				outcomes <- outcome{kind: "disconnect", err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				connected, err := f.engine.HandleConnect(ctx, wsSessionTestConnectEvent(
					"connection-race-third"+suffix, "alice", first.SessionID, first.ResumeCredential,
				))
				outcomes <- outcome{kind: "reconnect", connected: connected, err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				_, err := f.engine.ReplayOffline(ctx, first.SessionID)
				outcomes <- outcome{kind: "replay", err: err}
			}()
			go func() {
				defer wg.Done()
				<-start
				outcomes <- outcome{kind: "ack", err: f.engine.HandleMessage(ctx, ackEvent)}
			}()
			close(start)
			wg.Wait()
			close(outcomes)

			var current ws.ConnectResult
			for outcome := range outcomes {
				switch outcome.kind {
				case "reconnect":
					if outcome.err != nil {
						t.Fatalf("iteration %d reconnect error = %v", iteration, outcome.err)
					}
					current = outcome.connected
				case "disconnect", "ack":
					if outcome.err != nil {
						t.Fatalf("iteration %d %s error = %v", iteration, outcome.kind, outcome.err)
					}
				case "replay":
					if outcome.err != nil && !errors.Is(outcome.err, ws.ErrConnectionNotFound) &&
						!errors.Is(outcome.err, ws.ErrSessionNotFound) && !errors.Is(outcome.err, ws.ErrStaleConnection) &&
						!wsstore.IsConditionConflict(outcome.err) && !wsstore.IsNotFound(outcome.err) {
						t.Fatalf("iteration %d replay returned non-fencing error = %v", iteration, outcome.err)
					}
				}
			}
			if current.ConnectionID != "connection-race-third"+suffix || current.Generation != 3 || !current.Reconnected {
				t.Fatalf("iteration %d winning reconnect = %+v", iteration, current)
			}
			stored, err := f.store.GetSession(ctx, first.SessionID)
			if err != nil || stored.CurrentConnectionID != current.ConnectionID || stored.Generation != current.Generation {
				t.Fatalf("iteration %d final binding = %+v, %v; current = %+v", iteration, stored, err, current)
			}
			connection, err := f.store.GetConnection(ctx, current.ConnectionID)
			if err != nil || connection.Generation != current.Generation || connection.State != wsstore.ConnectionStateActive {
				t.Fatalf("iteration %d current connection = %+v, %v", iteration, connection, err)
			}
			if _, err := f.store.GetConnection(ctx, second.ConnectionID); !wsstore.IsNotFound(err) {
				t.Fatalf("iteration %d replaced connection remains: %v", iteration, err)
			}

			// Any ack which won before reconnect may already have removed the entry.
			// Otherwise, a replay which overlapped the generation transition may have
			// retained the old sent state. Advance past ackTimeout and drain through
			// the winning Engine binding before issuing the final idempotent ack.
			f.clock.Advance(f.options.Offline.AckTimeout)
			if _, err := f.engine.ReplayOffline(ctx, first.SessionID); err != nil {
				t.Fatalf("iteration %d final replay error = %v", iteration, err)
			}
			mustWSOfflineTestAck(t, f.engine, current, messageID)
			pending, err := f.store.ListPending(ctx, first.SessionID, 10)
			if err != nil || len(pending) != 0 {
				t.Fatalf("iteration %d pending after winning ack = %+v, %v", iteration, pending, err)
			}
			beforeReplay := f.pusher.CallCount()
			replay, err := f.engine.ReplayOffline(ctx, first.SessionID)
			if err != nil || replay.Attempted != 0 || f.pusher.CallCount() != beforeReplay {
				t.Fatalf("iteration %d acknowledged message replayed again: %+v, %v", iteration, replay, err)
			}
		}
	})
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

最终章直接运行仓库现有完整 WebSocket 测试，不再使用前面章节的精简测试。

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

最终章还需要执行：

```sh
go vet -mod=readonly ./...
go test -mod=readonly -race -count=20 ./tests -run "TestWSIntegrationConcurrentDisconnectReconnectReplayAck|TestWSIntegrationRaceBroadcastAndCleanup"
```

### 预期结果

全部普通测试、race、vet 和关键并发重复场景通过；最终 ws 无缺失、额外或哈希差异。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 把 DynamoDB fake 当成 DynamoDB Local 或真实 AWS。
- 只运行工作目录测试而不测试 ZIP 解包结果。
- 只比较最终文件数量，忽略额外文件和逐文件哈希。

### 小结

本章完成了“跨后端集成与故障注入”。全部普通测试、race、vet 和关键并发重复场景通过；最终 ws 无缺失、额外或哈希差异。

教程结束：请保留最终 PDF、ZIP、测试日志和终态哈希报告，便于复核。
