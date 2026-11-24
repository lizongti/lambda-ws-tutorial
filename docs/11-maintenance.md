# 第 11 节：心跳与 Maintenance

> 目标：完成本章代码 ZIP 中的“心跳与 Maintenance”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

死连接不能永久占用会话和房间。应用心跳与外部触发的 Maintenance 共同提供可重入清理，但不改变 API Gateway 的硬生命周期。

完成本节后，你将能够：

- 解释并应用 **last-seen**：合法 pong 或应用消息通过 generation 条件更新。
- 解释并应用 **stale cleanup**：超时连接按 generation 与 last-seen 条件删除。
- 解释并应用 **Maintenance**：由定时 Lambda 或应用显式调用，不依赖进程内永久 goroutine。

本章与前一节的关系：保留第 10 节“认证、授权与 Principal”的累计代码，再加入本章能力。

### 过渡实现与后续替换

用 maintenance.go 的 HandleDisconnect、cleanupConnection 和 MaintenanceResult 替换 stage_core_support.go 的对应过渡实现，临时文件只保留重连与离线符号。

## 2. 关键概念与设计

### last-seen

合法 pong 或应用消息通过 generation 条件更新。

### stale cleanup

超时连接按 generation 与 last-seen 条件删除。

### Maintenance

由定时 Lambda 或应用显式调用，不依赖进程内永久 goroutine。

### 实现约束

1. Pusher gone 与定时维护共用清理函数。
1. 维护按批次处理，单条冲突或失败不破坏其他记录。
1. 过期 Session、离线和幂等记录也有界清理。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 心跳配置读取

超时截止时间完全来自注入 clock 与配置，测试不需要真实等待。

在 `ws/heartbeat.go` 中加入或保留以下实现：

```go path=ws/heartbeat.go
func (e *Engine) heartbeatCutoff(now time.Time) time.Time {
	return now.Add(-e.Options.Heartbeat.Timeout)
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 条件断连清理

重复与旧 generation 事件成为安全 no-op，缺失连接也能重试次级清理。

在 `ws/maintenance.go` 中加入或保留以下实现：

```go path=ws/maintenance.go
func (e *Engine) HandleDisconnect(ctx context.Context, event DisconnectEvent) (err error) {
	if e == nil || e.Options == nil {
		return ErrInternal
	}
	defer func() {
		if recover() != nil {
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return ErrInvalidEvent
	}
	if err := ValidateConnectionID(event.ConnectionID); err != nil {
		return fmt.Errorf("%w: connectionId", ErrInvalidEvent)
	}
	connection, err := e.Options.StateStore.GetConnection(ctx, event.ConnectionID)
	if wsstore.IsNotFound(err) {
		// A previous attempt may have deleted the physical connection before one
		// of the secondary writes failed. Retry those writes with the fencing
		// metadata carried by the event instead of treating NotFound as proof
		// that the whole disconnect completed.
		if err := e.cleanupMissingConnection(ctx, event); err != nil {
			e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "failure", RequestID: event.RequestID, ConnectionID: event.ConnectionID, SessionID: event.SessionID, Generation: event.Generation, ErrorCode: safeErrorCode(err)})
			return err
		}
		e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "duplicate", RequestID: event.RequestID, ConnectionID: event.ConnectionID})
		return nil
	}
	if err != nil {
		return err
	}
	if event.Generation != 0 && event.Generation != connection.Generation {
		return nil
	}
	if event.SessionID != "" && event.SessionID != connection.SessionID {
		return nil
	}
	if err := e.cleanupConnection(ctx, connection, "disconnect"); err != nil {
		if isFencingError(err) {
			return nil
		}
		e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "failure", RequestID: event.RequestID, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, ErrorCode: safeErrorCode(err)})
		return err
	}
	e.observability.log(ctx, LogEvent{Name: LogEventDisconnect, Outcome: "success", RequestID: event.RequestID, ConnectionID: connection.ConnectionID, SessionID: connection.SessionID, Generation: connection.Generation})
	return nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 批量维护

汇总 stale、expired、conflict、failed 和跳过数量，结果不包含 payload 或秘密。

在 `ws/maintenance.go` 中加入或保留以下实现：

```go path=ws/maintenance.go
func (e *Engine) RunMaintenanceWithResult(ctx context.Context, now time.Time) (result MaintenanceResult, err error) {
	if e == nil || e.Options == nil {
		return result, ErrInternal
	}
	defer func() {
		if recover() != nil {
			err = ErrInternal
		}
	}()
	if ctx == nil {
		return result, ErrInvalidEvent
	}
	if now.IsZero() {
		now = e.Options.Clock.Now()
	}
	var errs []error
	type candidateKey struct {
		connectionID string
		generation   uint64
	}
	seen := make(map[candidateKey]struct{})
	connections, listErr := e.Options.StateStore.ListStaleConnections(ctx, e.heartbeatCutoff(now), e.Options.Heartbeat.MaintenanceBatchSize)
	if listErr != nil {
		result.Failed++
		errs = append(errs, listErr)
	} else {
		result.StaleScanned = len(connections)
		for _, connection := range connections {
			seen[candidateKey{connectionID: connection.ConnectionID, generation: connection.Generation}] = struct{}{}
			if cleanupErr := e.cleanupConnection(ctx, connection, "stale"); cleanupErr != nil {
				if isFencingError(cleanupErr) {
					result.Conflicts++
					continue
				}
				result.Failed++
				errs = append(errs, cleanupErr)
				continue
			}
			result.StaleCleaned++
		}
	}

	if _, supported := e.stateStore.(wsstore.ConnectionExpirationStore); !supported {
		result.Skipped++
	} else {
		expirationStore := e.Options.StateStore.(wsstore.ConnectionExpirationStore)
		expiredConnections, listErr := expirationStore.ListExpiredConnections(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize)
		if listErr != nil {
			result.Failed++
			errs = append(errs, listErr)
		} else {
			result.ExpiredScanned = len(expiredConnections)
			for _, connection := range expiredConnections {
				key := candidateKey{connectionID: connection.ConnectionID, generation: connection.Generation}
				if _, duplicate := seen[key]; duplicate {
					result.Skipped++
					continue
				}
				seen[key] = struct{}{}
				if cleanupErr := e.cleanupConnection(ctx, connection, "expired"); cleanupErr != nil {
					if isFencingError(cleanupErr) {
						result.Conflicts++
						continue
					}
					result.Failed++
					errs = append(errs, cleanupErr)
					continue
				}
				result.ExpiredCleaned++
			}
		}
	}

	if err := e.Options.StateStore.DeleteExpiredSessions(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredSessionBatches = 1
	}
	if err := e.Options.StateStore.DeleteExpired(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredOfflineBatches = 1
	}
	if err := e.Options.StateStore.DeleteExpiredIdempotency(ctx, now, e.Options.Heartbeat.MaintenanceBatchSize); err != nil {
		result.Failed++
		errs = append(errs, err)
	} else {
		result.ExpiredIdempotencyBatches = 1
	}

	joined := errors.Join(errs...)
	outcome := "success"
	if joined != nil {
		outcome = "partial_failure"
	}
	e.recordMaintenanceObservability(ctx, result, outcome, joined)
	return result, joined
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_maintenance_test.go
func TestChapter11HeartbeatAndMaintenance(t *testing.T) {
	engine, _, store := newChapter09Engine(t, nil)
	now := engine.Clock.Now()
	chapter09Seed(t, engine, store, "stale", "stale-session")
	stale, _ := store.GetConnection(context.Background(), "stale")
	stale.LastSeenAt = now.Add(-2 * engine.HeartbeatTimeout())
	if err := store.PutConnection(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunMaintenanceWithResult(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if result.StaleCleaned != 1 {
		t.Fatalf("maintenance=%#v", result)
	}
	if _, err := store.GetConnection(context.Background(), "stale"); !wsstore.IsNotFound(err) {
		t.Fatalf("stale connection remains: %v", err)
	}
	if engine.HeartbeatInterval() <= 0 || engine.HeartbeatTimeout() <= engine.HeartbeatInterval() {
		t.Fatal("invalid heartbeat configuration")
	}
	_ = time.Second
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

超过 timeout 的连接被清理，当前绑定与 Session 保留策略不被旧事件破坏。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 认为 ping/pong 能延长 API Gateway 的最长连接时间。
- 用 stale 快照无条件删除连接。
- 在 Lambda 初始化时启动永久维护 goroutine。

### 小结

本章完成了“心跳与 Maintenance”。超过 timeout 的连接被清理，当前绑定与 Session 保留策略不被旧事件破坏。

下一节：第 12 节“Session 与原子重连”。
