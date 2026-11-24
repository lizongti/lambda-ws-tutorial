# 第 01 节：JSON Envelope 与协议校验

> 目标：完成本章代码 ZIP 中的“JSON Envelope 与协议校验”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。

## 本节目录

1. 学习目标与问题
2. 关键概念与设计
3. 按步骤实现
4. 运行与验证
5. 常见错误与小结

## 1. 学习目标与问题

在连接、会话和 AWS 适配之前，先固定线上 JSON 协议。这样后续每一层都能共享同一套类型、错误码和边界检查。

完成本节后，你将能够：

- 解释并应用 **Envelope**：传输无关的消息信封，业务 payload 保持 json.RawMessage。
- 解释并应用 **ProtocolError**：可安全返回客户端的稳定错误，不携带堆栈或依赖细节。
- 解释并应用 **边界校验**：在业务 Handler 之前拒绝过大、非法 UTF-8、未知版本和缺字段消息。

本章与前一节的关系：从空 Go 模块开始，先建立最小可测试协议。

## 2. 关键概念与设计

### Envelope

传输无关的消息信封，业务 payload 保持 json.RawMessage。

### ProtocolError

可安全返回客户端的稳定错误，不携带堆栈或依赖细节。

### 边界校验

在业务 Handler 之前拒绝过大、非法 UTF-8、未知版本和缺字段消息。

### 实现约束

1. 先定义 request、response、event、ping、pong、ack、error 七类语义。
1. 编码和解码都执行大小限制，解码还要拒绝第二个顶层 JSON 值。
1. 响应只继承可信 requestId；业务 DTO 不进入框架协议。

### AWS WebSocket 在整体流程中的位置

客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。

本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。

## 3. 按步骤实现

### 3.1 定义消息信封

字段只描述传输语义，omitempty 控制可选字段，业务数据不会被提前解码成固定结构。

在 `ws/protocol.go` 中加入或保留以下实现：

```go path=ws/protocol.go
type Envelope struct {
	Version   int             `json:"version"`
	Type      string          `json:"type"`
	Path      string          `json:"path,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
	MessageID string          `json:"messageId,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     *ErrorBody      `json:"error,omitempty"`
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.2 严格解码入口

先检查大小，再用 Decoder.DisallowUnknownFields 和 EOF 检查阻止未知字段与拼接 JSON。

在 `ws/protocol.go` 中加入或保留以下实现：

```go path=ws/protocol.go
func DecodeEnvelopeWithLimit(data []byte, maxBytes int) (*Envelope, error) {
	maxBytes = effectiveMessageLimit(maxBytes)
	if len(data) > maxBytes {
		return nil, newProtocolError(ErrorCodeMessageTooLarge, "", nil)
	}
	if !utf8.Valid(data) {
		return nil, newProtocolError(ErrorCodeInvalidUTF8, "", nil)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, newProtocolError(ErrorCodeInvalidJSON, "", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, newProtocolError(ErrorCodeInvalidJSON, "", err)
	}
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	return &envelope, nil
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.3 安全错误输出

只有明确安全的 HandlerError 才能暴露 code/message，其余错误统一映射为 internal_error。

在 `ws/protocol.go` 中加入或保留以下实现：

```go path=ws/protocol.go
func NewErrorFromError(requestID string, err error) OutboundMessage {
	return NewError(requestID, SafeErrorBody(err))
}
```

这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。

### 3.4 用测试固定本章行为

每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。

```go path=tests/ws_protocol_test.go
func TestChapter01EnvelopeRoundTripAndValidation(t *testing.T) {
	original := ws.Envelope{Version: ws.ProtocolVersion, Type: ws.TypeRequest, Path: "/echo", RequestID: "req-1", MessageID: "msg-1", Payload: json.RawMessage(`{"text":"你好"}`)}
	body, err := ws.EncodeEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ws.DecodeEnvelopeWithLimit(body, ws.MaxMessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Path != original.Path || string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("round trip mismatch: %#v", decoded)
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(`{"version":1,"type":"request"}`), ws.MaxMessageBytes); err == nil {
		t.Fatal("missing path was accepted")
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(`{"version":2,"type":"request","path":"/"}`), ws.MaxMessageBytes); err == nil {
		t.Fatal("unsupported version was accepted")
	}
	if _, err := ws.DecodeEnvelopeWithLimit([]byte(strings.Repeat(" ", ws.MaxMessageBytes+1)), ws.MaxMessageBytes); err == nil {
		t.Fatal("oversized input was accepted")
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

round-trip 保留字段与中文 payload；未知版本、缺 path 和超限输入均被拒绝。

测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。

### 验证边界

本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。

## 5. 常见错误与小结

- 只检查 payload 大小而忘记整个 Envelope 的大小。
- 把 json.Unmarshal 的错误原文直接发送给客户端。
- 在协议层引入游戏 DTO，导致通用框架与业务耦合。

### 小结

本章完成了“JSON Envelope 与协议校验”。round-trip 保留字段与中文 payload；未知版本、缺 path 和超限输入均被拒绝。

下一节：第 02 节“Options、默认配置与 YAML”。
