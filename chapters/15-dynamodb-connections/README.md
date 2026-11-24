# 第 15 节：DynamoDB 连接与会话

这是本教程的累计完整代码快照，不需要复制其他章节的文件。

## 环境与运行

- Go 1.24.1（工具链固定为本地版本）。
- 首次构建执行 `go mod download` 获取模块依赖。
- 验证：`go test -mod=readonly -count=1 ./...`
- 并发验证：`go test -mod=readonly -race -count=1 ./...`

## 本阶段阅读方式

先阅读同编号的 15-dynamodb-connections.pdf，再查看 `ws/` 与 `tests/`。PDF 中的代码摘录来自本 ZIP；后续章节可能重构本阶段的临时衔接实现。

## 验证边界

所有测试均在本地执行。DynamoDB 使用进程内 `dynamodbtest.Fake`，Redis 使用 miniredis，API Gateway 使用 SDK fake/FakePusher；它们不等同于真实 AWS、DynamoDB Local、Redis 或 API Gateway 网络验证。
