export const guides = {
1: {
  problem: '在连接、会话和 AWS 适配之前，先固定线上 JSON 协议。这样后续每一层都能共享同一套类型、错误码和边界检查。',
  concepts: [['Envelope', '传输无关的消息信封，业务 payload 保持 json.RawMessage。'], ['ProtocolError', '可安全返回客户端的稳定错误，不携带堆栈或依赖细节。'], ['边界校验', '在业务 Handler 之前拒绝过大、非法 UTF-8、未知版本和缺字段消息。']],
  design: ['先定义 request、response、event、ping、pong、ack、error 七类语义。', '编码和解码都执行大小限制，解码还要拒绝第二个顶层 JSON 值。', '响应只继承可信 requestId；业务 DTO 不进入框架协议。'],
  steps: [
    ['定义消息信封', 'ws/protocol.go', 'type Envelope struct', '字段只描述传输语义，omitempty 控制可选字段，业务数据不会被提前解码成固定结构。'],
    ['严格解码入口', 'ws/protocol.go', 'func DecodeEnvelopeWithLimit', '先检查大小，再用 Decoder.DisallowUnknownFields 和 EOF 检查阻止未知字段与拼接 JSON。'],
    ['安全错误输出', 'ws/protocol.go', 'func NewErrorFromError', '只有明确安全的 HandlerError 才能暴露 code/message，其余错误统一映射为 internal_error。']],
  mistakes: ['只检查 payload 大小而忘记整个 Envelope 的大小。', '把 json.Unmarshal 的错误原文直接发送给客户端。', '在协议层引入游戏 DTO，导致通用框架与业务耦合。'],
  expected: 'round-trip 保留字段与中文 payload；未知版本、缺 path 和超限输入均被拒绝。'
},
2: {
  problem: '协议已经可运行，下一步需要一种可组合、可校验的启动配置，避免把默认值、YAML 和测试依赖散落在业务代码里。',
  concepts: [['Option', '按参数顺序修改 Options，代码配置与 YAML 使用同一覆盖路径。'], ['Validate', '启动时集中检查后端、认证、时间、批次和 endpoint 组合。'], ['依赖注入', 'Clock、IDGenerator、Pusher、StateStore 等由调用方提供，测试不需要真实服务。']],
  design: ['默认值先复制再应用 Option，避免全局默认对象被某次启动修改。', '解析错误在 Option 应用时 panic，与仓库其他 Lambda 模块一致；Engine 启动前再执行 Validate。', '敏感 endpoint、Redis 地址和依赖值不进入 String/GoString。'],
  steps: [
    ['组合配置与依赖', 'ws/options.go', 'type Options struct', '配置字段按 YAML 名称分组，运行时依赖集中在同一结构中，Factory 后续只消费这一份配置。'],
    ['应用 Option', 'ws/options.go', 'func NewOptions', '复制默认值后按顺序应用，最后一个设置生效，同时保留可预测的覆盖规则。'],
    ['解析严格 YAML', 'ws/config.go', 'func WithConfig', '支持 ws 子树和完整文档，未知字段与类型错误不会悄悄通过。']],
  mistakes: ['直接修改全局 defaultOptions。', '只在 YAML 路径校验而绕过代码 Option 路径。', '为了打印调试信息泄露 Redis URL 中的凭据。'],
  expected: '10s/30s 的 YAML 覆盖生效；共享部署使用 memory 时 Validate 失败；中国区 endpoint 正确生成。'
},
3: {
  problem: '连接、会话、房间和离线数据会跨 Lambda 实例共享。先写清存储契约，才能用同一组语义实现 Memory、DynamoDB 和 Redis。',
  concepts: [['Connection_ID', '一次 API Gateway 物理连接。'], ['Session_ID', '跨重连保留的逻辑会话。'], ['generation', '绑定代数，用于拒绝旧连接迟到的事件。'], ['条件更新', '比较 generation 后在存储内部原子写入，不能由调用方先读后写。']],
  design: ['StateStore 组合五类接口，保证初始化时选择一套一致后端。', '可选扩展接口表达 freshness、expiration 和 fenced room 操作，避免破坏已有自定义后端。', '统一错误码把 not_found、condition_conflict、unavailable 等语义与具体数据库错误分离。'],
  steps: [
    ['统一存储错误', 'ws/store/store.go', 'func NewError', '外部错误文本只包含操作和稳定分类，底层原因保留在 Unwrap 中供服务端诊断。'],
    ['连接条件接口', 'ws/store/store.go', 'type ConnectionStore interface', 'Delete、Touch 和 stale 查询直接表达 generation 与分页约束。'],
    ['会话原子绑定', 'ws/store/store.go', 'type SessionStore interface', 'BindConnection 必须原子检查 expectedGeneration、递增代数并替换当前连接。']],
  mistakes: ['把 Connection_ID 当成可跨重连身份。', '在 Engine 中先 Get 再 Put 并称之为原子绑定。', '把 DynamoDB/Redis 原始错误文本暴露给客户端。'],
  expected: '错误分类和模型绑定测试通过，后续三个后端可以复用同一契约。'
},
4: {
  problem: '先用单进程 Memory 实现连接和会话条件语义，建立可观察、可做 race 验证的参照实现。',
  concepts: [['RWMutex', '同一锁保护连接、会话和索引，保证跨模型操作的一致性。'], ['副本隔离', '返回结构或 RawMessage 的副本，避免调用方绕过锁修改内部状态。'], ['CAS', 'BindConnection 在同一个临界区检查并递增 generation。']],
  design: ['Memory 只用于开发和测试，不能用于多实例共享部署。', 'stale 查询使用稳定排序和明确批次上限。', '旧连接解绑必须失败或成为 no-op，不能清除新连接。'],
  steps: [
    ['并发安全 Store', 'ws/store/memory/store.go', 'type Store struct', '所有 map 与序列状态归属于同一 Store，后续房间和离线数据也复用这把锁。'],
    ['条件删除连接', 'ws/store/memory/connection_session.go', 'func (s *Store) DeleteConnectionIfMatch', '记录不存在时幂等成功，generation 不同则返回条件冲突。'],
    ['原子重绑会话', 'ws/store/memory/connection_session.go', 'func (s *Store) BindConnection', '比较当前连接和 generation 后一次性更新绑定，旧事件无法覆盖新值。']],
  mistakes: ['给每个 map 使用互不相关的锁，造成跨模型中间状态。', '返回内部 map 或字节切片的别名。', '忽略 go test -race，误以为加锁声明就足够。'],
  expected: '重绑后 generation 为 2，旧解绑返回冲突，8 个并发 Touch 在 race 下保持正确状态。'
},
5: {
  problem: '连接和会话已经可靠，本节补齐 Memory 的完整 StateStore：通用房间、离线队列和入站幂等。',
  concepts: [['幂等成员', 'roomId + sessionId 决定唯一成员，重复 Join 不产生重复记录。'], ['离线 sequence', '每个 Session 内单调递增，为重连补发提供确定顺序。'], ['幂等预留', 'messageId 首次 Begin 获得处理权，后续调用看到已有记录。']],
  design: ['队列限制和 reject-new/evict-oldest 在锁内执行。', 'Ack 校验 Session、messageId 和 generation，旧世代不能删除新世代消息。', '删除 Session 时同步清理房间、离线和幂等索引。'],
  steps: [
    ['加入通用房间', 'ws/store/memory/room.go', 'func (s *Store) Join', '成员只保存框架绑定，不包含任何游戏房间状态。'],
    ['有界离线入队', 'ws/store/memory/offline.go', 'func (s *Store) Append', '分配 sequence、复制 Envelope 并应用队列上限，避免重试共享底层字节。'],
    ['预留消息处理权', 'ws/store/memory/idempotency.go', 'func (s *Store) BeginIdempotency', '同一个临界区完成查找与创建，并发调用者不能同时获得 acquired=true。']],
  mistakes: ['只限制消息条数却忘记过期索引。', '把 postSuccess 当作客户端业务确认。', '允许旧 generation 的 Ack 删除当前消息。'],
  expected: '房间成员、离线 sequence、MarkSent/Ack 和重复 Begin 的行为符合契约，并通过 race。'
},
6: {
  problem: 'Lambda 不持有客户端 socket。所有业务出站消息都必须通过 API Gateway Management API 的 PostToConnection 发回已有 Connection_ID。',
  concepts: [['Pusher', '只接收 connectionID 和序列化字节的传输抽象。'], ['gone', '目标连接已失效，应清理而不是继续暂时性重试。'], ['有界退避', '仅 throttled/unavailable 等暂时错误按上限重试。']],
  design: ['管理 endpoint 只来自可信部署配置，不从消息 payload 构造。', '请求体在调用前后复制，FakePusher 记录也避免别名。', 'Lambda 返回值只表示集成状态，不替代应用推送。'],
  steps: [
    ['定义推送抽象', 'ws/pusher.go', 'type Pusher interface', '接口足够小，生产使用 AWS 适配器，测试使用 FakePusher。'],
    ['分类推送错误', 'ws/pusher.go', 'func NewPushError', 'gone/invalid 永久失败，throttled/unavailable 才标记 Retryable。'],
    ['构造可信 endpoint', 'ws/apigateway_pusher.go', 'func BuildAPIGatewayManagementEndpoint', '校验 API ID、region、stage，并按 AWS 分区选择域名。']],
  mistakes: ['尝试在 Lambda 中保存可写 WebSocket。', '对 410 Gone 做指数退避。', '让客户端 Envelope 指定任意管理 endpoint。'],
  expected: 'FakePusher 记录同一 Connection_ID 和原始 JSON，gone 与暂时错误分类正确。'
},
7: {
  problem: '消息协议和推送已经就绪，需要把 path 分发给应用 Handler，同时保留可信连接身份和可控的中间件终止语义。',
  concepts: [['Context', '保存 Connection_ID、Session_ID、Principal、requestId 和原始 payload。'], ['中间件链', 'Use 注册公共逻辑，Abort 阻止后续 Handler。'], ['通配路径', '精确匹配之外支持仓库已有的 *path 约定。']],
  design: ['路由 panic 在边界恢复并转为安全 internal_error。', ' originating connection 在分发边界固定，Handler 不能通过修改 payload 改推送目标。', 'NoRoute 返回稳定 route_not_found，而不是静默成功。'],
  steps: [
    ['保存可信上下文', 'ws/context.go', 'type Context struct', '公开必要的请求信息，内部字段保存原始目标和路由匹配状态。'],
    ['注册路由', 'ws/router.go', 'func (r *Router) Handle', '规范化 pattern 并保存 Handler 链，业务只关心 path 与 payload。'],
    ['执行分发', 'ws/router.go', 'func (r *Router) Dispatch', '按确定顺序执行中间件与 Handler，并处理 Abort、NoRoute 和 panic。']],
  mistakes: ['让业务 Handler 直接读取 AWS SDK 事件。', 'Abort 后仍继续执行后续 Handler。', '把 route key 与 Envelope path 混为一层。'],
  expected: '中间件先于 Handler，/echo/*path 提取 /42，未知路径被安全中止。'
},
8: {
  problem: '本节把 API Gateway 的 $connect、$disconnect、$default 事件转换为内部事件，建立 Lambda 与传输无关 Engine 之间的薄适配层。',
  concepts: [['$connect', '认证与创建/恢复绑定的入口。'], ['$disconnect', '按存储中的当前绑定进行条件清理。'], ['$default', '文本 Envelope 的默认消息入口；自定义 route key 可映射内部 path。']],
  design: ['只复制批准的 headers、query 和 request context，认证格式仍由应用决定。', 'ResumeCredential 不进入 String/GoString 或日志。', '适配器返回 HTTP 状态码，应用消息始终走 Pusher。'],
  steps: [
    ['内部生命周期事件', 'ws/events.go', 'type ConnectEvent struct', '内部模型保留连接元数据与恢复凭据，但不绑定 AWS SDK 类型。'],
    ['映射连接事件', 'ws/server.go', 'func ConnectEventFromAPIGateway', '提取 Connection_ID、requestId、route key、认证元数据和连接时间。'],
    ['统一事件入口', 'ws/server.go', 'func (e *Engine) HandleEvent', '按 route key 选择生命周期方法，默认与未知自定义路由进入消息处理。']],
  mistakes: ['在 Lambda return body 中返回业务推送。', '把 base64 二进制数据直接当 JSON 文本。', '日志打印整个 ConnectEvent 或请求 headers。'],
  expected: '$connect 元数据、$default body 和 Connection_ID 正确映射，格式化结果不含恢复凭据。'
},
9: {
  problem: '现在把协议、存储、路由和 Pusher 组合成 Engine，完成“入站请求从同一逻辑连接收到回复”的核心闭环。',
  concepts: [['同连接回复', 'Reply 使用 Context 捕获的 originating Connection_ID。'], ['活动绑定校验', '消息处理前确认 Connection、Session、Principal 和 generation 一致。'], ['入站幂等', 'messageId 预留处理权，已完成响应可直接重放。']],
  design: ['无效消息在业务 Handler 前变成 error Envelope。', '显式 Push 不猜测目标，未知 Connection_ID 返回 not-found。', 'Pusher gone 进入统一清理与离线策略，暂时错误才有界重试。'],
  steps: [
    ['构造 Engine', 'ws/engine.go', 'func NewEngine', '校验完整依赖并包装可重试 StateStore，但不启动后台 goroutine 或网络调用。'],
    ['从原连接回复', 'ws/engine.go', 'func (e *Engine) Reply', '响应目标来自不可变 originating 字段，而不是客户端 payload。'],
    ['校验活动绑定', 'ws/engine.go', 'func (e *Engine) validateActiveBinding', '同时校验物理连接、逻辑会话、主体和 generation，拒绝旧连接继续操作。']],
  mistakes: ['依赖 Lambda 实例亲和性保存连接。', 'Reply 从 payload 读取目标 Connection_ID。', '协议错误也进入业务重试。'],
  expected: '/echo 的处理只产生一次 PostToConnection，目标严格等于入站 connection-A。'
},
10: {
  problem: '通用框架不能绑定某一种 token。通过 Authenticator/Authorizer 注入身份与策略，并保证认证失败前没有任何状态写入。',
  concepts: [['Principal', '稳定 Subject 加应用字符串 Claims，不包含游戏对象。'], ['Authenticator', '从批准的连接元数据建立身份。'], ['Authorizer', '对 path、房间和 Session 恢复等 Action 做策略判断。']],
  design: ['匿名认证使用独立类型，只有显式 development + anonymous 才能通过启动校验。', '认证失败的底层 token 错误不向外暴露。', '授权中间件在业务 Handler 前 Abort。'],
  steps: [
    ['身份模型', 'ws/auth.go', 'type Principal struct', 'Subject 是框架需要的最小稳定身份，Claims 可复制且默认格式化不泄露值。'],
    ['生产认证校验', 'ws/auth.go', 'func ValidateAuthConfig', '生产缺少认证器、匿名模式未显式开启或配置不一致都在启动时失败。'],
    ['路径授权中间件', 'ws/auth.go', 'func RequireAuthorization', '检查 Subject、调用策略并以安全 authorization_denied 终止后续处理。']],
  mistakes: ['把原始 token 放入日志或 Principal.String。', '先创建 Session 再认证。', '将游戏权限模型硬编码到 ws 框架。'],
  expected: '生产无认证器被拒绝，失败连接不留状态，授权失败转为安全错误。'
},
11: {
  problem: '死连接不能永久占用会话和房间。应用心跳与外部触发的 Maintenance 共同提供可重入清理，但不改变 API Gateway 的硬生命周期。',
  concepts: [['last-seen', '合法 pong 或应用消息通过 generation 条件更新。'], ['stale cleanup', '超时连接按 generation 与 last-seen 条件删除。'], ['Maintenance', '由定时 Lambda 或应用显式调用，不依赖进程内永久 goroutine。']],
  design: ['Pusher gone 与定时维护共用清理函数。', '维护按批次处理，单条冲突或失败不破坏其他记录。', '过期 Session、离线和幂等记录也有界清理。'],
  steps: [
    ['心跳配置读取', 'ws/heartbeat.go', 'func (e *Engine) heartbeatCutoff', '超时截止时间完全来自注入 clock 与配置，测试不需要真实等待。'],
    ['条件断连清理', 'ws/maintenance.go', 'func (e *Engine) HandleDisconnect', '重复与旧 generation 事件成为安全 no-op，缺失连接也能重试次级清理。'],
    ['批量维护', 'ws/maintenance.go', 'func (e *Engine) RunMaintenanceWithResult', '汇总 stale、expired、conflict、failed 和跳过数量，结果不包含 payload 或秘密。']],
  mistakes: ['认为 ping/pong 能延长 API Gateway 的最长连接时间。', '用 stale 快照无条件删除连接。', '在 Lambda 初始化时启动永久维护 goroutine。'],
  expected: '超过 timeout 的连接被清理，当前绑定与 Session 保留策略不被旧事件破坏。'
},
12: {
  problem: '网络中断后，客户端应凭恢复凭据把同一 Session 绑定到新 Connection_ID，同时旧连接的迟到事件不能破坏新绑定。',
  concepts: [['恢复凭据', '原始值只返回连接发起方，存储域分离 SHA-256 表示。'], ['原子重连', 'BindConnection 保留 Session_ID 并递增 generation。'], ['fencing', '消息、心跳、disconnect 和 ack 都校验当前绑定。']],
  design: ['恢复时同时校验 Session、Principal、凭据和过期时间。', 'replaceExisting 决定已有活动连接是拒绝还是替换。', '断连只条件解绑，Session 在重连窗口内继续保留。'],
  steps: [
    ['保护恢复凭据', 'ws/session.go', 'func HashResumeCredential', '哈希输入包含协议域与 Session_ID，避免凭据哈希被移植到其他会话。'],
    ['恢复已有会话', 'ws/session.go', 'func (e *Engine) resumeSession', '验证授权与凭据后执行存储原子绑定，并触发离线补发。'],
    ['创建当前连接记录', 'ws/session.go', 'func (e *Engine) newConnection', '物理连接继承当前 Session、Principal 与 generation，并初始化 last-seen。']],
  mistakes: ['在 Session 中保存明文恢复凭据。', '重连时生成新的 Session_ID。', '只校验 Connection_ID 而不校验 generation。'],
  expected: 'c1 断开后 c2 恢复同一 Session 且 generation 增加，旧 c1 disconnect 不影响 c2。'
},
13: {
  problem: 'Session 没有活动连接时，重要事件进入有界离线队列；重连后按 sequence 补发，并用明确确认策略控制删除。',
  concepts: [['postSuccess', 'API Gateway 接受推送后删除，简单但不表示客户端业务处理成功。'], ['clientAck', '推送后保留 sent 状态，当前 generation 的有效 Ack 才删除。'], ['至少一次', '重试保留原 messageId，客户端负责去重。']],
  design: ['实时 gone 或绑定变化可按策略转离线，不把持久入队报告成实时发送。', '队列有长度、过期和溢出策略。', 'replay 中断时未确认消息保留，确认消息不重复进入 pending。'],
  steps: [
    ['区分投递结果', 'ws/offline.go', 'type DeliveryResult struct', 'sent 与 offline 明确区分，调用方无需把 nil error 误解为客户端已收到。'],
    ['按会话投递', 'ws/offline.go', 'func (e *Engine) PushToSessionWithResult', '解析当前绑定、处理竞争并在无活动连接时持久入队。'],
    ['处理客户端确认', 'ws/offline.go', 'func (e *Engine) acknowledge', 'Ack 同时校验消息所有权、Session 和当前 generation。']],
  mistakes: ['补发时生成新 messageId。', '无限制保存离线消息。', 'postSuccess 成功后仍声称具备客户端确认。'],
  expected: '断连期间消息入队，重连补发一次；postSuccess 模式下成功后队列清空。'
},
14: {
  problem: '房间只管理通用成员关系。广播需要分页、有限并发和逐目标失败隔离，不能因一个失效连接取消整个批次。',
  concepts: [['RoomMember', 'roomId、sessionId、connectionId、generation 的通用绑定。'], ['有界广播', 'worker 数量受配置限制，逐目标收集结果。'], ['发送者排除', '使用可信 SenderSessionID/ConnectionID，而非 payload 字段。']],
  design: ['Join/Leave 幂等且可经过 Authorizer。', '断连、stale 和 gone 都清理成员及反向索引。', '空房间成功返回零投递，不隐式创建游戏房间。'],
  steps: [
    ['广播选项与结果', 'ws/room.go', 'type BroadcastOptions struct', '发送者与 Principal 都来自可信调用上下文，结果分别统计成功、gone、离线和失败。'],
    ['加入房间', 'ws/room.go', 'func (e *Engine) JoinRoom', '先校验当前绑定与授权，再写入或更新当前 generation 的成员关系。'],
    ['执行有界广播', 'ws/room.go', 'func (e *Engine) Broadcast', '分页读取成员并使用有限并发推送，单目标失败只进入自己的 DeliveryResult。']],
  mistakes: ['把游戏战斗状态写入通用 RoomStore。', '为每个成员无上限启动 goroutine。', '一个连接 gone 就停止整个广播。'],
  expected: '两个成员都收到广播，排除发送者后只有另一成员收到，统计与调用次数一致。'
},
15: {
  problem: 'Memory 的语义已经明确，本节用 DynamoDB 条件写、事务、GSI 和 TTL 字段实现共享连接与会话。',
  concepts: [['PK/SK', '单表内区分 Connection、Session 与索引记录。'], ['条件表达式', '在 DynamoDB 内部拒绝 generation 或绑定不匹配。'], ['TTL', '只提供最终异步删除，读取和维护仍需逻辑过期判断。']],
  design: ['codec 统一属性编解码，时间以可排序格式保存。', ' stale/expiry 查询使用 GSI 与 LastEvaluatedKey 分页。', 'ConditionalCheckFailed、限流和网络错误映射为统一存储分类。'],
  steps: [
    ['DynamoDB 客户端契约', 'ws/store/dynamodb/store.go', 'type Client interface', '窄接口覆盖实际需要的 SDK 操作，进程内 fake 可以实现同一行为。'],
    ['条件删除连接', 'ws/store/dynamodb/connection_session.go', 'func (s *Store) DeleteConnectionIfMatch', '删除与次级索引清理共享条件，旧 generation 不会误删新记录。'],
    ['事务绑定会话', 'ws/store/dynamodb/connection_session.go', 'func (s *Store) BindConnection', '事务同时更新 Session 与相关连接记录，防止共享部署出现半完成绑定。']],
  mistakes: ['把 DynamoDB TTL 当实时删除。', '忽略分页的 LastEvaluatedKey。', '用先读后写替代条件表达式。'],
  expected: '在 dynamodbtest.Fake 上完成绑定和旧解绑冲突验证；没有访问真实 DynamoDB。'
},
16: {
  problem: '本节把 DynamoDB 连接/会话扩展到完整 StateStore，并处理房间反向索引、离线队列计数和幂等记录的部分失败。',
  concepts: [['反向索引', '按 Connection_ID 找到房间成员，支持断连清理。'], ['事务队列', '消息写入、计数与溢出策略需要一致提交。'], ['故障分类', '条件冲突、暂时不可用与永久错误采用不同处理。']],
  design: ['Members 游标稳定且过滤当前 generation。', '离线 Append/ListPending/MarkSent/Ack 保留 sequence 与世代语义。', '清理部分失败返回错误并允许 Maintenance 重试，不虚报成功。'],
  steps: [
    ['清理连接房间索引', 'ws/store/dynamodb/room.go', 'func (s *Store) RemoveConnection', '通过 Connection_ID 反向索引定位成员，逐批删除且不影响其他连接。'],
    ['原子离线入队', 'ws/store/dynamodb/offline.go', 'func (s *Store) Append', '队列元数据与消息记录使用条件/事务协调 sequence 和溢出策略。'],
    ['条件确认消息', 'ws/store/dynamodb/offline.go', 'func (s *Store) Ack', '只有 Session、messageId 和 generation 匹配才删除，重复 Ack 保持幂等。']],
  mistakes: ['删除成员后遗留 Connection_ID 反向索引。', '把条件冲突当暂时错误无限重试。', '部分清理失败后直接返回全部成功。'],
  expected: 'fake 上房间、离线和幂等操作与 Memory 契约一致，故障路径可重复验证。'
},
17: {
  problem: 'Redis 共享实现使用 Lua 把条件检查和写入放在服务端一次原子执行，避免多实例读改写竞争。',
  concepts: [['key prefix', '所有连接、会话和索引共享可配置命名空间。'], ['Lua CAS', '比较 JSON 中的 generation/绑定后原子更新。'], ['sorted-set stale 索引', 'last-seen 作为分数，按截止时间有界查询。']],
  design: ['逻辑对象使用 JSON 编解码并设置 TTL。', 'redis.Nil 映射 not_found，连接与超时映射 unavailable。', '注入 clock 使逻辑过期与测试确定，miniredis 仅作为行为模拟。'],
  steps: [
    ['Redis Store 与配置', 'ws/store/redis/store.go', 'type Store struct', 'client 由调用方持有，Store 保存前缀、时钟和队列策略。'],
    ['Lua 条件删除', 'ws/store/redis/connection_session.go', 'func (s *Store) DeleteConnectionIfMatch', '读取、比较、删除及索引清理由脚本一次执行。'],
    ['Lua 原子重绑', 'ws/store/redis/connection_session.go', 'func (s *Store) BindConnection', '在脚本内校验 expectedGeneration、递增代数并更新当前连接。']],
  mistakes: ['在 Go 侧 GET 后再无条件 SET。', '消息主体过期却留下 stale/房间索引。', '把 miniredis 结果描述为真实 Redis 网络验证。'],
  expected: 'miniredis 上绑定与条件冲突符合契约，Lua 脚本原子路径被执行。'
},
18: {
  problem: '本节完成 Redis 房间、离线和幂等实现，重点保证脚本原子性与各类索引的过期清理。',
  concepts: [['房间反向索引', '连接失效时可找到并删除所有相关成员。'], ['Lua 队列', 'sequence 分配、条数上限与溢出在同一原子路径完成。'], ['幂等窗口', 'set-if-not-exists 加过期时间预留 messageId。']],
  design: ['离线和成员读取使用稳定顺序与有界批次。', 'Ack/MarkSent 都按 generation 做条件迁移。', '主体 key 过期时同步维护索引，避免无限残留。'],
  steps: [
    ['原子清理房间成员', 'ws/store/redis/room.go', 'func (s *Store) RemoveConnection', 'Lua 同时处理成员集合与 Connection_ID 反向索引。'],
    ['有界离线 Append', 'ws/store/redis/offline.go', 'func (s *Store) Append', '脚本协调 sequence、消息 body、队列计数与 reject/evict 行为。'],
    ['世代条件 Ack', 'ws/store/redis/offline.go', 'func (s *Store) Ack', '当前 generation 不匹配时不能删除消息，重复确认保持幂等。']],
  mistakes: ['把多条独立 Redis 命令误当成一个原子操作。', '只清理消息 body 而忘记索引。', '无界读取大房间或长离线队列。'],
  expected: 'miniredis 上房间、队列与幂等契约通过；真实 Redis 未纳入验证声明。'
},
19: {
  problem: '核心框架和三后端已经完整，本节接入现有 server、统一依赖构造与资源所有权，并提供安全日志和指标钩子。',
  concepts: [['Factory', '构造一套一致后端与 Pusher，只关闭自己创建的资源。'], ['lambda: ws', '沿用现有 server Option/YAML 分支，不改变其他 Lambda 类型。'], ['可观测性', '结构化 allowlist 字段记录生命周期和投递结果，不记录 payload/token。']],
  design: ['显式注入的 StateStore/Pusher 优先，所有权仍属于调用方。', 'Factory.Build 只做构造与配置加载，不主动 ping 外部服务。', 'Close 逆序关闭自建资源且可重复调用。'],
  steps: [
    ['构造完整 Engine', 'ws/factory.go', 'func (f *Factory) Build', '验证配置、选择 StateStore 和 Pusher 后创建 Engine，失败时联合同步清理已创建资源。'],
    ['选择单一后端', 'ws/factory.go', 'func (f *Factory) buildStateStore', 'memory、dynamodb、redis 只选一套完整实现，注入依赖优先。'],
    ['接入 server 选项', 'server/config.go', 'func WithWsOptions', '把 ws.Option 追加到 server.Options，与 event/http/reqresp/sqs 保持同样组合方式。']],
  mistakes: ['关闭调用方注入的 Redis client。', '连接用 DynamoDB、Session 却隐式使用 Redis。', '日志记录 payload、恢复凭据或原始 AWS 错误。'],
  expected: 'server 能接收 lambda: ws 选项，Factory/Engine 依赖完整，日志字段保持安全。'
},
20: {
  problem: '最后用同一组集成场景运行 Memory、DynamoDB fake 和 Redis miniredis，并执行故障注入、race、vet、重复并发和终态哈希核对。',
  concepts: [['后端矩阵', '同一测试函数对三个后端断言相同可观察语义。'], ['故障注入', '脚本化 gone、暂时错误、慢目标与存储冲突，验证边界而非只测成功路径。'], ['终态一致', 'ZIP 解包后的 ws 路径集合与 SHA-256 必须和只读源快照完全一致。']],
  design: ['集成测试使用真实 Engine 和 StateStore 代码，外部服务全部进程内模拟。', '并发场景覆盖 reconnect/disconnect、replay/ack 和 broadcast/cleanup。', '测试退出码、日志、PDF 检查与哈希报告共同构成证据，不能只信生成器成功。'],
  steps: [
    ['运行三后端矩阵', 'tests/ws_integration_test.go', 'func runWSIntegrationBackendsConfigured', '每个子测试构造独立 fixture，复用同一业务场景且明确后端名称。'],
    ['验证完整连接闭环', 'tests/ws_integration_test.go', 'func TestWSIntegrationConnectRequestReplyDisconnect', '从 API Gateway 事件进入 Engine，检查同连接回复、推送与断连清理。'],
    ['交错重连与确认', 'tests/ws_integration_test.go', 'func TestWSIntegrationConcurrentDisconnectReconnectReplayAck', '并发执行生命周期与离线操作，用 race 和重复运行暴露世代竞争。']],
  mistakes: ['把 DynamoDB fake 当成 DynamoDB Local 或真实 AWS。', '只运行工作目录测试而不测试 ZIP 解包结果。', '只比较最终文件数量，忽略额外文件和逐文件哈希。'],
  expected: '全部普通测试、race、vet 和关键并发重复场景通过；最终 ws 无缺失、额外或哈希差异。'
}
};
