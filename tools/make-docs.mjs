import fs from 'node:fs';
import path from 'node:path';
import { root, chapters, chapterKey, snapshotDir, writeFile, assert } from './common.mjs';
import { guides } from './guides.mjs';

function extractBlock(file, marker) {
  const text = fs.readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
  const lines = text.split('\n');
  const start = lines.findIndex(line => line.includes(marker));
  assert(start >= 0, `${file}: marker not found: ${marker}`);
  let depth = 0;
  let opened = false;
  for (let index = start; index < lines.length; index++) {
    const line = lines[index];
    for (const character of line) {
      if (character === '{') { depth++; opened = true; }
      if (character === '}') depth--;
    }
    if (opened && depth === 0) return lines.slice(start, index + 1).join('\n');
  }
  throw new Error(`${file}: unterminated block ${marker}`);
}

function testMarker(number) {
  const prefix = `func TestChapter${String(number).padStart(2, '0')}`;
  return prefix;
}

const transitions = {
  2: '本章用 stage_runtime_dependencies.go、stage_auth_dependencies.go 和最小 store 契约隔离未来依赖。它们只用于让配置阶段独立编译，第 03、06、10 节会分别用完整契约、Pusher 和认证实现替换。',
  3: '用完整 ws/store/store.go 替换第 02 节的最小 StateStore 占位，因为后续后端需要统一模型、错误和原子操作语义。',
  5: '恢复第 04 节暂时移除的 Memory StateStore 编译断言；房间、离线和幂等方法现已齐备，不再需要过渡声明。',
  6: '删除 stage_runtime_dependencies.go 中的 Pusher、SleepFunc 和 endpoint 过渡实现，改用本章真实 pusher.go 与 apigateway_pusher.go。',
  8: '本章仅用 stage08_engine.go、stage08_factory.go 和 clone 辅助函数保持事件适配器可编译。第 09 节会替换临时 Engine/Factory，第 12 节会把 clone 辅助函数收敛到 session.go。',
  9: '用真实 engine.go 与 Factory 过渡构造器替换第 08 节的空 Engine。stage_core_support.go 暂时承接维护、重连和离线符号，第 11、12、13 节会逐一替换，第 19 节再替换 Factory。',
  10: '删除 stage_auth_dependencies.go，改用本章完整认证、授权和 Principal 实现；配置 API 保持不变，因此前两章测试仍应通过。',
  11: '用 maintenance.go 的 HandleDisconnect、cleanupConnection 和 MaintenanceResult 替换 stage_core_support.go 的对应过渡实现，临时文件只保留重连与离线符号。',
  12: '用 session.go 的 HandleConnect 和 clone 辅助函数替换临时实现；恢复凭据、原子重连和离线补发入口现在由真实会话流程负责。',
  13: '用 offline.go 的 acknowledge、enqueueOffline、replaySession 和 ReplayResult 替换最后一批核心过渡符号，并删除 stage_core_support.go。',
  15: '本节先实现 DynamoDB 连接与会话，暂时移除完整 StateStore 编译断言；第 16 节补齐其余方法后恢复，避免用空方法伪装完整后端。',
  16: '恢复 DynamoDB 的完整 StateStore 编译断言，房间、离线和幂等操作不再有过渡占位。',
  17: '本节先实现 Redis 连接与会话，完整 StateStore 断言延到第 18 节；Lua 条件更新从第一节共享后端实现就保持真实语义。',
  18: '恢复 Redis 的完整 StateStore 编译断言，并用真实房间、离线和幂等操作完成共享后端。',
  19: '用 factory.go 替换 stage_factory.go 的简单构造器，新增一致后端选择、AWS/Redis 客户端构造和明确的资源所有权。',
  20: '用 Source_Snapshot 的完整 ws 和现有 WebSocket 测试替换前 19 节的精简教学测试；最终 ws 不做格式化或改写，并逐文件核对 SHA-256。',
};

for (let index = 0; index < chapters.length; index++) {
  const chapter = chapters[index];
  const guide = guides[chapter.number];
  assert(guide, `missing guide ${chapter.number}`);
  const number = String(chapter.number).padStart(2, '0');
  const key = chapterKey(chapter);
  const snapshot = snapshotDir(chapter);
  const lines = [];
  lines.push(`# 第 ${number} 节：${chapter.title}`, '');
  lines.push(`> 目标：完成本章代码 ZIP 中的“${chapter.title}”阶段。读者需要会 Go 基础语法、接口和测试，不需要已有 AWS WebSocket 经验。`, '');
  lines.push('## 本节目录', '', '1. 学习目标与问题', '2. 关键概念与设计', '3. 按步骤实现', '4. 运行与验证', '5. 常见错误与小结', '');
  lines.push('## 1. 学习目标与问题', '', guide.problem, '');
  lines.push('完成本节后，你将能够：', '');
  for (const concept of guide.concepts) lines.push(`- 解释并应用 **${concept[0]}**：${concept[1]}`);
  lines.push('', `本章与前一节的关系：${index === 0 ? '从空 Go 模块开始，先建立最小可测试协议。' : `保留第 ${String(index).padStart(2, '0')} 节“${chapters[index - 1].title}”的累计代码，再加入本章能力。`}`, '');
  if (transitions[chapter.number]) lines.push('### 过渡实现与后续替换', '', transitions[chapter.number], '');
  lines.push('## 2. 关键概念与设计', '');
  for (const [term, explanation] of guide.concepts) lines.push(`### ${term}`, '', explanation, '');
  lines.push('### 实现约束', '');
  for (const item of guide.design) lines.push(`1. ${item}`);
  lines.push('', '### AWS WebSocket 在整体流程中的位置', '', '客户端实际连接由 API Gateway 持有。Lambda 接收事件，Engine 读取或更新 StateStore，并通过 Pusher 调用 Management API。Connection_ID 指向一次物理连接，Session_ID 才是可以跨重连保留的逻辑身份。', '', '本章仍采用同样的边界：Lambda return body 只表达集成状态，业务 response/event 通过 PostToConnection 推送。Memory、DynamoDB fake 和 miniredis 仅用于本地验证。', '');
  lines.push('## 3. 按步骤实现', '');
  for (let step = 0; step < guide.steps.length; step++) {
    const [title, relative, marker, explanation] = guide.steps[step];
    lines.push(`### 3.${step + 1} ${title}`, '', explanation, '', `在 \`${relative}\` 中加入或保留以下实现：`, '');
    const code = extractBlock(path.join(snapshot, relative), marker);
    lines.push('```go path=' + relative, code, '```', '');
    lines.push('这段代码是本章 ZIP 的连续原文。先理解输入、输出和不变量，再查看 ZIP 中该文件的其余部分；不要只复制片段而忽略它依赖的累计代码。', '');
  }
  lines.push('### 3.4 用测试固定本章行为', '', '每章都保留独立测试。测试不仅覆盖成功路径，也把本章最容易破坏的边界写成可重复断言。', '');
  if (chapter.number < 20) {
    const testFiles = fs.readdirSync(path.join(snapshot, 'tests')).filter(file => file.endsWith('_test.go')).sort();
    let selected;
    for (const file of testFiles) {
      const full = path.join(snapshot, 'tests', file);
      const text = fs.readFileSync(full, 'utf8');
      if (text.includes(testMarker(chapter.number))) { selected = { full, relative: `tests/${file}`, text }; break; }
    }
    assert(selected, `chapter ${number} test not found`);
    lines.push('```go path=' + selected.relative, extractBlock(selected.full, testMarker(chapter.number)), '```', '');
  } else {
    lines.push('最终章直接运行仓库现有完整 WebSocket 测试，不再使用前面章节的精简测试。', '');
  }
  lines.push('## 4. 运行与验证', '', '将本章 ZIP 解压到空目录，进入其根目录后执行：', '', '```sh', 'go version', 'go mod download', chapter.testCommand, '```', '');
  if (chapter.race) lines.push('再执行 race 验证：', '', '```sh', 'go test -mod=readonly -race -count=1 ./...', '```', '');
  if (chapter.number === 20) lines.push('最终章还需要执行：', '', '```sh', 'go vet -mod=readonly ./...', 'go test -mod=readonly -race -count=20 ./tests -run "TestWSIntegrationConcurrentDisconnectReconnectReplayAck|TestWSIntegrationRaceBroadcastAndCleanup"', '```', '');
  lines.push('### 预期结果', '', guide.expected, '', '测试输出中的具体耗时可以不同，但所有包必须返回 `ok` 或 `[no test files]`，命令退出码必须为 0。`-mod=readonly` 用来防止测试静默改写依赖文件。', '');
  lines.push('### 验证边界', '', '本章使用本地 Go 工具和进程内 fake。DynamoDB fake 不是 DynamoDB Local，miniredis 不是实际 Redis 服务，FakePusher/SDK fake 不会访问 API Gateway。本章没有声明真实 AWS 部署或网络验证。', '');
  lines.push('## 5. 常见错误与小结', '');
  for (const mistake of guide.mistakes) lines.push(`- ${mistake}`);
  lines.push('', '### 小结', '', `本章完成了“${chapter.title}”。${guide.expected}`, '');
  if (index + 1 < chapters.length) lines.push(`下一节：第 ${String(chapter.number + 1).padStart(2, '0')} 节“${chapters[index + 1].title}”。`, '');
  else lines.push('教程结束：请保留最终 PDF、ZIP、测试日志和终态哈希报告，便于复核。', '');
  writeFile(path.join(root, 'docs', `${key}.md`), lines.join('\n'));
  console.log(`wrote docs/${key}.md`);
}
