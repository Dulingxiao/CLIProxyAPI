# Codex 防封、额度观测与超刷池使用说明

本文说明如何启用和运维 Codex 官方 OAuth 请求身份、TLS A-B 画像、主动额度查询、credits 保护、FIFO 超刷池、耗尽探测及内建计费。

## 1. 适用范围

- Go 1.26+ 构建的 CLIProxyAPI。
- 通过 `--codex-login` 或 `--codex-device-login` 生成的官方 Codex 文件型 OAuth 凭据。
- 官方目标 `https://chatgpt.com/backend-api/codex`。
- 每个凭据都应带有 `account_id`；主动 `/wham/usage` 查询以它设置 `Chatgpt-Account-Id`。

自定义 `base_url`、API Key 凭据和插件虚拟凭据走原有调度，不进入 Codex 超刷池。

## 2. 构建、登录与启动

```bash
go build -o cli-proxy-api ./cmd/server

# 浏览器 OAuth
./cli-proxy-api --config config.yaml --codex-login

# 或设备码 OAuth
./cli-proxy-api --config config.yaml --codex-device-login

# 启动服务
./cli-proxy-api --config config.yaml
```

OAuth 文件默认写入 `auth-dir`。服务器启动后会为符合条件的 Codex Auth 建立额度观测状态；打开超刷池后还会建立持久化调度状态。

## 3. 推荐启用顺序

### 3.1 第一阶段：只开额度观测

先观察至少一个完整的短窗口，确认每个 Auth 都有主动额度快照。

```yaml
codex:
  tls-profile: chrome
  tls-reuse-connections: true
  fingerprint-mode: off
  disable-fingerprint-auto-sync: false
  quota:
    enabled: true
    stale-after: 10m
    near-threshold-stale-after: 60s
    min-active-interval: 60s
    startup-jitter: 30s
    active-query-concurrency: 8
  overdraft:
    enabled: false
    quota-threshold-percent: "98"
    arm-threshold-percent: "90"
    max-in-flight: 40
    per-auth-max-in-flight: 0
    late-admission-bypass-cooldown: true
    late-admission-max-attempts: 3
    allow-credit-spend: false
    exhaustion-probe-failures: 10
    probe-min-interval: 1s
    probe-model: "PROBE_MODEL"

accounting:
  enabled: false
  storage-path: "AUTH_STATE_DIR/accounting.db"
```

`stale-after` 是普通主动查询周期；达到 `arm-threshold-percent` 后使用 `near-threshold-stale-after`。每个 Auth 的实际主动查询起点仍受 `min-active-interval` 硬下限约束。

### 3.2 第二阶段：开启计费流水

```yaml
accounting:
  enabled: true
  storage-path: "AUTH_STATE_DIR/accounting.db"
```

`AUTH_STATE_DIR` 会解析为 `auth-dir`。建议把该目录放在持久化磁盘并纳入备份。计费服务同步保存发送 Intent 和终态 Usage，价格计算异步执行。

### 3.3 第三阶段：小并发开启超刷池

先把 `PROBE_MODEL` 换成该账号可调用且稳定的 Codex 模型，再从较小并发开始：

```yaml
codex:
  overdraft:
    enabled: true
    quota-threshold-percent: "98"
    arm-threshold-percent: "90"
    max-in-flight: 8
    per-auth-max-in-flight: 4
    late-admission-bypass-cooldown: true
    late-admission-max-attempts: 3
    allow-credit-spend: false
    exhaustion-probe-failures: 10
    probe-min-interval: 1s
    probe-model: "PROBE_MODEL"

accounting:
  enabled: true
```

超刷池依赖以下三项同时成立：

1. `codex.quota.enabled: true`
2. `codex.overdraft.enabled: true`
3. `accounting.enabled: true`

`arm-threshold-percent` 应小于或等于 `quota-threshold-percent`。`probe-model` 已不再用于耗尽确认。

## 4. 调度状态说明

| 状态 | 含义 | 运维关注点 |
|---|---|---|
| `NORMAL` | 普通调度 | 等待主动额度快照 |
| `CANDIDATE` | 已达到阈值，进入 FIFO | 查看候选队列长度和队首等待时间 |
| `ACTIVE_DRAIN` | 当前唯一超刷 owner | 业务租约同时受全局和单 Auth 上限约束；`probe_failures` 是连续额度 429 次数 |
| `EXHAUSTED` | 超刷后连续十次结构化 `usage_limit_reached` 429 | 等待主动额度查询确认新周期恢复 |
| `DISABLED` | 运维或认证状态禁用 | 检查 Auth 生命周期和认证状态 |

重要行为：

- 同一时刻只有一个 `ACTIVE_DRAIN` owner。
- `CANDIDATE` 仍参与普通轮询；只有 `EXHAUSTED` 和 Overlay `DISABLED` 会退出普通选择。兼容超刷的请求优先走 `ACTIVE_DRAIN` 租约，租约拿不到时溢出到 `NORMAL`/`CANDIDATE`，不会绕过 `max-in-flight`。
- `max-in-flight` 是所有超刷租约和继承请求的全局上限。
- `per-auth-max-in-flight: 0` 表示沿用全局上限；正整数表示额外的单 Auth 上限。
- 每个超刷上游请求都会在最终 Codex `input` 末尾追加相邻的 `zz` 调用和输出，内部重试会生成新的 Attempt ID 与 Call ID。
- 没有独立的耗尽确认池。只有超刷业务上的结构化 `usage_limit_reached` 429 计入连续失败；RPM、Cloudflare、401 和 5xx 会清零该计数并继续使用各自的失败与冷却语义。
- 主动额度查询确认新周期恢复后，调度状态统一返回 `NORMAL`。

## 5. credits 保护

默认 `allow-credit-spend: false`。当额度窗口已满且上游报告存在有限 credits 时：

- Auth 被标记为 `credit_protection` 额度冷却，并带未来恢复时间。
- 普通选择、超刷 business 和 probe 都会跳过该 Auth。
- 状态不会写入 `Disabled`，也不会覆盖认证、Cloudflare 或其他真实故障。

显式设置以下配置后，窗口满时可继续使用 credits：

```yaml
codex:
  overdraft:
    allow-credit-spend: true
```

计费事件会用 `consumption_class: over_window_on_credits` 标记这类消耗；普通窗口内消耗标记为 `within_window`。

## 6. 应用身份与 TLS 画像

### 6.1 fingerprint-mode

| 模式 | 行为 |
|---|---|
| `off` | 保留客户端应用身份；官方目标仍发送 gating 所需的 UA、Originator 和 Version |
| `device` | 收敛安装身份 |
| `session` | 在 device 基础上收敛账号会话，并从入站会话派生线程 |
| `full` | 进一步收敛账号线程 |

官方目标的 HTTP 和 WebSocket 头按源码推导的独立契约发送；可选头遵循“来源不确定即省略”。入站 `x-oai-attestation` 和 `x-openai-internal-codex-residency` 只做原值透传。

### 6.2 TLS 画像

| 值 | 用途 |
|---|---|
| `chrome` | 默认画像，适合作为主通道 |
| `safari-like` | macOS/WebKit 类 A-B 通道 |
| `go-standard` | Go 标准 TLS 对照或回退通道 |

```yaml
codex:
  tls-profile: chrome
  tls-reuse-connections: true
```

连接池和 TLS 会话缓存按 `(Auth, proxy, profile)` 隔离。关闭 `tls-reuse-connections` 后，每次创建独立传输并关闭 keep-alive。

### 6.3 Auth 级覆盖

文件型 OAuth JSON 可增加以下顶层字段；SDK 使用者也可把同名值放入 `Auth.Attributes`：

```json
{
  "codex_fingerprint_mode": "session",
  "openai_device_id": "PERSISTED_DEVICE_ID",
  "codex_user_agent": "codex_cli_rs/VERSION (PLATFORM)",
  "tls-profile": "safari-like",
  "codex_overdraft_max_in_flight": "4"
}
```

覆盖顺序为 Auth > 全局 > 默认。`codex_user_agent` 需要以 `codex_cli_rs/` 开头并满足 HTTP 头格式；单 Auth 并发值应位于 `1..max-in-flight`。

## 7. 管理 API

管理接口统一位于 `/v0/management`，请求需携带管理密钥：

```bash
export BASE_URL="http://127.0.0.1:8317"
export MANAGEMENT_KEY="MANAGEMENT_KEY"

curl -sS \
  -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota"
```

也可使用 `X-Management-Key: MANAGEMENT_KEY`。`remote-management.secret-key` 为空时管理路由不启用；远程访问还需要 `remote-management.allow-remote: true`。

### 7.1 额度

```bash
# 所有 Auth 最新快照
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota"

# 单 Auth 快照
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota?auth_id=AUTH_ID"

# 主动刷新单 Auth；仍遵守 singleflight、并发和每 Auth 查询下限
curl -sS -X POST -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota/AUTH_ID/refresh"

# 额度采集持久化健康状态
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota/health"

# 最近一次 /wham/usage 的允许列表视图
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/quota/debug?auth_id=AUTH_ID"
```

`used_micropct` 使用 1e6 定点百分比，例如 `98000000` 表示 `98%`。

### 7.2 超刷池状态与开关

```bash
# 状态、owner、候选队列、并发占用和 credits 保护数量
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/overdraft/status"

# 读取开关及乐观并发 revision
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/overdraft/enabled"

# 使用上一步返回的 revision 切换开关
curl -sS -X PATCH \
  -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"enabled":true,"revision":REVISION}' \
  "${BASE_URL}/v0/management/codex/overdraft/enabled"
```

重点字段：

- `owner_auth_id`：当前 owner。
- `candidate_queue.length` / `head_wait_ms`：FIFO 排队情况。
- `total_in_flight` / `max_in_flight`：全局占用。
- `per_auth_in_flight` / `per_auth_max_in_flight`：单 Auth 占用与上限。
- `credit_protected_auths`：因 credits 保护被排除的 Auth 数量。
- `drain_cycle_accounting`：当前记录对应的业务、探测、Token 与费用汇总。

### 7.3 脱敏失败诊断

```bash
# 所有最近失败
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/upstream-failures"

# 按 Auth 和模型过滤
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/codex/upstream-failures?auth_id=AUTH_ID&model=MODEL"
```

每个 `(Auth, model)` 只保留最近 20 条，单 Auth 最多保留 32 个模型。输出只有错误 code/type、HTTP 状态、时间和 request-scoped 标记，不包含响应 body、prompt 或凭据。

### 7.4 计费

```bash
# 健康状态
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/accounting/health"

# 事件分页和过滤
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/accounting/events?auth_id=AUTH_ID&limit=100&offset=0"

# 按 Auth 汇总
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/accounting/aggregate?auth_id=AUTH_ID"

# 全部 Auth 汇总
curl -sS -H "Authorization: Bearer ${MANAGEMENT_KEY}" \
  "${BASE_URL}/v0/management/accounting/aggregates"
```

事件与费用查询支持 `auth_id`、`provider`、`model`、`tier`、`drain_cycle_id`、RFC3339 `from`/`to`、`offset` 和 `limit`，单页上限 500。

## 8. 上线观察清单

1. `/codex/quota/health` 返回 `healthy: true`。
2. 每个目标 Auth 的快照 `source` 为 `active_usage`，且 `auth_generation` 与当前生命周期一致。
3. `unknown_schema` 保持空或 false；出现新形状时查看 `/codex/quota/debug`。
4. `candidate_queue.head_wait_ms` 持续增长时检查 owner、并发上限和持久化健康。
5. `credit_protected_auths` 增长时确认是否符合费用策略。
6. `/codex/upstream-failures` 中持续出现 401 时检查 OAuth 刷新；持续出现 403/challenge 时按 Auth 或出口做 TLS A-B。
7. accounting 的 `healthy`、`queue_depth` 和 `last_error` 保持正常。
8. 对比正常业务、超刷业务和 probe 的 attempts/successes/failures。

## 9. 常见现象

### 主动额度查询提示缺少 account_id

重新执行 Codex OAuth，或检查对应凭据 JSON 顶层 `account_id`。查询会跳过该 Auth，从而避免跨账号查询。

### 启用超刷池时报配置校验错误

依次检查 quota、accounting、`probe-model`、阈值顺序、正整数并发上限和正时长字段。

### 返回 auth_unavailable 或 auth_not_found

查看错误 message 中的 `reason` 与 `retry_after`，再查询 `/codex/upstream-failures` 和 `/codex/quota`。错误 code 保持兼容，具体失败集合位于附加信息中。

### credits 账号在额度满后被跳过

这是默认费用保护。确认付费策略后设置 `allow-credit-spend: true`，配置热更新会立即重新评估已有主动快照。

### TLS A-B

固定同一 Auth、模型、代理和请求负载，分别设置 `chrome`、`safari-like`、`go-standard`，比较 401、403、challenge、5xx、断流率和延迟。每个实验只调整一个变量。

## 10. 快速停用与回退

优先通过管理 API 或配置把 `codex.overdraft.enabled` 设为 `false`。此操作会关闭候选、活动、验证和耗尽 Overlay；quota 与 accounting 可继续独立运行。

传输层对照可切到：

```yaml
codex:
  tls-profile: go-standard
```

身份收敛可切到：

```yaml
codex:
  fingerprint-mode: off
```

额度查询也需要暂停时，再设置 `codex.quota.enabled: false`。

## 11. 发布前验证

```bash
test -z "$(gofmt -l .)"
go test ./...
go test -race ./internal/codexoverdraft ./sdk/cliproxy/auth
go vet ./internal/config ./internal/codexoverdraft ./internal/usage/accounting \
  ./sdk/cliproxy/auth ./internal/runtime/executor ./internal/runtime/executor/helps \
  ./internal/api/handlers/management ./sdk/api/handlers
go build -o test-output ./cmd/server && rm test-output
git diff --check
```
