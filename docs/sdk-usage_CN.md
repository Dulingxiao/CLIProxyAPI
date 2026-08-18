# CLI Proxy SDK 使用指南

`sdk/cliproxy` 模块将代理能力以 Go 库的形式对外暴露，方便在其它服务中内嵌路由、鉴权、热更新与翻译层，而无需依赖可执行的 CLI 程序。

## 安装与导入

```bash
go get github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy
```

```go
import (
    "context"
    "errors"
    "time"

    "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy"
    "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)
```

注意模块路径包含 `/v7`。

## 最小可用示例

```go
cfg, err := config.LoadConfig("config.yaml")
if err != nil { panic(err) }

svc, err := cliproxy.NewBuilder().
    WithConfig(cfg).
    WithConfigPath("config.yaml"). // 绝对路径或工作目录相对路径
    Build()
if err != nil { panic(err) }

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

if err := svc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    panic(err)
}
```

服务内部会管理配置与认证文件的监听、后台令牌刷新与优雅关闭。取消上下文即可停止服务。

## 服务器可选项（中间件、路由、日志）

通过 `WithServerOptions` 自定义：

```go
svc, _ := cliproxy.NewBuilder().
  WithConfig(cfg).
  WithConfigPath("config.yaml").
  WithServerOptions(
    // 追加全局中间件
    cliproxy.WithMiddleware(func(c *gin.Context) { c.Header("X-Embed", "1"); c.Next() }),
    // 提前调整 gin 引擎（如 CORS、trusted proxies）
    cliproxy.WithEngineConfigurator(func(e *gin.Engine) { e.ForwardedByClientIP = true }),
    // 在默认路由之后追加自定义路由
    cliproxy.WithRouterConfigurator(func(e *gin.Engine, _ *handlers.BaseAPIHandler, _ *config.Config) {
      e.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
    }),
    // 覆盖请求日志的创建（启用/目录）
    cliproxy.WithRequestLoggerFactory(func(cfg *config.Config, cfgPath string) logging.RequestLogger {
      return logging.NewFileRequestLogger(true, "logs", filepath.Dir(cfgPath))
    }),
  ).
  Build()
```

这些选项与 CLI 服务器内部用法保持一致。

## 管理 API（内嵌时）

- 仅当 `config.yaml` 中设置了 `remote-management.secret-key` 时才会挂载管理端点。
- 远程访问还需要 `remote-management.allow-remote: true`。
- 具体端点见 MANAGEMENT_API_CN.md。内嵌服务器会在配置端口下暴露 `/v0/management`。

## Codex 额度、超刷与计费

这些服务使用传给 Builder 的同一个 `config.Config`。从文件加载时会自动补齐默认值；直接构造配置时，建议从导出的默认配置函数开始：

```go
quota := config.DefaultCodexQuotaConfig()

overdraft := config.DefaultCodexOverdraftConfig()
overdraft.Enabled = true
overdraft.ProbeModel = "gpt-5.3-codex"

ledger := config.DefaultAccountingConfig()
ledger.Enabled = true

cfg.Codex.Quota = quota
cfg.Codex.Overdraft = overdraft
cfg.Accounting = ledger
```

开启超刷需要同时开启额度采集和 Accounting，并配置探测模型。依赖关闭时，Manager 会先关闭超刷准入，再排空 Accounting、停止额度调度。取消 `Service.Run` 时也会先排空内建 Usage 队列，再关闭计费存储。

被动额度观察通过有界、按 Auth 保序的后台路径处理；业务请求不等待主动额度查询或价格计算。Codex 实际发送前会同步持久化 Attempt Intent，终态 Usage Part 也会在 Usage 投递返回前同步持久化；只有定价任务异步执行。Accounting 默认存储在 `AUTH_STATE_DIR/accounting.db`，超刷协调状态与额度历史分别使用配套的 `<base>-overdraft.db` 和 `<base>-quota.db`。

Accounting 写入故障会阻断准入，并保持为粘性故障，直到服务重启并完成 reconcile；请求完成后的持久化错误属于请求级错误，不会换凭据重放已经完成的上游工作。额度库存储故障只关闭超刷 Overlay，通过 `CodexQuotaRuntimeHealth()` 和 `GET /v0/management/codex/quota/health` 暴露，普通调度继续可用。Event/Cost 管理分页支持 Auth.ID、provider、model、tier、drain cycle、RFC3339 `[from,to)` 时间范围和有界分页筛选。

`codex.fingerprint-mode` 默认是 `off`。`device`、`session`、`full` 会逐级收敛官方 Codex 应用身份，Auth 属性 `codex_fingerprint_mode` 与 `openai_device_id` 可覆盖全局设置。Turn State 来源保护与此独立：换号时会剥离已知跨 Auth 的 `X-Codex-Turn-State`，并且即使关闭通用响应头透传，也会把最终上游状态转发给下游。

内嵌运维集成可使用核心 Manager 的 `CodexOverdraftCoordinator()`、`CodexQuotaSnapshot(s)`、`CodexQuotaRuntimeHealth()`、`RefreshCodexQuota` 和 `AccountingService()`。运行态记录和管理筛选统一使用 `Auth.ID`。

## 使用核心鉴权管理器

服务内部使用核心 `auth.Manager` 负责选择、执行、自动刷新。内嵌时可自定义其传输或钩子：

```go
core := coreauth.NewManager(coreauth.NewFileStore(cfg.AuthDir), nil, nil)
core.SetRoundTripperProvider(myRTProvider) // 按账户返回 *http.Transport

svc, _ := cliproxy.NewBuilder().
    WithConfig(cfg).
    WithConfigPath("config.yaml").
    WithCoreAuthManager(core).
    Build()
```

实现每个账户的自定义传输：

```go
type myRTProvider struct{}
func (myRTProvider) RoundTripperFor(a *coreauth.Auth) http.RoundTripper {
    if a == nil || a.ProxyURL == "" { return nil }
    u, _ := url.Parse(a.ProxyURL)
    return &http.Transport{ Proxy: http.ProxyURL(u) }
}
```

管理器提供编程式执行接口：

```go
// 非流式
resp, err := core.Execute(ctx, []string{"gemini"}, req, opts)

// 流式
chunks, err := core.ExecuteStream(ctx, []string{"gemini"}, req, opts)
for ch := range chunks { /* ... */ }
```

说明：运行 `Service` 时会自动注册内置的提供商执行器；若仅单独使用 `Manager` 而不启动 HTTP 服务器，则需要自行实现并注册满足 `auth.ProviderExecutor` 的执行器。

## 自定义凭据来源

当凭据不在本地文件系统时，替换默认加载器：

```go
type memoryTokenProvider struct{}
func (p *memoryTokenProvider) Load(ctx context.Context, cfg *config.Config) (*cliproxy.TokenClientResult, error) {
    // 从内存/远端加载并返回数量统计
    return &cliproxy.TokenClientResult{}, nil
}

svc, _ := cliproxy.NewBuilder().
  WithConfig(cfg).
  WithConfigPath("config.yaml").
  WithTokenClientProvider(&memoryTokenProvider{}).
  WithAPIKeyClientProvider(cliproxy.NewAPIKeyClientProvider()).
  Build()
```

## 启动钩子

无需修改内部代码即可观察生命周期：

```go
hooks := cliproxy.Hooks{
  OnBeforeStart: func(cfg *config.Config) { log.Infof("starting on :%d", cfg.Port) },
  OnAfterStart:  func(s *cliproxy.Service) { log.Info("ready") },
}
svc, _ := cliproxy.NewBuilder().WithConfig(cfg).WithConfigPath("config.yaml").WithHooks(hooks).Build()
```

## 关闭

`Run` 内部会延迟调用 `Shutdown`，因此只需取消父上下文即可。若需手动停止：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = svc.Shutdown(ctx)
```

## 说明

- 热更新：`config.yaml` 与 `auths/` 变化会被自动侦测并应用。
- 请求日志可通过管理 API 在运行时开关。
- `gemini-web.*` 相关配置在内嵌服务器中会被遵循。
