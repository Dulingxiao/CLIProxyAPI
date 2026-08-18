# CLI Proxy SDK Guide

The `sdk/cliproxy` module exposes the proxy as a reusable Go library so external programs can embed the routing, authentication, hot‑reload, and translation layers without depending on the CLI binary.

## Install & Import

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

Note the `/v7` module path.

## Minimal Embed

```go
cfg, err := config.LoadConfig("config.yaml")
if err != nil { panic(err) }

svc, err := cliproxy.NewBuilder().
    WithConfig(cfg).
    WithConfigPath("config.yaml"). // absolute or working-dir relative
    Build()
if err != nil { panic(err) }

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

if err := svc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    panic(err)
}
```

The service manages config/auth watching, background token refresh, and graceful shutdown. Cancel the context to stop it.

## Server Options (middleware, routes, logs)

The server accepts options via `WithServerOptions`:

```go
svc, _ := cliproxy.NewBuilder().
  WithConfig(cfg).
  WithConfigPath("config.yaml").
  WithServerOptions(
    // Add global middleware
    cliproxy.WithMiddleware(func(c *gin.Context) { c.Header("X-Embed", "1"); c.Next() }),
    // Tweak gin engine early (CORS, trusted proxies, etc.)
    cliproxy.WithEngineConfigurator(func(e *gin.Engine) { e.ForwardedByClientIP = true }),
    // Add your own routes after defaults
    cliproxy.WithRouterConfigurator(func(e *gin.Engine, _ *handlers.BaseAPIHandler, _ *config.Config) {
      e.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
    }),
    // Override request log writer/dir
    cliproxy.WithRequestLoggerFactory(func(cfg *config.Config, cfgPath string) logging.RequestLogger {
      return logging.NewFileRequestLogger(true, "logs", filepath.Dir(cfgPath))
    }),
  ).
  Build()
```

These options mirror the internals used by the CLI server.

## Management API (when embedded)

- Management endpoints are mounted only when `remote-management.secret-key` is set in `config.yaml`.
- Remote access additionally requires `remote-management.allow-remote: true`.
- See MANAGEMENT_API.md for endpoints. Your embedded server exposes them under `/v0/management` on the configured port.

## Codex Quota, Overdraft, and Accounting

These services are configured on the same `config.Config` passed to the builder. File loading supplies defaults; when constructing a config directly, start from the exported default helpers:

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

Overdraft enablement requires quota collection, accounting, and a probe model. The manager applies dependency shutdown in order: it closes overdraft admission first, then drains accounting and stops quota scheduling. Cancelling `Service.Run` also drains the built-in usage queue before the ledger is closed.

Quota response observations are submitted to a bounded, per-auth ordered background path; business requests do not wait for active quota queries or price calculations. Durable Attempt Intents are written immediately before Codex network sends, and terminal Usage Parts are durably written synchronously before usage delivery returns; only pricing runs asynchronously. Accounting storage defaults to `AUTH_STATE_DIR/accounting.db`; overdraft coordination and quota history use the companion `<base>-overdraft.db` and `<base>-quota.db` files.

Accounting write faults are admission-blocking and remain sticky until the service is restarted and reconciled; a post-completion persistence error is request-scoped, so completed upstream work is not replayed through another credential. Quota-store write faults instead close only the overdraft overlay and are exposed by `CodexQuotaRuntimeHealth()` and `GET /v0/management/codex/quota/health`; ordinary scheduling remains available. Event/cost management pages support Auth.ID, provider, model, tier, drain cycle, RFC3339 `[from,to)`, and bounded pagination filters.

`codex.fingerprint-mode` defaults to `off`. The `device`, `session`, and `full` modes progressively converge official Codex application identity; per-auth `codex_fingerprint_mode` and `openai_device_id` attributes override the global setting. Turn-state provenance remains independent: known cross-auth `X-Codex-Turn-State` values are removed during failover and the final upstream state is relayed downstream even when general header passthrough is disabled.

The core manager exposes `CodexOverdraftCoordinator()`, `CodexQuotaSnapshot(s)`, `CodexQuotaRuntimeHealth()`, `RefreshCodexQuota`, and `AccountingService()` for embedded operator integrations. Runtime records and management filters use `Auth.ID` only.

## Using the Core Auth Manager

The service uses a core `auth.Manager` for selection, execution, and auto‑refresh. When embedding, you can provide your own manager to customize transports or hooks:

```go
core := coreauth.NewManager(coreauth.NewFileStore(cfg.AuthDir), nil, nil)
core.SetRoundTripperProvider(myRTProvider) // per‑auth *http.Transport

svc, _ := cliproxy.NewBuilder().
    WithConfig(cfg).
    WithConfigPath("config.yaml").
    WithCoreAuthManager(core).
    Build()
```

Implement a custom per‑auth transport:

```go
type myRTProvider struct{}
func (myRTProvider) RoundTripperFor(a *coreauth.Auth) http.RoundTripper {
    if a == nil || a.ProxyURL == "" { return nil }
    u, _ := url.Parse(a.ProxyURL)
    return &http.Transport{ Proxy: http.ProxyURL(u) }
}
```

Programmatic execution is available on the manager:

```go
// Non‑streaming
resp, err := core.Execute(ctx, []string{"gemini"}, req, opts)

// Streaming
chunks, err := core.ExecuteStream(ctx, []string{"gemini"}, req, opts)
for ch := range chunks { /* ... */ }
```

Note: Built‑in provider executors are wired automatically when you run the `Service`. If you want to use `Manager` stand‑alone without the HTTP server, you must register your own executors that implement `auth.ProviderExecutor`.

## Custom Client Sources

Replace the default loaders if your creds live outside the local filesystem:

```go
type memoryTokenProvider struct{}
func (p *memoryTokenProvider) Load(ctx context.Context, cfg *config.Config) (*cliproxy.TokenClientResult, error) {
    // Populate from memory/remote store and return counts
    return &cliproxy.TokenClientResult{}, nil
}

svc, _ := cliproxy.NewBuilder().
  WithConfig(cfg).
  WithConfigPath("config.yaml").
  WithTokenClientProvider(&memoryTokenProvider{}).
  WithAPIKeyClientProvider(cliproxy.NewAPIKeyClientProvider()).
  Build()
```

## Hooks

Observe lifecycle without patching internals:

```go
hooks := cliproxy.Hooks{
  OnBeforeStart: func(cfg *config.Config) { log.Infof("starting on :%d", cfg.Port) },
  OnAfterStart:  func(s *cliproxy.Service) { log.Info("ready") },
}
svc, _ := cliproxy.NewBuilder().WithConfig(cfg).WithConfigPath("config.yaml").WithHooks(hooks).Build()
```

## Shutdown

`Run` defers `Shutdown`, so cancelling the parent context is enough. To stop manually:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
_ = svc.Shutdown(ctx)
```

## Notes

- Hot reload: changes to `config.yaml` and `auths/` are picked up automatically.
- Request logging can be toggled at runtime via the Management API.
- Gemini Web features (`gemini-web.*`) are honored in the embedded server.
