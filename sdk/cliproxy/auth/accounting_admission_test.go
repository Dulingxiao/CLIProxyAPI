package auth

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	usageaccounting "github.com/router-for-me/CLIProxyAPI/v7/internal/usage/accounting"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

type accountingAdmissionFailingUsageStore struct {
	usageaccounting.Store
}

func (s *accountingAdmissionFailingUsageStore) AppendUsage(usageaccounting.UsageEvent) error {
	return errors.New("injected durable usage failure")
}

type accountingAdmissionExecutor struct {
	provider    string
	calls       atomic.Int32
	streamCalls atomic.Int32
	countCalls  atomic.Int32
}

func (e *accountingAdmissionExecutor) Identifier() string { return e.provider }
func (e *accountingAdmissionExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls.Add(1)
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}
func (e *accountingAdmissionExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.streamCalls.Add(1)
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}
func (*accountingAdmissionExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (e *accountingAdmissionExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.countCalls.Add(1)
	return cliproxyexecutor.Response{}, nil
}
func (*accountingAdmissionExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestManagerPausesCodexAdmissionWhenRequiredAccountingStartupFails(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseAccounting()
	defer m.StopCodexQuota()

	registerSchedulerModels(t, "codex", "gpt-test", "auth-1")
	executor := &accountingAdmissionExecutor{provider: "codex"}
	m.RegisterExecutor(executor)
	if _, errRegister := m.Register(context.Background(), fileCodexAuth("auth-1")); errRegister != nil {
		t.Fatal(errRegister)
	}

	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	accounting.StoragePath = t.TempDir()
	quota := internalconfig.DefaultCodexQuotaConfig()
	quota.Enabled = false
	m.SetConfig(&internalconfig.Config{
		Accounting: accounting,
		Codex:      internalconfig.CodexConfig{Quota: quota},
	})

	_, errExecute := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("Codex admission succeeded without required accounting storage")
	}
	_, errStream := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{Stream: true})
	if errStream == nil {
		t.Fatal("Codex stream admission succeeded without required accounting storage")
	}
	_, errCount := m.ExecuteCount(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{})
	if errCount == nil {
		t.Fatal("Codex count admission succeeded without required accounting storage")
	}
	if got := executor.calls.Load(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	if got := executor.streamCalls.Load(); got != 0 {
		t.Fatalf("stream executor calls = %d, want 0", got)
	}
	if got := executor.countCalls.Load(); got != 0 {
		t.Fatalf("count executor calls = %d, want 0", got)
	}
}

func TestManagerKeepsNonCodexFallbackWhenRequiredAccountingStartupFails(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseAccounting()
	defer m.StopCodexQuota()

	registerSchedulerModels(t, "codex", "gpt-test", "codex-1")
	registerSchedulerModels(t, "claude", "gpt-test", "claude-1")
	codexExecutor := &accountingAdmissionExecutor{provider: "codex"}
	claudeExecutor := &accountingAdmissionExecutor{provider: "claude"}
	m.RegisterExecutor(codexExecutor)
	m.RegisterExecutor(claudeExecutor)
	for _, auth := range []*Auth{
		{ID: "codex-1", Provider: "codex", Status: StatusActive},
		{ID: "claude-1", Provider: "claude", Status: StatusActive},
	} {
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	accounting.StoragePath = t.TempDir()
	quota := internalconfig.DefaultCodexQuotaConfig()
	quota.Enabled = false
	m.SetConfig(&internalconfig.Config{
		Accounting: accounting,
		Codex:      internalconfig.CodexConfig{Quota: quota},
	})

	if _, errExecute := m.Execute(context.Background(), []string{"codex", "claude"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatal(errExecute)
	}
	if got := codexExecutor.calls.Load(); got != 0 {
		t.Fatalf("Codex executor calls = %d, want 0", got)
	}
	if got := claudeExecutor.calls.Load(); got != 1 {
		t.Fatalf("Claude executor calls = %d, want 1", got)
	}
}

func TestManagerPausesCodexAdmissionAfterTerminalAccountingFailure(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseAccounting()
	defer m.StopCodexQuota()

	registerSchedulerModels(t, "codex", "gpt-test", "codex-1")
	registerSchedulerModels(t, "claude", "gpt-test", "claude-1")
	codexExecutor := &accountingAdmissionExecutor{provider: "codex"}
	claudeExecutor := &accountingAdmissionExecutor{provider: "claude"}
	m.RegisterExecutor(codexExecutor)
	m.RegisterExecutor(claudeExecutor)
	for _, auth := range []*Auth{
		{ID: "codex-1", Provider: "codex", Status: StatusActive},
		{ID: "claude-1", Provider: "claude", Status: StatusActive},
	} {
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	service := usageaccounting.NewService(&accountingAdmissionFailingUsageStore{Store: usageaccounting.NewMemoryStore()}, 8)
	m.accountingMu.Lock()
	m.accountingService = service
	m.accountingMu.Unlock()
	m.accountingRequired.Store(true)
	if errPersist := service.HandleUsageDurable(context.Background(), coreusage.Record{Provider: "codex", AuthID: "codex-1", UpstreamAttemptID: "attempt", UsageReported: true}); errPersist == nil {
		t.Fatal("HandleUsageDurable() succeeded with failing durable store")
	}

	if _, errExecute := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Codex admission succeeded after terminal accounting failure")
	}
	if _, errExecute := m.Execute(context.Background(), []string{"codex", "claude"}, cliproxyexecutor.Request{Model: "gpt-test"}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatal(errExecute)
	}
	if got := codexExecutor.calls.Load(); got != 0 {
		t.Fatalf("Codex executor calls = %d, want 0", got)
	}
	if got := claudeExecutor.calls.Load(); got != 1 {
		t.Fatalf("Claude executor calls = %d, want 1", got)
	}
}
