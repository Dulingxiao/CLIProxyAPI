package auth

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type probeTestExecutor struct {
	calls       atomic.Int32
	limitBefore int32
	err         error
}

func (e *probeTestExecutor) Identifier() string { return "codex" }
func (e *probeTestExecutor) Execute(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts)
	if execution == nil || execution.Kind != cliproxyexecutor.CodexOverdraftProbe {
		return cliproxyexecutor.Response{}, &Error{Code: "missing_probe_lease", Message: "missing probe lease", HTTPStatus: http.StatusBadRequest}
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		return cliproxyexecutor.Response{}, errBegin
	}
	call := e.calls.Add(1)
	if e.err != nil {
		return cliproxyexecutor.Response{}, e.err
	}
	if call <= e.limitBefore {
		return cliproxyexecutor.Response{}, &Error{Code: "usage_limit_reached", Message: "limit", HTTPStatus: http.StatusTooManyRequests, Retryable: true}
	}
	return cliproxyexecutor.Response{}, nil
}
func (e *probeTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (e *probeTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (e *probeTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *probeTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func configureProbeTestManager(t *testing.T, executor *probeTestExecutor) (*Manager, *cliproxyexecutor.CodexOverdraftExecution) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	m.RegisterExecutor(executor)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	overdraft := internalconfig.DefaultCodexOverdraftConfig()
	overdraft.Enabled = true
	overdraft.ProbeModel = "gpt-test"
	overdraft.ProbeMinInterval = "1ms"
	m.runtimeConfig.Store(&internalconfig.Config{Codex: internalconfig.CodexConfig{Overdraft: overdraft}})
	auth := fileCodexAuth("auth-1")
	_, _ = m.Register(context.Background(), auth)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)
	execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{}, map[string]struct{}{})
	if !ok {
		t.Fatal("business overdraft execution was not acquired")
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		t.Fatal(errBegin)
	}
	return m, execution
}

func TestBusinessUsageLimitsExhaustAfterTenConsecutiveFailures(t *testing.T) {
	executor := &probeTestExecutor{limitBefore: 10}
	m, leftover := configureProbeTestManager(t, executor)
	leftover.Release()
	for i := 0; i < 10; i++ {
		execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
			Model:   "gpt-test",
			Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`),
		}, cliproxyexecutor.Options{}, map[string]struct{}{})
		if !ok {
			t.Fatalf("business execution %d was not acquired", i+1)
		}
		if errBegin := execution.BeginSend(); errBegin != nil {
			t.Fatal(errBegin)
		}
		if errLimit := execution.BusinessUsageLimited(); errLimit != nil {
			t.Fatal(errLimit)
		}
		execution.Release()
	}
	record := m.CodexOverdraftCoordinator().Record("auth-1")
	if record.State != "EXHAUSTED" || record.ProbeFailures != 10 {
		t.Fatalf("record = %#v", record)
	}
}

func TestBusinessSuccessClearsUsageLimitStreak(t *testing.T) {
	m, first := configureProbeTestManager(t, &probeTestExecutor{limitBefore: 10})
	if errLimit := first.BusinessUsageLimited(); errLimit != nil {
		t.Fatal(errLimit)
	}
	first.Release()
	execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{}, map[string]struct{}{})
	if !ok {
		t.Fatal("second business execution was not acquired")
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		t.Fatal(errBegin)
	}
	if errComplete := execution.BusinessCompleted(); errComplete != nil {
		t.Fatal(errComplete)
	}
	execution.Release()
	record := m.CodexOverdraftCoordinator().Record("auth-1")
	if record.State != "ACTIVE_DRAIN" || record.ProbeFailures != 0 {
		t.Fatalf("record = %#v", record)
	}
}
