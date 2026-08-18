package auth

import (
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type overdraftModelPoolStreamExecutor struct {
	calls int
}

func (e *overdraftModelPoolStreamExecutor) Identifier() string { return "codex" }
func (e *overdraftModelPoolStreamExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *overdraftModelPoolStreamExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls++
	return nil, &Error{Code: "usage_limit_reached", HTTPStatus: http.StatusTooManyRequests, Retryable: true}
}
func (e *overdraftModelPoolStreamExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (e *overdraftModelPoolStreamExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *overdraftModelPoolStreamExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

type overdraftFallbackExecutor struct {
	overdraftAuthID string
	calls           []string
}

func (e *overdraftFallbackExecutor) Identifier() string { return "codex" }
func (e *overdraftFallbackExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls = append(e.calls, auth.ID)
	if execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts); execution != nil {
		if errBegin := execution.BeginSend(); errBegin != nil {
			return cliproxyexecutor.Response{}, errBegin
		}
	}
	if auth.ID == e.overdraftAuthID {
		return cliproxyexecutor.Response{}, &Error{Code: "usage_limit_reached", HTTPStatus: http.StatusTooManyRequests, Retryable: true}
	}
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}
func (e *overdraftFallbackExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (e *overdraftFallbackExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (e *overdraftFallbackExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *overdraftFallbackExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func testOverdraftCoordinator(t *testing.T) *codexoverdraft.Coordinator {
	t.Helper()
	coordinator, errNew := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{
		Enabled:                 true,
		ThresholdMicropct:       98_000_000,
		MaxInFlight:             40,
		ExhaustionProbeFailures: 10,
	}, nil, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	return coordinator
}

func fileCodexAuth(id string) *Auth {
	return &Auth{
		ID:       id,
		Provider: "codex",
		FileName: id + ".json",
		Status:   StatusActive,
		Attributes: map[string]string{
			AttributeAuthKind: AuthKindOAuth,
			AttributeSource:   AuthSourceFile,
		},
		Metadata: map[string]any{"account_id": "acct-" + id},
	}
}

func TestCodexOverdraftEligibilityUsesFileOAuthAuthID(t *testing.T) {
	eligible := fileCodexAuth("auth-1")
	if !isCodexOverdraftEligibleAuth(eligible) {
		t.Fatal("file OAuth Codex auth was not eligible")
	}
	for _, auth := range []*Auth{
		{ID: "api-key", Provider: "codex", FileName: "a.json", Attributes: map[string]string{AttributeAuthKind: AuthKindAPIKey}},
		{ID: "memory", Provider: "codex", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth, AttributeRuntimeOnly: "true"}},
		{ID: "custom", Provider: "codex", FileName: "a.json", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth, "base_url": "https://example.com"}},
		{ID: "plugin", Provider: "codex", FileName: "a.json", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth, AttributePluginVirtual: "true"}},
		{ID: "other", Provider: "claude", FileName: "a.json", Attributes: map[string]string{AttributeAuthKind: AuthKindOAuth}},
	} {
		if isCodexOverdraftEligibleAuth(auth) {
			t.Fatalf("auth %#v unexpectedly eligible", auth)
		}
	}
}

func TestCodexOverdraftExecutionRechecksFenceOnRetrySend(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("auth-fence")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errObserve := coordinator.ObserveThreshold(auth.ID, 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test", Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`)}, cliproxyexecutor.Options{}, map[string]struct{}{})
	if !ok {
		t.Fatal("overdraft execution was not acquired")
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		t.Fatal(errBegin)
	}
	if errDisable := coordinator.ApplyConfig(2, codexoverdraft.CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}); errDisable != nil {
		t.Fatal(errDisable)
	}
	if errBegin := execution.BeginSend(); errBegin == nil {
		t.Fatal("retry send bypassed disabled coordinator fence")
	}
}

func TestManagerExcludesOverlayStatesFromNormalSelection(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)

	for _, id := range []string{"active", "candidate", "normal", "exhausted", "disabled"} {
		auth := fileCodexAuth(id)
		m.auths[id] = auth
		if errRegister := coordinator.RegisterAuth(id, 1, 0); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	_ = coordinator.ObserveThreshold("active", 1, 99_000_000)
	_ = coordinator.ObserveThreshold("candidate", 1, 99_000_000)
	if errForce := coordinator.ForceExhausted("exhausted", 1); errForce != nil {
		t.Fatal(errForce)
	}
	if errDisable := coordinator.SetDisabled("disabled", 1, true); errDisable != nil {
		t.Fatal(errDisable)
	}

	tried := make(map[string]struct{})
	m.excludeCodexOverdraftOverlay(tried)
	if _, ok := tried["active"]; ok {
		t.Fatal("ACTIVE_DRAIN auth excluded from ordinary selection")
	}
	if _, ok := tried["candidate"]; ok {
		t.Fatal("CANDIDATE auth excluded from ordinary selection")
	}
	if _, ok := tried["normal"]; ok {
		t.Fatal("NORMAL auth excluded from ordinary selection")
	}
	if _, ok := tried["exhausted"]; !ok {
		t.Fatal("EXHAUSTED auth remains in ordinary selection")
	}
	if _, ok := tried["disabled"]; !ok {
		t.Fatal("DISABLED overlay auth remains in ordinary selection")
	}
}

func TestAcquireFailureKeepsDrainOwnerOffOrdinaryRoundRobin(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator, errNew := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{
		Enabled:                 true,
		ThresholdMicropct:       98_000_000,
		MaxInFlight:             1,
		ExhaustionProbeFailures: 10,
	}, nil, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	m.SetCodexOverdraftCoordinator(coordinator)
	owner := fileCodexAuth("active")
	fallback := fileCodexAuth("candidate")
	m.auths[owner.ID] = owner
	m.auths[fallback.ID] = fallback
	if errRegister := coordinator.RegisterAuth(owner.ID, 1, 0); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errRegister := coordinator.RegisterAuth(fallback.ID, 1, 0); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errObserve := coordinator.ObserveThreshold(owner.ID, 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	if errObserve := coordinator.ObserveThreshold(fallback.ID, 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}

	req := cliproxyexecutor.Request{Model: "gpt-5.5", Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`)}
	firstTried := make(map[string]struct{})
	first, _, okFirst := m.acquireCodexOverdraftExecution([]string{"codex"}, req, cliproxyexecutor.Options{}, firstTried)
	if !okFirst || first == nil {
		t.Fatal("first overdraft lease was not acquired")
	}
	t.Cleanup(first.Release)

	secondTried := make(map[string]struct{})
	if _, _, okSecond := m.acquireCodexOverdraftExecution([]string{"codex"}, req, cliproxyexecutor.Options{}, secondTried); okSecond {
		t.Fatal("second overdraft lease bypassed MaxInFlight")
	}
	if _, ok := secondTried[owner.ID]; !ok {
		t.Fatal("capacity miss left ACTIVE_DRAIN owner eligible for ordinary RR")
	}
	if _, ok := secondTried[fallback.ID]; ok {
		t.Fatal("CANDIDATE overflow account was marked tried on owner capacity miss")
	}

	incompatibleTried := make(map[string]struct{})
	incompatibleOpts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.RequestPathMetadataKey: "/v1/images/generations"}}
	if _, _, okImages := m.acquireCodexOverdraftExecution([]string{"codex"}, req, incompatibleOpts, incompatibleTried); okImages {
		t.Fatal("image request acquired an overdraft lease")
	}
	if _, ok := incompatibleTried[owner.ID]; ok {
		t.Fatal("incompatible request marked ACTIVE_DRAIN owner tried")
	}
}

func TestManagerAcquiresOpaqueExecutionForActiveOwner(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("active")
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)

	execution, selected, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, map[string]struct{}{})
	if !ok || selected == nil || selected.ID != auth.ID || execution == nil {
		t.Fatalf("selection = (%#v, %#v, %t)", execution, selected, ok)
	}
	if execution.AuthID != auth.ID || execution.DrainCycleID == "" || execution.Kind != cliproxyexecutor.CodexOverdraftBusiness {
		t.Fatalf("execution = %#v", execution)
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		t.Fatalf("BeginSend() error = %v", errBegin)
	}
	execution.Release()
	execution.Release()
}

func TestManagerAppliesAuthOverdraftLimitOverride(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := &internalconfig.Config{}
	cfg.Codex.Overdraft = internalconfig.DefaultCodexOverdraftConfig()
	cfg.Codex.Overdraft.MaxInFlight = 40
	cfg.Codex.Overdraft.PerAuthMaxInFlight = 8
	m.runtimeConfig.Store(cfg)
	coordinator, errNew := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{
		Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 40, PerAuthMaxInFlight: 8, ExhaustionProbeFailures: 10,
	}, nil, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("limited")
	auth.Attributes["codex_overdraft_max_in_flight"] = "3"
	m.auths[auth.ID] = auth
	m.syncCodexOverdraftAuth(auth)
	if got := coordinator.Record(auth.ID).PerAuthMaxInFlight; got != 3 {
		t.Fatalf("PerAuthMaxInFlight = %d, want auth override 3", got)
	}
	auth.Attributes["codex_overdraft_max_in_flight"] = "41"
	m.syncCodexOverdraftAuth(auth)
	if got := coordinator.Record(auth.ID).PerAuthMaxInFlight; got != 8 {
		t.Fatalf("PerAuthMaxInFlight after invalid override = %d, want global per-auth default 8", got)
	}
}

func TestManagerDoesNotAcquireUnavailableOverdraftOwner(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("active-unavailable")
	auth.Unavailable = true
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)

	execution, selected, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, map[string]struct{}{})
	if ok || execution != nil || selected != nil {
		t.Fatalf("unavailable owner selection = (%#v, %#v, %t)", execution, selected, ok)
	}
}

func TestManagerLateAdmissionBypassesOnlyStructuredUsageLimitCooldown(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   string
		reason string
		want   bool
	}{
		{name: "usage limit", code: "usage_limit_reached", reason: "quota", want: true},
		{name: "rpm rate limit", code: "rate_limit_exceeded", reason: "quota", want: false},
		{name: "cloudflare challenge", code: "cloudflare_challenge", reason: "cloudflare challenge", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			cfg := &internalconfig.Config{}
			cfg.Codex.Overdraft = internalconfig.DefaultCodexOverdraftConfig()
			m.runtimeConfig.Store(cfg)
			coordinator := testOverdraftCoordinator(t)
			m.SetCodexOverdraftCoordinator(coordinator)
			auth := fileCodexAuth("active-late")
			auth.Unavailable = true
			auth.ModelStates = map[string]*ModelState{
				"gpt-test": {
					Status: StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Minute),
					LastError: &Error{Code: test.code, HTTPStatus: http.StatusTooManyRequests, Retryable: true},
					Quota:     QuotaState{Exceeded: true, Reason: test.reason, NextRecoverAt: time.Now().Add(time.Minute)},
				},
			}
			m.auths[auth.ID] = auth
			_ = coordinator.RegisterAuth(auth.ID, 1, 0)
			_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)
			execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{
				Model: "gpt-test", Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`),
			}, cliproxyexecutor.Options{}, map[string]struct{}{})
			if ok != test.want {
				t.Fatalf("acquired = %t, want %t", ok, test.want)
			}
			if execution != nil {
				execution.Release()
			}
		})
	}
}

func TestManagerLateAdmissionAttemptLimit(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := &internalconfig.Config{}
	cfg.Codex.Overdraft = internalconfig.DefaultCodexOverdraftConfig()
	cfg.Codex.Overdraft.LateAdmissionMaxAttempts = 3
	m.runtimeConfig.Store(cfg)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("active-late-limit")
	auth.ModelStates = map[string]*ModelState{"gpt-test": {
		Status: StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Minute),
		LastError: &Error{Code: "usage_limit_reached", HTTPStatus: http.StatusTooManyRequests, Retryable: true},
		Quota:     QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: time.Now().Add(time.Minute)},
	}}
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)
	for i := 0; i < 3; i++ {
		execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test", Payload: []byte(`{"input":[]}`)}, cliproxyexecutor.Options{}, map[string]struct{}{})
		if !ok {
			t.Fatalf("attempt %d not admitted", i+1)
		}
		execution.Release()
	}
	if execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, cliproxyexecutor.Request{Model: "gpt-test", Payload: []byte(`{"input":[]}`)}, cliproxyexecutor.Options{}, map[string]struct{}{}); ok || execution != nil {
		t.Fatal("late admission exceeded the configured per-cycle attempt limit")
	}
	if got := coordinator.Record(auth.ID).State; got != codexoverdraft.StateActiveDrain {
		t.Fatalf("state after late-admission limit = %s, want ACTIVE_DRAIN", got)
	}
}

func TestManagerCredentialUnauthorizedReleasesOverdraftOwner(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("active-unauthorized")
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)

	m.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: "codex", Success: false, Error: &Error{Code: "unauthorized", HTTPStatus: http.StatusUnauthorized}})
	record := coordinator.Record(auth.ID)
	if coordinator.Owner() != "" || record.State != codexoverdraft.StateDisabled || !record.Disabled {
		t.Fatalf("coordinator after unauthorized = owner %q, record %#v", coordinator.Owner(), record)
	}

	m.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: "codex", Success: true})
	record = coordinator.Record(auth.ID)
	if record.State != codexoverdraft.StateNormal || record.Disabled {
		t.Fatalf("coordinator after credential recovery = %#v", record)
	}
}

func TestManagerRetriesOrdinaryPoolAfterOverdraftUsageLimit(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 0, 1)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	overdraftAuth := fileCodexAuth("auth-overdraft")
	normalAuth := fileCodexAuth("auth-normal")
	executor := &overdraftFallbackExecutor{overdraftAuthID: overdraftAuth.ID}
	m.RegisterExecutor(executor)
	if _, errRegister := m.Register(context.Background(), overdraftAuth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, errRegister := m.Register(context.Background(), normalAuth); errRegister != nil {
		t.Fatal(errRegister)
	}
	registerSchedulerModels(t, "codex", "gpt-test", overdraftAuth.ID, normalAuth.ID)
	if errObserve := coordinator.ObserveThreshold(overdraftAuth.ID, 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}

	response, errExecute := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute error = %v, calls = %#v", errExecute, executor.calls)
	}
	if string(response.Payload) != `{"ok":true}` {
		t.Fatalf("response = %s", response.Payload)
	}
	if len(executor.calls) != 2 || executor.calls[0] != overdraftAuth.ID || executor.calls[1] != normalAuth.ID {
		t.Fatalf("auth call order = %#v", executor.calls)
	}
	if got := coordinator.Record(overdraftAuth.ID); got.State != codexoverdraft.StateActiveDrain || got.ProbeFailures != 1 {
		t.Fatalf("overdraft record = %#v, want ACTIVE_DRAIN with 1 usage-limit failure", got)
	}
}

func TestManagerOverdraftExecutionHonorsPinnedAndExcludedRequests(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("active")
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	_ = coordinator.ObserveThreshold(auth.ID, 1, 99_000_000)

	request := cliproxyexecutor.Request{Model: "gpt-test", Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`)}
	for _, opts := range []cliproxyexecutor.Options{
		{Alt: "responses/compact"},
		{Metadata: map[string]any{cliproxyexecutor.RequestPathMetadataKey: "/v1/images/generations"}},
		{Metadata: map[string]any{cliproxyexecutor.PinnedAuthMetadataKey: "another"}},
	} {
		if execution, _, ok := m.acquireCodexOverdraftExecution([]string{"codex"}, request, opts, map[string]struct{}{}); ok || execution != nil {
			t.Fatalf("excluded request acquired execution for opts %#v", opts)
		}
	}
}

func TestManagerLifecycleSyncsDisabledAndRemoval(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("auth-1")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if got := coordinator.Record(auth.ID); got.State != codexoverdraft.StateNormal || got.AuthGeneration != 1 {
		t.Fatalf("registered record = %#v", got)
	}
	auth.Disabled = true
	auth.Status = StatusDisabled
	auth.UpdatedAt = time.Now()
	if _, errUpdate := m.Update(context.Background(), auth); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if got := coordinator.Record(auth.ID).State; got != codexoverdraft.StateDisabled {
		t.Fatalf("disabled state = %s", got)
	}
	m.Remove(context.Background(), auth.ID)
	if got := coordinator.Record(auth.ID); got.AuthID != "" {
		t.Fatalf("removed record = %#v", got)
	}
}

func TestManagerLifecycleReturnsReenabledAuthToNormal(t *testing.T) {
	m := NewManager(nil, nil, nil)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("auth-1")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	disabled := auth.Clone()
	disabled.Disabled = true
	disabled.Status = StatusDisabled
	if _, errUpdate := m.Update(context.Background(), disabled); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	reenabled := disabled.Clone()
	reenabled.Disabled = false
	reenabled.Status = StatusActive
	if _, errUpdate := m.Update(context.Background(), reenabled); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if got := coordinator.Record(auth.ID).State; got != codexoverdraft.StateNormal {
		t.Fatalf("reenabled state = %s, want NORMAL", got)
	}
}

func TestManagerConfigCreatesAndDisablesDurableCoordinator(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseCodexOverdraft()
	defer m.CloseAccounting()
	defer m.StopCodexQuota()
	quota := internalconfig.DefaultCodexQuotaConfig()
	overdraft := internalconfig.DefaultCodexOverdraftConfig()
	overdraft.Enabled = true
	overdraft.ProbeModel = "gpt-test"
	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	accounting.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	m.SetConfig(&internalconfig.Config{
		Codex:      internalconfig.CodexConfig{Quota: quota, Overdraft: overdraft},
		Accounting: accounting,
	})
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() {
		t.Fatal("enabled config did not create coordinator")
	}

	overdraft.Enabled = false
	m.SetConfig(&internalconfig.Config{
		Codex:      internalconfig.CodexConfig{Quota: quota, Overdraft: overdraft},
		Accounting: accounting,
	})
	if coordinator.Enabled() || len(coordinator.Records()) != 0 {
		t.Fatalf("disabled coordinator state = %#v", coordinator.Records())
	}
	if m.AccountingService() == nil {
		t.Fatal("accounting service stopped with the overdraft switch")
	}
	m.codexQuotaRuntimeMu.RLock()
	quotaRuntime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if quotaRuntime == nil {
		t.Fatal("quota collection stopped with the overdraft switch")
	}
}

func TestManagerEnablePreservesAlreadyRunningNormalRequests(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseCodexOverdraft()
	defer m.CloseAccounting()
	defer m.StopCodexQuota()
	auth := fileCodexAuth("auth-1")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	endNormal := m.beginCodexLegacyInFlight(auth, cliproxyexecutor.Options{})
	defer endNormal()

	quota := internalconfig.DefaultCodexQuotaConfig()
	overdraft := internalconfig.DefaultCodexOverdraftConfig()
	overdraft.Enabled = true
	overdraft.ProbeModel = "gpt-test"
	overdraft.MaxInFlight = 1
	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	accounting.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	m.SetConfig(&internalconfig.Config{
		Codex:      internalconfig.CodexConfig{Quota: quota, Overdraft: overdraft},
		Accounting: accounting,
	})
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil {
		t.Fatal("coordinator is nil")
	}
	if errObserve := coordinator.ObserveThreshold(auth.ID, 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	if got := coordinator.Record(auth.ID).InheritedInFlight; got != 1 {
		t.Fatalf("inherited in-flight = %d, want 1", got)
	}
	if _, errAcquire := coordinator.AcquireBusiness("new-overdraft"); errAcquire != codexoverdraft.ErrCapacity {
		t.Fatalf("AcquireBusiness error = %v, want ErrCapacity", errAcquire)
	}
}

func TestManagerKeepsOverdraftClosedWhenAccountingStartupFails(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.CloseCodexOverdraft()
	defer m.CloseAccounting()
	defer m.StopCodexQuota()

	quota := internalconfig.DefaultCodexQuotaConfig()
	overdraft := internalconfig.DefaultCodexOverdraftConfig()
	overdraft.Enabled = true
	overdraft.ProbeModel = "gpt-test"
	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	accounting.StoragePath = t.TempDir()

	m.SetConfig(&internalconfig.Config{
		Codex:      internalconfig.CodexConfig{Quota: quota, Overdraft: overdraft},
		Accounting: accounting,
	})
	if coordinator := m.CodexOverdraftCoordinator(); coordinator != nil && coordinator.Enabled() {
		t.Fatal("overdraft admission is active without a durable accounting service")
	}
}

func TestOverdraftStreamCancellationReleasesLeaseWithoutDownstreamDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	var releaseOnce sync.Once
	execution := cliproxyexecutor.NewCodexOverdraftExecution(
		"auth-1", 1, "cycle-1", 1, 1, "dispatch-1", cliproxyexecutor.CodexOverdraftBusiness,
		nil,
		func() { releaseOnce.Do(func() { close(released) }) },
		nil,
	)
	source := make(chan cliproxyexecutor.StreamChunk, 1)
	source <- cliproxyexecutor.StreamChunk{Payload: []byte("chunk")}
	wrapped := wrapCodexOverdraftStream(ctx, &cliproxyexecutor.StreamResult{Chunks: source}, execution)
	if wrapped == nil || wrapped.Chunks == nil {
		t.Fatal("wrapped stream is nil")
	}
	cancel()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("lease was not released after downstream cancellation")
	}
	close(source)
}

func TestOverdraftStreamErrorLeavesAuthBeforeTryingNextPooledModel(t *testing.T) {
	m := NewManager(nil, nil, nil)
	executor := &overdraftModelPoolStreamExecutor{}
	execution := cliproxyexecutor.NewCodexOverdraftExecution(
		"auth-1", 1, "cycle-1", 1, 1, "dispatch-1", cliproxyexecutor.CodexOverdraftBusiness,
		nil, nil, nil,
	)
	opts := attachCodexOverdraftExecution(cliproxyexecutor.Options{Stream: true}, execution)
	_, errStream := m.executeStreamWithModelPool(
		context.Background(), executor, fileCodexAuth("auth-1"), "codex",
		cliproxyexecutor.Request{Model: "public", Payload: []byte(`{"input":[{"role":"user","content":"hello"}]}`)},
		opts, "public", "", []string{"model-a", "model-b"}, true, OAuthModelAliasResult{}, nil, true, false, nil,
	)
	if errStream == nil {
		t.Fatal("overdraft stream error = nil")
	}
	if executor.calls != 1 {
		t.Fatalf("overdraft model pool calls = %d, want 1 before ordinary-pool retry", executor.calls)
	}
}
