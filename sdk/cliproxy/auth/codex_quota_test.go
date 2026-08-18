package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	usageaccounting "github.com/router-for-me/CLIProxyAPI/v7/internal/usage/accounting"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type quotaTestExecutor struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	body    string
}

type quotaUnauthorizedExecutor struct {
	refreshCalls       atomic.Int32
	alwaysUnauthorized bool
}

func (e *quotaUnauthorizedExecutor) Identifier() string { return "codex" }
func (e *quotaUnauthorizedExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *quotaUnauthorizedExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (e *quotaUnauthorizedExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.refreshCalls.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "new-token"
	return updated, nil
}
func (e *quotaUnauthorizedExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *quotaUnauthorizedExecutor) HttpRequest(_ context.Context, auth *Auth, request *http.Request) (*http.Response, error) {
	status := http.StatusUnauthorized
	body := ""
	if !e.alwaysUnauthorized && authAccessToken(auth) == "new-token" {
		status = http.StatusOK
		body = `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1999999999}}}`
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type quotaRefreshStore struct {
	mu       sync.Mutex
	last     *Auth
	failSave bool
}

func (s *quotaRefreshStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *quotaRefreshStore) Save(_ context.Context, auth *Auth) (string, error) {
	if s.failSave {
		return "", errors.New("injected credential persistence failure")
	}
	s.mu.Lock()
	s.last = auth.Clone()
	s.mu.Unlock()
	return "", nil
}
func (s *quotaRefreshStore) Delete(context.Context, string) error { return nil }
func (s *quotaRefreshStore) lastToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return authAccessToken(s.last)
}

func (e *quotaTestExecutor) Identifier() string { return "codex" }
func (e *quotaTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *quotaTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (e *quotaTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (e *quotaTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (e *quotaTestExecutor) HttpRequest(_ context.Context, _ *Auth, request *http.Request) (*http.Response, error) {
	e.calls.Add(1)
	if e.started != nil {
		select {
		case e.started <- struct{}{}:
		default:
		}
	}
	if e.release != nil {
		<-e.release
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(e.body)), Request: request}, nil
}

func quotaRuntimeConfigForTest(t *testing.T, quota internalconfig.CodexQuotaConfig) *internalconfig.Config {
	t.Helper()
	return &internalconfig.Config{
		AuthDir: t.TempDir(),
		Codex: internalconfig.CodexConfig{
			Quota:     quota,
			Overdraft: internalconfig.DefaultCodexOverdraftConfig(),
		},
	}
}

func TestManagerPassiveQuotaObservationUsesAuthIDAndTriggersThreshold(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("auth-1")
	if _, errRegister := m.Register(nil, auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	observer := m.codexQuotaObserverForAuth(auth)
	if observer == nil {
		t.Fatal("quota observer = nil")
	}
	observer(cliproxyexecutor.CodexQuotaHeadersObservation{
		AuthID:     auth.ID,
		AttemptID:  "attempt-1",
		ObservedAt: time.Now(),
		Header: http.Header{
			"X-Codex-Primary-Used-Percent":   []string{"98.5"},
			"X-Codex-Primary-Window-Minutes": []string{"300"},
		},
	})

	var snapshot codexoverdraft.QuotaSnapshot
	var ok bool
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot, ok = m.CodexQuotaSnapshot(auth.ID)
		if ok && coordinator.Record(auth.ID).State == codexoverdraft.StateActiveDrain {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !ok || snapshot.AuthID != auth.ID || snapshot.SourceAttemptID != "attempt-1" || snapshot.Sequence == 0 {
		t.Fatalf("snapshot = (%#v, %t)", snapshot, ok)
	}
	if got := coordinator.Record(auth.ID).State; got != codexoverdraft.StateActiveDrain {
		t.Fatalf("pool state = %s, want ACTIVE_DRAIN", got)
	}
}

func TestCodexCreditProtectionUsesAccountQuotaCooldown(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig())
	cfg.Codex.Overdraft.AllowCreditSpend = false
	m.runtimeConfig.Store(cfg)
	auth := fileCodexAuth("credit-auth")
	auth.ModelStates = map[string]*ModelState{"gpt-test": {Status: StatusActive}}
	m.auths[auth.ID] = auth
	m.codexOverdraftGenerations[auth.ID] = 1
	resetAt := time.Now().Add(5 * time.Minute)
	snapshot := codexoverdraft.QuotaSnapshot{
		AuthID: auth.ID, AuthGeneration: 1, Source: codexoverdraft.QuotaSourceActive, ObservedAt: time.Now(), Allowed: true,
		HasCredits: true, Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 100_000_000, ResetAt: resetAt}},
	}
	if protected := m.applyCodexCreditProtection(snapshot); !protected {
		t.Fatal("credit protection was not activated")
	}
	updated := m.auths[auth.ID]
	if updated.Disabled || !updated.Unavailable || !updated.Quota.Exceeded || updated.Quota.Reason != "credit_protection" || !updated.Quota.NextRecoverAt.Equal(resetAt) {
		t.Fatalf("protected auth = %#v", updated)
	}
	blocked, reason, retryAt := isAuthBlockedForModel(updated, "gpt-test", time.Now())
	if !blocked || reason != blockReasonCooldown || !retryAt.Equal(resetAt) {
		t.Fatalf("selection block = (%t, %v, %v), want quota cooldown at %v", blocked, reason, retryAt, resetAt)
	}
	if _, errSelect := m.availableAuthsForRouteModel([]*Auth{updated}, "codex", "gpt-test", time.Now()); errSelect == nil {
		t.Fatal("credit-protected selection error = nil")
	} else if _, ok := errSelect.(*modelCooldownError); !ok {
		t.Fatalf("credit-protected selection error = %v, want model_cooldown", errSelect)
	}
	if got := m.CodexCreditProtectionCount(); got != 1 {
		t.Fatalf("CodexCreditProtectionCount() = %d, want 1", got)
	}
}

func TestCodexCreditProtectionFallbackAndClearPaths(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*codexoverdraft.QuotaSnapshot, *internalconfig.Config)
	}{
		{name: "credits disappear", mutate: func(s *codexoverdraft.QuotaSnapshot, _ *internalconfig.Config) { s.HasCredits = false }},
		{name: "credits unlimited", mutate: func(s *codexoverdraft.QuotaSnapshot, _ *internalconfig.Config) { s.CreditsUnlimited = true }},
		{name: "window recovers", mutate: func(s *codexoverdraft.QuotaSnapshot, _ *internalconfig.Config) {
			s.Windows[0].UsedMicropct = 99_000_000
		}},
		{name: "operator allows spend", mutate: func(_ *codexoverdraft.QuotaSnapshot, cfg *internalconfig.Config) {
			cfg.Codex.Overdraft.AllowCreditSpend = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			cfg := quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig())
			m.runtimeConfig.Store(cfg)
			auth := fileCodexAuth("credit-clear")
			m.auths[auth.ID] = auth
			m.codexOverdraftGenerations[auth.ID] = 1
			snapshot := codexoverdraft.QuotaSnapshot{AuthID: auth.ID, AuthGeneration: 1, ObservedAt: time.Now(), HasCredits: true, Windows: []codexoverdraft.QuotaWindow{{UsedMicropct: 100_000_000}}}
			m.applyCodexCreditProtection(snapshot)
			if m.auths[auth.ID].Quota.NextRecoverAt.IsZero() {
				t.Fatal("missing fallback recovery time")
			}
			test.mutate(&snapshot, cfg)
			m.runtimeConfig.Store(cfg)
			snapshot.ObservedAt = snapshot.ObservedAt.Add(time.Second)
			m.applyCodexCreditProtection(snapshot)
			if m.auths[auth.ID].Quota.Reason == "credit_protection" || m.auths[auth.ID].Unavailable {
				t.Fatalf("credit protection was not cleared: %#v", m.auths[auth.ID])
			}
		})
	}
}

func TestCodexCreditProtectionDoesNotOverwriteRealFailure(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.runtimeConfig.Store(quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig()))
	auth := fileCodexAuth("credit-real-failure")
	auth.Unavailable = true
	auth.Quota = QuotaState{Exceeded: true, Reason: "cloudflare challenge", NextRecoverAt: time.Now().Add(time.Minute)}
	m.auths[auth.ID] = auth
	m.codexOverdraftGenerations[auth.ID] = 1
	snapshot := codexoverdraft.QuotaSnapshot{AuthID: auth.ID, AuthGeneration: 1, ObservedAt: time.Now(), HasCredits: true, LimitReached: true}
	m.applyCodexCreditProtection(snapshot)
	if got := m.auths[auth.ID].Quota.Reason; got != "cloudflare challenge" {
		t.Fatalf("Quota.Reason = %q, want real failure preserved", got)
	}
}

func TestCodexCreditProtectionReevaluatesActiveSnapshotAfterConfigChange(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig())
	m.runtimeConfig.Store(cfg)
	auth := fileCodexAuth("credit-config-change")
	m.auths[auth.ID] = auth
	m.codexOverdraftGenerations[auth.ID] = 1
	snapshot := codexoverdraft.QuotaSnapshot{
		AuthID:         auth.ID,
		AuthGeneration: 1,
		ObservedAt:     time.Now(),
		Source:         codexoverdraft.QuotaSourceActive,
		HasCredits:     true,
		Windows:        []codexoverdraft.QuotaWindow{{UsedMicropct: 100_000_000}},
	}
	m.codexQuotaSnapshots[auth.ID] = snapshot
	m.applyCodexCreditProtection(snapshot)

	cfg.Codex.Overdraft.AllowCreditSpend = true
	m.runtimeConfig.Store(cfg)
	if changed := m.reevaluateCodexCreditProtection(); !changed {
		t.Fatal("reevaluateCodexCreditProtection() did not report the cleared state")
	}
	if got := m.auths[auth.ID].Quota.Reason; got == "credit_protection" {
		t.Fatalf("Quota.Reason = %q after opt-in", got)
	}
}

func TestCodexCreditSpendOptInPreservesDrainBehavior(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig())
	cfg.Codex.Overdraft.AllowCreditSpend = true
	m.runtimeConfig.Store(cfg)
	coordinator := testOverdraftCoordinator(t)
	m.SetCodexOverdraftCoordinator(coordinator)
	auth := fileCodexAuth("credit-opt-in")
	m.auths[auth.ID] = auth
	_ = coordinator.RegisterAuth(auth.ID, 1, 0)
	snapshot := codexoverdraft.QuotaSnapshot{AuthID: auth.ID, AuthGeneration: 1, ObservedAt: time.Now(), HasCredits: true, LimitReached: true, Windows: []codexoverdraft.QuotaWindow{{UsedMicropct: 100_000_000, ResetAt: time.Now().Add(time.Hour)}}}
	m.applyActiveCodexQuotaSnapshot(snapshot)
	if m.auths[auth.ID].Quota.Reason == "credit_protection" || coordinator.Record(auth.ID).State != codexoverdraft.StateActiveDrain {
		t.Fatalf("opt-in state = auth %#v record %#v", m.auths[auth.ID], coordinator.Record(auth.ID))
	}
}

func TestCodexCreditSpendAccountingClass(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig())
	cfg.Codex.Overdraft.AllowCreditSpend = true
	m.runtimeConfig.Store(cfg)
	m.codexQuotaSnapshots["credit-accounting"] = codexoverdraft.QuotaSnapshot{
		AuthID: "credit-accounting", HasCredits: true,
		Windows: []codexoverdraft.QuotaWindow{{UsedMicropct: 100_000_000}},
	}
	if got := m.codexConsumptionClass("credit-accounting"); got != "over_window_on_credits" {
		t.Fatalf("codexConsumptionClass() = %q", got)
	}
	cfg.Codex.Overdraft.AllowCreditSpend = false
	m.runtimeConfig.Store(cfg)
	if got := m.codexConsumptionClass("credit-accounting"); got != "within_window" {
		t.Fatalf("protected codexConsumptionClass() = %q", got)
	}
}

func TestManagerQuotaObservationRejectsOldGeneration(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.codexQuotaEnabled.Store(true)
	auth := fileCodexAuth("auth-1")
	m.codexOverdraftGenerations[auth.ID] = 2
	m.observeCodexQuotaHeaders(cliproxyexecutor.CodexQuotaHeadersObservation{
		AuthID:         auth.ID,
		AuthGeneration: 1,
		ObservedAt:     time.Now(),
		Header:         http.Header{"X-Codex-Primary-Used-Percent": []string{"99"}},
	})
	if _, ok := m.CodexQuotaSnapshot(auth.ID); ok {
		t.Fatal("old generation observation was stored")
	}
}

func TestManagerInconsistentPassiveQuotaSchedulesEarlyCalibration(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	now := time.Now()
	m.observeCodexQuotaHeaders(cliproxyexecutor.CodexQuotaHeadersObservation{
		AuthID:     "auth-1",
		ObservedAt: now,
		Header: http.Header{
			"X-Codex-Primary-Used-Percent":        []string{"50"},
			"X-Codex-Primary-Reset-After-Seconds": []string{"60"},
			"X-Codex-Primary-Reset-At":            []string{strconv.FormatInt(now.Add(30*time.Second).Unix(), 10)},
		},
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		m.codexQuotaRuntimeMu.RLock()
		runtime := m.codexQuotaRuntime
		m.codexQuotaRuntimeMu.RUnlock()
		if runtime != nil {
			runtime.mu.Lock()
			runAt, scheduled := runtime.scheduledAt["auth-1"]
			runtime.mu.Unlock()
			if scheduled {
				want := now.Add(33 * time.Second)
				if runAt.Before(want.Add(-2*time.Second)) || runAt.After(want.Add(2*time.Second)) {
					t.Fatalf("calibration runAt = %s, want near %s", runAt, want)
				}
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("inconsistent quota observation did not schedule calibration")
}

func TestManagerActiveQuotaRefreshParsesUsageWithoutTokenAccounting(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaTestExecutor{body: `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1999999999}}}`}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-1")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	snapshot, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID)
	if errRefresh != nil {
		t.Fatal(errRefresh)
	}
	if snapshot.Source != codexoverdraft.QuotaSourceActive || len(snapshot.Windows) != 1 || snapshot.Windows[0].UsedMicropct != 42_000_000 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestManagerActiveQuotaRefreshSkipsMissingAccountID(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaTestExecutor{body: `{"rate_limit":{"allowed":true}}`}
	m.RegisterExecutor(executor)
	m.SetConfig(quotaRuntimeConfigForTest(t, internalconfig.DefaultCodexQuotaConfig()))
	auth := fileCodexAuth("auth-missing-account")
	delete(auth.Metadata, "account_id")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID); errRefresh == nil || !strings.Contains(errRefresh.Error(), "account_id") {
		t.Fatalf("RefreshCodexQuota() error = %v, want missing account_id", errRefresh)
	}
	if calls := executor.calls.Load(); calls != 0 {
		t.Fatalf("quota executor calls = %d, want 0", calls)
	}
}

func TestManagerActiveQuotaRefreshRetainsAllowlistedDebugPayload(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaTestExecutor{body: `{"plan_type":"plus","access_token":"TOKEN","credits":{"has_credits":true,"balance":"3.25","private":"secret"},"rate_limit":{"allowed":true,"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1999999999},"prompt":"do not retain"}}`}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-debug")
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID); errRefresh != nil {
		t.Fatal(errRefresh)
	}
	debugPayload, ok := m.CodexQuotaDebugPayload(auth.ID)
	if !ok || debugPayload.AuthID != auth.ID {
		t.Fatalf("debug payload = %#v, %t", debugPayload, ok)
	}
	encoded := string(debugPayload.Payload)
	for _, expected := range []string{`"plan_type":"plus"`, `"has_credits":true`, `"used_percent":42`} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("debug payload = %s, want %s", encoded, expected)
		}
	}
	for _, forbidden := range []string{"TOKEN", "secret", "prompt", "access_token", "private"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("debug payload leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestManagerActiveQuota401PublishesAndPersistsSharedRefresh(t *testing.T) {
	store := &quotaRefreshStore{}
	m := NewManager(store, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaUnauthorizedExecutor{}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-refresh")
	auth.Metadata = map[string]any{"access_token": "old-token", "refresh_token": "refresh-token", "account_id": "acct-auth-refresh"}
	m.mu.Lock()
	m.auths[auth.ID] = auth.Clone()
	m.mu.Unlock()
	if _, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID); errRefresh != nil {
		t.Fatal(errRefresh)
	}
	m.mu.RLock()
	current := m.auths[auth.ID].Clone()
	m.mu.RUnlock()
	if authAccessToken(current) != "new-token" || store.lastToken() != "new-token" || executor.refreshCalls.Load() != 1 {
		t.Fatalf("refresh state current=%q persisted=%q calls=%d", authAccessToken(current), store.lastToken(), executor.refreshCalls.Load())
	}
}

func TestManagerActiveQuota401DoesNotPublishUnpersistedRefresh(t *testing.T) {
	store := &quotaRefreshStore{failSave: true}
	m := NewManager(store, nil, nil)
	defer m.StopCodexQuota()
	m.RegisterExecutor(&quotaUnauthorizedExecutor{})
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-refresh-fail")
	auth.Metadata = map[string]any{"access_token": "old-token", "refresh_token": "refresh-token", "account_id": "acct-auth-refresh-fail"}
	m.mu.Lock()
	m.auths[auth.ID] = auth.Clone()
	m.mu.Unlock()
	if _, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID); errRefresh == nil {
		t.Fatal("RefreshCodexQuota() error = nil, want credential persistence failure")
	}
	m.mu.RLock()
	current := m.auths[auth.ID].Clone()
	m.mu.RUnlock()
	if authAccessToken(current) != "old-token" {
		t.Fatalf("unpersisted token was published: %q", authAccessToken(current))
	}
}

func TestManagerActiveQuotaSecond401UsesCredentialFailureState(t *testing.T) {
	m := NewManager(&quotaRefreshStore{}, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaUnauthorizedExecutor{alwaysUnauthorized: true}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-refresh-still-unauthorized")
	auth.Metadata = map[string]any{"access_token": "old-token", "refresh_token": "refresh-token", "account_id": "acct-auth-refresh-still-unauthorized"}
	m.mu.Lock()
	m.auths[auth.ID] = auth.Clone()
	m.mu.Unlock()

	if _, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID); errRefresh == nil {
		t.Fatal("RefreshCodexQuota() error = nil after a second 401")
	}
	current, ok := m.GetByID(auth.ID)
	if !ok || current.LastError == nil || current.LastError.StatusCode() != http.StatusUnauthorized || !current.Unavailable {
		t.Fatalf("auth after second 401 = %#v", current)
	}
	if executor.refreshCalls.Load() != 1 {
		t.Fatalf("refresh calls = %d, want 1", executor.refreshCalls.Load())
	}
}

func TestManagerActiveQuotaRefreshSingleflight(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaTestExecutor{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
		body:    `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1999999999}}}`,
	}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-1")
	m.mu.Lock()
	m.auths[auth.ID] = auth
	m.mu.Unlock()

	var wait sync.WaitGroup
	var ready sync.WaitGroup
	wait.Add(2)
	ready.Add(2)
	start := make(chan struct{})
	errorsCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wait.Done()
			ready.Done()
			<-start
			_, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID)
			errorsCh <- errRefresh
		}()
	}
	ready.Wait()
	close(start)
	<-executor.started
	time.Sleep(10 * time.Millisecond)
	close(executor.release)
	wait.Wait()
	close(errorsCh)
	for errRefresh := range errorsCh {
		if errRefresh != nil {
			t.Fatal(errRefresh)
		}
	}
	if calls := executor.calls.Load(); calls != 1 {
		t.Fatalf("active query calls = %d, want 1", calls)
	}
}

func TestManagerActiveQuotaRefreshRejectsRemovedGeneration(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	executor := &quotaTestExecutor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		body:    `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":42,"limit_window_seconds":18000,"reset_at":1999999999}}}`,
	}
	m.RegisterExecutor(executor)
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	auth := fileCodexAuth("auth-1")
	m.mu.Lock()
	m.auths[auth.ID] = auth
	m.mu.Unlock()
	m.codexOverdraftMu.Lock()
	m.codexOverdraftGenerations[auth.ID] = 1
	m.codexOverdraftMu.Unlock()

	result := make(chan error, 1)
	go func() {
		_, errRefresh := m.RefreshCodexQuota(context.Background(), auth.ID)
		result <- errRefresh
	}()
	<-executor.started
	m.removeCodexOverdraftAuth(auth.ID)
	close(executor.release)
	if errRefresh := <-result; errRefresh == nil {
		t.Fatal("old-generation active quota response was accepted")
	}
	if _, ok := m.CodexQuotaSnapshot(auth.ID); ok {
		t.Fatal("old-generation active quota response replaced the current snapshot")
	}
}

func TestManagerRestoresDurableQuotaSnapshotAndSequence(t *testing.T) {
	authDir := t.TempDir()
	quota := internalconfig.DefaultCodexQuotaConfig()
	quota.StaleAfter = "24h"
	quota.StartupJitter = "24h"
	cfg := &internalconfig.Config{
		AuthDir: authDir,
		Codex:   internalconfig.CodexConfig{Quota: quota},
	}

	first := NewManager(nil, nil, nil)
	first.SetConfig(cfg)
	snapshot := codexoverdraft.QuotaSnapshot{
		AuthID:          "auth-durable",
		AuthGeneration:  1,
		Source:          codexoverdraft.QuotaSourcePassive,
		SourceAttemptID: "attempt-40",
		ObservedAt:      time.Now(),
		Sequence:        40,
		Allowed:         true,
		Windows: []codexoverdraft.QuotaWindow{{
			Name:         codexoverdraft.WindowPrimary,
			UsedMicropct: 98_000_000,
			ResetAt:      time.Now().Add(time.Hour),
		}},
	}
	first.codexQuotaSequence.Store(snapshot.Sequence)
	first.applyPassiveCodexQuotaSnapshot(snapshot)
	first.StopCodexQuota()

	second := NewManager(nil, nil, nil)
	defer second.StopCodexQuota()
	if _, errRegister := second.Register(context.Background(), fileCodexAuth(snapshot.AuthID)); errRegister != nil {
		t.Fatal(errRegister)
	}
	second.SetConfig(cfg)
	restored, ok := second.CodexQuotaSnapshot(snapshot.AuthID)
	if !ok {
		t.Fatal("durable quota snapshot was not restored")
	}
	if restored.Sequence != snapshot.Sequence || restored.SourceAttemptID != snapshot.SourceAttemptID {
		t.Fatalf("restored snapshot = %#v", restored)
	}
	if got := second.codexQuotaSequence.Load(); got != snapshot.Sequence {
		t.Fatalf("restored sequence = %d, want %d", got, snapshot.Sequence)
	}
}

func TestManagerRestoresAuthGenerationFenceAfterRemoval(t *testing.T) {
	authDir := t.TempDir()
	quota := internalconfig.DefaultCodexQuotaConfig()
	cfg := &internalconfig.Config{AuthDir: authDir, Codex: internalconfig.CodexConfig{Quota: quota}}

	first := NewManager(nil, nil, nil)
	first.SetConfig(cfg)
	auth := fileCodexAuth("auth-recreated")
	if _, errRegister := first.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	first.Remove(context.Background(), auth.ID)
	first.StopCodexQuota()

	second := NewManager(nil, nil, nil)
	defer second.StopCodexQuota()
	if _, errRegister := second.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	second.SetConfig(cfg)
	if got := second.codexAuthGeneration(auth.ID); got != 2 {
		t.Fatalf("restored auth generation = %d, want 2", got)
	}
}

func TestManagerQuotaShutdownDrainPersistsQueuedObservation(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	quota := internalconfig.DefaultCodexQuotaConfig()
	m.SetConfig(quotaRuntimeConfigForTest(t, quota))
	snapshot := codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-drain",
		AuthGeneration: 1,
		Source:         codexoverdraft.QuotaSourcePassive,
		ObservedAt:     time.Now(),
		Sequence:       1,
		Windows:        []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 50_000_000}},
	}
	runtime := &codexQuotaRuntime{
		observations:    make(chan codexoverdraft.QuotaSnapshot, 1),
		observationWake: make(chan struct{}, 1),
		pending:         make(map[string]codexQuotaPending),
	}
	runtime.observations <- snapshot
	m.drainCodexQuotaObservationBus(runtime)

	m.codexQuotaStoreMu.Lock()
	history, errHistory := m.codexQuotaStore.History()
	m.codexQuotaStoreMu.Unlock()
	if errHistory != nil {
		t.Fatal(errHistory)
	}
	if len(history) != 1 || history[0].Sequence != snapshot.Sequence {
		t.Fatalf("durable quota history = %#v", history)
	}
}

func TestManagerLoadSchedulesColdStartQuotaForLoadedAuths(t *testing.T) {
	auth := fileCodexAuth("auth-1")
	m := NewManager(&schedulerLoadStore{auths: []*Auth{auth}}, nil, nil)
	defer m.StopCodexQuota()
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	runtime := &codexQuotaRuntime{
		ctx:         runtimeCtx,
		cancel:      runtimeCancel,
		jitter:      time.Hour,
		timers:      make(map[string]*time.Timer),
		scheduledAt: make(map[string]time.Time),
	}
	m.codexQuotaRuntime = runtime
	m.codexQuotaEnabled.Store(true)
	if errLoad := m.Load(context.Background()); errLoad != nil {
		t.Fatal(errLoad)
	}

	runtime.mu.Lock()
	_, scheduled := runtime.scheduledAt[auth.ID]
	runtime.mu.Unlock()
	if !scheduled {
		t.Fatal("loaded auth did not receive a cold-start quota refresh")
	}
}

func TestCodexQuotaPendingDropsPeakFromOlderResetCycle(t *testing.T) {
	firstReset := time.Unix(2_000_000_000, 0)
	secondReset := firstReset.Add(time.Hour)
	pending := mergeCodexQuotaPending(codexQuotaPending{}, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       1,
		ObservedAt:     firstReset.Add(-time.Minute),
		Windows: []codexoverdraft.QuotaWindow{{
			Name:         codexoverdraft.WindowPrimary,
			UsedMicropct: 99_000_000,
			ResetAt:      firstReset,
		}},
	})
	pending = mergeCodexQuotaPending(pending, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       2,
		ObservedAt:     firstReset,
		Windows: []codexoverdraft.QuotaWindow{{
			Name:         codexoverdraft.WindowPrimary,
			UsedMicropct: 1_000_000,
			ResetAt:      secondReset,
		}},
	})

	snapshots := codexQuotaPendingSnapshots(pending)
	if len(snapshots) != 1 || snapshots[0].Sequence != 2 {
		t.Fatalf("coalesced snapshots = %#v, want only the new reset cycle", snapshots)
	}
	if got := snapshots[0].Windows[0]; got.ResetAt != secondReset || got.UsedMicropct != 1_000_000 {
		t.Fatalf("latest window = %#v", got)
	}
}

func TestCodexQuotaPendingPreservesSameCycleWindowPeaks(t *testing.T) {
	primaryReset := time.Unix(2_000_000_000, 0)
	secondaryReset := primaryReset.Add(24 * time.Hour)
	pending := mergeCodexQuotaPending(codexQuotaPending{}, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       1,
		Windows: []codexoverdraft.QuotaWindow{{
			Name:         codexoverdraft.WindowPrimary,
			UsedMicropct: 99_000_000,
			ResetAt:      primaryReset,
		}},
	})
	pending = mergeCodexQuotaPending(pending, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       2,
		Windows: []codexoverdraft.QuotaWindow{
			{Name: codexoverdraft.WindowPrimary, UsedMicropct: 20_000_000, ResetAt: primaryReset},
			{Name: codexoverdraft.WindowSecondary, UsedMicropct: 98_000_000, ResetAt: secondaryReset},
		},
	})
	pending = mergeCodexQuotaPending(pending, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       3,
		Windows: []codexoverdraft.QuotaWindow{
			{Name: codexoverdraft.WindowPrimary, UsedMicropct: 10_000_000, ResetAt: primaryReset},
			{Name: codexoverdraft.WindowSecondary, UsedMicropct: 20_000_000, ResetAt: secondaryReset},
		},
	})

	snapshots := codexQuotaPendingSnapshots(pending)
	if len(snapshots) != 3 {
		t.Fatalf("coalesced snapshot count = %d, want 3: %#v", len(snapshots), snapshots)
	}
	if snapshots[0].Sequence != 1 || len(snapshots[0].Windows) != 1 || snapshots[0].Windows[0].Name != codexoverdraft.WindowPrimary || snapshots[0].Windows[0].UsedMicropct != 99_000_000 {
		t.Fatalf("primary peak snapshot = %#v", snapshots[0])
	}
	if snapshots[1].Sequence != 2 || len(snapshots[1].Windows) != 1 || snapshots[1].Windows[0].Name != codexoverdraft.WindowSecondary || snapshots[1].Windows[0].UsedMicropct != 98_000_000 {
		t.Fatalf("secondary peak snapshot = %#v", snapshots[1])
	}
	if snapshots[2].Sequence != 3 || len(snapshots[2].Windows) != 2 {
		t.Fatalf("latest snapshot = %#v", snapshots[2])
	}
}

func TestCodexQuotaPendingKeepsUnchangedWindowAcrossOtherReset(t *testing.T) {
	oldPrimaryReset := time.Unix(2_000_000_000, 0)
	newPrimaryReset := oldPrimaryReset.Add(time.Hour)
	secondaryReset := oldPrimaryReset.Add(24 * time.Hour)
	pending := mergeCodexQuotaPending(codexQuotaPending{}, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       1,
		Windows: []codexoverdraft.QuotaWindow{
			{Name: codexoverdraft.WindowPrimary, UsedMicropct: 95_000_000, ResetAt: oldPrimaryReset},
			{Name: codexoverdraft.WindowSecondary, UsedMicropct: 99_000_000, ResetAt: secondaryReset},
		},
	})
	pending = mergeCodexQuotaPending(pending, codexoverdraft.QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		Sequence:       2,
		Windows: []codexoverdraft.QuotaWindow{
			{Name: codexoverdraft.WindowPrimary, UsedMicropct: 1_000_000, ResetAt: newPrimaryReset},
			{Name: codexoverdraft.WindowSecondary, UsedMicropct: 20_000_000, ResetAt: secondaryReset},
		},
	})

	snapshots := codexQuotaPendingSnapshots(pending)
	if len(snapshots) != 2 {
		t.Fatalf("coalesced snapshot count = %d, want 2: %#v", len(snapshots), snapshots)
	}
	if snapshots[0].Sequence != 1 || len(snapshots[0].Windows) != 1 || snapshots[0].Windows[0].Name != codexoverdraft.WindowSecondary {
		t.Fatalf("retained peak snapshot = %#v, want only secondary", snapshots[0])
	}
	if snapshots[1].Sequence != 2 {
		t.Fatalf("latest snapshot = %#v", snapshots[1])
	}
}

func TestPredictCodexQuotaRefreshDelayUsesFixedPointRate(t *testing.T) {
	reset := time.Unix(2_000_000_000, 0)
	base := time.Unix(1_900_000_000, 0)
	prev := codexoverdraft.QuotaSnapshot{AuthGeneration: 1, ObservedAt: base, Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 80_000_000, ResetAt: reset}}}
	latest := codexoverdraft.QuotaSnapshot{AuthGeneration: 1, ObservedAt: base.Add(10 * time.Minute), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 90_000_000, ResetAt: reset}}}
	if got := predictCodexQuotaRefreshDelay(prev, latest, 98_000_000, time.Minute, 10*time.Minute); got != 4*time.Minute {
		t.Fatalf("predicted delay = %v, want 4m", got)
	}
	closer := codexoverdraft.QuotaSnapshot{AuthGeneration: 1, ObservedAt: base.Add(16 * time.Minute), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 96_000_000, ResetAt: reset}}}
	if got := predictCodexQuotaRefreshDelay(latest, closer, 98_000_000, time.Minute, 10*time.Minute); got != time.Minute {
		t.Fatalf("closer predicted delay = %v, want 1m", got)
	}
}

func TestPredictCodexQuotaRefreshDelayFallsBackForInvalidHistory(t *testing.T) {
	now := time.Now()
	reset := now.Add(time.Hour)
	latest := codexoverdraft.QuotaSnapshot{AuthGeneration: 2, ObservedAt: now, Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 90_000_000, ResetAt: reset}}}
	for name, prev := range map[string]codexoverdraft.QuotaSnapshot{
		"missing":    {},
		"generation": {AuthGeneration: 1, ObservedAt: now.Add(-time.Minute), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 80_000_000, ResetAt: reset}}},
		"reset":      {AuthGeneration: 2, ObservedAt: now.Add(-time.Minute), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 80_000_000, ResetAt: reset.Add(-time.Hour)}}},
		"decreasing": {AuthGeneration: 2, ObservedAt: now.Add(-time.Minute), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: 95_000_000, ResetAt: reset}}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := predictCodexQuotaRefreshDelay(prev, latest, 98_000_000, time.Minute, 10*time.Minute); got != 10*time.Minute {
				t.Fatalf("delay = %v, want stale fallback", got)
			}
		})
	}
}

func TestManagerQuotaSnapshotHistoryFencesGeneration(t *testing.T) {
	m := NewManager(nil, nil, nil)
	first := codexoverdraft.QuotaSnapshot{AuthID: "auth-history", AuthGeneration: 1, Sequence: 1}
	second := codexoverdraft.QuotaSnapshot{AuthID: "auth-history", AuthGeneration: 1, Sequence: 2}
	m.codexQuotaMu.Lock()
	m.setCodexQuotaSnapshotLocked(first.AuthID, first)
	m.setCodexQuotaSnapshotLocked(second.AuthID, second)
	prev := m.codexQuotaPrevSnapshots[first.AuthID]
	m.codexQuotaMu.Unlock()
	if prev.Sequence != first.Sequence {
		t.Fatalf("previous snapshot = %#v", prev)
	}
	third := codexoverdraft.QuotaSnapshot{AuthID: "auth-history", AuthGeneration: 2, Sequence: 3}
	m.codexQuotaMu.Lock()
	m.setCodexQuotaSnapshotLocked(third.AuthID, third)
	_, retained := m.codexQuotaPrevSnapshots[first.AuthID]
	m.codexQuotaMu.Unlock()
	if retained {
		t.Fatal("previous generation snapshot was retained")
	}
}

func TestScheduleCodexQuotaAfterResultAcceleratesArmedAndPredictedAuths(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		previous int64
		latest   int64
		elapsed  time.Duration
		arm      int64
		wantMin  time.Duration
		wantMax  time.Duration
	}{
		{name: "armed", previous: 89_000_000, latest: 91_000_000, elapsed: time.Minute, arm: 90_000_000, wantMin: time.Minute, wantMax: 70 * time.Second},
		{name: "predicted", previous: 80_000_000, latest: 90_000_000, elapsed: 10 * time.Minute, arm: 91_000_000, wantMin: 4 * time.Minute, wantMax: 4*time.Minute + 10*time.Second},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			auth := fileCodexAuth("auth-schedule-" + testCase.name)
			m.mu.Lock()
			m.auths[auth.ID] = auth
			m.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runtime := &codexQuotaRuntime{
				ctx: ctx, cancel: cancel, staleAfter: 10 * time.Minute, nearThresholdStaleAfter: time.Minute,
				minActiveInterval: time.Minute, armThresholdMicropct: testCase.arm, thresholdMicropct: 98_000_000,
				timers: make(map[string]*time.Timer), scheduledAt: make(map[string]time.Time), errorSteps: make(map[string]int), noResetSteps: make(map[string]int),
			}
			observedAt := time.Now()
			reset := observedAt.Add(time.Hour)
			previous := codexoverdraft.QuotaSnapshot{AuthID: auth.ID, AuthGeneration: 1, ObservedAt: observedAt.Add(-testCase.elapsed), Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: testCase.previous, ResetAt: reset}}}
			latest := codexoverdraft.QuotaSnapshot{AuthID: auth.ID, AuthGeneration: 1, ObservedAt: observedAt, Windows: []codexoverdraft.QuotaWindow{{Name: codexoverdraft.WindowPrimary, UsedMicropct: testCase.latest, ResetAt: reset}}}
			m.codexQuotaMu.Lock()
			m.codexQuotaPrevSnapshots[auth.ID] = previous
			m.codexQuotaSnapshots[auth.ID] = latest
			m.codexQuotaMu.Unlock()
			before := time.Now()
			m.scheduleCodexQuotaAfterResult(runtime, auth.ID, latest, nil)
			runtime.mu.Lock()
			runAt, scheduled := runtime.scheduledAt[auth.ID]
			if timer := runtime.timers[auth.ID]; timer != nil {
				timer.Stop()
			}
			runtime.mu.Unlock()
			if !scheduled {
				t.Fatal("refresh was not scheduled")
			}
			delay := runAt.Sub(before)
			if delay < testCase.wantMin || delay > testCase.wantMax {
				t.Fatalf("scheduled delay = %v, want [%v,%v]", delay, testCase.wantMin, testCase.wantMax)
			}
		})
	}
}

func TestCodexQuotaMinActiveIntervalIsPerAuthHardFloor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &codexQuotaRuntime{ctx: ctx, minActiveInterval: 30 * time.Millisecond, lastQueryAt: make(map[string]time.Time)}
	if errWait := waitCodexQuotaMinInterval(runtime, "auth"); errWait != nil {
		t.Fatal(errWait)
	}
	recordCodexQuotaQueryStart(runtime, "auth")
	started := time.Now()
	if errWait := waitCodexQuotaMinInterval(runtime, "auth"); errWait != nil {
		t.Fatal(errWait)
	}
	recordCodexQuotaQueryStart(runtime, "auth")
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("second active query waited %v, want hard floor", elapsed)
	}
	started = time.Now()
	if errWait := waitCodexQuotaMinInterval(runtime, "other-auth"); errWait != nil {
		t.Fatal(errWait)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Millisecond {
		t.Fatalf("independent auth unexpectedly waited %v", elapsed)
	}
}

func TestCodexQuotaArmedWaiterHasSemaphorePriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &codexQuotaRuntime{ctx: ctx, semaphore: make(chan struct{}, 1)}
	runtime.semaphore <- struct{}{}
	acquired := make(chan string, 2)
	go func() {
		if acquireCodexQuotaSlot(runtime, false) == nil {
			acquired <- "normal"
		}
	}()
	time.Sleep(time.Millisecond)
	go func() {
		if acquireCodexQuotaSlot(runtime, true) == nil {
			acquired <- "armed"
		}
	}()
	time.Sleep(time.Millisecond)
	<-runtime.semaphore
	select {
	case got := <-acquired:
		if got != "armed" {
			t.Fatalf("first semaphore waiter = %q, want armed", got)
		}
		<-runtime.semaphore
	case <-time.After(time.Second):
		t.Fatal("quota semaphore waiter timed out")
	}
}

func TestManagerSubmitCodexQuotaKeepsLaterAuthObservationsPending(t *testing.T) {
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
	runtime := &codexQuotaRuntime{
		ctx:             runtimeCtx,
		observations:    make(chan codexoverdraft.QuotaSnapshot, 1),
		observationWake: make(chan struct{}, 1),
		pending:         make(map[string]codexQuotaPending),
	}
	m := &Manager{codexQuotaRuntime: runtime}
	runtime.observations <- codexoverdraft.QuotaSnapshot{AuthID: "auth-1", Sequence: 1}
	m.submitCodexQuotaObservation(codexoverdraft.QuotaSnapshot{AuthID: "auth-1", Sequence: 2})
	<-runtime.observations
	m.submitCodexQuotaObservation(codexoverdraft.QuotaSnapshot{AuthID: "auth-1", Sequence: 3})

	if got := len(runtime.observations); got != 0 {
		t.Fatalf("later observation bypassed pending coalescing: channel length = %d", got)
	}
	runtime.mu.Lock()
	pending := runtime.pending["auth-1"]
	runtime.mu.Unlock()
	if pending.latest.Sequence != 3 {
		t.Fatalf("pending latest sequence = %d, want 3", pending.latest.Sequence)
	}
}

func TestManagerIdenticalQuotaConfigKeepsRuntime(t *testing.T) {
	m := NewManager(nil, nil, nil)
	defer m.StopCodexQuota()
	quota := internalconfig.DefaultCodexQuotaConfig()
	cfg := quotaRuntimeConfigForTest(t, quota)
	m.SetConfig(cfg)
	m.codexQuotaRuntimeMu.RLock()
	first := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if first == nil {
		t.Fatal("quota runtime was not started")
	}

	reloaded := cfg.CloneForRuntime()
	reloaded.Port = 9123
	m.SetConfig(reloaded)
	m.codexQuotaRuntimeMu.RLock()
	second := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if second != first {
		t.Fatal("an unrelated config change restarted the quota runtime")
	}
}

func TestManagerQuotaPersistenceFailurePausesAndRecoveryRebuildsOverdraft(t *testing.T) {
	m := NewManager(nil, nil, nil)
	service := usageaccounting.NewService(usageaccounting.NewMemoryStore(), 8)
	defer service.Close()
	m.accountingMu.Lock()
	m.accountingService = service
	m.accountingMu.Unlock()
	m.accountingRequired.Store(true)

	quota := internalconfig.DefaultCodexQuotaConfig()
	overdraft := internalconfig.DefaultCodexOverdraftConfig()
	overdraft.Enabled = true
	overdraft.ProbeModel = "gpt-test"
	accounting := internalconfig.DefaultAccountingConfig()
	accounting.Enabled = true
	cfg := &internalconfig.Config{Codex: internalconfig.CodexConfig{Quota: quota, Overdraft: overdraft}, Accounting: accounting}
	m.runtimeConfig.Store(cfg)
	threshold, errThreshold := overdraft.ThresholdMicropct()
	if errThreshold != nil {
		t.Fatal(errThreshold)
	}
	coordinator, errCoordinator := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{Enabled: true, ThresholdMicropct: threshold, MaxInFlight: overdraft.MaxInFlight, ExhaustionProbeFailures: overdraft.ExhaustionProbeFailures}, nil, nil)
	if errCoordinator != nil {
		t.Fatal(errCoordinator)
	}
	m.SetCodexOverdraftCoordinator(coordinator)
	m.codexQuotaEnabled.Store(true)
	m.initializeCodexQuotaHealth()

	m.recordCodexQuotaPersistenceResult(errors.New("injected quota persistence failure"))
	health := m.CodexQuotaRuntimeHealth()
	if !health.Enabled || health.Healthy || health.LastError == "" {
		t.Fatalf("quota health after failure = %#v", health)
	}
	if coordinator.Enabled() {
		t.Fatal("overdraft admission remained enabled after quota persistence failure")
	}

	m.recordCodexQuotaPersistenceResult(nil)
	health = m.CodexQuotaRuntimeHealth()
	if !health.Enabled || !health.Healthy || health.LastError != "" {
		t.Fatalf("quota health after recovery = %#v", health)
	}
	if !coordinator.Enabled() {
		t.Fatal("overdraft admission did not rebuild after durable quota recovery")
	}
}
