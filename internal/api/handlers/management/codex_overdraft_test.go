package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	usageaccounting "github.com/router-for-me/CLIProxyAPI/v7/internal/usage/accounting"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestGetCodexOverdraftStatusUsesAuthIDRecords(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	coordinator, errNew := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}, nil, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	manager.SetCodexOverdraftCoordinator(coordinator)
	_ = coordinator.RegisterAuth("auth-file-id", 1, 0)
	_ = coordinator.ObserveThreshold("auth-file-id", 1, 99_000_000)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/overdraft/status", nil)
	handler.GetCodexOverdraftStatus(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"owner_auth_id":"auth-file-id"`)) {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetCodexOverdraftStatusReportsGlobalOccupancy(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	coordinator, errNew := codexoverdraft.NewCoordinator(codexoverdraft.CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}, nil, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	manager.SetCodexOverdraftCoordinator(coordinator)
	if errRegister := coordinator.RegisterAuth("auth-file-id", 1, 3); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errObserve := coordinator.ObserveThreshold("auth-file-id", 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	lease, errAcquire := coordinator.AcquireBusiness("dispatch-1")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	defer func() { _ = coordinator.Release(lease) }()

	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/overdraft/status", nil)
	handler.GetCodexOverdraftStatus(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"lease_in_flight":1`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"inherited_in_flight":3`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"total_in_flight":4`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"max_in_flight":40`)) {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetCodexUpstreamFailuresFiltersSanitizedHistory(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{ID: "auth-failure", Provider: "codex", Status: coreauth.StatusActive}); errRegister != nil {
		t.Fatal(errRegister)
	}
	manager.MarkResult(context.Background(), coreauth.Result{
		AuthID: "auth-failure",
		Model:  "gpt-5.5(high)",
		Error: &coreauth.Error{
			Code:       "upstream_failure",
			Message:    `{"error":{"type":"server_error","message":"TOKEN prompt"}}`,
			HTTPStatus: http.StatusInternalServerError,
		},
	})

	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/upstream-failures?auth_id=auth-failure&model=gpt-5.5", nil)
	handler.GetCodexUpstreamFailures(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, expected := range []string{`"auth_id":"auth-failure"`, `"model":"gpt-5.5"`, `"type":"server_error"`, `"http_status":500`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("body = %s, want %s", body, expected)
		}
	}
	for _, forbidden := range []string{"TOKEN", "prompt"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("body leaked %q: %s", forbidden, body)
		}
	}
}

func TestGetCodexQuotaDebugRequiresKnownAuth(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)

	missing := httptest.NewRecorder()
	missingCtx, _ := gin.CreateTestContext(missing)
	missingCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/quota/debug", nil)
	handler.GetCodexQuotaDebug(missingCtx)
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("missing auth status = %d body = %s", missing.Code, missing.Body.String())
	}

	unknown := httptest.NewRecorder()
	unknownCtx, _ := gin.CreateTestContext(unknown)
	unknownCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/quota/debug?auth_id=auth-debug", nil)
	handler.GetCodexQuotaDebug(unknownCtx)
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown auth status = %d body = %s", unknown.Code, unknown.Body.String())
	}
}

func TestModelPricingManagementCreatesImmutableVersion(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	defer manager.CloseAccounting()
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	accountingConfig.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	manager.SetConfig(&config.Config{Accounting: accountingConfig})
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	body, _ := json.Marshal(map[string]any{
		"model": "gpt-test", "tier": "default", "effective_from": time.Now().Add(-time.Hour),
		"input_nano_usd_per_million": 1000, "output_nano_usd_per_million": 2000,
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/v0/management/model-pricing", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	handler.PutModelPrice(ctx)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var stored map[string]any
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &stored); errDecode != nil || stored["version_id"] == "" {
		t.Fatalf("stored = %#v error = %v", stored, errDecode)
	}
}

func TestPatchCodexOverdraftSwitchChecksRevision(t *testing.T) {
	quota := config.DefaultCodexQuotaConfig()
	overdraft := config.DefaultCodexOverdraftConfig()
	overdraft.ProbeModel = "gpt-test"
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	handler := NewHandlerWithoutConfigFilePath(&config.Config{Codex: config.CodexConfig{Quota: quota, Overdraft: overdraft}, Accounting: accountingConfig}, nil)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/codex/overdraft/enabled", bytes.NewBufferString(`{"enabled":true,"revision":0}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	handler.PatchCodexOverdraftSwitch(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}

	conflict := httptest.NewRecorder()
	conflictCtx, _ := gin.CreateTestContext(conflict)
	conflictCtx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/codex/overdraft/enabled", bytes.NewBufferString(`{"enabled":false,"revision":0}`))
	conflictCtx.Request.Header.Set("Content-Type", "application/json")
	handler.PatchCodexOverdraftSwitch(conflictCtx)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
}

func TestGetAccountingAggregatesReturnsPerAuthItems(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	defer manager.CloseAccounting()
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	accountingConfig.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	manager.SetConfig(&config.Config{Accounting: accountingConfig})
	service := manager.AccountingService()
	if service == nil {
		t.Fatal("accounting service is not active")
	}
	service.HandleUsage(t.Context(), coreusage.Record{AuthID: "auth-file-id", UpstreamAttemptID: "attempt-1", DrainMode: "overdraft", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 17}})
	if errFlush := service.Flush(t.Context()); errFlush != nil {
		t.Fatal(errFlush)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/accounting/aggregates", nil)
	handler.GetAccountingAggregates(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"auth_id":"auth-file-id"`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"overdraft_business_total_tokens":17`)) {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAccountingManagementFiltersEventsAndCosts(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	defer manager.CloseAccounting()
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	accountingConfig.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	manager.SetConfig(&config.Config{Accounting: accountingConfig})
	service := manager.AccountingService()
	if service == nil {
		t.Fatal("accounting service is not active")
	}
	requestedAt := time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)
	enabled := true
	price, errPrice := service.PutPrice(usageaccounting.PriceVersion{
		VersionID:              "price-gpt-a",
		Provider:               "codex",
		Model:                  "gpt-a",
		Tier:                   "gold",
		Enabled:                &enabled,
		EffectiveFrom:          requestedAt.Add(-time.Hour),
		InputNanoUSDPerMillion: 1_000_000,
	})
	if errPrice != nil {
		t.Fatal(errPrice)
	}
	breakdown := coreusage.TokenBreakdown{
		SchemaVersion: coreusage.TokenAccountingSchemaVersion,
		Quality:       coreusage.TokenAccountingQualityComplete,
		TotalTokens:   1,
		Input:         coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1},
	}
	service.HandleUsage(t.Context(), coreusage.Record{Provider: "codex", AuthID: "auth-a", Model: "gpt-a", ServiceTier: "gold", UpstreamAttemptID: "attempt-a", RequestedAt: requestedAt, UsageReported: true, Detail: coreusage.Detail{TotalTokens: 1, TokenBreakdown: breakdown}})
	service.HandleUsage(t.Context(), coreusage.Record{Provider: "codex", AuthID: "auth-b", Model: "gpt-b", ServiceTier: "default", UpstreamAttemptID: "attempt-b", RequestedAt: requestedAt.Add(2 * time.Hour), UsageReported: true, Detail: coreusage.Detail{TotalTokens: 1, TokenBreakdown: breakdown}})
	if errFlush := service.Flush(t.Context()); errFlush != nil {
		t.Fatal(errFlush)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	from := url.QueryEscape(requestedAt.Add(-time.Minute).Format(time.RFC3339))
	to := url.QueryEscape(requestedAt.Add(time.Minute).Format(time.RFC3339))
	eventsRecorder := httptest.NewRecorder()
	eventsContext, _ := gin.CreateTestContext(eventsRecorder)
	eventsContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/accounting/events?auth_id=auth-a&model=gpt-a&tier=gold&from="+from+"&to="+to, nil)
	handler.GetAccountingEvents(eventsContext)
	if eventsRecorder.Code != http.StatusOK || !bytes.Contains(eventsRecorder.Body.Bytes(), []byte(`"total":1`)) || !bytes.Contains(eventsRecorder.Body.Bytes(), []byte(`"upstream_attempt_id":"attempt-a"`)) || bytes.Contains(eventsRecorder.Body.Bytes(), []byte("attempt-b")) {
		t.Fatalf("events status = %d body = %s", eventsRecorder.Code, eventsRecorder.Body.String())
	}

	costsRecorder := httptest.NewRecorder()
	costsContext, _ := gin.CreateTestContext(costsRecorder)
	costsContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/accounting/costs?auth_id=auth-a&model=gpt-a&tier=gold&from="+from+"&to="+to+"&cost_status=PRICED&price_version_id="+url.QueryEscape(price.VersionID)+"&limit=1", nil)
	handler.GetAccountingCosts(costsContext)
	if costsRecorder.Code != http.StatusOK || !bytes.Contains(costsRecorder.Body.Bytes(), []byte(`"total":1`)) || !bytes.Contains(costsRecorder.Body.Bytes(), []byte(`"price_version_id":"price-gpt-a"`)) || bytes.Contains(costsRecorder.Body.Bytes(), []byte("attempt-b")) {
		t.Fatalf("costs status = %d body = %s", costsRecorder.Code, costsRecorder.Body.String())
	}
}

func TestAccountingManagementRejectsInvalidTimeFilter(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	defer manager.CloseAccounting()
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	accountingConfig.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	manager.SetConfig(&config.Config{Accounting: accountingConfig})
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/accounting/events?from=not-a-time", nil)
	handler.GetAccountingEvents(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAccountingHealthReportsRequiredServiceUnavailable(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.Enabled = true
	handler := NewHandlerWithoutConfigFilePath(&config.Config{Accounting: accountingConfig}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/accounting/health", nil)
	handler.GetAccountingHealth(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"enabled":true`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"healthy":false`)) {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCodexQuotaHealthReportsRuntimeState(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	defer manager.StopCodexQuota()
	quota := config.DefaultCodexQuotaConfig()
	accountingConfig := config.DefaultAccountingConfig()
	accountingConfig.StoragePath = filepath.Join(t.TempDir(), "accounting.db")
	manager.SetConfig(&config.Config{Codex: config.CodexConfig{Quota: quota}, Accounting: accountingConfig})
	handler := NewHandlerWithoutConfigFilePath(&config.Config{Codex: config.CodexConfig{Quota: quota}}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/codex/quota/health", nil)
	handler.GetCodexQuotaHealth(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"enabled":true`)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"healthy":true`)) {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}
