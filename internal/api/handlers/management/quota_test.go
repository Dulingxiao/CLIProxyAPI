package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestResetQuota_UsesAuthIndex(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	next := time.Now().Add(time.Hour)
	auth := &coreauth.Auth{
		ID:             "reset-auth-id",
		FileName:       "reset-auth-file.json",
		Provider:       "claude",
		Status:         coreauth.StatusError,
		StatusMessage:  "quota exhausted",
		Unavailable:    true,
		NextRetryAfter: next,
		Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
		ModelStates: map[string]*coreauth.ModelState{
			"claude-reset-model": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				NextRetryAfter: next,
				Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
			},
		},
	}
	authIndex := auth.EnsureIndex()
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/reset-quota", strings.NewReader(`{"auth_index":"`+authIndex+`"}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.ResetQuota(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
		t.Fatalf("failed to decode response: %v", errUnmarshal)
	}
	if payload["auth_index"] != authIndex {
		t.Fatalf("auth_index = %#v, want %q", payload["auth_index"], authIndex)
	}

	updated, ok := manager.GetByID("reset-auth-id")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after reset")
	}
	if updated.Status != coreauth.StatusActive || updated.StatusMessage != "" || updated.Unavailable || !updated.NextRetryAfter.IsZero() {
		t.Fatalf("updated auth state = status %q message %q unavailable %v next %v", updated.Status, updated.StatusMessage, updated.Unavailable, updated.NextRetryAfter)
	}
	if updated.Quota.Exceeded || updated.Quota.Reason != "" || !updated.Quota.NextRecoverAt.IsZero() || updated.Quota.BackoffLevel != 0 {
		t.Fatalf("updated auth quota = %+v, want cleared", updated.Quota)
	}
	state := updated.ModelStates["claude-reset-model"]
	if state == nil {
		t.Fatalf("expected model state to remain")
	}
	if state.Status != coreauth.StatusActive || state.StatusMessage != "" || state.Unavailable || !state.NextRetryAfter.IsZero() {
		t.Fatalf("updated model state = status %q message %q unavailable %v next %v", state.Status, state.StatusMessage, state.Unavailable, state.NextRetryAfter)
	}
	if state.Quota.Exceeded || state.Quota.Reason != "" || !state.Quota.NextRecoverAt.IsZero() || state.Quota.BackoffLevel != 0 {
		t.Fatalf("updated model quota = %+v, want cleared", state.Quota)
	}
}

func TestResetQuota_DoesNotAcceptAuthIDOrFileName(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "reset-auth-id-only",
		FileName: "reset-auth-file-only.json",
		Provider: "claude",
		Status:   coreauth.StatusError,
	}
	authIndex := auth.EnsureIndex()
	if authIndex == auth.ID || authIndex == auth.FileName {
		t.Fatalf("test auth_index unexpectedly matches id or file name: %q", authIndex)
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "auth_id field ignored", body: `{"auth_id":"reset-auth-id-only"}`, wantCode: http.StatusBadRequest},
		{name: "id field ignored", body: `{"id":"reset-auth-id-only"}`, wantCode: http.StatusBadRequest},
		{name: "file name is not an index", body: `{"auth_index":"reset-auth-file-only.json"}`, wantCode: http.StatusNotFound},
		{name: "auth id is not an index", body: `{"auth_index":"reset-auth-id-only"}`, wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(http.MethodPost, "/v0/management/reset-quota", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			ctx.Request = req
			h.ResetQuota(ctx)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d with body %s", rec.Code, tt.wantCode, rec.Body.String())
			}
		})
	}
}

func TestResetModelCooldown_ClearsOnlyRequestedModel(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	updatedAt := time.Now().Add(-time.Minute).UTC()
	observedAt := updatedAt.Add(time.Second)
	next := time.Now().Add(time.Hour)
	auth := &coreauth.Auth{
		ID:          "reset-model-auth-id",
		FileName:    "reset-model-auth.json",
		Provider:    "codex",
		Status:      coreauth.StatusError,
		Unavailable: true,
		ModelStates: map[string]*coreauth.ModelState{
			"gpt-reset-a": quotaLimitedManagementModelState(updatedAt, next),
			"gpt-reset-b": quotaLimitedManagementModelState(updatedAt, next),
		},
	}
	authIndex := auth.EnsureIndex()
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	body := fmt.Sprintf(`{"auth_index":%q,"model":"gpt-reset-a","observed_at":%q}`, authIndex, observedAt.Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodPost, "/v0/management/reset-model-cooldown", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.ResetModelCooldown(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var payload struct {
		Status    string `json:"status"`
		AuthIndex string `json:"auth_index"`
		Model     string `json:"model"`
		Reset     bool   `json:"reset"`
	}
	if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
		t.Fatalf("failed to decode response: %v", errUnmarshal)
	}
	if payload.Status != "ok" || payload.AuthIndex != authIndex || payload.Model != "gpt-reset-a" || !payload.Reset {
		t.Fatalf("response = %+v", payload)
	}

	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after reset")
	}
	if state := updated.ModelStates["gpt-reset-a"]; state == nil || state.Status != coreauth.StatusActive || state.Unavailable || state.Quota.Exceeded {
		t.Fatalf("requested model state = %+v, want cleared", state)
	}
	if state := updated.ModelStates["gpt-reset-b"]; state == nil || state.Status != coreauth.StatusError || !state.Unavailable || !state.Quota.Exceeded {
		t.Fatalf("unrelated model state = %+v, want preserved", state)
	}
}

func TestResetModelCooldown_PreservesNewerFailure(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	observedAt := time.Now().Add(-time.Minute).UTC()
	updatedAt := observedAt.Add(time.Second)
	next := time.Now().Add(time.Hour)
	auth := &coreauth.Auth{
		ID:       "reset-newer-model-auth-id",
		Provider: "codex",
		Status:   coreauth.StatusError,
		ModelStates: map[string]*coreauth.ModelState{
			"gpt-reset-newer": quotaLimitedManagementModelState(updatedAt, next),
		},
	}
	authIndex := auth.EnsureIndex()
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	body := fmt.Sprintf(`{"auth_index":%q,"model":"gpt-reset-newer","observed_at":%q}`, authIndex, observedAt.Format(time.RFC3339Nano))
	req := httptest.NewRequest(http.MethodPost, "/v0/management/reset-model-cooldown", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.ResetModelCooldown(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	var payload struct {
		Reset bool `json:"reset"`
	}
	if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
		t.Fatalf("failed to decode response: %v", errUnmarshal)
	}
	if payload.Reset {
		t.Fatalf("response reset = true, want newer failure preserved")
	}
	updated, _ := manager.GetByID(auth.ID)
	if state := updated.ModelStates["gpt-reset-newer"]; state == nil || state.Status != coreauth.StatusError || !state.Quota.Exceeded {
		t.Fatalf("newer model state = %+v, want preserved", state)
	}
}

func TestResetModelCooldown_ValidatesRequest(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, coreauth.NewManager(nil, nil, nil))
	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "invalid json", body: `{`, code: http.StatusBadRequest},
		{name: "missing auth index", body: `{"model":"gpt-test"}`, code: http.StatusBadRequest},
		{name: "missing model", body: `{"auth_index":"missing"}`, code: http.StatusBadRequest},
		{name: "invalid observed at", body: `{"auth_index":"missing","model":"gpt-test","observed_at":"yesterday"}`, code: http.StatusBadRequest},
		{name: "unknown auth", body: `{"auth_index":"missing","model":"gpt-test"}`, code: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(http.MethodPost, "/v0/management/reset-model-cooldown", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			ctx.Request = req
			h.ResetModelCooldown(ctx)
			if rec.Code != tt.code {
				t.Fatalf("status = %d, want %d with body %s", rec.Code, tt.code, rec.Body.String())
			}
		})
	}
}

func quotaLimitedManagementModelState(updatedAt, next time.Time) *coreauth.ModelState {
	return &coreauth.ModelState{
		Status:         coreauth.StatusError,
		StatusMessage:  "quota exhausted",
		Unavailable:    true,
		NextRetryAfter: next,
		Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
		UpdatedAt:      updatedAt,
	}
}
