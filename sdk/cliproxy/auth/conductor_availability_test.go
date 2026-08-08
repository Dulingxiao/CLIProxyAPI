package auth

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func TestUpdateAggregatedAvailability_UnavailableWithoutNextRetryDoesNotBlockAuth(t *testing.T) {
	t.Parallel()

	now := time.Now()
	model := "test-model"
	auth := &Auth{
		ID: "a",
		ModelStates: map[string]*ModelState{
			model: {
				Status:      StatusError,
				Unavailable: true,
			},
		},
	}

	updateAggregatedAvailability(auth, now)

	if auth.Unavailable {
		t.Fatalf("auth.Unavailable = true, want false")
	}
	if !auth.NextRetryAfter.IsZero() {
		t.Fatalf("auth.NextRetryAfter = %v, want zero", auth.NextRetryAfter)
	}
}

func TestUpdateAggregatedAvailability_FutureNextRetryBlocksAuth(t *testing.T) {
	t.Parallel()

	now := time.Now()
	model := "test-model"
	next := now.Add(5 * time.Minute)
	auth := &Auth{
		ID: "a",
		ModelStates: map[string]*ModelState{
			model: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: next,
			},
		},
	}

	updateAggregatedAvailability(auth, now)

	if !auth.Unavailable {
		t.Fatalf("auth.Unavailable = false, want true")
	}
	if auth.NextRetryAfter.IsZero() {
		t.Fatalf("auth.NextRetryAfter = zero, want %v", next)
	}
	if auth.NextRetryAfter.Sub(next) > time.Second || next.Sub(auth.NextRetryAfter) > time.Second {
		t.Fatalf("auth.NextRetryAfter = %v, want %v", auth.NextRetryAfter, next)
	}
}

func TestManager_AvailableProvidersAndHasProviderAuth_ExcludeDisabled(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()

	if _, err := manager.Register(ctx, &Auth{ID: "active", Provider: "claude", Status: StatusActive}); err != nil {
		t.Fatalf("register active auth: %v", err)
	}
	// Provider gemini only has an auth with the Disabled flag set.
	if _, err := manager.Register(ctx, &Auth{ID: "flag-disabled", Provider: "gemini", Disabled: true}); err != nil {
		t.Fatalf("register flag-disabled auth: %v", err)
	}
	// Provider codex only has an auth whose Status is StatusDisabled.
	if _, err := manager.Register(ctx, &Auth{ID: "status-disabled", Provider: "codex", Status: StatusDisabled}); err != nil {
		t.Fatalf("register status-disabled auth: %v", err)
	}

	providers := manager.AvailableProviders()
	present := make(map[string]bool, len(providers))
	for _, p := range providers {
		present[p] = true
	}
	if !present["claude"] {
		t.Errorf("AvailableProviders() = %v, want to include active provider claude", providers)
	}
	if present["gemini"] {
		t.Errorf("AvailableProviders() = %v, want to exclude Disabled provider gemini", providers)
	}
	if present["codex"] {
		t.Errorf("AvailableProviders() = %v, want to exclude StatusDisabled provider codex", providers)
	}

	if !manager.HasProviderAuth("claude") {
		t.Errorf("HasProviderAuth(claude) = false, want true")
	}
	if manager.HasProviderAuth("gemini") {
		t.Errorf("HasProviderAuth(gemini) = true, want false (only Disabled auth registered)")
	}
	if manager.HasProviderAuth("codex") {
		t.Errorf("HasProviderAuth(codex) = true, want false (only StatusDisabled auth registered)")
	}
}

func TestManager_ResetQuotaClearsRuntimeAndRegistryState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	authID := "reset-quota-auth"
	model := "reset-quota-model"
	next := time.Now().Add(time.Hour)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})

	if _, errRegister := manager.Register(ctx, &Auth{
		ID:             authID,
		Provider:       "claude",
		Status:         StatusError,
		StatusMessage:  "quota exhausted",
		Unavailable:    true,
		NextRetryAfter: next,
		Quota:          QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
		ModelStates: map[string]*ModelState{
			model: {
				Status:         StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				NextRetryAfter: next,
				Quota:          QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
				UpdatedAt:      next,
			},
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	reg.SetModelQuotaExceeded(authID, model)
	reg.SuspendClientModel(authID, model, "quota")
	if count := reg.GetModelCount(model); count != 0 {
		t.Fatalf("registry model count before reset = %d, want 0", count)
	}

	updated, models, errReset := manager.ResetQuota(ctx, authID)
	if errReset != nil {
		t.Fatalf("ResetQuota() error = %v", errReset)
	}
	if updated == nil {
		t.Fatalf("ResetQuota() updated auth is nil")
	}
	if len(models) != 1 || models[0] != model {
		t.Fatalf("ResetQuota() models = %v, want [%s]", models, model)
	}
	if updated.Status != StatusActive || updated.StatusMessage != "" || updated.Unavailable || !updated.NextRetryAfter.IsZero() {
		t.Fatalf("updated auth state = status %q message %q unavailable %v next %v", updated.Status, updated.StatusMessage, updated.Unavailable, updated.NextRetryAfter)
	}
	if updated.Quota.Exceeded || updated.Quota.Reason != "" || !updated.Quota.NextRecoverAt.IsZero() || updated.Quota.BackoffLevel != 0 {
		t.Fatalf("updated auth quota = %+v, want cleared", updated.Quota)
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("updated model state missing")
	}
	if state.Status != StatusActive || state.StatusMessage != "" || state.Unavailable || !state.NextRetryAfter.IsZero() {
		t.Fatalf("updated model state = status %q message %q unavailable %v next %v", state.Status, state.StatusMessage, state.Unavailable, state.NextRetryAfter)
	}
	if state.Quota.Exceeded || state.Quota.Reason != "" || !state.Quota.NextRecoverAt.IsZero() || state.Quota.BackoffLevel != 0 {
		t.Fatalf("updated model quota = %+v, want cleared", state.Quota)
	}
	if count := reg.GetModelCount(model); count != 1 {
		t.Fatalf("registry model count after reset = %d, want 1", count)
	}
}

func TestManager_ResetModelCooldownClearsOnlyObservedModel(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	authID := "reset-model-cooldown-auth"
	modelA := "reset-model-a"
	modelB := "reset-model-b"
	now := time.Now()
	next := now.Add(time.Hour)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: modelA}, {ID: modelB}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})

	if _, errRegister := manager.Register(ctx, &Auth{
		ID:          authID,
		Provider:    "claude",
		Status:      StatusError,
		Unavailable: true,
		ModelStates: map[string]*ModelState{
			modelA: quotaLimitedModelState(now, next),
			modelB: quotaLimitedModelState(now, next),
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg.SetModelQuotaExceeded(authID, modelA)
	reg.SuspendClientModel(authID, modelA, "quota")
	reg.SetModelQuotaExceeded(authID, modelB)
	reg.SuspendClientModel(authID, modelB, "quota")

	updated, resetModel, cleared, errReset := manager.ResetModelCooldown(ctx, authID, modelA, now.Add(time.Second))
	if errReset != nil {
		t.Fatalf("ResetModelCooldown() error = %v", errReset)
	}
	if updated == nil || resetModel != modelA || !cleared {
		t.Fatalf("ResetModelCooldown() = auth %#v model %q cleared %v", updated, resetModel, cleared)
	}
	if updated.Success != 0 || updated.Failed != 0 {
		t.Fatalf("diagnostic recovery changed request counters: success=%d failed=%d", updated.Success, updated.Failed)
	}
	if state := updated.ModelStates[modelA]; state == nil || !modelStateIsClean(state) {
		t.Fatalf("reset model state = %#v, want clean", state)
	}
	if state := updated.ModelStates[modelB]; state == nil || modelStateIsClean(state) {
		t.Fatalf("unrelated model state = %#v, want cooldown preserved", state)
	}
	if updated.Unavailable || updated.Status != StatusError {
		t.Fatalf("aggregate auth state = status %q unavailable %v, want routable auth with remaining model error", updated.Status, updated.Unavailable)
	}
	if count := reg.GetModelCount(modelA); count != 1 {
		t.Fatalf("reset model registry count = %d, want 1", count)
	}
	if count := reg.GetModelCount(modelB); count != 0 {
		t.Fatalf("unrelated model registry count = %d, want 0", count)
	}
}

func TestManager_ResetModelCooldownPreservesNewerFailure(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	authID := "reset-model-newer-failure-auth"
	model := "reset-model-newer-failure"
	observedAt := time.Now()
	updatedAt := observedAt.Add(time.Second)
	next := updatedAt.Add(time.Hour)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})

	if _, errRegister := manager.Register(ctx, &Auth{
		ID:          authID,
		Provider:    "claude",
		Status:      StatusError,
		Unavailable: true,
		ModelStates: map[string]*ModelState{model: quotaLimitedModelState(updatedAt, next)},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg.SetModelQuotaExceeded(authID, model)
	reg.SuspendClientModel(authID, model, "quota")

	updated, resetModel, cleared, errReset := manager.ResetModelCooldown(ctx, authID, model, observedAt)
	if errReset != nil {
		t.Fatalf("ResetModelCooldown() error = %v", errReset)
	}
	if updated == nil || resetModel != model || cleared {
		t.Fatalf("ResetModelCooldown() = auth %#v model %q cleared %v", updated, resetModel, cleared)
	}
	if state := updated.ModelStates[model]; state == nil || modelStateIsClean(state) {
		t.Fatalf("newer model state = %#v, want cooldown preserved", state)
	}
	if count := reg.GetModelCount(model); count != 0 {
		t.Fatalf("newer failure registry count = %d, want 0", count)
	}
}

func TestManager_ResetModelCooldownDoesNotCreateCleanModelState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	authID := "reset-missing-model-state-auth"
	model := "reset-missing-model-state"

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})

	if _, errRegister := manager.Register(ctx, &Auth{ID: authID, Provider: "claude", Status: StatusActive}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg.SetModelQuotaExceeded(authID, model)
	reg.SuspendClientModel(authID, model, "quota")

	updated, resetModel, cleared, errReset := manager.ResetModelCooldown(ctx, authID, model, time.Now())
	if errReset != nil {
		t.Fatalf("ResetModelCooldown() error = %v", errReset)
	}
	if updated == nil || resetModel != model || cleared {
		t.Fatalf("ResetModelCooldown() = auth %#v model %q cleared %v", updated, resetModel, cleared)
	}
	if _, exists := updated.ModelStates[model]; exists {
		t.Fatalf("successful probe created model state: %#v", updated.ModelStates[model])
	}
	if count := reg.GetModelCount(model); count != 1 {
		t.Fatalf("registry model count = %d, want stale suspension cleared", count)
	}
}

func TestManager_ResetModelCooldownResumesRegistryAfterNewerSuccess(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	authID := "reset-newer-success-auth"
	model := "reset-newer-success-model"
	observedAt := time.Now()

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})

	if _, errRegister := manager.Register(ctx, &Auth{
		ID:       authID,
		Provider: "claude",
		Status:   StatusActive,
		ModelStates: map[string]*ModelState{
			model: {Status: StatusActive, UpdatedAt: observedAt.Add(time.Second)},
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg.SetModelQuotaExceeded(authID, model)
	reg.SuspendClientModel(authID, model, "quota")

	updated, _, cleared, errReset := manager.ResetModelCooldown(ctx, authID, model, observedAt)
	if errReset != nil {
		t.Fatalf("ResetModelCooldown() error = %v", errReset)
	}
	if updated == nil || cleared {
		t.Fatalf("ResetModelCooldown() = auth %#v cleared %v", updated, cleared)
	}
	if count := reg.GetModelCount(model); count != 1 {
		t.Fatalf("registry model count = %d, want stale suspension cleared", count)
	}
}

func quotaLimitedModelState(updatedAt, next time.Time) *ModelState {
	return &ModelState{
		Status:         StatusError,
		StatusMessage:  "quota exhausted",
		Unavailable:    true,
		NextRetryAfter: next,
		Quota:          QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: next, BackoffLevel: 2},
		UpdatedAt:      updatedAt,
	}
}
