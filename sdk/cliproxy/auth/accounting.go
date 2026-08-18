package auth

import (
	"context"
	"crypto/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	usageaccounting "github.com/router-for-me/CLIProxyAPI/v7/internal/usage/accounting"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

const accountingAttemptIDMetadataKey = "accounting_attempt_id"

func (m *Manager) applyAccountingConfig(cfg *internalconfig.Config) error {
	if m == nil || cfg == nil {
		return nil
	}
	m.accountingRequired.Store(cfg.Accounting.Enabled)
	path := resolveAccountingStoragePath(cfg)
	m.accountingMu.RLock()
	current := m.accountingService
	currentPath := m.accountingPath
	m.accountingMu.RUnlock()
	if cfg.Accounting.Enabled && current != nil && currentPath == path {
		return nil
	}
	m.closeAccounting()
	if !cfg.Accounting.Enabled {
		return nil
	}
	store, errOpen := usageaccounting.OpenBoltStore(path)
	if errOpen != nil {
		return errOpen
	}
	service := usageaccounting.NewService(store, 4096)
	if errReconcile := service.Reconcile(context.Background()); errReconcile != nil {
		_ = service.Close()
		return errReconcile
	}
	m.accountingMu.Lock()
	m.accountingStore = store
	m.accountingService = service
	m.accountingPath = path
	m.accountingMu.Unlock()
	coreusage.DefaultManager().SetBuiltinSink(service)
	return nil
}

func (m *Manager) providersAllowedByAccounting(providers []string) ([]string, error) {
	if m == nil || !m.accountingRequired.Load() {
		return providers, nil
	}
	service := m.AccountingService()
	if service != nil && service.AdmissionHealthy() {
		return providers, nil
	}
	allowed := make([]string, 0, len(providers))
	for _, provider := range providers {
		if !strings.EqualFold(strings.TrimSpace(provider), "codex") {
			allowed = append(allowed, provider)
		}
	}
	if len(allowed) > 0 {
		return allowed, nil
	}
	return nil, &Error{
		Code:       requestScopedErrorCode,
		Message:    "durable accounting is unavailable",
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

func resolveAccountingStoragePath(cfg *internalconfig.Config) string {
	path := strings.TrimSpace(cfg.Accounting.StoragePath)
	authDir := strings.TrimSpace(cfg.AuthDir)
	if authDir == "" {
		authDir = "auths"
	}
	path = strings.ReplaceAll(path, "AUTH_STATE_DIR", authDir)
	if path == "" {
		path = filepath.Join(authDir, "accounting.db")
	}
	return path
}

func (m *Manager) closeAccounting() {
	if m == nil {
		return
	}
	m.accountingMu.Lock()
	service := m.accountingService
	m.accountingService = nil
	m.accountingStore = nil
	m.accountingPath = ""
	m.accountingMu.Unlock()
	if service != nil {
		coreusage.DefaultManager().SetBuiltinSink(nil)
		if errClose := service.Close(); errClose != nil {
			logEntryWithRequestID(nil).Warnf("failed to close accounting service: %v", errClose)
		}
	}
}

// CloseAccounting drains and closes the built-in accounting service.
func (m *Manager) CloseAccounting() { m.closeAccounting() }

// AccountingService returns the built-in accounting service.
func (m *Manager) AccountingService() *usageaccounting.Service {
	if m == nil {
		return nil
	}
	m.accountingMu.RLock()
	defer m.accountingMu.RUnlock()
	return m.accountingService
}

func (m *Manager) attachAccountingAttempt(opts cliproxyexecutor.Options, auth *Auth, model string) cliproxyexecutor.Options {
	service := m.AccountingService()
	if service == nil || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return opts
	}
	requestID := metadataString(opts.Metadata, "request_id")
	execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts)
	drainMode := "normal"
	var generation, coordinatorEpoch, stateEpoch uint64
	var drainCycleID, drainRequestKind, dispatchID string
	if execution != nil {
		requestID = execution.DispatchID
		dispatchID = execution.DispatchID
		drainMode = "overdraft"
		generation = execution.AuthGeneration
		coordinatorEpoch = execution.CoordinatorEpoch
		stateEpoch = execution.AuthStateEpoch
		drainCycleID = execution.DrainCycleID
		drainRequestKind = string(execution.Kind)
	}
	if requestID == "" {
		requestID = "request_" + strings.ToLower(rand.Text())
	}
	if generation == 0 && isCodexOverdraftEligibleAuth(auth) {
		generation = m.codexAuthGeneration(auth.ID)
	}
	consumptionClass := m.codexConsumptionClass(auth.ID)
	type attemptState struct {
		sync.Mutex
		currentID string
		recorded  bool
		closed    map[string]struct{}
	}
	state := &attemptState{currentID: "attempt_" + strings.ToLower(rand.Text()), closed: make(map[string]struct{})}
	intentFor := func(attemptID string) usageaccounting.AttemptIntent {
		return usageaccounting.AttemptIntent{UpstreamAttemptID: attemptID, RequestID: requestID, DispatchID: dispatchID, AuthID: auth.ID, AuthGeneration: generation, Provider: auth.Provider, RequestedModel: requestedModelAliasFromOptions(opts, model), ResolvedModel: model, RequestServiceTier: serviceTierFromOptions(opts), RequestedAt: time.Now(), DrainMode: drainMode, DrainCycleID: drainCycleID, DrainRequestKind: drainRequestKind, ConsumptionClass: consumptionClass, CoordinatorEpoch: coordinatorEpoch, AuthStateEpoch: stateEpoch}
	}
	hooks := &cliproxyexecutor.AccountingAttemptHooks{
		RecordIntent: func() error {
			state.Lock()
			defer state.Unlock()
			if state.recorded {
				return nil
			}
			if errIntent := service.RecordIntent(context.Background(), intentFor(state.currentID)); errIntent != nil {
				return errIntent
			}
			state.recorded = true
			return nil
		},
		RecordRetryIntent: func() error {
			state.Lock()
			defer state.Unlock()
			previousID := state.currentID
			if state.recorded {
				if _, closed := state.closed[previousID]; !closed {
					if errClose := service.RecordClosure(context.Background(), usageaccounting.AttemptClosure{UpstreamAttemptID: previousID, RequestID: requestID, AuthID: auth.ID, DrainCycleID: drainCycleID, DrainRequestKind: drainRequestKind, FailureClass: usageaccounting.FailureIndeterminate}); errClose != nil {
						return errClose
					}
					state.closed[previousID] = struct{}{}
				}
			}
			nextID := "attempt_" + strings.ToLower(rand.Text())
			if errIntent := service.RecordIntent(context.Background(), intentFor(nextID)); errIntent != nil {
				return errIntent
			}
			state.currentID = nextID
			state.recorded = true
			return nil
		},
		RecordNotDispatched: func() {
			state.Lock()
			defer state.Unlock()
			if _, closed := state.closed[state.currentID]; closed {
				return
			}
			_ = service.RecordClosure(context.Background(), usageaccounting.AttemptClosure{UpstreamAttemptID: state.currentID, RequestID: requestID, AuthID: auth.ID, DrainCycleID: drainCycleID, DrainRequestKind: drainRequestKind, FailureClass: usageaccounting.FailureNotDispatched})
			state.closed[state.currentID] = struct{}{}
		},
		CurrentAttemptID: func() string {
			state.Lock()
			defer state.Unlock()
			return state.currentID
		},
	}
	metadata := make(map[string]any, len(opts.Metadata)+2)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[cliproxyexecutor.AccountingAttemptMetadataKey] = hooks
	metadata[accountingAttemptIDMetadataKey] = state.currentID
	metadata["codex_consumption_class"] = consumptionClass
	opts.Metadata = metadata
	return opts
}

func (m *Manager) executeAccountingAttempt(ctx context.Context, executor ProviderExecutor, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	callReq, guardedOpts := m.guardCodexTurnState(req, opts, auth)
	callOpts := m.attachAccountingAttempt(guardedOpts, auth, callReq.Model)
	callCtx := accountingContextForExecution(ctx, callOpts)
	endLegacy := m.beginCodexLegacyInFlight(auth, callOpts)
	defer endLegacy()
	return executor.Execute(callCtx, auth, callReq, callOpts)
}

func (m *Manager) executeAccountingStreamAttempt(ctx context.Context, executor ProviderExecutor, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	callReq, guardedOpts := m.guardCodexTurnState(req, opts, auth)
	callOpts := m.attachAccountingAttempt(guardedOpts, auth, callReq.Model)
	callCtx := accountingContextForExecution(ctx, callOpts)
	endLegacy := m.beginCodexLegacyInFlight(auth, callOpts)
	result, errExecute := executor.ExecuteStream(callCtx, auth, callReq, callOpts)
	if errExecute != nil || result == nil || result.Chunks == nil {
		endLegacy()
		return result, errExecute
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer endLegacy()
		for chunk := range result.Chunks {
			out <- chunk
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}, nil
}
