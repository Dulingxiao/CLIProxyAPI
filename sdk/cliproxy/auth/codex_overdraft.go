package auth

import (
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

const officialCodexBaseURL = "https://chatgpt.com/backend-api/codex"

func (m *Manager) applyCodexOverdraftConfig(cfg *internalconfig.Config) error {
	if m == nil || cfg == nil {
		return nil
	}
	overdraftConfig := cfg.Codex.Overdraft
	m.codexOverdraftMu.Lock()
	m.codexOverdraftRevision++
	revision := m.codexOverdraftRevision
	coordinator := m.codexOverdraft
	if !overdraftConfig.Enabled && coordinator == nil {
		m.codexOverdraftMu.Unlock()
		return nil
	}
	threshold, errThreshold := overdraftConfig.ThresholdMicropct()
	if errThreshold != nil {
		if overdraftConfig.Enabled {
			m.codexOverdraftMu.Unlock()
			return errThreshold
		}
		defaults := internalconfig.DefaultCodexOverdraftConfig()
		threshold, _ = defaults.ThresholdMicropct()
		if overdraftConfig.MaxInFlight <= 0 {
			overdraftConfig.MaxInFlight = defaults.MaxInFlight
		}
		if overdraftConfig.ExhaustionProbeFailures <= 0 {
			overdraftConfig.ExhaustionProbeFailures = defaults.ExhaustionProbeFailures
		}
	}
	runtimeConfig := codexoverdraft.CoordinatorConfig{
		Enabled:                 overdraftConfig.Enabled,
		ThresholdMicropct:       threshold,
		MaxInFlight:             overdraftConfig.MaxInFlight,
		PerAuthMaxInFlight:      overdraftConfig.PerAuthMaxInFlight,
		ExhaustionProbeFailures: overdraftConfig.ExhaustionProbeFailures,
	}
	if coordinator != nil {
		m.codexOverdraftMu.Unlock()
		errApply := coordinator.ApplyConfig(revision, runtimeConfig)
		if !overdraftConfig.Enabled {
			m.stopAllCodexProbes()
		} else if errApply == nil {
			for _, auth := range m.snapshotAuths() {
				m.syncCodexOverdraftAuth(auth)
			}
		}
		return errApply
	}
	storagePath := resolveCodexOverdraftStoragePath(cfg)
	store, errStore := codexoverdraft.OpenBoltStore(storagePath)
	if errStore != nil {
		m.codexOverdraftMu.Unlock()
		return errStore
	}
	coordinator, errCoordinator := codexoverdraft.NewCoordinator(runtimeConfig, store, nil)
	if errCoordinator != nil {
		m.codexOverdraftMu.Unlock()
		if errClose := store.Close(); errClose != nil {
			return fmt.Errorf("create Codex overdraft coordinator: %w; close store: %v", errCoordinator, errClose)
		}
		return errCoordinator
	}
	m.codexOverdraft = coordinator
	m.codexOverdraftStore = store
	if m.codexOverdraftGenerations == nil {
		m.codexOverdraftGenerations = make(map[string]uint64)
	}
	if m.codexLegacyInFlight == nil {
		m.codexLegacyInFlight = make(map[string]int)
	}
	m.codexOverdraftMu.Unlock()
	for _, auth := range m.snapshotAuths() {
		m.syncCodexOverdraftAuth(auth)
	}
	return nil
}

func resolveCodexOverdraftStoragePath(cfg *internalconfig.Config) string {
	path := strings.TrimSpace(cfg.Accounting.StoragePath)
	authDir := strings.TrimSpace(cfg.AuthDir)
	if authDir == "" {
		authDir = "auths"
	}
	path = strings.ReplaceAll(path, "AUTH_STATE_DIR", authDir)
	if path == "" {
		path = filepath.Join(authDir, "accounting.db")
	}
	extension := filepath.Ext(path)
	base := strings.TrimSuffix(path, extension)
	return base + "-overdraft" + extension
}

// CloseCodexOverdraft flushes and closes the manager-owned durable state store.
func (m *Manager) CloseCodexOverdraft() {
	if m == nil {
		return
	}
	m.codexOverdraftMu.Lock()
	store := m.codexOverdraftStore
	m.codexOverdraftStore = nil
	m.codexOverdraft = nil
	m.codexOverdraftGenerations = make(map[string]uint64)
	m.codexLegacyInFlight = make(map[string]int)
	m.codexOverdraftMu.Unlock()
	m.stopAllCodexProbes()
	if store != nil {
		if errClose := store.Close(); errClose != nil {
			logEntryWithRequestID(nil).Warnf("failed to close Codex overdraft store: %v", errClose)
		}
	}
}

// SetCodexOverdraftCoordinator replaces the manager-owned pool coordinator.
func (m *Manager) SetCodexOverdraftCoordinator(coordinator *codexoverdraft.Coordinator) {
	if m == nil {
		return
	}
	m.codexOverdraftMu.Lock()
	m.codexOverdraft = coordinator
	if m.codexOverdraftGenerations == nil {
		m.codexOverdraftGenerations = make(map[string]uint64)
	}
	m.codexOverdraftMu.Unlock()
	if coordinator == nil {
		return
	}
	for _, auth := range m.snapshotAuths() {
		m.syncCodexOverdraftAuth(auth)
	}
}

// CodexOverdraftCoordinator returns the current coordinator for management services.
func (m *Manager) CodexOverdraftCoordinator() *codexoverdraft.Coordinator {
	if m == nil {
		return nil
	}
	m.codexOverdraftMu.RLock()
	defer m.codexOverdraftMu.RUnlock()
	return m.codexOverdraft
}

func isCodexOverdraftEligibleAuth(auth *Auth) bool {
	if auth == nil || strings.TrimSpace(auth.ID) == "" || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if auth.AuthKind() != AuthKindOAuth || auth.AuthSourceKind() != AuthSourceFile || IsPluginVirtualAuth(auth) {
		return false
	}
	baseURL := strings.TrimRight(strings.TrimSpace(authAttribute(auth, "base_url")), "/")
	return baseURL == "" || strings.EqualFold(baseURL, officialCodexBaseURL)
}

func (m *Manager) syncCodexOverdraftAuth(auth *Auth) {
	if m == nil || auth == nil || !isCodexOverdraftEligibleAuth(auth) {
		return
	}
	m.codexOverdraftMu.Lock()
	coordinator := m.codexOverdraft
	generation := m.codexOverdraftGenerations[auth.ID]
	inheritedInFlight := m.codexLegacyInFlight[auth.ID]
	if generation == 0 {
		if coordinator != nil {
			generation = coordinator.Record(auth.ID).AuthGeneration
		}
		if generation == 0 {
			generation = 1
		}
		m.codexOverdraftGenerations[auth.ID] = generation
	}
	m.codexOverdraftMu.Unlock()
	m.persistCodexAuthGeneration(auth.ID, generation)
	if coordinator == nil {
		return
	}
	if errRegister := coordinator.RegisterAuth(auth.ID, generation, inheritedInFlight); errRegister != nil {
		logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warnf("failed to register Codex overdraft auth: %v", errRegister)
		return
	}
	if errLimit := coordinator.SetAuthMaxInFlight(auth.ID, generation, m.codexOverdraftAuthMaxInFlight(auth)); errLimit != nil {
		logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warnf("failed to apply Codex per-auth overdraft limit: %v", errLimit)
	}
	if auth.Disabled || auth.Status == StatusDisabled || hasUnauthorizedAuthFailure(auth) {
		if errDisabled := coordinator.SetDisabled(auth.ID, generation, true); errDisabled != nil {
			logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warnf("failed to disable Codex overdraft auth: %v", errDisabled)
		}
	} else if errEnabled := coordinator.SetDisabled(auth.ID, generation, false); errEnabled != nil {
		logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warnf("failed to re-enable Codex overdraft auth: %v", errEnabled)
	}
}

func (m *Manager) codexOverdraftAuthMaxInFlight(auth *Auth) int {
	raw := authAttribute(auth, "codex_overdraft_max_in_flight")
	if raw == "" {
		raw = authMetadataString(auth, "codex_overdraft_max_in_flight")
	}
	if raw == "" {
		return 0
	}
	limit, errParse := strconv.Atoi(raw)
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	globalLimit := 0
	if cfg != nil {
		globalLimit = cfg.Codex.Overdraft.MaxInFlight
	}
	if errParse != nil || limit <= 0 || globalLimit <= 0 || limit > globalLimit {
		logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warn("ignoring invalid codex_overdraft_max_in_flight override")
		return 0
	}
	return limit
}

func (m *Manager) beginCodexLegacyInFlight(auth *Auth, opts cliproxyexecutor.Options) func() {
	if m == nil || !isCodexOverdraftEligibleAuth(auth) || cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts) != nil {
		return func() {}
	}
	m.codexOverdraftMu.Lock()
	coordinator := m.codexOverdraft
	generation := m.codexOverdraftGenerations[auth.ID]
	if generation == 0 {
		generation = 1
		m.codexOverdraftGenerations[auth.ID] = generation
	}
	m.codexLegacyInFlight[auth.ID]++
	count := m.codexLegacyInFlight[auth.ID]
	m.codexOverdraftMu.Unlock()
	if coordinator != nil && coordinator.Enabled() && coordinator.Record(auth.ID).State != codexoverdraft.StateNormal {
		_ = coordinator.UpdateInheritedInFlight(auth.ID, generation, count)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			m.codexOverdraftMu.Lock()
			currentCoordinator := m.codexOverdraft
			currentGeneration := m.codexOverdraftGenerations[auth.ID]
			if m.codexLegacyInFlight[auth.ID] > 0 {
				m.codexLegacyInFlight[auth.ID]--
			}
			remaining := m.codexLegacyInFlight[auth.ID]
			m.codexOverdraftMu.Unlock()
			if currentCoordinator != nil {
				if currentGeneration == generation && currentCoordinator.Record(auth.ID).State != codexoverdraft.StateNormal {
					_ = currentCoordinator.UpdateInheritedInFlight(auth.ID, generation, remaining)
				} else {
					_ = currentCoordinator.UpdateRetiredInheritedInFlight(auth.ID, generation, remaining)
				}
			}
		})
	}
}

func (m *Manager) codexLegacyInFlightCount(authID string) int {
	if m == nil || authID == "" {
		return 0
	}
	m.codexOverdraftMu.RLock()
	defer m.codexOverdraftMu.RUnlock()
	return m.codexLegacyInFlight[authID]
}

func (m *Manager) removeCodexOverdraftAuth(authID string) {
	if m == nil || authID == "" {
		return
	}
	m.codexOverdraftMu.Lock()
	coordinator := m.codexOverdraft
	generation := m.codexOverdraftGenerations[authID]
	if generation == 0 {
		generation = 1
	}
	m.codexOverdraftGenerations[authID] = generation + 1
	m.codexOverdraftMu.Unlock()
	m.persistCodexAuthGeneration(authID, generation+1)
	m.codexQuotaState.Remove(authID)
	m.cancelCodexQuotaRefresh(authID)
	m.codexQuotaMu.Lock()
	delete(m.codexQuotaSnapshots, authID)
	delete(m.codexQuotaPrevSnapshots, authID)
	delete(m.codexQuotaDebug, authID)
	m.codexQuotaMu.Unlock()
	if coordinator != nil && generation != 0 {
		if errRemove := coordinator.RemoveAuth(authID, generation); errRemove != nil {
			logEntryWithRequestID(nil).WithField("auth_id", authID).Warnf("failed to remove Codex overdraft auth: %v", errRemove)
		}
	}
}

func (m *Manager) excludeCodexOverdraftOverlay(tried map[string]struct{}) {
	if m == nil || tried == nil {
		return
	}
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() {
		return
	}
	// Only terminal overlay states leave ordinary round-robin. CANDIDATE
	// accounts still serve normal traffic while they wait in the FIFO.
	// ACTIVE_DRAIN is admitted through acquireCodexOverdraftExecution; a
	// failed lease marks that owner tried so MaxInFlight cannot be bypassed.
	for authID, record := range coordinator.Records() {
		switch record.State {
		case codexoverdraft.StateExhausted, codexoverdraft.StateDisabled:
			tried[authID] = struct{}{}
		}
	}
}

func (m *Manager) acquireCodexOverdraftExecution(providers []string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, tried map[string]struct{}) (*cliproxyexecutor.CodexOverdraftExecution, *Auth, bool) {
	if m == nil || !codexOverdraftRequestCompatible(providers, req, opts) {
		return nil, nil, false
	}
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() {
		return nil, nil, false
	}
	owner := coordinator.Owner()
	if owner == "" {
		return nil, nil, false
	}
	if _, alreadyTried := tried[owner]; alreadyTried {
		return nil, nil, false
	}
	if pinned := pinnedAuthIDFromMetadata(opts.Metadata); pinned != "" && pinned != owner {
		return nil, nil, false
	}
	// Compatible traffic already chose the drain owner. Any miss must keep
	// that owner off ordinary RR so overflow uses NORMAL/CANDIDATE accounts
	// instead of bypassing the overlay lease ceiling.
	rejectOwner := func() {
		if tried != nil {
			tried[owner] = struct{}{}
		}
	}
	m.mu.RLock()
	auth := m.auths[owner]
	if auth != nil {
		auth = auth.Clone()
	}
	m.mu.RUnlock()
	if !isCodexOverdraftEligibleAuth(auth) || auth.Disabled || auth.Status == StatusDisabled {
		rejectOwner()
		return nil, nil, false
	}
	blocked, _, _ := isAuthBlockedForModel(auth, req.Model, time.Now())
	lateAdmission := false
	lateAdmissionMaxAttempts := 0
	if blocked {
		lateAdmission, lateAdmissionMaxAttempts = m.canBypassLateCodexUsageLimitCooldown(auth, req.Model, coordinator)
		if !lateAdmission {
			rejectOwner()
			return nil, nil, false
		}
	}
	var lease *codexoverdraft.DrainLease
	var errAcquire error
	if lateAdmission {
		lease, errAcquire = coordinator.AcquireLateBusiness(dispatchIDFromOptions(opts), lateAdmissionMaxAttempts)
	} else {
		lease, errAcquire = coordinator.AcquireBusiness(dispatchIDFromOptions(opts))
	}
	if errAcquire != nil {
		rejectOwner()
		return nil, nil, false
	}
	var (
		release     sync.Once
		outcomeOnce sync.Once
		outcomeErr  error
	)
	execution := cliproxyexecutor.NewCodexOverdraftExecutionWithCompletion(
		lease.AuthID,
		lease.AuthGeneration,
		lease.DrainCycleID,
		lease.CoordinatorEpoch,
		lease.AuthStateEpoch,
		lease.DispatchID,
		cliproxyexecutor.CodexOverdraftBusiness,
		func() error { return coordinator.BeginSend(lease) },
		func() { release.Do(func() { _ = coordinator.Release(lease) }) },
		func() error {
			outcomeOnce.Do(func() {
				outcomeErr = coordinator.BusinessUsageLimit(lease)
			})
			return outcomeErr
		},
		func() error {
			outcomeOnce.Do(func() {
				outcomeErr = coordinator.BusinessCompleted(lease)
			})
			return outcomeErr
		},
	)
	return execution, auth, true
}

func (m *Manager) canBypassLateCodexUsageLimitCooldown(auth *Auth, model string, coordinator *codexoverdraft.Coordinator) (bool, int) {
	if auth == nil || coordinator == nil || coordinator.Owner() != auth.ID {
		return false, 0
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil || !cfg.Codex.Overdraft.LateAdmissionBypassCooldown || cfg.Codex.Overdraft.LateAdmissionMaxAttempts <= 0 {
		return false, 0
	}
	record := coordinator.Record(auth.ID)
	if record.State != codexoverdraft.StateActiveDrain || record.DrainCycleID == "" {
		return false, 0
	}
	modelKey := canonicalModelKey(model)
	matchedUsageLimit := false
	now := time.Now()
	for stateModel, state := range auth.ModelStates {
		if state == nil || canonicalModelKey(stateModel) != modelKey {
			continue
		}
		blocked, _, _ := availabilityBlock(state.Unavailable, state.Quota.Exceeded, state.NextRetryAfter, state.Quota.NextRecoverAt, now)
		if !blocked {
			continue
		}
		if state.LastError == nil || !strings.EqualFold(strings.TrimSpace(state.LastError.Code), "usage_limit_reached") {
			return false, 0
		}
		matchedUsageLimit = true
	}
	return matchedUsageLimit, cfg.Codex.Overdraft.LateAdmissionMaxAttempts
}

func codexOverdraftRequestCompatible(providers []string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) bool {
	hasCodex := false
	for _, provider := range providers {
		if strings.EqualFold(strings.TrimSpace(provider), "codex") {
			hasCodex = true
			break
		}
	}
	if !hasCodex || len(req.Payload) == 0 || !gjson.ValidBytes(req.Payload) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(opts.Alt), "responses/compact") {
		return false
	}
	path := strings.ToLower(strings.TrimSpace(metadataString(opts.Metadata, cliproxyexecutor.RequestPathMetadataKey)))
	return !strings.Contains(path, "/images/") && !strings.Contains(path, "/videos/") && !strings.Contains(path, "/responses/compact") && !strings.Contains(path, "count_tokens")
}

func dispatchIDFromOptions(opts cliproxyexecutor.Options) string {
	for _, key := range []string{"request_id", "dispatch_id"} {
		if value := metadataString(opts.Metadata, key); value != "" {
			return value
		}
	}
	return "dispatch_" + strings.ToLower(rand.Text())
}

func metadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	switch value := metadata[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case []byte:
		return strings.TrimSpace(string(value))
	default:
		return ""
	}
}

func attachCodexOverdraftExecution(opts cliproxyexecutor.Options, execution *cliproxyexecutor.CodexOverdraftExecution) cliproxyexecutor.Options {
	if execution == nil {
		return opts
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[cliproxyexecutor.CodexOverdraftExecutionMetadataKey] = execution
	opts.Metadata = metadata
	return opts
}

func isCodexUsageLimitError(err *Error) bool {
	return err != nil && strings.EqualFold(strings.TrimSpace(err.Code), "usage_limit_reached")
}

func accountingContextForExecution(ctx context.Context, opts cliproxyexecutor.Options) context.Context {
	metadata := coreusage.AccountingMetadata{UpstreamAttemptID: metadataString(opts.Metadata, accountingAttemptIDMetadataKey), ConsumptionClass: metadataString(opts.Metadata, "codex_consumption_class")}
	if hooks := cliproxyexecutor.AccountingAttemptHooksFromOptions(opts); hooks != nil {
		metadata.CurrentAttemptID = hooks.CurrentAttemptID
	}
	if execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts); execution != nil {
		metadata.RequestID = execution.DispatchID
		metadata.DrainMode = "overdraft"
		metadata.DrainCycleID = execution.DrainCycleID
		metadata.DrainRequestKind = string(execution.Kind)
	} else {
		metadata.RequestID = metadataString(opts.Metadata, "request_id")
		metadata.UpstreamAttemptID = metadataString(opts.Metadata, accountingAttemptIDMetadataKey)
		metadata.DrainMode = "normal"
	}
	if metadata.RequestID == "" {
		metadata.RequestID = metadata.UpstreamAttemptID
	}
	return coreusage.WithAccountingMetadata(ctx, metadata)
}

func (m *Manager) codexConsumptionClass(authID string) string {
	snapshot, ok := m.CodexQuotaSnapshot(authID)
	if !ok || !snapshot.HasCredits || snapshot.CreditsUnlimited {
		return "within_window"
	}
	full := snapshot.LimitReached
	for _, window := range snapshot.Windows {
		if window.UsedMicropct >= 100_000_000 {
			full = true
			break
		}
	}
	if !full {
		return "within_window"
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg != nil && cfg.Codex.Overdraft.AllowCreditSpend {
		return "over_window_on_credits"
	}
	return "within_window"
}

func finishCodexOverdraftExecution(execution *cliproxyexecutor.CodexOverdraftExecution, err error) {
	if execution == nil {
		return
	}
	if isCodexUsageLimitError(resultErrorFromError(err)) {
		_ = execution.BusinessUsageLimited()
	} else {
		_ = execution.BusinessCompleted()
	}
	execution.Release()
}

func wrapCodexOverdraftStream(ctx context.Context, result *cliproxyexecutor.StreamResult, execution *cliproxyexecutor.CodexOverdraftExecution) *cliproxyexecutor.StreamResult {
	if result == nil || result.Chunks == nil || execution == nil {
		return result
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		sawUsageLimit := false
		defer func() {
			if !sawUsageLimit {
				_ = execution.BusinessCompleted()
			}
			execution.Release()
		}()
		for chunk := range result.Chunks {
			if isCodexUsageLimitError(resultErrorFromError(chunk.Err)) {
				sawUsageLimit = true
				_ = execution.BusinessUsageLimited()
			}
			if ctx == nil {
				out <- chunk
				continue
			}
			select {
			case out <- chunk:
			case <-ctx.Done():
				discardStreamChunks(result.Chunks)
				return
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}
}
