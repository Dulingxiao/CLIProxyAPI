package auth

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type codexQuotaCall struct {
	done     chan struct{}
	snapshot codexoverdraft.QuotaSnapshot
	err      error
}

type codexQuotaPending struct {
	latest codexoverdraft.QuotaSnapshot
	peaks  map[codexoverdraft.WindowName]codexQuotaPendingWindow
}

type codexQuotaPendingWindow struct {
	snapshot codexoverdraft.QuotaSnapshot
	window   codexoverdraft.QuotaWindow
}

// CodexQuotaRuntimeHealth describes the durable quota observation dependency.
type CodexQuotaRuntimeHealth struct {
	Enabled   bool   `json:"enabled"`
	Healthy   bool   `json:"healthy"`
	LastError string `json:"last_error,omitempty"`
}

type codexQuotaRuntime struct {
	ctx                     context.Context
	cancel                  context.CancelFunc
	storePath               string
	semaphore               chan struct{}
	staleAfter              time.Duration
	nearThresholdStaleAfter time.Duration
	minActiveInterval       time.Duration
	armThresholdMicropct    int64
	thresholdMicropct       int64
	jitter                  time.Duration

	mu              sync.Mutex
	calls           map[string]*codexQuotaCall
	timers          map[string]*time.Timer
	scheduledAt     map[string]time.Time
	errorSteps      map[string]int
	noResetSteps    map[string]int
	lastQueryAt     map[string]time.Time
	priorityWaiters int
	observations    chan codexoverdraft.QuotaSnapshot
	observationWake chan struct{}
	observationDone chan struct{}
	pending         map[string]codexQuotaPending
}

var codexQuotaErrorBackoff = []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute}
var codexQuotaNoResetCadence = []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute}

const quotaRatePredictionSafetyFactorNumerator int64 = 1
const quotaRatePredictionSafetyFactorDenominator int64 = 2

func isCodexQuotaEligibleAuth(auth *Auth) bool {
	return isCodexOverdraftEligibleAuth(auth) && !auth.Disabled && auth.Status != StatusDisabled && !hasUnauthorizedAuthFailure(auth)
}

func (m *Manager) codexQuotaObserverForAuth(auth *Auth) cliproxyexecutor.CodexQuotaObserver {
	if m == nil || auth == nil || !isCodexQuotaEligibleAuth(auth) || !m.codexQuotaEnabled.Load() {
		return nil
	}
	authID := auth.ID
	generation := m.codexAuthGeneration(authID)
	return func(observation cliproxyexecutor.CodexQuotaHeadersObservation) {
		if observation.AuthID == "" {
			observation.AuthID = authID
		}
		if observation.AuthGeneration == 0 {
			observation.AuthGeneration = generation
		}
		m.observeCodexQuotaHeaders(observation)
	}
}

func (m *Manager) observeCodexQuotaHeaders(observation cliproxyexecutor.CodexQuotaHeadersObservation) {
	if m == nil || !m.codexQuotaEnabled.Load() || observation.AuthID == "" {
		return
	}
	currentGeneration := m.codexAuthGeneration(observation.AuthID)
	if observation.AuthGeneration == 0 {
		observation.AuthGeneration = currentGeneration
	}
	if observation.AuthGeneration != currentGeneration {
		return
	}
	if observation.ObservedAt.IsZero() {
		observation.ObservedAt = time.Now()
	}
	snapshot, errParse := codexoverdraft.ParsePassiveQuotaHeaders(
		observation.AuthID,
		observation.AuthGeneration,
		observation.AttemptID,
		observation.Header,
		observation.ObservedAt,
	)
	if errParse != nil {
		return
	}
	snapshot.Sequence = m.codexQuotaSequence.Add(1)
	m.submitCodexQuotaObservation(snapshot)
}

func (m *Manager) submitCodexQuotaObservation(snapshot codexoverdraft.QuotaSnapshot) {
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime == nil || runtime.ctx.Err() != nil {
		return
	}
	runtime.mu.Lock()
	if pending, exists := runtime.pending[snapshot.AuthID]; exists {
		runtime.pending[snapshot.AuthID] = mergeCodexQuotaPending(pending, snapshot)
		runtime.mu.Unlock()
		select {
		case runtime.observationWake <- struct{}{}:
		default:
		}
		return
	}
	select {
	case runtime.observations <- snapshot:
		runtime.mu.Unlock()
		return
	default:
	}
	runtime.pending[snapshot.AuthID] = mergeCodexQuotaPending(runtime.pending[snapshot.AuthID], snapshot)
	runtime.mu.Unlock()
	select {
	case runtime.observationWake <- struct{}{}:
	default:
	}
}

func mergeCodexQuotaPending(pending codexQuotaPending, snapshot codexoverdraft.QuotaSnapshot) codexQuotaPending {
	if pending.latest.AuthID == "" || snapshot.Sequence > pending.latest.Sequence {
		pending.latest = snapshot
		retained := make(map[codexoverdraft.WindowName]codexQuotaPendingWindow, 2)
		latestWindows := codexQuotaWindowsByName(snapshot)
		for name, peak := range pending.peaks {
			latestWindow, exists := latestWindows[name]
			if exists && peak.snapshot.AuthGeneration == snapshot.AuthGeneration && peak.window.ResetAt.Equal(latestWindow.ResetAt) {
				retained[name] = peak
			}
		}
		pending.peaks = retained
	}
	if pending.peaks == nil {
		pending.peaks = make(map[codexoverdraft.WindowName]codexQuotaPendingWindow, 2)
	}
	latestWindows := codexQuotaWindowsByName(pending.latest)
	for _, window := range snapshot.Windows {
		if window.Name != codexoverdraft.WindowPrimary && window.Name != codexoverdraft.WindowSecondary {
			continue
		}
		latestWindow, exists := latestWindows[window.Name]
		if !exists || snapshot.AuthGeneration != pending.latest.AuthGeneration || !window.ResetAt.Equal(latestWindow.ResetAt) {
			continue
		}
		peak, exists := pending.peaks[window.Name]
		if !exists || window.UsedMicropct > peak.window.UsedMicropct || (window.UsedMicropct == peak.window.UsedMicropct && snapshot.Sequence > peak.snapshot.Sequence) {
			pending.peaks[window.Name] = codexQuotaPendingWindow{snapshot: snapshot, window: window}
		}
	}
	return pending
}

func codexQuotaWindowsByName(snapshot codexoverdraft.QuotaSnapshot) map[codexoverdraft.WindowName]codexoverdraft.QuotaWindow {
	windows := make(map[codexoverdraft.WindowName]codexoverdraft.QuotaWindow, 2)
	for _, window := range snapshot.Windows {
		if window.Name == codexoverdraft.WindowPrimary || window.Name == codexoverdraft.WindowSecondary {
			windows[window.Name] = window
		}
	}
	return windows
}

func codexQuotaPendingSnapshots(pending codexQuotaPending) []codexoverdraft.QuotaSnapshot {
	if pending.latest.AuthID == "" {
		return nil
	}
	bySequence := make(map[uint64]codexoverdraft.QuotaSnapshot, len(pending.peaks))
	for _, peak := range pending.peaks {
		if peak.snapshot.Sequence == pending.latest.Sequence {
			continue
		}
		partial, exists := bySequence[peak.snapshot.Sequence]
		if !exists {
			partial = peak.snapshot
			partial.Windows = nil
		}
		partial.Windows = append(partial.Windows, peak.window)
		bySequence[peak.snapshot.Sequence] = partial
	}
	sequences := make([]uint64, 0, len(bySequence))
	for sequence := range bySequence {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	snapshots := make([]codexoverdraft.QuotaSnapshot, 0, len(sequences)+1)
	for _, sequence := range sequences {
		partial := bySequence[sequence]
		sort.Slice(partial.Windows, func(i, j int) bool { return partial.Windows[i].Name < partial.Windows[j].Name })
		snapshots = append(snapshots, partial)
	}
	return append(snapshots, pending.latest)
}

func (m *Manager) runCodexQuotaObservationBus(runtime *codexQuotaRuntime) {
	if runtime.observationDone != nil {
		defer close(runtime.observationDone)
	}
	for {
		select {
		case snapshot := <-runtime.observations:
			m.applyPassiveCodexQuotaSnapshot(snapshot)
			continue
		default:
		}
		select {
		case <-runtime.ctx.Done():
			m.drainCodexQuotaObservationBus(runtime)
			return
		case snapshot := <-runtime.observations:
			m.applyPassiveCodexQuotaSnapshot(snapshot)
		case <-runtime.observationWake:
			m.drainPendingCodexQuotaObservations(runtime)
		}
	}
}

func (m *Manager) drainCodexQuotaObservationBus(runtime *codexQuotaRuntime) {
	if runtime == nil {
		return
	}
	for {
		select {
		case snapshot := <-runtime.observations:
			m.applyPassiveCodexQuotaSnapshot(snapshot)
		default:
			m.drainPendingCodexQuotaObservations(runtime)
			return
		}
	}
}

func (m *Manager) drainPendingCodexQuotaObservations(runtime *codexQuotaRuntime) {
	for {
		runtime.mu.Lock()
		var selectedID string
		var selected codexQuotaPending
		for authID, pending := range runtime.pending {
			if selectedID == "" || pending.latest.Sequence < selected.latest.Sequence {
				selectedID = authID
				selected = pending
			}
		}
		if selectedID != "" {
			delete(runtime.pending, selectedID)
		}
		runtime.mu.Unlock()
		if selectedID == "" {
			return
		}
		for _, snapshot := range codexQuotaPendingSnapshots(selected) {
			m.applyPassiveCodexQuotaSnapshot(snapshot)
		}
	}
}

func (m *Manager) applyPassiveCodexQuotaSnapshot(snapshot codexoverdraft.QuotaSnapshot) {
	if m == nil || !m.codexQuotaEnabled.Load() {
		return
	}
	currentGeneration := m.codexAuthGeneration(snapshot.AuthID)
	if snapshot.AuthGeneration != currentGeneration {
		return
	}
	if snapshot.Inconsistent {
		if errPersist := m.persistCodexQuotaSnapshot(snapshot, true); errPersist != nil {
			m.recordCodexQuotaPersistenceResult(errPersist)
			logEntryWithRequestID(nil).WithField("auth_id", snapshot.AuthID).Warnf("failed to persist inconsistent Codex quota snapshot: %v", errPersist)
			return
		}
	} else {
		accepted, errMerge := m.codexQuotaState.MergeDurable(snapshot, currentGeneration, func() error {
			return m.persistCodexQuotaSnapshot(snapshot, true)
		})
		if errMerge != nil {
			m.recordCodexQuotaPersistenceResult(errMerge)
			logEntryWithRequestID(nil).WithField("auth_id", snapshot.AuthID).Warnf("failed to persist Codex quota snapshot: %v", errMerge)
			return
		}
		if !accepted {
			if errPersist := m.persistCodexQuotaSnapshot(snapshot, false); errPersist != nil {
				m.recordCodexQuotaPersistenceResult(errPersist)
				logEntryWithRequestID(nil).WithField("auth_id", snapshot.AuthID).Warnf("failed to persist historical Codex quota snapshot: %v", errPersist)
				return
			}
			m.recordCodexQuotaPersistenceResult(nil)
			return
		}
	}
	m.recordCodexQuotaPersistenceResult(nil)
	m.codexQuotaMu.Lock()
	m.setCodexQuotaSnapshotLocked(snapshot.AuthID, snapshot)
	m.codexQuotaMu.Unlock()
	if snapshot.Inconsistent {
		delay := time.Duration(0)
		if !snapshot.CalibrationAt.IsZero() {
			delay = time.Until(snapshot.CalibrationAt.Add(3 * time.Second))
			if delay < 0 {
				delay = 0
			}
		}
		m.scheduleCodexQuotaRefresh(snapshot.AuthID, delay)
		return
	}
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() {
		return
	}
	_ = coordinator.UpdateInheritedInFlight(snapshot.AuthID, snapshot.AuthGeneration, m.codexLegacyInFlightCount(snapshot.AuthID))
	for _, window := range snapshot.Windows {
		if !window.ResetAt.IsZero() && !window.ResetAt.After(snapshot.ObservedAt) {
			continue
		}
		if errObserve := coordinator.ObserveThreshold(snapshot.AuthID, snapshot.AuthGeneration, window.UsedMicropct); errObserve != nil {
			logEntryWithRequestID(nil).WithField("auth_id", snapshot.AuthID).Warnf("failed to apply Codex quota threshold: %v", errObserve)
		}
	}
	m.scheduleCodexQuotaAfterSnapshot(snapshot)
}

// CodexQuotaSnapshot returns the latest accepted quota observation for an auth ID.
func (m *Manager) CodexQuotaSnapshot(authID string) (codexoverdraft.QuotaSnapshot, bool) {
	if m == nil {
		return codexoverdraft.QuotaSnapshot{}, false
	}
	m.codexQuotaMu.RLock()
	defer m.codexQuotaMu.RUnlock()
	snapshot, ok := m.codexQuotaSnapshots[authID]
	return snapshot, ok
}

// CodexQuotaSnapshots returns copies of all latest quota observations keyed by Auth.ID.
func (m *Manager) CodexQuotaSnapshots() map[string]codexoverdraft.QuotaSnapshot {
	if m == nil {
		return nil
	}
	m.codexQuotaMu.RLock()
	defer m.codexQuotaMu.RUnlock()
	snapshots := make(map[string]codexoverdraft.QuotaSnapshot, len(m.codexQuotaSnapshots))
	for authID, snapshot := range m.codexQuotaSnapshots {
		snapshot.Windows = append([]codexoverdraft.QuotaWindow(nil), snapshot.Windows...)
		snapshots[authID] = snapshot
	}
	return snapshots
}

func (m *Manager) codexAuthGeneration(authID string) uint64 {
	if m == nil || authID == "" {
		return 0
	}
	m.codexOverdraftMu.Lock()
	generation := m.codexOverdraftGenerations[authID]
	created := false
	if generation == 0 {
		generation = 1
		m.codexOverdraftGenerations[authID] = generation
		created = true
	}
	m.codexOverdraftMu.Unlock()
	if created {
		m.persistCodexAuthGeneration(authID, generation)
	}
	return generation
}

func (m *Manager) attachCodexQuotaObserver(opts cliproxyexecutor.Options, auth *Auth) cliproxyexecutor.Options {
	observer := m.codexQuotaObserverForAuth(auth)
	if observer == nil {
		return opts
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[cliproxyexecutor.CodexQuotaObserverMetadataKey] = observer
	opts.Metadata = metadata
	return opts
}

func (m *Manager) applyCodexQuotaRuntime(cfg *internalconfig.Config) error {
	if m == nil || cfg == nil {
		return nil
	}
	if !cfg.Codex.Quota.Enabled {
		m.StopCodexQuota()
		return nil
	}
	staleAfter, jitter, errDurations := cfg.Codex.Quota.Durations()
	if errDurations != nil {
		return errDurations
	}
	if cfg.Codex.Quota.ActiveQueryConcurrency <= 0 {
		return fmt.Errorf("codex.quota.active-query-concurrency must be positive")
	}
	nearThreshold, minActiveInterval, errActiveDurations := cfg.Codex.Quota.ActiveDurations()
	if errActiveDurations != nil {
		return errActiveDurations
	}
	overdraftConfig := cfg.Codex.Overdraft
	defaults := internalconfig.DefaultCodexOverdraftConfig()
	if strings.TrimSpace(overdraftConfig.ArmThresholdPercent) == "" {
		overdraftConfig.ArmThresholdPercent = defaults.ArmThresholdPercent
	}
	if strings.TrimSpace(overdraftConfig.QuotaThresholdPercent) == "" {
		overdraftConfig.QuotaThresholdPercent = defaults.QuotaThresholdPercent
	}
	armThreshold, errArmThreshold := overdraftConfig.ArmThresholdMicropct()
	if errArmThreshold != nil {
		return errArmThreshold
	}
	threshold, errThreshold := overdraftConfig.ThresholdMicropct()
	if errThreshold != nil {
		return errThreshold
	}
	storePath := resolveCodexQuotaStoragePath(cfg)
	m.codexQuotaRuntimeMu.RLock()
	current := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if current != nil && current.ctx.Err() == nil && current.storePath == storePath && current.staleAfter == staleAfter && current.nearThresholdStaleAfter == nearThreshold && current.minActiveInterval == minActiveInterval && current.armThresholdMicropct == armThreshold && current.thresholdMicropct == threshold && current.jitter == jitter && cap(current.semaphore) == cfg.Codex.Quota.ActiveQueryConcurrency {
		return nil
	}
	if current != nil && cfg.Codex.Overdraft.Enabled {
		m.pauseCodexOverdraftForQuotaDependency()
	}
	m.StopCodexQuota()
	store, errStore := codexoverdraft.OpenQuotaStore(storePath)
	if errStore != nil {
		return errStore
	}
	maxSequence, errSequence := store.MaxSequence()
	if errSequence != nil {
		_ = store.Close()
		return fmt.Errorf("load Codex quota sequence: %w", errSequence)
	}
	latestSnapshots, errLatest := store.LatestSnapshots()
	if errLatest != nil {
		_ = store.Close()
		return fmt.Errorf("load latest Codex quota snapshots: %w", errLatest)
	}
	generations, errGenerations := store.Generations()
	if errGenerations != nil {
		_ = store.Close()
		return fmt.Errorf("load Codex auth generations: %w", errGenerations)
	}
	m.codexQuotaStoreMu.Lock()
	m.codexQuotaStore = store
	m.codexQuotaStorePath = storePath
	m.codexQuotaStoreMu.Unlock()
	m.codexQuotaSequence.Store(maxSequence)
	m.codexOverdraftMu.Lock()
	for authID, generation := range generations {
		if generation > m.codexOverdraftGenerations[authID] {
			m.codexOverdraftGenerations[authID] = generation
		}
	}
	m.codexOverdraftMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &codexQuotaRuntime{
		ctx:                     ctx,
		cancel:                  cancel,
		storePath:               storePath,
		semaphore:               make(chan struct{}, cfg.Codex.Quota.ActiveQueryConcurrency),
		staleAfter:              staleAfter,
		nearThresholdStaleAfter: nearThreshold,
		minActiveInterval:       minActiveInterval,
		armThresholdMicropct:    armThreshold,
		thresholdMicropct:       threshold,
		jitter:                  jitter,
		calls:                   make(map[string]*codexQuotaCall),
		timers:                  make(map[string]*time.Timer),
		scheduledAt:             make(map[string]time.Time),
		errorSteps:              make(map[string]int),
		noResetSteps:            make(map[string]int),
		lastQueryAt:             make(map[string]time.Time),
		observations:            make(chan codexoverdraft.QuotaSnapshot, 4096),
		observationWake:         make(chan struct{}, 1),
		observationDone:         make(chan struct{}),
		pending:                 make(map[string]codexQuotaPending),
	}
	m.codexQuotaRuntimeMu.Lock()
	m.codexQuotaRuntime = runtime
	m.codexQuotaRuntimeMu.Unlock()
	m.codexQuotaEnabled.Store(true)
	m.initializeCodexQuotaHealth()
	m.restoreCodexQuotaSnapshots(latestSnapshots)
	go m.runCodexQuotaObservationBus(runtime)
	go m.runCodexQuotaScheduler(runtime)
	return nil
}

func resolveCodexQuotaStoragePath(cfg *internalconfig.Config) string {
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
	return base + "-quota" + extension
}

func (m *Manager) persistCodexQuotaSnapshot(snapshot codexoverdraft.QuotaSnapshot, latest bool) error {
	m.codexQuotaStoreMu.Lock()
	defer m.codexQuotaStoreMu.Unlock()
	if m.codexQuotaStore == nil {
		return fmt.Errorf("Codex quota store is not available")
	}
	return m.codexQuotaStore.Append(snapshot, latest)
}

func (m *Manager) persistCodexAuthGeneration(authID string, generation uint64) {
	m.codexQuotaStoreMu.Lock()
	if m.codexQuotaStore == nil {
		m.codexQuotaStoreMu.Unlock()
		if m.codexQuotaEnabled.Load() {
			m.recordCodexQuotaPersistenceResult(fmt.Errorf("Codex quota store is not available"))
		}
		return
	}
	errPersist := m.codexQuotaStore.SetGeneration(authID, generation)
	m.codexQuotaStoreMu.Unlock()
	m.recordCodexQuotaPersistenceResult(errPersist)
	if errPersist != nil {
		logEntryWithRequestID(nil).WithField("auth_id", authID).Warnf("failed to persist Codex auth generation: %v", errPersist)
	}
}

func (m *Manager) restoreCodexQuotaSnapshots(snapshots map[string]codexoverdraft.QuotaSnapshot) {
	for _, auth := range m.snapshotAuths() {
		if !isCodexQuotaEligibleAuth(auth) {
			continue
		}
		snapshot, exists := snapshots[auth.ID]
		if !exists {
			continue
		}
		m.restoreCodexQuotaSnapshot(auth, snapshot)
	}
}

func (m *Manager) restoreCodexQuotaSnapshotForAuth(auth *Auth) {
	if m == nil || !isCodexQuotaEligibleAuth(auth) {
		return
	}
	m.codexQuotaStoreMu.Lock()
	store := m.codexQuotaStore
	if store == nil {
		m.codexQuotaStoreMu.Unlock()
		return
	}
	snapshot, exists, errLatest := store.Latest(auth.ID)
	m.codexQuotaStoreMu.Unlock()
	if errLatest != nil {
		m.recordCodexQuotaPersistenceResult(errLatest)
		logEntryWithRequestID(nil).WithField("auth_id", auth.ID).Warnf("failed to restore Codex quota snapshot: %v", errLatest)
		return
	}
	if exists {
		m.restoreCodexQuotaSnapshot(auth, snapshot)
	}
}

func (m *Manager) restoreCodexQuotaSnapshot(auth *Auth, snapshot codexoverdraft.QuotaSnapshot) {
	generation := m.codexAuthGeneration(auth.ID)
	if snapshot.AuthGeneration != generation {
		return
	}
	if !snapshot.Inconsistent {
		_ = m.codexQuotaState.Merge(snapshot, generation)
	}
	m.codexQuotaMu.Lock()
	current, exists := m.codexQuotaSnapshots[auth.ID]
	if !exists || snapshot.Sequence >= current.Sequence {
		m.setCodexQuotaSnapshotLocked(auth.ID, snapshot)
	}
	m.codexQuotaMu.Unlock()
}

func (m *Manager) runCodexQuotaScheduler(runtime *codexQuotaRuntime) {
	for _, auth := range m.snapshotAuths() {
		if !isCodexQuotaEligibleAuth(auth) {
			continue
		}
		m.scheduleCodexQuotaStartupRefreshWithRuntime(runtime, auth.ID)
	}
	interval := runtime.staleAfter / 2
	if runtime.nearThresholdStaleAfter > 0 && runtime.nearThresholdStaleAfter < interval {
		interval = runtime.nearThresholdStaleAfter
	}
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case now := <-ticker.C:
			for _, auth := range m.snapshotAuths() {
				if !isCodexQuotaEligibleAuth(auth) {
					continue
				}
				snapshot, ok := m.CodexQuotaSnapshot(auth.ID)
				freshness := runtime.staleAfter
				if ok {
					m.codexQuotaMu.RLock()
					previous := m.codexQuotaPrevSnapshots[auth.ID]
					m.codexQuotaMu.RUnlock()
					if codexQuotaSnapshotAtOrAbove(snapshot, runtime.armThresholdMicropct) {
						freshness = maxQuotaRefreshDelay(runtime.minActiveInterval, runtime.nearThresholdStaleAfter)
					} else {
						freshness = predictCodexQuotaRefreshDelay(previous, snapshot, runtime.thresholdMicropct, runtime.minActiveInterval, runtime.staleAfter)
					}
				}
				if !ok || now.Sub(snapshot.ObservedAt) >= freshness {
					m.scheduleCodexQuotaRefreshWithRuntime(runtime, auth.ID, 0)
				}
			}
		}
	}
}

func (m *Manager) scheduleCodexQuotaStartupRefresh(authID string) {
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime != nil {
		m.scheduleCodexQuotaStartupRefreshWithRuntime(runtime, authID)
	}
}

func (m *Manager) scheduleCodexQuotaStartupRefreshWithRuntime(runtime *codexQuotaRuntime, authID string) {
	if runtime == nil || authID == "" {
		return
	}
	delay := time.Duration(0)
	if runtime.jitter > 0 {
		delay = time.Duration(rand.Int64N(int64(runtime.jitter)))
	}
	m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, delay)
}

func (m *Manager) scheduleCodexQuotaRefresh(authID string, delay time.Duration) {
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime != nil {
		m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, delay)
	}
}

func (m *Manager) cancelCodexQuotaRefresh(authID string) {
	if m == nil || authID == "" {
		return
	}
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if timer := runtime.timers[authID]; timer != nil {
		timer.Stop()
	}
	delete(runtime.timers, authID)
	delete(runtime.scheduledAt, authID)
	delete(runtime.errorSteps, authID)
	delete(runtime.noResetSteps, authID)
	runtime.mu.Unlock()
}

func (m *Manager) scheduleCodexQuotaRefreshWithRuntime(runtime *codexQuotaRuntime, authID string, delay time.Duration) {
	if runtime == nil || authID == "" {
		return
	}
	if delay < 0 {
		delay = 0
	}
	runAt := time.Now().Add(delay)
	runtime.mu.Lock()
	if existingAt, exists := runtime.scheduledAt[authID]; exists && !runAt.Before(existingAt) {
		runtime.mu.Unlock()
		return
	}
	if existing := runtime.timers[authID]; existing != nil {
		existing.Stop()
	}
	runtime.scheduledAt[authID] = runAt
	runtime.timers[authID] = time.AfterFunc(delay, func() {
		runtime.mu.Lock()
		delete(runtime.timers, authID)
		delete(runtime.scheduledAt, authID)
		runtime.mu.Unlock()
		_, _ = m.refreshCodexQuotaWithRuntime(runtime.ctx, runtime, authID)
	})
	runtime.mu.Unlock()
}

// RefreshCodexQuota performs or joins one active usage query for an auth ID.
func (m *Manager) RefreshCodexQuota(ctx context.Context, authID string) (codexoverdraft.QuotaSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime == nil {
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("Codex quota runtime is disabled")
	}
	return m.refreshCodexQuotaWithRuntime(ctx, runtime, authID)
}

func (m *Manager) refreshCodexQuotaWithRuntime(ctx context.Context, runtime *codexQuotaRuntime, authID string) (codexoverdraft.QuotaSnapshot, error) {
	generation := m.codexAuthGeneration(authID)
	callKey := fmt.Sprintf("%s:%d", authID, generation)
	runtime.mu.Lock()
	if existing := runtime.calls[callKey]; existing != nil {
		runtime.mu.Unlock()
		select {
		case <-ctx.Done():
			return codexoverdraft.QuotaSnapshot{}, ctx.Err()
		case <-existing.done:
			return existing.snapshot, existing.err
		}
	}
	call := &codexQuotaCall{done: make(chan struct{})}
	runtime.calls[callKey] = call
	runtime.mu.Unlock()

	go func() {
		call.snapshot, call.err = m.queryCodexQuota(runtime, authID, generation)
		runtime.mu.Lock()
		delete(runtime.calls, callKey)
		close(call.done)
		runtime.mu.Unlock()
		m.scheduleCodexQuotaAfterResult(runtime, authID, call.snapshot, call.err)
	}()
	select {
	case <-ctx.Done():
		return codexoverdraft.QuotaSnapshot{}, ctx.Err()
	case <-call.done:
		return call.snapshot, call.err
	}
}

func (m *Manager) queryCodexQuota(runtime *codexQuotaRuntime, authID string, generation uint64) (codexoverdraft.QuotaSnapshot, error) {
	if errWait := waitCodexQuotaMinInterval(runtime, authID); errWait != nil {
		return codexoverdraft.QuotaSnapshot{}, errWait
	}
	if errAcquire := acquireCodexQuotaSlot(runtime, m.codexQuotaAuthIsArmed(authID, runtime.armThresholdMicropct)); errAcquire != nil {
		return codexoverdraft.QuotaSnapshot{}, errAcquire
	}
	if runtime.semaphore != nil {
		defer func() { <-runtime.semaphore }()
	}
	recordCodexQuotaQueryStart(runtime, authID)

	m.mu.RLock()
	auth := m.auths[authID]
	if auth != nil {
		auth = auth.Clone()
	}
	executor := m.executors["codex"]
	m.mu.RUnlock()
	if !isCodexQuotaEligibleAuth(auth) {
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("auth %q is not eligible for active Codex quota", authID)
	}
	if authMetadataString(auth, "account_id") == "" {
		logEntryWithRequestID(nil).WithField("auth_id", authID).Warn("skipping active Codex quota query because account_id is missing")
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("active Codex quota query requires account_id")
	}
	if executor == nil {
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("Codex executor is not registered")
	}

	query := func(queryAuth *Auth) (*http.Response, error) {
		request, errRequest := http.NewRequestWithContext(runtime.ctx, http.MethodGet, "https://chatgpt.com/backend-api/wham/usage", nil)
		if errRequest != nil {
			return nil, errRequest
		}
		accountID := authMetadataString(queryAuth, "account_id")
		if accountID == "" {
			return nil, fmt.Errorf("active Codex quota query requires account_id")
		}
		request.Header.Set("Chatgpt-Account-Id", accountID)
		return executor.HttpRequest(runtime.ctx, queryAuth, request)
	}
	response, errQuery := query(auth)
	if errQuery != nil {
		return codexoverdraft.QuotaSnapshot{}, errQuery
	}
	if response == nil {
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("active Codex quota query returned no response")
	}
	if response.StatusCode == http.StatusUnauthorized {
		closeQuotaResponse(response)
		refreshed, errRefresh := m.refreshAuthForRequest(runtime.ctx, auth.ID, authAccessToken(auth))
		if errRefresh != nil {
			return codexoverdraft.QuotaSnapshot{}, errRefresh
		}
		if refreshed != nil {
			auth = refreshed
		}
		response, errQuery = query(auth)
		if errQuery != nil {
			return codexoverdraft.QuotaSnapshot{}, errQuery
		}
		if response == nil {
			return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("active Codex quota retry returned no response")
		}
	}
	defer closeQuotaResponse(response)
	if response.StatusCode == http.StatusUnauthorized {
		m.markCodexQuotaUnauthorized(authID)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, response.Body)
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("active Codex quota query status %d", response.StatusCode)
	}
	body, errRead := io.ReadAll(response.Body)
	if errRead != nil {
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("read active Codex quota response: %w", errRead)
	}
	if errContext := runtime.ctx.Err(); errContext != nil {
		return codexoverdraft.QuotaSnapshot{}, errContext
	}
	observedAt := time.Now()
	m.storeCodexQuotaDebugPayload(authID, body, observedAt)
	snapshot, errParse := codexoverdraft.ParseActiveQuotaPayload(authID, generation, "query_"+strings.ToLower(randText()), body, observedAt)
	if errParse != nil {
		return codexoverdraft.QuotaSnapshot{}, errParse
	}
	snapshot.Sequence = m.codexQuotaSequence.Add(1)
	currentGeneration := m.codexAuthGeneration(authID)
	accepted, errMerge := m.codexQuotaState.MergeDurable(snapshot, currentGeneration, func() error {
		return m.persistCodexQuotaSnapshot(snapshot, true)
	})
	if errMerge != nil {
		m.recordCodexQuotaPersistenceResult(errMerge)
		return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("persist active Codex quota response: %w", errMerge)
	}
	if !accepted {
		if errPersist := m.persistCodexQuotaSnapshot(snapshot, false); errPersist != nil {
			m.recordCodexQuotaPersistenceResult(errPersist)
			return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("persist stale active Codex quota response: %w", errPersist)
		}
		m.recordCodexQuotaPersistenceResult(nil)
		if snapshot.AuthGeneration != currentGeneration || snapshot.Inconsistent {
			return codexoverdraft.QuotaSnapshot{}, fmt.Errorf("active Codex quota response was stale")
		}
		// The merge gate keeps the stored windows when upstream reports an
		// equal-or-older view (reset timestamps can wobble backwards a few
		// seconds between queries). The query itself still succeeded, so the
		// lifecycle decisions below must run: dropping them left restarted
		// calibration-required candidates stuck outside the overdraft pool.
		m.applyActiveCodexQuotaSnapshot(snapshot)
		return snapshot, nil
	}
	m.recordCodexQuotaPersistenceResult(nil)
	m.codexQuotaMu.Lock()
	m.setCodexQuotaSnapshotLocked(authID, snapshot)
	m.codexQuotaMu.Unlock()
	m.applyActiveCodexQuotaSnapshot(snapshot)
	return snapshot, nil
}

func waitCodexQuotaMinInterval(runtime *codexQuotaRuntime, authID string) error {
	if runtime == nil || runtime.minActiveInterval <= 0 {
		return nil
	}
	for {
		runtime.mu.Lock()
		if runtime.lastQueryAt == nil {
			runtime.lastQueryAt = make(map[string]time.Time)
		}
		remaining := runtime.minActiveInterval - time.Since(runtime.lastQueryAt[authID])
		if remaining <= 0 {
			runtime.mu.Unlock()
			return nil
		}
		runtime.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-runtime.ctx.Done():
			timer.Stop()
			return runtime.ctx.Err()
		case <-timer.C:
		}
	}
}

func recordCodexQuotaQueryStart(runtime *codexQuotaRuntime, authID string) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	if runtime.lastQueryAt == nil {
		runtime.lastQueryAt = make(map[string]time.Time)
	}
	runtime.lastQueryAt[authID] = time.Now()
	runtime.mu.Unlock()
}

func acquireCodexQuotaSlot(runtime *codexQuotaRuntime, priority bool) error {
	if runtime == nil || runtime.semaphore == nil {
		return nil
	}
	if priority {
		runtime.mu.Lock()
		runtime.priorityWaiters++
		runtime.mu.Unlock()
		select {
		case <-runtime.ctx.Done():
			runtime.mu.Lock()
			runtime.priorityWaiters--
			runtime.mu.Unlock()
			return runtime.ctx.Err()
		case runtime.semaphore <- struct{}{}:
			runtime.mu.Lock()
			runtime.priorityWaiters--
			runtime.mu.Unlock()
			return nil
		}
	}
	for {
		runtime.mu.Lock()
		priorityWaiting := runtime.priorityWaiters > 0
		runtime.mu.Unlock()
		if !priorityWaiting {
			select {
			case runtime.semaphore <- struct{}{}:
				return nil
			default:
			}
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-runtime.ctx.Done():
			timer.Stop()
			return runtime.ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *Manager) scheduleCodexQuotaAfterSnapshot(snapshot codexoverdraft.QuotaSnapshot) {
	m.codexQuotaRuntimeMu.RLock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntimeMu.RUnlock()
	if runtime == nil {
		return
	}
	m.scheduleCodexQuotaAfterResult(runtime, snapshot.AuthID, snapshot, nil)
}

func (m *Manager) scheduleCodexQuotaAfterResult(runtime *codexQuotaRuntime, authID string, snapshot codexoverdraft.QuotaSnapshot, errQuery error) {
	if runtime == nil || authID == "" || runtime.ctx.Err() != nil {
		return
	}
	m.mu.RLock()
	auth := m.auths[authID]
	if auth != nil {
		auth = auth.Clone()
	}
	m.mu.RUnlock()
	if !isCodexQuotaEligibleAuth(auth) {
		m.cancelCodexQuotaRefresh(authID)
		return
	}
	if errQuery != nil {
		runtime.mu.Lock()
		step := runtime.errorSteps[authID]
		if step >= len(codexQuotaErrorBackoff) {
			step = len(codexQuotaErrorBackoff) - 1
		}
		delay := codexQuotaErrorBackoff[step]
		if m.codexQuotaAuthIsArmed(authID, runtime.armThresholdMicropct) && runtime.nearThresholdStaleAfter > 0 && delay > runtime.nearThresholdStaleAfter {
			delay = runtime.nearThresholdStaleAfter
		}
		if runtime.errorSteps[authID] < len(codexQuotaErrorBackoff)-1 {
			runtime.errorSteps[authID]++
		}
		runtime.mu.Unlock()
		logEntryWithRequestID(nil).WithField("auth_id", authID).WithField("retry_in", delay.String()).WithError(errQuery).Warn("active Codex quota query failed")
		m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, delay)
		return
	}
	runtime.mu.Lock()
	delete(runtime.errorSteps, authID)
	runtime.mu.Unlock()
	m.codexQuotaMu.RLock()
	previous := m.codexQuotaPrevSnapshots[authID]
	m.codexQuotaMu.RUnlock()
	normalDelay := runtime.staleAfter
	if codexQuotaSnapshotAtOrAbove(snapshot, runtime.armThresholdMicropct) {
		normalDelay = maxQuotaRefreshDelay(runtime.minActiveInterval, runtime.nearThresholdStaleAfter)
		if jitterLimit := normalDelay / 10; jitterLimit > 0 {
			normalDelay += time.Duration(rand.Int64N(int64(jitterLimit) + 1))
		}
	} else {
		normalDelay = predictCodexQuotaRefreshDelay(previous, snapshot, runtime.thresholdMicropct, runtime.minActiveInterval, runtime.staleAfter)
	}
	if normalDelay > 0 {
		m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, normalDelay)
	}

	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() {
		return
	}
	record := coordinator.Record(authID)
	if record.State == "" || record.State == codexoverdraft.StateNormal || record.State == codexoverdraft.StateDisabled {
		runtime.mu.Lock()
		delete(runtime.noResetSteps, authID)
		runtime.mu.Unlock()
		return
	}
	var earliestReset time.Time
	for _, window := range snapshot.Windows {
		if window.ResetAt.After(time.Now()) && (earliestReset.IsZero() || window.ResetAt.Before(earliestReset)) {
			earliestReset = window.ResetAt
		}
	}
	if !earliestReset.IsZero() {
		runtime.mu.Lock()
		delete(runtime.noResetSteps, authID)
		runtime.mu.Unlock()
		delay := time.Until(earliestReset.Add(3 * time.Second))
		if delay < 0 {
			delay = 0
		}
		m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, delay)
		return
	}
	runtime.mu.Lock()
	step := runtime.noResetSteps[authID]
	if step >= len(codexQuotaNoResetCadence) {
		step = len(codexQuotaNoResetCadence) - 1
	}
	delay := codexQuotaNoResetCadence[step]
	if runtime.noResetSteps[authID] < len(codexQuotaNoResetCadence)-1 {
		runtime.noResetSteps[authID]++
	}
	runtime.mu.Unlock()
	m.scheduleCodexQuotaRefreshWithRuntime(runtime, authID, delay)
}

func codexQuotaSnapshotAtOrAbove(snapshot codexoverdraft.QuotaSnapshot, threshold int64) bool {
	if threshold <= 0 {
		return false
	}
	for _, window := range snapshot.Windows {
		if window.UsedMicropct >= threshold && (window.ResetAt.IsZero() || window.ResetAt.After(snapshot.ObservedAt)) {
			return true
		}
	}
	return false
}

func maxQuotaRefreshDelay(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func (m *Manager) codexQuotaAuthIsArmed(authID string, threshold int64) bool {
	snapshot, ok := m.CodexQuotaSnapshot(authID)
	return ok && codexQuotaSnapshotAtOrAbove(snapshot, threshold)
}

func (m *Manager) applyActiveCodexQuotaSnapshot(snapshot codexoverdraft.QuotaSnapshot) {
	creditProtected := m.applyCodexCreditProtection(snapshot)
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil || !coordinator.Enabled() || snapshot.Inconsistent {
		return
	}
	if creditProtected {
		// Credits protection uses the ordinary account-level quota cooldown. It
		// must not leave an account in the drain or verification overlay.
		_ = coordinator.ConfirmRecovery(snapshot.AuthID, snapshot.AuthGeneration)
		return
	}
	thresholdReached := false
	validWindows := 0
	for _, window := range snapshot.Windows {
		if !window.ResetAt.IsZero() && !window.ResetAt.After(snapshot.ObservedAt) {
			continue
		}
		validWindows++
		if window.UsedMicropct >= coordinatorThreshold(coordinator) {
			thresholdReached = true
		}
	}
	if validWindows == 0 {
		return
	}
	if snapshot.Allowed && !snapshot.LimitReached && !thresholdReached {
		_ = coordinator.ConfirmRecovery(snapshot.AuthID, snapshot.AuthGeneration)
		return
	}
	_ = coordinator.UpdateInheritedInFlight(snapshot.AuthID, snapshot.AuthGeneration, m.codexLegacyInFlightCount(snapshot.AuthID))
	for _, window := range snapshot.Windows {
		if !window.ResetAt.IsZero() && !window.ResetAt.After(snapshot.ObservedAt) {
			continue
		}
		_ = coordinator.ObserveThreshold(snapshot.AuthID, snapshot.AuthGeneration, window.UsedMicropct)
	}
	_ = coordinator.ConfirmCalibration(snapshot.AuthID, snapshot.AuthGeneration)
}

func (m *Manager) applyCodexCreditProtection(snapshot codexoverdraft.QuotaSnapshot) bool {
	protected, _ := m.applyCodexCreditProtectionSnapshot(snapshot, true)
	return protected
}

func (m *Manager) applyCodexCreditProtectionSnapshot(snapshot codexoverdraft.QuotaSnapshot, persist bool) (bool, bool) {
	if m == nil || snapshot.AuthID == "" || snapshot.Inconsistent || snapshot.AuthGeneration != m.codexAuthGeneration(snapshot.AuthID) {
		return false, false
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	allowCreditSpend := cfg != nil && cfg.Codex.Overdraft.AllowCreditSpend
	now := snapshot.ObservedAt
	if now.IsZero() {
		now = time.Now()
	}
	windowFull := snapshot.LimitReached
	earliestReset := time.Time{}
	for _, window := range snapshot.Windows {
		if !window.ResetAt.IsZero() && !window.ResetAt.After(now) {
			continue
		}
		if window.UsedMicropct >= 100_000_000 {
			windowFull = true
		}
		if window.ResetAt.After(now) && (earliestReset.IsZero() || window.ResetAt.Before(earliestReset)) {
			earliestReset = window.ResetAt
		}
	}
	on := snapshot.HasCredits && !snapshot.CreditsUnlimited && windowFull && !allowCreditSpend
	if on && earliestReset.IsZero() {
		staleAfter := 10 * time.Minute
		if cfg != nil {
			if parsed, _, errDurations := cfg.Codex.Quota.Durations(); errDurations == nil {
				staleAfter = parsed
			}
		}
		earliestReset = now.Add(staleAfter)
	}
	changed := false
	m.mu.Lock()
	auth := m.auths[snapshot.AuthID]
	if auth != nil && !auth.Disabled && auth.Status != StatusDisabled && (auth.Quota.Reason == "" || auth.Quota.Reason == "credit_protection") {
		if on {
			auth.Unavailable = true
			auth.Status = StatusError
			auth.StatusMessage = "credit_protection"
			auth.NextRetryAfter = earliestReset
			auth.Quota.Exceeded = true
			auth.Quota.Reason = "credit_protection"
			auth.Quota.NextRecoverAt = earliestReset
			auth.UpdatedAt = now
			changed = true
		} else if auth.Quota.Reason == "credit_protection" {
			auth.Unavailable = false
			auth.NextRetryAfter = time.Time{}
			auth.Quota = QuotaState{}
			if auth.StatusMessage == "credit_protection" {
				auth.Status = StatusActive
				auth.StatusMessage = ""
			}
			auth.UpdatedAt = now
			if len(auth.ModelStates) > 0 {
				updateAggregatedAvailability(auth, now)
			}
			changed = true
		}
	}
	m.mu.Unlock()
	if changed && persist {
		m.persistCooldownStates(context.Background())
	}
	return on, changed
}

func (m *Manager) reevaluateCodexCreditProtection() bool {
	if m == nil {
		return false
	}
	m.codexQuotaMu.RLock()
	snapshots := make([]codexoverdraft.QuotaSnapshot, 0, len(m.codexQuotaSnapshots))
	for _, snapshot := range m.codexQuotaSnapshots {
		if snapshot.Source == codexoverdraft.QuotaSourceActive {
			snapshots = append(snapshots, snapshot)
		}
	}
	m.codexQuotaMu.RUnlock()
	changed := false
	for _, snapshot := range snapshots {
		_, snapshotChanged := m.applyCodexCreditProtectionSnapshot(snapshot, false)
		changed = changed || snapshotChanged
	}
	return changed
}

// CodexCreditProtectionCount reports account-level exclusions without exposing
// credential material.
func (m *Manager) CodexCreditProtectionCount() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, auth := range m.auths {
		if auth != nil && auth.Quota.Exceeded && auth.Quota.Reason == "credit_protection" {
			count++
		}
	}
	return count
}

func coordinatorThreshold(coordinator *codexoverdraft.Coordinator) int64 {
	return coordinator.ThresholdMicropct()
}

func (m *Manager) setCodexQuotaSnapshotLocked(authID string, snapshot codexoverdraft.QuotaSnapshot) {
	if m.codexQuotaSnapshots == nil {
		m.codexQuotaSnapshots = make(map[string]codexoverdraft.QuotaSnapshot)
	}
	if m.codexQuotaPrevSnapshots == nil {
		m.codexQuotaPrevSnapshots = make(map[string]codexoverdraft.QuotaSnapshot)
	}
	current, exists := m.codexQuotaSnapshots[authID]
	if exists && current.AuthGeneration == snapshot.AuthGeneration {
		m.codexQuotaPrevSnapshots[authID] = current
	} else {
		delete(m.codexQuotaPrevSnapshots, authID)
	}
	m.codexQuotaSnapshots[authID] = snapshot
}

func predictCodexQuotaRefreshDelay(prev, latest codexoverdraft.QuotaSnapshot, threshold int64, minInterval, staleAfter time.Duration) time.Duration {
	if minInterval <= 0 || staleAfter <= 0 || prev.AuthGeneration == 0 || prev.AuthGeneration != latest.AuthGeneration {
		return staleAfter
	}
	delta := latest.ObservedAt.Sub(prev.ObservedAt)
	if delta <= 0 {
		return staleAfter
	}
	previousByName := codexQuotaWindowsByName(prev)
	best := staleAfter
	found := false
	for _, window := range latest.Windows {
		previous, exists := previousByName[window.Name]
		if !exists || !previous.ResetAt.Equal(window.ResetAt) {
			continue
		}
		if window.UsedMicropct >= threshold {
			return minInterval
		}
		deltaUsed := window.UsedMicropct - previous.UsedMicropct
		remaining := threshold - window.UsedMicropct
		if deltaUsed <= 0 || remaining <= 0 {
			continue
		}
		deltaMillis := delta.Milliseconds()
		if deltaMillis <= 0 || remaining > math.MaxInt64/deltaMillis {
			continue
		}
		projectedMillis := remaining * deltaMillis / deltaUsed
		projectedMillis = projectedMillis * quotaRatePredictionSafetyFactorNumerator / quotaRatePredictionSafetyFactorDenominator
		candidate := time.Duration(projectedMillis) * time.Millisecond
		if candidate < minInterval {
			candidate = minInterval
		}
		if candidate > staleAfter {
			candidate = staleAfter
		}
		if !found || candidate < best {
			best = candidate
			found = true
		}
	}
	if !found {
		return staleAfter
	}
	return best
}

func closeQuotaResponse(response *http.Response) {
	if response == nil || response.Body == nil {
		return
	}
	if errClose := response.Body.Close(); errClose != nil {
		logEntryWithRequestID(nil).Warnf("failed to close active Codex quota response: %v", errClose)
	}
}

func randText() string {
	return fmt.Sprintf("%x", rand.Uint64())
}

func (m *Manager) markCodexQuotaUnauthorized(authID string) {
	if m == nil || authID == "" {
		return
	}
	resultErr := &Error{
		Code:       "unauthorized",
		Message:    "active Codex quota query remained unauthorized after credential refresh",
		HTTPStatus: http.StatusUnauthorized,
	}
	var snapshot *Auth
	m.mu.Lock()
	if auth := m.auths[authID]; auth != nil {
		applyAuthFailureState(auth, resultErr, nil, time.Now(), m.cooldownDisabledForAuth(auth))
		if errPersist := m.persist(context.Background(), auth); errPersist != nil {
			logEntryWithRequestID(nil).WithField("auth_id", authID).Warnf("failed to persist Codex quota authorization failure: %v", errPersist)
		}
		snapshot = auth.Clone()
	}
	m.mu.Unlock()
	if snapshot == nil {
		return
	}
	if m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	m.syncCodexOverdraftAuth(snapshot)
}

func (m *Manager) initializeCodexQuotaHealth() {
	if m == nil {
		return
	}
	m.codexQuotaHealthy.Store(true)
	m.codexQuotaHealthMu.Lock()
	m.codexQuotaLastError = ""
	m.codexQuotaHealthMu.Unlock()
}

func (m *Manager) recordCodexQuotaPersistenceResult(err error) {
	if m == nil || !m.codexQuotaEnabled.Load() {
		return
	}
	if err != nil {
		wasHealthy := m.codexQuotaHealthy.Swap(false)
		m.codexQuotaHealthMu.Lock()
		m.codexQuotaLastError = err.Error()
		m.codexQuotaHealthMu.Unlock()
		if wasHealthy {
			m.pauseCodexOverdraftForQuotaDependency()
		}
		return
	}
	if m.codexQuotaHealthy.Swap(true) {
		return
	}
	m.codexQuotaHealthMu.Lock()
	m.codexQuotaLastError = ""
	m.codexQuotaHealthMu.Unlock()
	m.resumeCodexOverdraftAfterQuotaRecovery()
}

func (m *Manager) pauseCodexOverdraftForQuotaDependency() {
	if m == nil {
		return
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil || !cfg.Codex.Overdraft.Enabled {
		return
	}
	disabled := cfg.CloneForRuntime()
	disabled.Codex.Overdraft.Enabled = false
	if errApply := m.applyCodexOverdraftConfig(disabled); errApply != nil {
		logEntryWithRequestID(nil).Warnf("failed to pause Codex overdraft after quota dependency fault: %v", errApply)
	}
}

func (m *Manager) resumeCodexOverdraftAfterQuotaRecovery() {
	if m == nil {
		return
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil || !cfg.Codex.Overdraft.Enabled || !cfg.Codex.Quota.Enabled || !cfg.Accounting.Enabled {
		return
	}
	service := m.AccountingService()
	if service == nil || !service.AdmissionHealthy() {
		return
	}
	if errApply := m.applyCodexOverdraftConfig(cfg); errApply != nil {
		logEntryWithRequestID(nil).Warnf("failed to rebuild Codex overdraft after quota recovery: %v", errApply)
	}
}

// CodexQuotaRuntimeHealth returns quota dependency health without exposing snapshots.
func (m *Manager) CodexQuotaRuntimeHealth() CodexQuotaRuntimeHealth {
	if m == nil {
		return CodexQuotaRuntimeHealth{Healthy: true}
	}
	enabled := m.codexQuotaEnabled.Load()
	if !enabled {
		return CodexQuotaRuntimeHealth{Healthy: true}
	}
	m.codexQuotaHealthMu.RLock()
	health := CodexQuotaRuntimeHealth{Enabled: true, Healthy: m.codexQuotaHealthy.Load(), LastError: m.codexQuotaLastError}
	m.codexQuotaHealthMu.RUnlock()
	return health
}

// StopCodexQuota stops active query scheduling and future retries.
func (m *Manager) StopCodexQuota() {
	if m == nil {
		return
	}
	m.codexQuotaRuntimeMu.Lock()
	runtime := m.codexQuotaRuntime
	m.codexQuotaRuntime = nil
	m.codexQuotaRuntimeMu.Unlock()
	if runtime != nil {
		runtime.cancel()
		runtime.mu.Lock()
		for authID, timer := range runtime.timers {
			if timer != nil {
				timer.Stop()
			}
			delete(runtime.timers, authID)
			delete(runtime.scheduledAt, authID)
		}
		runtime.mu.Unlock()
		if runtime.observationDone != nil {
			<-runtime.observationDone
		}
	}
	m.codexQuotaEnabled.Store(false)
	m.codexQuotaHealthy.Store(false)
	m.codexQuotaHealthMu.Lock()
	m.codexQuotaLastError = ""
	m.codexQuotaHealthMu.Unlock()
	m.codexQuotaStoreMu.Lock()
	store := m.codexQuotaStore
	m.codexQuotaStore = nil
	m.codexQuotaStorePath = ""
	m.codexQuotaStoreMu.Unlock()
	if store != nil {
		if errClose := store.Close(); errClose != nil {
			logEntryWithRequestID(nil).Warnf("failed to close Codex quota store: %v", errClose)
		}
	}
}
