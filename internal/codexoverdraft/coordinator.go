package codexoverdraft

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	// ErrNoActiveOwner indicates that no auth currently owns overdraft admission.
	ErrNoActiveOwner = errors.New("no active overdraft owner")
	// ErrCapacity indicates that the configured concurrent lease ceiling is full.
	ErrCapacity = errors.New("overdraft capacity reached")
	// ErrLateAdmissionExhausted indicates the drain owner used its full
	// late-admission budget while still blocked, so it cannot admit more
	// speculative traffic and should be retired for a fresh candidate.
	ErrLateAdmissionExhausted = errors.New("overdraft late-admission budget exhausted")
	// ErrStaleLease indicates that a lease failed a generation or epoch fence.
	ErrStaleLease = errors.New("stale overdraft lease")
	// ErrInvalidState indicates that an operation does not apply to the current state.
	ErrInvalidState = errors.New("invalid overdraft state")
)

// Coordinator serializes all pool state transitions and lease admission.
type Coordinator struct {
	mu               sync.RWMutex
	config           CoordinatorConfig
	durableConfig    CoordinatorConfig
	store            Store
	clock            Clock
	state            PersistentState
	durableState     PersistentState
	leases           map[string]*DrainLease
	leaseInFlight    int
	perAuthInFlight  map[string]int
	retiredInherited map[authLifecycle]int
	healthy          bool
	lastErr          string
}

type authLifecycle struct {
	authID     string
	generation uint64
}

// NewCoordinator loads durable state, increments the process epoch, and applies config.
func NewCoordinator(config CoordinatorConfig, store Store, clock Clock) (*Coordinator, error) {
	if errValidate := validateCoordinatorConfig(config); errValidate != nil {
		return nil, errValidate
	}
	if clock == nil {
		clock = systemClock{}
	}
	state := PersistentState{SchemaVersion: 1, Records: make(map[string]Record)}
	if store != nil {
		loaded, errLoad := store.Load()
		if errLoad != nil {
			return nil, fmt.Errorf("load overdraft state: %w", errLoad)
		}
		if loaded.Records != nil {
			state = loaded
		}
	}
	state.SchemaVersion = 1
	state.CoordinatorEpoch++
	if state.Records == nil {
		state.Records = make(map[string]Record)
	}
	if !config.Enabled {
		clearOverlay(&state)
	}
	coordinator := &Coordinator{
		config:           config,
		durableConfig:    config,
		store:            store,
		clock:            clock,
		state:            state,
		durableState:     clonePersistentState(state),
		leases:           make(map[string]*DrainLease),
		perAuthInFlight:  make(map[string]int),
		retiredInherited: make(map[authLifecycle]int),
		healthy:          true,
	}
	if config.Enabled {
		for authID, record := range state.Records {
			if record.State == StateVerifying {
				record.State = StateActiveDrain
				record.StateEpoch++
				record.VerificationPaused = false
				record.VerificationPauseCode = ""
			}
			if record.State != StateNormal && record.State != StateDisabled {
				record.CalibrationRequired = true
				state.Records[authID] = record
			}
		}
	}
	if errSave := coordinator.persistLocked(); errSave != nil {
		return nil, errSave
	}
	return coordinator, nil
}

func validateCoordinatorConfig(config CoordinatorConfig) error {
	if config.ThresholdMicropct <= 0 || config.ThresholdMicropct > 100_000_000 {
		return fmt.Errorf("threshold micropct must be between 1 and 100000000")
	}
	if config.MaxInFlight <= 0 {
		return fmt.Errorf("max in flight must be positive")
	}
	if config.PerAuthMaxInFlight < 0 || config.PerAuthMaxInFlight > config.MaxInFlight {
		return fmt.Errorf("per-auth max in flight must be zero or no greater than max in flight")
	}
	if config.ExhaustionProbeFailures <= 0 {
		return fmt.Errorf("exhaustion probe failures must be positive")
	}
	return nil
}

// RegisterAuth creates or refreshes the lifecycle record for an eligible auth.
func (c *Coordinator) RegisterAuth(authID string, generation uint64, inheritedInFlight int) error {
	if authID == "" || generation == 0 || inheritedInFlight < 0 {
		return fmt.Errorf("invalid auth registration")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled {
		return nil
	}
	record, exists := c.state.Records[authID]
	if !exists || record.AuthGeneration != generation {
		if c.state.Owner == authID {
			c.state.Owner = ""
		}
		record = Record{AuthID: authID, AuthGeneration: generation, State: StateNormal, StateEpoch: 1, InheritedInFlight: inheritedInFlight}
	} else {
		if record.InheritedInFlight == inheritedInFlight {
			return nil
		}
		record.InheritedInFlight = inheritedInFlight
	}
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// SetAuthMaxInFlight applies a lifecycle-fenced per-auth override. Zero inherits
// the global per-auth setting, which itself may inherit the global overlay limit.
func (c *Coordinator) SetAuthMaxInFlight(authID string, generation uint64, limit int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit < 0 || limit > c.config.MaxInFlight {
		return fmt.Errorf("per-auth max in flight must be zero or no greater than max in flight")
	}
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok || record.PerAuthMaxInFlight == limit {
		return nil
	}
	record.PerAuthMaxInFlight = limit
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// RemoveAuth clears runtime pool state while leaving accounting history untouched.
func (c *Coordinator) RemoveAuth(authID string, generation uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, exists := c.state.Records[authID]
	if !exists || record.AuthGeneration != generation {
		return nil
	}
	delete(c.state.Records, authID)
	if c.state.Owner == authID {
		c.state.Owner = ""
	}
	c.promoteLocked()
	if errPersist := c.persistLocked(); errPersist != nil {
		return errPersist
	}
	if record.InheritedInFlight > 0 {
		c.retiredInherited[authLifecycle{authID: authID, generation: generation}] = record.InheritedInFlight
	}
	return nil
}

// ObserveThreshold moves a normal auth into FIFO admission when a window crosses the threshold.
func (c *Coordinator) ObserveThreshold(authID string, generation uint64, usedMicropct int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled || usedMicropct < c.config.ThresholdMicropct {
		return nil
	}
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok || record.Disabled || record.State != StateNormal {
		return nil
	}
	enteredAt := c.clock.Now()
	if enteredAt.Before(c.state.LastCandidateEnteredAt) {
		enteredAt = c.state.LastCandidateEnteredAt
	}
	c.state.CandidateSequence++
	c.state.LastCandidateEnteredAt = enteredAt
	record.State = StateCandidate
	record.StateEpoch++
	record.CandidateEnteredAt = enteredAt
	record.CandidateSequence = c.state.CandidateSequence
	record.CandidateUsedMicropct = usedMicropct
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// UpdateInheritedInFlight updates pre-promotion request occupancy.
func (c *Coordinator) UpdateInheritedInFlight(authID string, generation uint64, count int) error {
	if count < 0 {
		return fmt.Errorf("inherited in-flight count must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok {
		return nil
	}
	record.InheritedInFlight = count
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// UpdateRetiredInheritedInFlight tracks pre-promotion requests after their Auth was removed.
func (c *Coordinator) UpdateRetiredInheritedInFlight(authID string, generation uint64, count int) error {
	if authID == "" || generation == 0 || count < 0 {
		return ErrInvalidState
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := authLifecycle{authID: authID, generation: generation}
	if _, exists := c.retiredInherited[key]; !exists {
		return nil
	}
	if count == 0 {
		delete(c.retiredInherited, key)
		return nil
	}
	c.retiredInherited[key] = count
	return nil
}

// SetDisabled applies the higher-priority disabled overlay state.
func (c *Coordinator) SetDisabled(authID string, generation uint64, disabled bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok {
		return nil
	}
	if disabled == record.Disabled {
		return nil
	}
	if disabled {
		record.Disabled = true
		record.State = StateDisabled
		record.StateEpoch++
		clearRecordCycle(&record)
		if c.state.Owner == authID {
			c.state.Owner = ""
		}
	} else if record.Disabled {
		record = Record{AuthID: authID, AuthGeneration: generation, State: StateNormal, StateEpoch: record.StateEpoch + 1, InheritedInFlight: record.InheritedInFlight, PerAuthMaxInFlight: record.PerAuthMaxInFlight}
	}
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// AcquireBusiness reserves one permit from the current ACTIVE_DRAIN owner.
func (c *Coordinator) AcquireBusiness(dispatchID string) (*DrainLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled || !c.healthy || c.state.Owner == "" {
		return nil, ErrNoActiveOwner
	}
	record := c.state.Records[c.state.Owner]
	if record.State != StateActiveDrain || record.Disabled || record.CalibrationRequired {
		return nil, ErrNoActiveOwner
	}
	return c.acquireLocked(record, dispatchID, LeaseBusiness)
}

// AcquireLateBusiness admits a request whose stale usage-limit cooldown belongs
// to the current drain owner. The per-cycle ceiling bounds speculative sends.
func (c *Coordinator) AcquireLateBusiness(dispatchID string, maxAttempts int) (*DrainLease, error) {
	if maxAttempts <= 0 {
		return nil, ErrInvalidState
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled || !c.healthy || c.state.Owner == "" {
		return nil, ErrNoActiveOwner
	}
	record := c.state.Records[c.state.Owner]
	if record.State != StateActiveDrain || record.Disabled || record.CalibrationRequired {
		return nil, ErrNoActiveOwner
	}
	if record.LateAdmissionAttempts >= maxAttempts {
		return nil, ErrLateAdmissionExhausted
	}
	lease, errAcquire := c.acquireLocked(record, dispatchID, LeaseBusiness)
	if errAcquire != nil {
		return nil, errAcquire
	}
	record = c.state.Records[record.AuthID]
	record.LateAdmissionAttempts++
	c.state.Records[record.AuthID] = record
	if errPersist := c.persistLocked(); errPersist != nil {
		c.releaseLocked(lease)
		return nil, errPersist
	}
	return lease, nil
}

// RecordAdmissionBlocked counts one request that the drain owner could not admit
// for a reason that leaves it unable to serve at all (auth-level block, no
// eligible late admission, spent late-admission budget). Exhaustion is otherwise
// only reached by counting usage-limit results from requests that actually
// reached upstream, so an owner blocked before dispatch would keep the drain slot
// forever while every request falls back to the ordinary pool. Reaching the same
// count as the usage-limit threshold retires the owner and promotes the next
// candidate. Any successful admission resets the streak.
func (c *Coordinator) RecordAdmissionBlocked(authID string) (bool, error) {
	if authID == "" {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled || c.state.Owner != authID {
		return false, nil
	}
	record, ok := c.state.Records[authID]
	if !ok || record.State != StateActiveDrain || record.Disabled || record.CalibrationRequired {
		return false, nil
	}
	record.AdmissionBlocks++
	if record.AdmissionBlocks < c.config.ExhaustionProbeFailures {
		c.state.Records[authID] = record
		return false, c.persistLocked()
	}
	record.State = StateExhausted
	record.StateEpoch++
	record.ProbeFailures = c.config.ExhaustionProbeFailures
	record.ExhaustedAt = c.clock.Now()
	c.state.Records[authID] = record
	c.state.Owner = ""
	c.promoteLocked()
	return true, c.persistLocked()
}

// AcquireProbe reserves the single serial probe permit for the VERIFYING owner.
func (c *Coordinator) AcquireProbe(dispatchID string) (*DrainLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Enabled || !c.healthy || c.state.Owner == "" {
		return nil, ErrNoActiveOwner
	}
	record := c.state.Records[c.state.Owner]
	if record.State != StateVerifying || record.Disabled || record.VerificationPaused || record.CalibrationRequired {
		return nil, ErrInvalidState
	}
	for _, lease := range c.leases {
		if lease.Kind == LeaseProbe && lease.Phase != LeaseReleased && lease.AuthID == record.AuthID {
			return nil, ErrCapacity
		}
	}
	return c.acquireLocked(record, dispatchID, LeaseProbe)
}

func (c *Coordinator) acquireLocked(record Record, dispatchID string, kind LeaseKind) (*DrainLease, error) {
	if c.leaseInFlight+c.inheritedInFlightLocked() >= c.config.MaxInFlight {
		return nil, ErrCapacity
	}
	if kind == LeaseBusiness && c.perAuthInFlight[record.AuthID]+record.InheritedInFlight >= c.effectiveAuthMaxInFlightLocked(record) {
		return nil, ErrCapacity
	}
	lease := &DrainLease{
		ID:               randomID("lease_"),
		AuthID:           record.AuthID,
		AuthGeneration:   record.AuthGeneration,
		DrainCycleID:     record.DrainCycleID,
		CoordinatorEpoch: c.state.CoordinatorEpoch,
		AuthStateEpoch:   record.StateEpoch,
		DispatchID:       dispatchID,
		Kind:             kind,
		Phase:            LeaseAcquired,
		AcquiredAt:       c.clock.Now(),
	}
	c.leases[lease.ID] = lease
	c.leaseInFlight++
	c.perAuthInFlight[record.AuthID]++
	if kind == LeaseBusiness {
		record.BusinessAttempts++
		record.AdmissionBlocks = 0
		c.state.Records[record.AuthID] = record
	}
	return lease, nil
}

// BeginSend linearizes actual upstream dispatch against config and state fences.
func (c *Coordinator) BeginSend(lease *DrainLease) error {
	if lease == nil {
		return ErrStaleLease
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || current.Phase != LeaseAcquired && current.Phase != LeaseSending || !c.leaseFenceValidLocked(current) {
		if ok && current.Phase != LeaseReleased {
			c.releaseLocked(current)
		}
		return ErrStaleLease
	}
	if current.Phase == LeaseAcquired {
		current.Phase = LeaseSending
	}
	lease.Phase = LeaseSending
	return nil
}

func (c *Coordinator) leaseFenceValidLocked(lease *DrainLease) bool {
	if !c.config.Enabled || !c.healthy || lease.CoordinatorEpoch != c.state.CoordinatorEpoch {
		return false
	}
	record, ok := c.currentRecordLocked(lease.AuthID, lease.AuthGeneration)
	if !ok || record.StateEpoch != lease.AuthStateEpoch || record.DrainCycleID != lease.DrainCycleID || c.state.Owner != lease.AuthID {
		return false
	}
	if lease.Kind == LeaseBusiness {
		return record.State == StateActiveDrain
	}
	return record.State == StateVerifying
}

// Release releases a lease exactly once.
func (c *Coordinator) Release(lease *DrainLease) error {
	if lease == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || current.Phase == LeaseReleased {
		lease.Phase = LeaseReleased
		return nil
	}
	c.releaseLocked(current)
	lease.Phase = LeaseReleased
	return nil
}

func (c *Coordinator) releaseLocked(lease *DrainLease) {
	lease.Phase = LeaseReleased
	delete(c.leases, lease.ID)
	if c.leaseInFlight > 0 {
		c.leaseInFlight--
	}
	if c.perAuthInFlight[lease.AuthID] <= 1 {
		delete(c.perAuthInFlight, lease.AuthID)
	} else {
		c.perAuthInFlight[lease.AuthID]--
	}
}

// BusinessUsageLimit counts one typed usage-limit 429 against the ACTIVE_DRAIN owner.
// Ten consecutive failures mark the owner EXHAUSTED. Other outcomes must call
// BusinessCompleted so the streak does not survive a later success.
func (c *Coordinator) BusinessUsageLimit(lease *DrainLease) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || current.Phase != LeaseSending || current.Kind != LeaseBusiness || !c.leaseFenceValidLocked(current) {
		return ErrStaleLease
	}
	record := c.state.Records[current.AuthID]
	if record.BusinessAttempts == 1 {
		record.EnteredAlreadyFull = true
	}
	record.ProbeFailures++
	if record.ProbeFailures >= c.config.ExhaustionProbeFailures {
		record.State = StateExhausted
		record.StateEpoch++
		record.ExhaustedAt = c.clock.Now()
		c.state.Owner = ""
	}
	c.state.Records[current.AuthID] = record
	if record.State == StateExhausted {
		c.promoteLocked()
	}
	return c.persistLocked()
}

// BusinessCompleted clears the consecutive usage-limit streak after a non-limit result.
func (c *Coordinator) BusinessCompleted(lease *DrainLease) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || current.Phase != LeaseSending || current.Kind != LeaseBusiness || !c.leaseFenceValidLocked(current) {
		return ErrStaleLease
	}
	record := c.state.Records[current.AuthID]
	if record.State != StateActiveDrain || record.ProbeFailures == 0 {
		return nil
	}
	record.ProbeFailures = 0
	c.state.Records[current.AuthID] = record
	return c.persistLocked()
}

// ProbeResult applies a dedicated terminal probe result.
func (c *Coordinator) ProbeResult(lease *DrainLease, outcome ProbeOutcome) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || current.Phase != LeaseSending || current.Kind != LeaseProbe || !c.leaseFenceValidLocked(current) {
		return ErrStaleLease
	}
	record := c.state.Records[current.AuthID]
	switch outcome {
	case ProbeUsageLimit:
		record.ProbeFailures++
		if record.ProbeFailures >= c.config.ExhaustionProbeFailures {
			record.State = StateExhausted
			record.StateEpoch++
			record.ExhaustedAt = c.clock.Now()
			c.state.Owner = ""
		}
	case ProbeSuccess:
		record.State = StateActiveDrain
		record.StateEpoch++
		record.ProbeFailures = 0
	case ProbeIndeterminate:
		// Indeterminate failures preserve the strict usage-limit streak.
	default:
		return fmt.Errorf("unknown probe outcome")
	}
	c.state.Records[current.AuthID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// PauseVerification records an operator-visible functional probe fault.
func (c *Coordinator) PauseVerification(authID string, generation uint64, code string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok || record.State != StateVerifying {
		return ErrInvalidState
	}
	record.VerificationPaused = true
	record.VerificationPauseCode = code
	c.state.Records[authID] = record
	return c.persistLocked()
}

// ConfirmRecovery always returns a recovered auth to NORMAL.
func (c *Coordinator) ConfirmRecovery(authID string, generation uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok || record.Disabled {
		return nil
	}
	if record.State == StateNormal {
		return nil
	}
	if c.state.Owner == authID {
		c.state.Owner = ""
	}
	record = Record{AuthID: authID, AuthGeneration: generation, State: StateNormal, StateEpoch: record.StateEpoch + 1, InheritedInFlight: record.InheritedInFlight, PerAuthMaxInFlight: record.PerAuthMaxInFlight}
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// ConfirmCalibration reopens a persisted active/verifying owner after an active quota query.
func (c *Coordinator) ConfirmCalibration(authID string, generation uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok || !record.CalibrationRequired {
		return nil
	}
	if record.State != StateCandidate && record.State != StateActiveDrain && record.State != StateVerifying && record.State != StateExhausted {
		return ErrInvalidState
	}
	record.CalibrationRequired = false
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// ThresholdMicropct returns the configured fixed-point admission threshold.
func (c *Coordinator) ThresholdMicropct() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.ThresholdMicropct
}

// ForceExhausted is used by lifecycle adapters after an already-confirmed persisted streak.
func (c *Coordinator) ForceExhausted(authID string, generation uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.currentRecordLocked(authID, generation)
	if !ok {
		return nil
	}
	record.State = StateExhausted
	record.StateEpoch++
	record.ProbeFailures = c.config.ExhaustionProbeFailures
	record.ExhaustedAt = c.clock.Now()
	if c.state.Owner == authID {
		c.state.Owner = ""
	}
	c.state.Records[authID] = record
	c.promoteLocked()
	return c.persistLocked()
}

// ApplyConfig serializes revision changes and invalidates old leases through the coordinator epoch.
func (c *Coordinator) ApplyConfig(revision uint64, config CoordinatorConfig) error {
	if errValidate := validateCoordinatorConfig(config); errValidate != nil {
		return errValidate
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if revision <= c.state.ConfigRevision {
		return nil
	}
	wasHealthy := c.healthy
	c.state.ConfigRevision = revision
	if wasHealthy && config == c.config {
		return c.persistLocked()
	}
	c.state.CoordinatorEpoch++
	c.config = config
	if !config.Enabled {
		clearOverlay(&c.state)
	} else if !wasHealthy {
		clearOverlay(&c.state)
	}
	desiredState := clonePersistentState(c.state)
	errPersist := c.persistLocked()
	if errPersist != nil && !config.Enabled {
		// Disabling is fail-closed: the configuration source is the restart truth,
		// while the in-memory gate and overlay must close immediately.
		c.config = config
		c.state = desiredState
	}
	return errPersist
}

// Enabled reports whether the overlay is admitting work.
func (c *Coordinator) Enabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Enabled && c.healthy
}

// Health reports whether durable state transitions are currently available.
func (c *Coordinator) Health() (bool, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.healthy, c.lastErr
}

// InFlightOccupancy returns the global lease and inherited request occupancy.
func (c *Coordinator) InFlightOccupancy() InFlightOccupancy {
	c.mu.RLock()
	defer c.mu.RUnlock()
	inherited := c.inheritedInFlightLocked()
	perAuth := make(map[string]int, len(c.state.Records)+len(c.retiredInherited))
	perAuthMax := make(map[string]int, len(c.state.Records))
	for authID, record := range c.state.Records {
		perAuth[authID] = c.perAuthInFlight[authID] + record.InheritedInFlight
		perAuthMax[authID] = c.effectiveAuthMaxInFlightLocked(record)
	}
	for lifecycle, count := range c.retiredInherited {
		perAuth[lifecycle.authID] += count
	}
	return InFlightOccupancy{
		LeaseInFlight:      c.leaseInFlight,
		InheritedInFlight:  inherited,
		TotalInFlight:      c.leaseInFlight + inherited,
		MaxInFlight:        c.config.MaxInFlight,
		PerAuthInFlight:    perAuth,
		PerAuthMaxInFlight: perAuthMax,
	}
}

// Epoch returns the current coordinator fence.
func (c *Coordinator) Epoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.CoordinatorEpoch
}

// Owner returns the active drain auth ID.
func (c *Coordinator) Owner() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.Owner
}

// Record returns a copy of one auth record.
func (c *Coordinator) Record(authID string) Record {
	c.mu.RLock()
	defer c.mu.RUnlock()
	record := c.state.Records[authID]
	return c.observableRecordLocked(record)
}

// Records returns a copy of all pool records.
func (c *Coordinator) Records() map[string]Record {
	c.mu.RLock()
	defer c.mu.RUnlock()
	records := make(map[string]Record, len(c.state.Records))
	for authID, record := range c.state.Records {
		records[authID] = c.observableRecordLocked(record)
	}
	return records
}

// Candidates returns the strict FIFO candidate list.
func (c *Coordinator) Candidates() []Record {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return candidatesFromRecords(c.state.Records)
}

// CandidateQueueMetrics returns queue depth and the current head wait.
func (c *Coordinator) CandidateQueueMetrics() CandidateQueueMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	candidates := candidatesFromRecords(c.state.Records)
	metrics := CandidateQueueMetrics{Length: len(candidates)}
	if len(candidates) > 0 && !candidates[0].CandidateEnteredAt.IsZero() {
		metrics.HeadWait = c.clock.Now().Sub(candidates[0].CandidateEnteredAt)
		if metrics.HeadWait < 0 {
			metrics.HeadWait = 0
		}
		metrics.HeadWaitMS = metrics.HeadWait.Milliseconds()
	}
	return metrics
}

func (c *Coordinator) currentRecordLocked(authID string, generation uint64) (Record, bool) {
	record, ok := c.state.Records[authID]
	return record, ok && record.AuthGeneration == generation
}

func (c *Coordinator) promoteLocked() {
	if !c.config.Enabled || c.state.Owner != "" {
		return
	}
	for _, candidate := range candidatesFromRecords(c.state.Records) {
		// A candidate that still needs calibration must not block calibrated
		// candidates queued behind it: restarts mark every persisted candidate
		// as calibration-required, and one auth with persistently failing
		// quota queries would otherwise stall the whole pool indefinitely.
		if candidate.CalibrationRequired {
			continue
		}
		if candidate.InheritedInFlight > c.config.MaxInFlight || candidate.InheritedInFlight > c.effectiveAuthMaxInFlightLocked(candidate) {
			return
		}
		candidate.State = StateActiveDrain
		candidate.StateEpoch++
		candidate.DrainCycleID = randomID("drain_")
		candidate.ProbeFailures = 0
		c.state.Records[candidate.AuthID] = candidate
		c.state.Owner = candidate.AuthID
		return
	}
}

func (c *Coordinator) effectiveAuthMaxInFlightLocked(record Record) int {
	limit := record.PerAuthMaxInFlight
	if limit <= 0 || limit > c.config.MaxInFlight {
		limit = c.config.PerAuthMaxInFlight
	}
	if limit <= 0 || limit > c.config.MaxInFlight {
		limit = c.config.MaxInFlight
	}
	return limit
}

func (c *Coordinator) observableRecordLocked(record Record) Record {
	if record.AuthID == "" {
		return record
	}
	record.InFlight = c.perAuthInFlight[record.AuthID] + record.InheritedInFlight
	record.PerAuthMaxInFlight = c.effectiveAuthMaxInFlightLocked(record)
	return record
}

func (c *Coordinator) inheritedInFlightLocked() int {
	total := 0
	for _, record := range c.state.Records {
		if record.State != StateNormal {
			total += record.InheritedInFlight
		}
	}
	for _, count := range c.retiredInherited {
		total += count
	}
	return total
}

func candidatesFromRecords(records map[string]Record) []Record {
	candidates := make([]Record, 0)
	for _, record := range records {
		if record.State == StateCandidate && !record.Disabled {
			candidates = append(candidates, record)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CandidateEnteredAt.Equal(candidates[j].CandidateEnteredAt) {
			return candidates[i].CandidateSequence < candidates[j].CandidateSequence
		}
		return candidates[i].CandidateEnteredAt.Before(candidates[j].CandidateEnteredAt)
	})
	return candidates
}

func clearRecordCycle(record *Record) {
	record.CandidateEnteredAt = time.Time{}
	record.CandidateSequence = 0
	record.CandidateUsedMicropct = 0
	record.DrainCycleID = ""
	record.BusinessAttempts = 0
	record.EnteredAlreadyFull = false
	record.LateAdmissionAttempts = 0
	record.ProbeFailures = 0
	record.AdmissionBlocks = 0
	record.ExhaustedAt = time.Time{}
	record.VerificationPaused = false
	record.VerificationPauseCode = ""
}

func clearOverlay(state *PersistentState) {
	state.Owner = ""
	state.Records = make(map[string]Record)
	state.CandidateSequence = 0
	state.LastCandidateEnteredAt = time.Time{}
}

func (c *Coordinator) persistLocked() error {
	if c.store == nil {
		c.durableState = clonePersistentState(c.state)
		c.durableConfig = c.config
		c.healthy = true
		c.lastErr = ""
		return nil
	}
	if errSave := c.store.Save(c.state); errSave != nil {
		c.state = clonePersistentState(c.durableState)
		c.config = c.durableConfig
		c.healthy = false
		c.lastErr = errSave.Error()
		return fmt.Errorf("persist overdraft state: %w", errSave)
	}
	c.durableState = clonePersistentState(c.state)
	c.durableConfig = c.config
	c.healthy = true
	c.lastErr = ""
	return nil
}

func clonePersistentState(state PersistentState) PersistentState {
	cloned := state
	cloned.Records = make(map[string]Record, len(state.Records))
	for authID, record := range state.Records {
		cloned.Records[authID] = record
	}
	return cloned
}

func randomID(prefix string) string {
	return prefix + rand.Text()
}
