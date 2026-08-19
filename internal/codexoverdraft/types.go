package codexoverdraft

import "time"

// PoolState is the scheduling overlay state for one Codex auth.
type PoolState string

const (
	StateNormal      PoolState = "NORMAL"
	StateCandidate   PoolState = "CANDIDATE"
	StateActiveDrain PoolState = "ACTIVE_DRAIN"
	StateVerifying   PoolState = "VERIFYING"
	StateExhausted   PoolState = "EXHAUSTED"
	StateDisabled    PoolState = "DISABLED"
)

// QuotaSource identifies how a quota observation was collected.
type QuotaSource string

const (
	QuotaSourcePassive QuotaSource = "passive_header"
	QuotaSourceActive  QuotaSource = "active_usage"
)

// WindowName is the upstream quota window name.
type WindowName string

const (
	WindowPrimary   WindowName = "primary"
	WindowSecondary WindowName = "secondary"
)

// WindowKind classifies quota windows by their reported length.
type WindowKind string

const (
	WindowShort   WindowKind = "short_window"
	WindowLong    WindowKind = "long_window"
	WindowUnknown WindowKind = "unknown"
)

// QuotaWindow is a fixed-point quota observation for one upstream window.
type QuotaWindow struct {
	Name          WindowName `json:"name"`
	Kind          WindowKind `json:"kind"`
	UsedMicropct  int64      `json:"used_micropct"`
	WindowSeconds int64      `json:"window_seconds,omitempty"`
	ResetAt       time.Time  `json:"reset_at,omitempty"`
}

// QuotaSnapshot is one immutable quota observation.
type QuotaSnapshot struct {
	AuthID               string        `json:"auth_id"`
	AuthGeneration       uint64        `json:"auth_generation"`
	Source               QuotaSource   `json:"source"`
	SourceAttemptID      string        `json:"source_attempt_id"`
	ObservedAt           time.Time     `json:"observed_at"`
	Sequence             uint64        `json:"observation_sequence"`
	Allowed              bool          `json:"allowed"`
	LimitReached         bool          `json:"limit_reached"`
	Inconsistent         bool          `json:"inconsistent,omitempty"`
	CalibrationAt        time.Time     `json:"calibration_at,omitempty"`
	Windows              []QuotaWindow `json:"windows"`
	SchemaVariant        string        `json:"schema_variant,omitempty"`
	UnknownSchema        bool          `json:"unknown_schema,omitempty"`
	UnknownKeys          []string      `json:"unknown_keys,omitempty"`
	HasCredits           bool          `json:"has_credits,omitempty"`
	CreditsUnlimited     bool          `json:"credits_unlimited,omitempty"`
	CreditsBalance       string        `json:"credits_balance,omitempty"`
	RateLimitReachedType string        `json:"rate_limit_reached_type,omitempty"`
	PlanType             string        `json:"plan_type,omitempty"`
}

// CoordinatorConfig is the normalized runtime configuration.
type CoordinatorConfig struct {
	Enabled                 bool  `json:"enabled"`
	ThresholdMicropct       int64 `json:"threshold_micropct"`
	MaxInFlight             int   `json:"max_in_flight"`
	PerAuthMaxInFlight      int   `json:"per_auth_max_in_flight"`
	ExhaustionProbeFailures int   `json:"exhaustion_probe_failures"`
}

// InFlightOccupancy is the global overdraft capacity snapshot.
type InFlightOccupancy struct {
	LeaseInFlight      int            `json:"lease_in_flight"`
	InheritedInFlight  int            `json:"inherited_in_flight"`
	TotalInFlight      int            `json:"total_in_flight"`
	MaxInFlight        int            `json:"max_in_flight"`
	PerAuthInFlight    map[string]int `json:"per_auth_in_flight"`
	PerAuthMaxInFlight map[string]int `json:"per_auth_max_in_flight"`
}

// Record is the durable pool state for one auth lifecycle.
type Record struct {
	AuthID                string    `json:"auth_id"`
	AuthGeneration        uint64    `json:"auth_generation"`
	State                 PoolState `json:"state"`
	StateEpoch            uint64    `json:"auth_state_epoch"`
	CandidateEnteredAt    time.Time `json:"candidate_entered_at,omitempty"`
	CandidateSequence     uint64    `json:"candidate_sequence,omitempty"`
	CandidateUsedMicropct int64     `json:"candidate_used_micropct,omitempty"`
	DrainCycleID          string    `json:"drain_cycle_id,omitempty"`
	BusinessAttempts      int64     `json:"overdraft_business_attempts,omitempty"`
	EnteredAlreadyFull    bool      `json:"entered_already_full,omitempty"`
	LateAdmissionAttempts int       `json:"late_admission_attempts,omitempty"`
	ProbeFailures         int       `json:"probe_failures,omitempty"`
	AdmissionBlocks       int       `json:"admission_blocks,omitempty"`
	ExhaustedAt           time.Time `json:"exhausted_at,omitempty"`
	Disabled              bool      `json:"disabled,omitempty"`
	InheritedInFlight     int       `json:"inherited_in_flight,omitempty"`
	InFlight              int       `json:"in_flight,omitempty"`
	PerAuthMaxInFlight    int       `json:"per_auth_max_in_flight,omitempty"`
	VerificationPaused    bool      `json:"verification_paused,omitempty"`
	VerificationPauseCode string    `json:"verification_pause_code,omitempty"`
	CalibrationRequired   bool      `json:"calibration_required,omitempty"`
}

// CandidateQueueMetrics exposes bounded scheduling latency without auth data.
type CandidateQueueMetrics struct {
	Length     int           `json:"length"`
	HeadWait   time.Duration `json:"head_wait"`
	HeadWaitMS int64         `json:"head_wait_ms"`
}

// LeaseKind distinguishes business admission from exhaustion probes.
type LeaseKind string

const (
	LeaseBusiness LeaseKind = "business"
	LeaseProbe    LeaseKind = "exhaustion_probe"
)

// LeasePhase tracks the atomic send boundary.
type LeasePhase string

const (
	LeaseAcquired LeasePhase = "acquired"
	LeaseSending  LeasePhase = "sending"
	LeaseReleased LeasePhase = "released"
)

// DrainLease is an opaque admission token passed to the executor.
type DrainLease struct {
	ID               string     `json:"id"`
	AuthID           string     `json:"auth_id"`
	AuthGeneration   uint64     `json:"auth_generation"`
	DrainCycleID     string     `json:"drain_cycle_id"`
	CoordinatorEpoch uint64     `json:"coordinator_epoch"`
	AuthStateEpoch   uint64     `json:"auth_state_epoch"`
	DispatchID       string     `json:"dispatch_id"`
	Kind             LeaseKind  `json:"kind"`
	Phase            LeasePhase `json:"phase"`
	AcquiredAt       time.Time  `json:"acquired_at"`
}

// ProbeOutcome classifies terminal results from dedicated exhaustion probes.
type ProbeOutcome int

const (
	ProbeIndeterminate ProbeOutcome = iota
	ProbeUsageLimit
	ProbeSuccess
)

// Clock allows deterministic coordinator tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
