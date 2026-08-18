package accounting

import (
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

const (
	FailureMissingUsage  = "missing_usage"
	FailureIndeterminate = "delivery_indeterminate"
	FailureNotDispatched = "not_dispatched"
	FailureRequestError  = "request_error"
	CostStatusPriced     = "PRICED"
	CostStatusUnpriced   = "UNPRICED"
	CostStatusPaused     = "PAUSED"
)

// AttemptIntent is persisted before a real upstream write.
type AttemptIntent struct {
	SchemaVersion      int       `json:"schema_version"`
	UpstreamAttemptID  string    `json:"upstream_attempt_id"`
	RequestID          string    `json:"request_id"`
	DispatchID         string    `json:"dispatch_id,omitempty"`
	AuthID             string    `json:"auth_id"`
	AuthGeneration     uint64    `json:"auth_generation"`
	Provider           string    `json:"provider"`
	RequestedModel     string    `json:"requested_model"`
	ResolvedModel      string    `json:"resolved_model"`
	RequestServiceTier string    `json:"request_service_tier"`
	RequestedAt        time.Time `json:"requested_at"`
	DrainMode          string    `json:"drain_mode"`
	DrainCycleID       string    `json:"drain_cycle_id,omitempty"`
	DrainRequestKind   string    `json:"drain_request_kind,omitempty"`
	ConsumptionClass   string    `json:"consumption_class,omitempty"`
	CoordinatorEpoch   uint64    `json:"coordinator_epoch,omitempty"`
	AuthStateEpoch     uint64    `json:"auth_state_epoch,omitempty"`
}

// UsageEvent is an immutable terminal usage part.
type UsageEvent struct {
	EventID           string           `json:"event_id"`
	SchemaVersion     int              `json:"schema_version"`
	UpstreamAttemptID string           `json:"upstream_attempt_id"`
	RequestID         string           `json:"request_id"`
	AuthID            string           `json:"auth_id"`
	Provider          string           `json:"provider"`
	Model             string           `json:"model"`
	Tier              string           `json:"tier"`
	DrainMode         string           `json:"drain_mode"`
	DrainCycleID      string           `json:"drain_cycle_id,omitempty"`
	DrainRequestKind  string           `json:"drain_request_kind,omitempty"`
	ConsumptionClass  string           `json:"consumption_class,omitempty"`
	UsagePart         string           `json:"usage_part"`
	UsageReported     bool             `json:"usage_reported"`
	Failed            bool             `json:"failed"`
	FailureClass      string           `json:"failure_class,omitempty"`
	Detail            coreusage.Detail `json:"detail"`
	RequestedAt       time.Time        `json:"requested_at"`
	ObservedAt        time.Time        `json:"observed_at"`
}

// Aggregate summarizes immutable events for one filter.
type Aggregate struct {
	AuthID                       string `json:"auth_id,omitempty"`
	DrainCycleID                 string `json:"drain_cycle_id,omitempty"`
	BusinessAttempts             int64  `json:"business_attempts"`
	BusinessSuccesses            int64  `json:"business_successes"`
	BusinessFailures             int64  `json:"business_failures"`
	BusinessTotalTokens          int64  `json:"business_total_tokens"`
	BusinessCostNanoUSD          int64  `json:"business_cost_nano_usd"`
	NormalBusinessAttempts       int64  `json:"normal_business_attempts"`
	NormalBusinessSuccesses      int64  `json:"normal_business_successes"`
	NormalBusinessFailures       int64  `json:"normal_business_failures"`
	NormalBusinessTotalTokens    int64  `json:"normal_business_total_tokens"`
	NormalBusinessCostNanoUSD    int64  `json:"normal_business_cost_nano_usd"`
	OverdraftBusinessAttempts    int64  `json:"overdraft_business_attempts"`
	OverdraftBusinessSuccesses   int64  `json:"overdraft_business_successes"`
	OverdraftBusinessFailures    int64  `json:"overdraft_business_failures"`
	OverdraftBusinessTotalTokens int64  `json:"overdraft_business_total_tokens"`
	OverdraftBusinessCostNanoUSD int64  `json:"overdraft_business_cost_nano_usd"`
	ProbeAttempts                int64  `json:"probe_attempts"`
	ProbeSuccesses               int64  `json:"probe_successes"`
	ProbeFailures                int64  `json:"probe_failures"`
	ProbeTotalTokens             int64  `json:"probe_total_tokens"`
	ProbeCostNanoUSD             int64  `json:"probe_cost_nano_usd"`
	IndeterminateAttempts        int64  `json:"indeterminate_attempts"`
	NotDispatched                int64  `json:"not_dispatched"`
}

// AttemptClosure records a terminal state that does not have a usage part.
type AttemptClosure struct {
	UpstreamAttemptID string    `json:"upstream_attempt_id"`
	RequestID         string    `json:"request_id"`
	AuthID            string    `json:"auth_id"`
	DrainCycleID      string    `json:"drain_cycle_id,omitempty"`
	DrainRequestKind  string    `json:"drain_request_kind,omitempty"`
	FailureClass      string    `json:"failure_class"`
	ClosedAt          time.Time `json:"closed_at"`
}

// UsageCostResult is an immutable pricing result for one usage event and price version.
type UsageCostResult struct {
	ResultID          string     `json:"result_id"`
	EventID           string     `json:"event_id"`
	UpstreamAttemptID string     `json:"upstream_attempt_id"`
	PricingRunID      string     `json:"pricing_run_id"`
	CostStatus        string     `json:"cost_status"`
	CalculatedAt      time.Time  `json:"calculated_at"`
	Cost              CostResult `json:"cost"`
}

// Health describes the accounting worker without exposing stored request contents.
type Health struct {
	Healthy         bool      `json:"healthy"`
	QueueDepth      int       `json:"queue_depth"`
	QueueCapacity   int       `json:"queue_capacity"`
	LastError       string    `json:"last_error,omitempty"`
	LastPersistedAt time.Time `json:"last_persisted_at,omitempty"`
	UsageEvents     int       `json:"usage_events"`
	CostResults     int       `json:"cost_results"`
}

// Store is the durable accounting ledger contract.
type Store interface {
	AppendIntent(AttemptIntent) error
	AppendUsage(UsageEvent) error
	AppendClosure(AttemptClosure) error
	AppendPrice(PriceVersion) error
	AppendCost(UsageCostResult) error
	Intent(string) (AttemptIntent, bool)
	UsageEvents() []UsageEvent
	Closures() []AttemptClosure
	PriceVersions() []PriceVersion
	CostResults() []UsageCostResult
	UnclosedIntents() []AttemptIntent
	Close() error
}
