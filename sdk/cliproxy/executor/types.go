package executor

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// RequestedModelMetadataKey stores the client-requested model name in Options.Metadata.
const RequestedModelMetadataKey = "requested_model"

// RequestPathMetadataKey stores the inbound HTTP request path (e.g. "/v1/images/generations") in Options.Metadata.
// It is optional and may be absent for non-HTTP executions.
const RequestPathMetadataKey = "request_path"

// DisallowFreeAuthMetadataKey instructs auth selection to skip known free-tier credentials.
const DisallowFreeAuthMetadataKey = "disallow_free_auth"

// AuthSelectionModelMetadataKey overrides the model used only for auth selection.
const AuthSelectionModelMetadataKey = "auth_selection_model"

// ReasoningEffortMetadataKey stores the client-requested reasoning effort for usage logs.
const ReasoningEffortMetadataKey = "reasoning_effort"

// ServiceTierMetadataKey stores the client-requested service tier for usage logs.
const ServiceTierMetadataKey = "service_tier"

// GenerateMetadataKey stores whether the client requested actual generation for usage logs.
// Missing or true means generation is enabled; only an explicit false disables generation.
const GenerateMetadataKey = "generate"

// CodexOverdraftExecutionMetadataKey carries an opaque, request-scoped send lease.
const CodexOverdraftExecutionMetadataKey = "codex_overdraft_execution"

// CodexQuotaObserverMetadataKey carries a callback for allowlisted quota observations.
const CodexQuotaObserverMetadataKey = "codex_quota_observer"

// AccountingAttemptMetadataKey carries durable intent hooks for one upstream attempt.
const AccountingAttemptMetadataKey = "accounting_attempt"

// CodexTurnStateClearMetadataKey instructs Codex transports to remove client turn state.
const CodexTurnStateClearMetadataKey = "codex_turn_state_clear"

// CodexTurnStateClearFromOptions reports whether transport-level turn state must be removed.
func CodexTurnStateClearFromOptions(opts Options) bool {
	if len(opts.Metadata) == 0 {
		return false
	}
	clear, _ := opts.Metadata[CodexTurnStateClearMetadataKey].(bool)
	return clear
}

// AccountingAttemptHooks persist an immutable intent before network dispatch.
type AccountingAttemptHooks struct {
	RecordIntent        func() error
	RecordRetryIntent   func() error
	RecordNotDispatched func()
	CurrentAttemptID    func() string
}

// AccountingAttemptHooksFromOptions extracts accounting hooks.
func AccountingAttemptHooksFromOptions(opts Options) *AccountingAttemptHooks {
	if len(opts.Metadata) == 0 {
		return nil
	}
	hooks, _ := opts.Metadata[AccountingAttemptMetadataKey].(*AccountingAttemptHooks)
	return hooks
}

// CodexQuotaHeadersObservation carries only allowlisted quota response headers.
type CodexQuotaHeadersObservation struct {
	AuthID         string
	AuthGeneration uint64
	AttemptID      string
	ObservedAt     time.Time
	Header         http.Header
}

// CodexQuotaObserver receives one response-header quota observation.
type CodexQuotaObserver func(CodexQuotaHeadersObservation)

// CodexOverdraftKind identifies the work admitted by an overdraft lease.
type CodexOverdraftKind string

const (
	// CodexOverdraftBusiness marks client business traffic.
	CodexOverdraftBusiness CodexOverdraftKind = "business"
	// CodexOverdraftProbe marks dedicated exhaustion probes.
	CodexOverdraftProbe CodexOverdraftKind = "exhaustion_probe"
)

// CodexOverdraftExecution exposes only the executor hooks needed at the send boundary.
type CodexOverdraftExecution struct {
	AuthID               string
	AuthGeneration       uint64
	DrainCycleID         string
	CoordinatorEpoch     uint64
	AuthStateEpoch       uint64
	DispatchID           string
	Kind                 CodexOverdraftKind
	beginSend            func() error
	release              func()
	businessUsageLimited func() error
	businessCompleted    func() error
}

// NewCodexOverdraftExecution constructs an opaque execution lease.
func NewCodexOverdraftExecution(authID string, authGeneration uint64, drainCycleID string, coordinatorEpoch, authStateEpoch uint64, dispatchID string, kind CodexOverdraftKind, beginSend func() error, release func(), businessUsageLimited func() error) *CodexOverdraftExecution {
	return NewCodexOverdraftExecutionWithCompletion(authID, authGeneration, drainCycleID, coordinatorEpoch, authStateEpoch, dispatchID, kind, beginSend, release, businessUsageLimited, nil)
}

// NewCodexOverdraftExecutionWithCompletion constructs a lease that can reset the usage-limit streak.
func NewCodexOverdraftExecutionWithCompletion(authID string, authGeneration uint64, drainCycleID string, coordinatorEpoch, authStateEpoch uint64, dispatchID string, kind CodexOverdraftKind, beginSend func() error, release func(), businessUsageLimited, businessCompleted func() error) *CodexOverdraftExecution {
	return &CodexOverdraftExecution{
		AuthID:               authID,
		AuthGeneration:       authGeneration,
		DrainCycleID:         drainCycleID,
		CoordinatorEpoch:     coordinatorEpoch,
		AuthStateEpoch:       authStateEpoch,
		DispatchID:           dispatchID,
		Kind:                 kind,
		beginSend:            beginSend,
		release:              release,
		businessUsageLimited: businessUsageLimited,
		businessCompleted:    businessCompleted,
	}
}

// BeginSend atomically rechecks the send fence for every actual upstream write.
func (e *CodexOverdraftExecution) BeginSend() error {
	if e == nil || e.beginSend == nil {
		return nil
	}
	return e.beginSend()
}

// Release returns the admission permit. The supplied hook is idempotent.
func (e *CodexOverdraftExecution) Release() {
	if e != nil && e.release != nil {
		e.release()
	}
}

// BusinessUsageLimited records one typed usage-limit 429 for the active owner.
func (e *CodexOverdraftExecution) BusinessUsageLimited() error {
	if e == nil || e.businessUsageLimited == nil {
		return nil
	}
	return e.businessUsageLimited()
}

// BusinessCompleted clears the consecutive usage-limit streak after a non-limit result.
func (e *CodexOverdraftExecution) BusinessCompleted() error {
	if e == nil || e.businessCompleted == nil {
		return nil
	}
	return e.businessCompleted()
}

// CodexOverdraftExecutionFromOptions extracts an opaque lease from request options.
func CodexOverdraftExecutionFromOptions(opts Options) *CodexOverdraftExecution {
	if len(opts.Metadata) == 0 {
		return nil
	}
	execution, _ := opts.Metadata[CodexOverdraftExecutionMetadataKey].(*CodexOverdraftExecution)
	return execution
}

const (
	// PinnedAuthMetadataKey locks execution to a specific auth ID.
	PinnedAuthMetadataKey = "pinned_auth_id"
	// SelectedAuthMetadataKey stores the auth ID selected by the scheduler.
	SelectedAuthMetadataKey = "selected_auth_id"
	// SelectedAuthCallbackMetadataKey carries an optional callback invoked with the selected auth ID.
	SelectedAuthCallbackMetadataKey = "selected_auth_callback"
	// SelectedAuthIndexMetadataKey stores the stable index of the auth selected by the scheduler.
	SelectedAuthIndexMetadataKey = "selected_auth_index"
	// SelectedAuthIndexCallbackMetadataKey carries an optional callback invoked with the selected auth index.
	SelectedAuthIndexCallbackMetadataKey = "selected_auth_index_callback"
	// ExecutionSessionMetadataKey identifies a long-lived downstream execution session.
	ExecutionSessionMetadataKey = "execution_session_id"
	// DerivedSessionIDMetadataKey stores a stable session identity inferred from request context.
	DerivedSessionIDMetadataKey = "derived_session_id"
	// CallerScopeMetadataKey isolates inferred session identities between downstream callers.
	CallerScopeMetadataKey = "caller_scope"
)

// Request encapsulates the translated payload that will be sent to a provider executor.
type Request struct {
	// Model is the upstream model identifier after translation.
	Model string
	// Payload is the provider specific JSON payload.
	Payload []byte
	// Format represents the provider payload schema.
	Format sdktranslator.Format
	// Metadata carries optional provider specific execution hints.
	Metadata map[string]any
}

// RequestAfterAuthInterceptor rewrites a request after credential selection and before executor translation.
type RequestAfterAuthInterceptor func(context.Context, RequestAfterAuthInterceptRequest) RequestAfterAuthInterceptResponse

// RequestAfterAuthInterceptRequest describes a selected-auth request before executor translation.
type RequestAfterAuthInterceptRequest struct {
	// SourceFormat is the original client protocol format.
	SourceFormat sdktranslator.Format
	// ToFormat is the selected upstream protocol format.
	ToFormat sdktranslator.Format
	// Model is the selected upstream model for this attempt.
	Model string
	// RequestedModel is the client-requested model before alias/model-pool rewriting.
	RequestedModel string
	// Stream reports whether the request expects streaming output.
	Stream bool
	// Headers contains the current upstream request headers.
	Headers http.Header
	// Body contains the current request payload.
	Body []byte
	// Metadata is a best-effort cloned context snapshot. Treat it as read-only and JSON-like.
	Metadata map[string]any
}

// RequestAfterAuthInterceptResponse returns selected-auth request modifications.
type RequestAfterAuthInterceptResponse struct {
	// Headers replaces matching current request headers and preserves headers not mentioned here.
	Headers http.Header
	// Body replaces the current request body only when non-empty.
	Body []byte
	// ClearHeaders explicitly removes current request headers before Headers is applied.
	ClearHeaders []string
	// Terminate prevents the selected executor from receiving the request.
	Terminate bool
	// StatusCode is the downstream HTTP status used when Terminate is true.
	StatusCode int
	// ResponseHeaders contains downstream response headers used when Terminate is true.
	ResponseHeaders http.Header
	// ResponseBody contains the downstream response body used when Terminate is true.
	ResponseBody []byte
}

// RequestTerminatedError carries a plugin-defined downstream response without executing upstream.
type RequestTerminatedError struct {
	HTTPStatus int
	Header     http.Header
	Body       []byte
}

func (e *RequestTerminatedError) Error() string {
	return "request terminated by plugin"
}

// StatusCode returns the plugin-defined downstream HTTP status.
func (e *RequestTerminatedError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}

// ResponseHeaders returns a copy of the plugin-defined downstream headers.
func (e *RequestTerminatedError) ResponseHeaders() http.Header {
	if e == nil {
		return nil
	}
	return e.Header.Clone()
}

// ResponseBody returns a copy of the plugin-defined downstream body.
func (e *RequestTerminatedError) ResponseBody() []byte {
	if e == nil {
		return nil
	}
	return append([]byte(nil), e.Body...)
}

// UpstreamNotDispatchedError marks a failure before an upstream write started.
type UpstreamNotDispatchedError struct {
	cause error
}

// NewUpstreamNotDispatchedError wraps a pre-send failure for retry accounting.
func NewUpstreamNotDispatchedError(cause error) error {
	if cause == nil {
		return nil
	}
	return &UpstreamNotDispatchedError{cause: cause}
}

func (e *UpstreamNotDispatchedError) Error() string {
	if e == nil || e.cause == nil {
		return "upstream request not dispatched"
	}
	return "upstream request not dispatched: " + e.cause.Error()
}

// Unwrap exposes the pre-send cause.
func (e *UpstreamNotDispatchedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// NotDispatched reports that no upstream attempt budget was consumed.
func (e *UpstreamNotDispatchedError) NotDispatched() bool { return e != nil }

// IsUpstreamNotDispatched identifies pre-send failures through wrapping.
func IsUpstreamNotDispatched(err error) bool {
	var notDispatched interface{ NotDispatched() bool }
	return errors.As(err, &notDispatched) && notDispatched.NotDispatched()
}

// Options controls execution behavior for both streaming and non-streaming calls.
type Options struct {
	// Stream toggles streaming mode.
	Stream bool
	// Alt carries optional alternate format hint (e.g. SSE JSON key).
	Alt string
	// Headers are forwarded to the provider request builder.
	Headers http.Header
	// Query contains optional query string parameters.
	Query url.Values
	// OriginalRequest preserves the inbound request bytes prior to translation.
	OriginalRequest []byte
	// SourceFormat identifies the inbound schema.
	SourceFormat sdktranslator.Format
	// ResponseFormat identifies the downstream response schema.
	// Empty means responses should use SourceFormat for backward compatibility.
	ResponseFormat sdktranslator.Format
	// Metadata carries extra execution hints shared across selection and executors.
	Metadata map[string]any
	// RequestAfterAuthInterceptor runs after credential selection and before executor translation.
	RequestAfterAuthInterceptor RequestAfterAuthInterceptor
	// ExecutionLifecycle owns Home-dispatched execution resources. Executors must not add it to request metadata.
	ExecutionLifecycle ExecutionLifecycle
}

// ResponseFormatOrSource returns the response target format for an execution.
func ResponseFormatOrSource(opts Options) sdktranslator.Format {
	if opts.ResponseFormat != "" {
		return opts.ResponseFormat
	}
	return opts.SourceFormat
}

// Response wraps either a full provider response or metadata for streaming flows.
type Response struct {
	// Payload is the provider response in the executor format.
	Payload []byte
	// Metadata exposes optional structured data for translators.
	Metadata map[string]any
	// Headers carries upstream HTTP response headers for passthrough to clients.
	Headers http.Header
}

// StreamChunk represents a single streaming payload unit emitted by provider executors.
type StreamChunk struct {
	// Payload is the raw provider chunk payload.
	Payload []byte
	// Err reports any terminal error encountered while producing chunks.
	Err error
}

// StreamResult wraps the streaming response, providing both the chunk channel
// and the upstream HTTP response headers captured before streaming begins.
type StreamResult struct {
	// Headers carries upstream HTTP response headers from the initial connection.
	Headers http.Header
	// Chunks is the channel of streaming payload units.
	Chunks <-chan StreamChunk
}

// StatusError represents an error that carries an HTTP-like status code.
// Provider executors should implement this when possible to enable
// better auth state updates on failures (e.g., 401/402/429).
type StatusError interface {
	error
	StatusCode() int
}

// RequestScopedError identifies a failure tied to the current request rather
// than the selected credential. Auth managers should not retry these errors
// across credentials or change credential availability because of them.
type RequestScopedError interface {
	error
	IsRequestScoped() bool
}

// UsagePersistenceError reports a post-dispatch terminal accounting failure.
// It is request-scoped so auth managers do not duplicate completed upstream work.
type UsagePersistenceError struct {
	cause error
}

// NewUsagePersistenceError wraps a durable terminal usage failure.
func NewUsagePersistenceError(cause error) error {
	if cause == nil {
		return nil
	}
	return &UsagePersistenceError{cause: cause}
}

func (e *UsagePersistenceError) Error() string {
	return "durable usage accounting failed"
}

// Unwrap exposes the storage cause to internal diagnostics.
func (e *UsagePersistenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsRequestScoped prevents cross-credential replay after upstream completion.
func (e *UsagePersistenceError) IsRequestScoped() bool { return e != nil }

// StatusCode maps the accounting outage to service unavailable.
func (e *UsagePersistenceError) StatusCode() int { return http.StatusServiceUnavailable }
