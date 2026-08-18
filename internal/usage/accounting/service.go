package accounting

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

type serviceTask struct {
	fn   func() error
	done chan error
}

// Service provides durable accounting ingestion and asynchronous pricing.
type Service struct {
	store   Store
	queue   chan serviceTask
	done    chan struct{}
	pricing *PricingBook

	costMu  sync.RWMutex
	costs   map[string]UsageCostResult
	costIDs map[string]struct{}

	lifecycleMu sync.RWMutex
	closed      bool
	closeOnce   sync.Once
	closeErr    error

	healthMu        sync.RWMutex
	healthy         bool
	lastError       string
	lastPersistedAt time.Time
}

// NewService starts the accounting worker and restores durable prices and costs.
func NewService(store Store, queueSize int) *Service {
	if queueSize <= 0 {
		queueSize = 1024
	}
	service := &Service{
		store:   store,
		queue:   make(chan serviceTask, queueSize),
		done:    make(chan struct{}),
		pricing: NewPricingBook(),
		costs:   make(map[string]UsageCostResult),
		costIDs: make(map[string]struct{}),
		healthy: store != nil,
	}
	if store != nil {
		for _, version := range store.PriceVersions() {
			_, _ = service.pricing.Put(version)
		}
		for _, result := range store.CostResults() {
			service.costIDs[result.ResultID] = struct{}{}
			current, exists := service.costs[result.EventID]
			if !exists || result.CalculatedAt.After(current.CalculatedAt) {
				service.costs[result.EventID] = result
			}
		}
		if errHealth := accountingStoreHealthError(store); errHealth != nil {
			service.markPersistError(errHealth)
		}
	}
	go service.run()
	if store != nil && len(store.UsageEvents()) > 0 {
		service.enqueue(serviceTask{fn: func() error { return service.repriceAll("startup") }}, false)
	}
	return service
}

func (s *Service) run() {
	defer close(s.done)
	for task := range s.queue {
		var errTask error
		if task.fn != nil {
			errTask = task.fn()
		}
		if errTask != nil {
			s.markPersistError(errTask)
		}
		if task.done != nil {
			task.done <- errTask
		}
	}
}

// RecordIntent durably records the pre-send intent synchronously.
func (s *Service) RecordIntent(_ context.Context, intent AttemptIntent) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("accounting service is not available")
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.closed {
		return fmt.Errorf("accounting service is closed")
	}
	if intent.SchemaVersion == 0 {
		intent.SchemaVersion = 1
	}
	errAppend := s.store.AppendIntent(intent)
	s.markPersistResult(errAppend)
	return errAppend
}

// HandleUsage persists one immutable terminal usage event before returning.
func (s *Service) HandleUsage(ctx context.Context, record coreusage.Record) {
	_ = s.HandleUsageDurable(ctx, record)
}

// HandleUsageDurable persists one immutable terminal usage event and reports failures.
func (s *Service) HandleUsageDurable(_ context.Context, record coreusage.Record) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("accounting service is not available")
	}
	if provider := strings.TrimSpace(record.Provider); provider != "" && !strings.EqualFold(provider, "codex") {
		return nil
	}
	event := usageEventFromRecord(record)
	s.lifecycleMu.RLock()
	if s.closed {
		s.lifecycleMu.RUnlock()
		return fmt.Errorf("accounting service is closed")
	}
	event, errPersist := s.persistUsage(event)
	s.lifecycleMu.RUnlock()
	s.markPersistResult(errPersist)
	if errPersist != nil {
		return errPersist
	}
	s.enqueue(serviceTask{fn: func() error {
		errPrice := s.priceEvent(event, "live")
		if errors.Is(errPrice, ErrUnpriced) {
			return nil
		}
		return errPrice
	}}, true)
	return nil
}

func (s *Service) enqueue(task serviceTask, synchronousWhenFull bool) bool {
	if s == nil {
		return false
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.closed {
		return false
	}
	select {
	case s.queue <- task:
		return true
	default:
		if synchronousWhenFull && task.fn != nil {
			errTask := task.fn()
			s.markPersistResult(errTask)
			if task.done != nil {
				task.done <- errTask
			}
			return true
		}
		return false
	}
}

func (s *Service) persistUsage(event UsageEvent) (UsageEvent, error) {
	intent, exists := s.store.Intent(event.UpstreamAttemptID)
	if !exists {
		intent = AttemptIntent{
			SchemaVersion:      1,
			UpstreamAttemptID:  event.UpstreamAttemptID,
			RequestID:          event.RequestID,
			AuthID:             event.AuthID,
			Provider:           event.Provider,
			RequestedModel:     event.Model,
			ResolvedModel:      event.Model,
			RequestServiceTier: event.Tier,
			RequestedAt:        event.RequestedAt,
			DrainMode:          event.DrainMode,
			DrainCycleID:       event.DrainCycleID,
			DrainRequestKind:   event.DrainRequestKind,
			ConsumptionClass:   event.ConsumptionClass,
		}
		if errIntent := s.store.AppendIntent(intent); errIntent != nil {
			return event, errIntent
		}
	}
	event.RequestID = intent.RequestID
	event.AuthID = intent.AuthID
	event.Provider = normalizedAccountingProvider(intent.Provider)
	if strings.TrimSpace(intent.ResolvedModel) != "" {
		event.Model = intent.ResolvedModel
	}
	event.RequestedAt = intent.RequestedAt
	event.DrainMode = intent.DrainMode
	event.DrainCycleID = intent.DrainCycleID
	event.DrainRequestKind = intent.DrainRequestKind
	event.ConsumptionClass = intent.ConsumptionClass
	if errAppend := s.store.AppendUsage(event); errAppend != nil {
		return event, errAppend
	}
	return event, nil
}

// RecordNotDispatched records an attempt rejected before an upstream write.
func (s *Service) RecordNotDispatched(_ context.Context, attemptID, requestID, authID string) error {
	return s.recordClosure(AttemptClosure{UpstreamAttemptID: attemptID, RequestID: requestID, AuthID: authID, FailureClass: FailureNotDispatched, ClosedAt: time.Now()})
}

// RecordIndeterminate closes an intent whose network write outcome is unknown.
func (s *Service) RecordIndeterminate(_ context.Context, attemptID, requestID, authID string) error {
	return s.recordClosure(AttemptClosure{UpstreamAttemptID: attemptID, RequestID: requestID, AuthID: authID, FailureClass: FailureIndeterminate, ClosedAt: time.Now()})
}

// RecordClosure persists a fully attributed terminal attempt closure.
func (s *Service) RecordClosure(_ context.Context, closure AttemptClosure) error {
	if closure.ClosedAt.IsZero() {
		closure.ClosedAt = time.Now()
	}
	return s.recordClosure(closure)
}

func (s *Service) recordClosure(closure AttemptClosure) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("accounting service is not available")
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.closed {
		return fmt.Errorf("accounting service is closed")
	}
	errAppend := s.store.AppendClosure(closure)
	s.markPersistResult(errAppend)
	return errAppend
}

// Reconcile closes intents that survived without terminal delivery.
func (s *Service) Reconcile(_ context.Context) error {
	if s == nil || s.store == nil {
		return nil
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.closed {
		return fmt.Errorf("accounting service is closed")
	}
	intents := s.store.UnclosedIntents()
	if errHealth := accountingStoreHealthError(s.store); errHealth != nil {
		s.markPersistError(errHealth)
		return fmt.Errorf("read accounting store during reconciliation: %w", errHealth)
	}
	for _, intent := range intents {
		if errClose := s.store.AppendClosure(AttemptClosure{UpstreamAttemptID: intent.UpstreamAttemptID, RequestID: intent.RequestID, AuthID: intent.AuthID, DrainCycleID: intent.DrainCycleID, DrainRequestKind: intent.DrainRequestKind, FailureClass: FailureIndeterminate, ClosedAt: time.Now()}); errClose != nil {
			s.markPersistError(errClose)
			return errClose
		}
	}
	s.markPersistResult(nil)
	return nil
}

func accountingStoreHealthError(store Store) error {
	if healthStore, ok := store.(interface{ HealthError() error }); ok {
		return healthStore.HealthError()
	}
	return nil
}

// Flush waits until all previously queued usage and pricing work is durable.
func (s *Service) Flush(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	s.lifecycleMu.RLock()
	if s.closed {
		s.lifecycleMu.RUnlock()
		return nil
	}
	select {
	case s.queue <- serviceTask{done: done}:
		s.lifecycleMu.RUnlock()
	case <-ctx.Done():
		s.lifecycleMu.RUnlock()
		return ctx.Err()
	}
	select {
	case errFlush := <-done:
		return errFlush
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close prevents new work, drains queued events, and closes the durable store.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.closed = true
		close(s.queue)
		s.lifecycleMu.Unlock()
		<-s.done
		if s.store != nil {
			s.closeErr = s.store.Close()
		}
	})
	return s.closeErr
}

func usageEventFromRecord(record coreusage.Record) UsageEvent {
	attemptID := strings.TrimSpace(record.UpstreamAttemptID)
	if attemptID == "" {
		attemptID = strings.TrimSpace(record.RequestID)
	}
	if attemptID == "" {
		attemptID = "attempt_" + fmt.Sprintf("%x", rand.Text())
	}
	usagePart := strings.TrimSpace(record.UsagePart)
	if usagePart == "" {
		usagePart = "primary"
	}
	failureClass := record.FailureClass
	if failureClass == "" && record.Failed && !record.UsageReported {
		failureClass = FailureMissingUsage
	}
	tier := record.ResponseServiceTier
	if tier == "" {
		tier = record.ServiceTier
	}
	if tier == "" {
		tier = coreusage.DefaultServiceTier
	}
	requestedAt := record.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now()
	}
	return UsageEvent{
		EventID:           usageEventID(attemptID, usagePart),
		SchemaVersion:     1,
		UpstreamAttemptID: attemptID,
		RequestID:         record.RequestID,
		AuthID:            record.AuthID,
		Provider:          normalizedAccountingProvider(record.Provider),
		Model:             record.Model,
		Tier:              tier,
		DrainMode:         record.DrainMode,
		DrainCycleID:      record.DrainCycleID,
		DrainRequestKind:  record.DrainRequestKind,
		ConsumptionClass:  record.ConsumptionClass,
		UsagePart:         usagePart,
		UsageReported:     record.UsageReported,
		Failed:            record.Failed,
		FailureClass:      failureClass,
		Detail:            record.Detail,
		RequestedAt:       requestedAt,
		ObservedAt:        time.Now(),
	}
}

func usageEventID(attemptID, usagePart string) string {
	digest := sha256.Sum256([]byte(attemptID + "\x00" + usagePart))
	return "usage_" + hex.EncodeToString(digest[:16])
}

func (s *Service) priceEvent(event UsageEvent, runID string) error {
	if s == nil {
		return nil
	}
	if runID == "" {
		runID = "live"
	}
	resultID := costResultID(event.EventID, runID)
	s.costMu.RLock()
	_, exists := s.costIDs[resultID]
	s.costMu.RUnlock()
	if exists {
		return nil
	}
	status := CostStatusPaused
	cost := CostResult{}
	breakdown := event.Detail.TokenBreakdown
	if event.UsageReported && !breakdown.Valid() {
		breakdown = coreusage.EnsureTokenBreakdown(event.Detail).TokenBreakdown
	}
	if event.UsageReported && breakdown.Valid() && breakdown.Quality == coreusage.TokenAccountingQualityComplete && breakdown.UnclassifiedTokens == 0 {
		priceAt := event.RequestedAt
		if priceAt.IsZero() {
			priceAt = event.ObservedAt
		}
		var errPrice error
		cost, errPrice = s.pricing.Price(event.Provider, event.Model, event.Tier, priceAt, breakdown)
		switch {
		case errPrice == nil:
			status = CostStatusPriced
		case errors.Is(errPrice, ErrUnpriced):
			status = CostStatusUnpriced
		default:
			return errPrice
		}
	}
	result := UsageCostResult{ResultID: resultID, EventID: event.EventID, UpstreamAttemptID: event.UpstreamAttemptID, PricingRunID: runID, CostStatus: status, CalculatedAt: time.Now(), Cost: cost}
	if errAppend := s.store.AppendCost(result); errAppend != nil {
		return errAppend
	}
	s.costMu.Lock()
	s.costIDs[result.ResultID] = struct{}{}
	s.costs[event.EventID] = result
	s.costMu.Unlock()
	return nil
}

func normalizedAccountingProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return "codex"
	}
	return provider
}

func costResultID(eventID, pricingRunID string) string {
	digest := sha256.Sum256([]byte(eventID + "\x00" + pricingRunID))
	return "cost_" + hex.EncodeToString(digest[:16])
}

// PutPrice adds an immutable model price and queues pricing for historical events.
func (s *Service) PutPrice(version PriceVersion) (PriceVersion, error) {
	if s == nil || s.store == nil {
		return PriceVersion{}, fmt.Errorf("accounting service is not available")
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if s.closed {
		return PriceVersion{}, fmt.Errorf("accounting service is closed")
	}
	stored, errPut := s.pricing.Put(version)
	if errPut != nil {
		return PriceVersion{}, errPut
	}
	if errPersist := s.store.AppendPrice(stored); errPersist != nil {
		s.pricing.remove(stored.VersionID)
		s.markPersistError(errPersist)
		return PriceVersion{}, errPersist
	}
	s.markPersistResult(nil)
	runID := "run_" + rand.Text()
	events := s.store.UsageEvents()
	select {
	case s.queue <- serviceTask{fn: func() error { return s.repriceEvents(runID, events) }}:
	default:
		if errReprice := s.repriceEvents(runID, events); errReprice != nil {
			s.markPersistError(errReprice)
		}
	}
	return stored, nil
}

func (s *Service) repriceAll(runID string) error {
	return s.repriceEvents(runID, s.store.UsageEvents())
}

func (s *Service) repriceEvents(runID string, events []UsageEvent) error {
	for _, event := range events {
		if errPrice := s.priceEvent(event, runID); errPrice != nil {
			return errPrice
		}
	}
	return nil
}

// Price returns one immutable price version.
func (s *Service) Price(versionID string) (PriceVersion, bool) {
	if s == nil {
		return PriceVersion{}, false
	}
	return s.pricing.Get(versionID)
}

// Prices returns all durable price versions.
func (s *Service) Prices() []PriceVersion {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.PriceVersions()
}

// Reprice queues an immutable historical pricing run and returns its ID.
func (s *Service) Reprice() (string, error) {
	if s == nil || s.store == nil {
		return "", fmt.Errorf("accounting service is not available")
	}
	runID := "run_" + rand.Text()
	done := make(chan error, 1)
	if !s.enqueue(serviceTask{fn: func() error { return s.repriceAll(runID) }, done: done}, true) {
		return "", fmt.Errorf("accounting service is closed")
	}
	if errRun := <-done; errRun != nil {
		return "", errRun
	}
	return runID, nil
}

type aggregateAttemptSummary struct {
	probe     bool
	failed    bool
	overdraft bool
	tokens    int64
	cost      int64
}

type aggregateAccumulator struct {
	result        Aggregate
	attempts      map[string]aggregateAttemptSummary
	indeterminate map[string]struct{}
	notDispatched map[string]struct{}
}

func newAggregateAccumulator(authID, drainCycleID string) *aggregateAccumulator {
	return &aggregateAccumulator{
		result:        Aggregate{AuthID: authID, DrainCycleID: drainCycleID},
		attempts:      make(map[string]aggregateAttemptSummary),
		indeterminate: make(map[string]struct{}),
		notDispatched: make(map[string]struct{}),
	}
}

func (a *aggregateAccumulator) addEvent(event UsageEvent, cost int64) {
	switch event.FailureClass {
	case FailureNotDispatched:
		a.notDispatched[event.UpstreamAttemptID] = struct{}{}
		return
	case FailureIndeterminate:
		a.indeterminate[event.UpstreamAttemptID] = struct{}{}
		return
	}
	summary := a.attempts[event.UpstreamAttemptID]
	summary.probe = summary.probe || event.DrainRequestKind == "exhaustion_probe"
	summary.failed = summary.failed || event.Failed
	summary.overdraft = summary.overdraft || event.DrainMode == "overdraft"
	summary.tokens += event.Detail.TotalTokens
	summary.cost += cost
	a.attempts[event.UpstreamAttemptID] = summary
}

func (a *aggregateAccumulator) addClosure(closure AttemptClosure) {
	if closure.FailureClass == FailureNotDispatched {
		a.notDispatched[closure.UpstreamAttemptID] = struct{}{}
	} else if closure.FailureClass == FailureIndeterminate {
		a.indeterminate[closure.UpstreamAttemptID] = struct{}{}
	}
}

func (a *aggregateAccumulator) finalize() Aggregate {
	for attemptID := range a.notDispatched {
		delete(a.attempts, attemptID)
	}
	for attemptID := range a.indeterminate {
		delete(a.attempts, attemptID)
	}
	for _, summary := range a.attempts {
		if summary.probe {
			a.result.ProbeAttempts++
			a.result.ProbeTotalTokens += summary.tokens
			a.result.ProbeCostNanoUSD += summary.cost
			if summary.failed {
				a.result.ProbeFailures++
			} else {
				a.result.ProbeSuccesses++
			}
			continue
		}
		a.result.BusinessAttempts++
		a.result.BusinessTotalTokens += summary.tokens
		a.result.BusinessCostNanoUSD += summary.cost
		if summary.failed {
			a.result.BusinessFailures++
		} else {
			a.result.BusinessSuccesses++
		}
		if summary.overdraft {
			a.result.OverdraftBusinessAttempts++
			a.result.OverdraftBusinessTotalTokens += summary.tokens
			a.result.OverdraftBusinessCostNanoUSD += summary.cost
			if summary.failed {
				a.result.OverdraftBusinessFailures++
			} else {
				a.result.OverdraftBusinessSuccesses++
			}
		} else {
			a.result.NormalBusinessAttempts++
			a.result.NormalBusinessTotalTokens += summary.tokens
			a.result.NormalBusinessCostNanoUSD += summary.cost
			if summary.failed {
				a.result.NormalBusinessFailures++
			} else {
				a.result.NormalBusinessSuccesses++
			}
		}
	}
	a.result.NotDispatched = int64(len(a.notDispatched))
	a.result.IndeterminateAttempts = int64(len(a.indeterminate))
	return a.result
}

func (s *Service) aggregateCostSnapshot() map[string]UsageCostResult {
	s.costMu.RLock()
	costs := make(map[string]UsageCostResult, len(s.costs))
	for eventID, cost := range s.costs {
		costs[eventID] = cost
	}
	s.costMu.RUnlock()
	return costs
}

// Aggregate rebuilds per-auth or per-cycle totals from immutable records.
func (s *Service) Aggregate(authID, drainCycleID string) Aggregate {
	accumulator := newAggregateAccumulator(authID, drainCycleID)
	if s == nil || s.store == nil {
		return accumulator.result
	}
	costs := s.aggregateCostSnapshot()
	for _, event := range s.store.UsageEvents() {
		if authID != "" && event.AuthID != authID || drainCycleID != "" && event.DrainCycleID != drainCycleID {
			continue
		}
		accumulator.addEvent(event, costs[event.EventID].Cost.TotalNanoUSD)
	}
	for _, closure := range s.store.Closures() {
		if authID != "" && closure.AuthID != authID || drainCycleID != "" && closure.DrainCycleID != drainCycleID {
			continue
		}
		accumulator.addClosure(closure)
	}
	return accumulator.finalize()
}

// AggregateDrainCycles rebuilds totals for many auth/drain-cycle pairs in one storage pass.
// The input maps Auth.ID to its drain cycle ID; the result is keyed by drain cycle ID.
func (s *Service) AggregateDrainCycles(cycles map[string]string) map[string]Aggregate {
	accumulators := make(map[string]*aggregateAccumulator, len(cycles))
	for authID, drainCycleID := range cycles {
		if authID == "" || drainCycleID == "" {
			continue
		}
		accumulators[authID] = newAggregateAccumulator(authID, drainCycleID)
	}
	results := make(map[string]Aggregate, len(accumulators))
	if s == nil || s.store == nil || len(accumulators) == 0 {
		for _, accumulator := range accumulators {
			results[accumulator.result.DrainCycleID] = accumulator.result
		}
		return results
	}
	costs := s.aggregateCostSnapshot()
	for _, event := range s.store.UsageEvents() {
		accumulator := accumulators[event.AuthID]
		if accumulator == nil || event.DrainCycleID != accumulator.result.DrainCycleID {
			continue
		}
		accumulator.addEvent(event, costs[event.EventID].Cost.TotalNanoUSD)
	}
	for _, closure := range s.store.Closures() {
		accumulator := accumulators[closure.AuthID]
		if accumulator == nil || closure.DrainCycleID != accumulator.result.DrainCycleID {
			continue
		}
		accumulator.addClosure(closure)
	}
	for _, accumulator := range accumulators {
		results[accumulator.result.DrainCycleID] = accumulator.finalize()
	}
	return results
}

// AggregatesByAuth rebuilds all per-auth totals in one bounded pass over immutable records.
func (s *Service) AggregatesByAuth() []Aggregate {
	if s == nil || s.store == nil {
		return nil
	}
	costs := s.aggregateCostSnapshot()
	accumulators := make(map[string]*aggregateAccumulator)
	get := func(authID string) *aggregateAccumulator {
		accumulator := accumulators[authID]
		if accumulator == nil {
			accumulator = newAggregateAccumulator(authID, "")
			accumulators[authID] = accumulator
		}
		return accumulator
	}
	for _, event := range s.store.UsageEvents() {
		if event.AuthID == "" {
			continue
		}
		get(event.AuthID).addEvent(event, costs[event.EventID].Cost.TotalNanoUSD)
	}
	for _, closure := range s.store.Closures() {
		if closure.AuthID == "" {
			continue
		}
		get(closure.AuthID).addClosure(closure)
	}
	authIDs := make([]string, 0, len(accumulators))
	for authID := range accumulators {
		authIDs = append(authIDs, authID)
	}
	sort.Strings(authIDs)
	results := make([]Aggregate, 0, len(authIDs))
	for _, authID := range authIDs {
		results = append(results, accumulators[authID].finalize())
	}
	return results
}

// UsageEvents returns immutable usage events for management queries.
func (s *Service) UsageEvents() []UsageEvent {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.UsageEvents()
}

// CostResults returns immutable price calculation results.
func (s *Service) CostResults() []UsageCostResult {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.CostResults()
}

// Health returns current queue and durable-write health.
func (s *Service) Health() Health {
	if s == nil || s.store == nil {
		return Health{Healthy: false}
	}
	usageEvents := len(s.store.UsageEvents())
	costResults := len(s.store.CostResults())
	if errHealth := accountingStoreHealthError(s.store); errHealth != nil {
		s.markPersistError(errHealth)
	}
	s.healthMu.RLock()
	health := Health{Healthy: s.healthy, QueueDepth: len(s.queue), QueueCapacity: cap(s.queue), LastError: s.lastError, LastPersistedAt: s.lastPersistedAt, UsageEvents: usageEvents, CostResults: costResults}
	s.healthMu.RUnlock()
	return health
}

// AdmissionHealthy reports whether new requests may rely on durable accounting.
// A persistence fault remains admission-blocking until the service is restarted.
func (s *Service) AdmissionHealthy() bool {
	if s == nil || s.store == nil {
		return false
	}
	s.healthMu.RLock()
	healthy := s.healthy
	s.healthMu.RUnlock()
	return healthy
}

func (s *Service) markPersistResult(err error) {
	if err != nil {
		s.markPersistError(err)
		return
	}
	s.healthMu.Lock()
	if s.healthy {
		s.lastError = ""
	}
	s.lastPersistedAt = time.Now()
	s.healthMu.Unlock()
}

func (s *Service) markPersistError(err error) {
	if s == nil || err == nil {
		return
	}
	s.healthMu.Lock()
	s.healthy = false
	s.lastError = err.Error()
	s.healthMu.Unlock()
}
