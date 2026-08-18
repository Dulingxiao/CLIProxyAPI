package accounting

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	bolt "go.etcd.io/bbolt"
)

type gatedUsageStore struct {
	Store
	entered chan struct{}
	release chan struct{}
}

type gatedCostStore struct {
	Store
	entered chan struct{}
	release chan struct{}
}

type failingUsageStore struct {
	Store
}

func (s *failingUsageStore) AppendUsage(UsageEvent) error {
	return errors.New("injected durable usage failure")
}

func (s *gatedCostStore) AppendCost(result UsageCostResult) error {
	close(s.entered)
	<-s.release
	return s.Store.AppendCost(result)
}

func (s *gatedUsageStore) AppendUsage(event UsageEvent) error {
	close(s.entered)
	<-s.release
	return s.Store.AppendUsage(event)
}

func TestServiceHandleUsageReturnsAfterTerminalEventIsDurable(t *testing.T) {
	memory := NewMemoryStore()
	store := &gatedUsageStore{Store: memory, entered: make(chan struct{}), release: make(chan struct{})}
	service := NewService(store, 8)
	returned := make(chan struct{})
	go func() {
		service.HandleUsage(context.Background(), coreusage.Record{AuthID: "auth", UpstreamAttemptID: "attempt", UsageReported: true})
		close(returned)
	}()
	<-store.entered
	returnedBeforeDurable := false
	select {
	case <-returned:
		returnedBeforeDurable = true
	default:
	}
	close(store.release)
	<-returned
	if errClose := service.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	if returnedBeforeDurable {
		t.Fatal("HandleUsage returned before AppendUsage completed")
	}
}

func TestServiceHandleUsageDurableReturnsPersistenceError(t *testing.T) {
	service := NewService(&failingUsageStore{Store: NewMemoryStore()}, 8)
	defer service.Close()
	errPersist := service.HandleUsageDurable(context.Background(), coreusage.Record{Provider: "codex", AuthID: "auth", UpstreamAttemptID: "attempt", UsageReported: true})
	if errPersist == nil || errPersist.Error() != "injected durable usage failure" {
		t.Fatalf("HandleUsageDurable() error = %v", errPersist)
	}
}

func TestServiceAdmissionHealthStaysUnhealthyAfterDurableWriteFailure(t *testing.T) {
	service := NewService(&failingUsageStore{Store: NewMemoryStore()}, 8)
	defer service.Close()

	if errPersist := service.HandleUsageDurable(context.Background(), coreusage.Record{Provider: "codex", AuthID: "auth", UpstreamAttemptID: "attempt", UsageReported: true}); errPersist == nil {
		t.Fatal("HandleUsageDurable() succeeded with failing durable store")
	}
	if service.AdmissionHealthy() {
		t.Fatal("AdmissionHealthy() = true after durable usage failure")
	}
	if errIntent := service.RecordIntent(context.Background(), AttemptIntent{UpstreamAttemptID: "later-attempt"}); errIntent != nil {
		t.Fatal(errIntent)
	}
	if service.AdmissionHealthy() {
		t.Fatal("AdmissionHealthy() recovered after an unrelated successful write")
	}
}

func TestServiceHandleUsageDoesNotWaitForPricing(t *testing.T) {
	memory := NewMemoryStore()
	store := &gatedCostStore{Store: memory, entered: make(chan struct{}), release: make(chan struct{})}
	service := NewService(store, 8)
	requestedAt := time.Now().UTC()
	if _, errPrice := service.PutPrice(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: requestedAt.Add(-time.Hour), InputNanoUSDPerMillion: 1_000_000}); errPrice != nil {
		t.Fatal(errPrice)
	}
	returned := make(chan struct{})
	go func() {
		service.HandleUsage(context.Background(), coreusage.Record{
			AuthID: "auth", Model: "gpt-test", UpstreamAttemptID: "attempt-priced", UsageReported: true, RequestedAt: requestedAt,
			Detail: coreusage.Detail{InputTokens: 1, TotalTokens: 1, TokenBreakdown: coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 1, Input: coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1}}},
		})
		close(returned)
	}()
	<-store.entered
	returnedBeforePricing := false
	select {
	case <-returned:
		returnedBeforePricing = true
	default:
	}
	close(store.release)
	<-returned
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if errClose := service.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	if !returnedBeforePricing {
		t.Fatal("HandleUsage waited for asynchronous pricing")
	}
}

func TestServicePricesUsageAtDurableAttemptIntentTime(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	boundary := time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)
	intentAt := boundary.Add(-time.Minute)
	usageReporterAt := boundary.Add(time.Minute)
	if _, errPrice := service.PutPrice(PriceVersion{VersionID: "price-before", Provider: "codex", Model: "gpt-test", Tier: "default", EffectiveFrom: boundary.Add(-time.Hour), EffectiveTo: boundary, InputNanoUSDPerMillion: 1_000_000}); errPrice != nil {
		t.Fatal(errPrice)
	}
	if _, errPrice := service.PutPrice(PriceVersion{VersionID: "price-after", Provider: "codex", Model: "gpt-test", Tier: "default", EffectiveFrom: boundary, InputNanoUSDPerMillion: 2_000_000}); errPrice != nil {
		t.Fatal(errPrice)
	}
	if errIntent := service.RecordIntent(context.Background(), AttemptIntent{UpstreamAttemptID: "attempt-time", AuthID: "auth", Provider: "codex", ResolvedModel: "gpt-test", RequestServiceTier: "default", RequestedAt: intentAt}); errIntent != nil {
		t.Fatal(errIntent)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 1, Input: coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1}}
	service.HandleUsage(context.Background(), coreusage.Record{Provider: "codex", AuthID: "auth", Model: "gpt-test", ServiceTier: "default", UpstreamAttemptID: "attempt-time", RequestedAt: usageReporterAt, UsageReported: true, Detail: coreusage.Detail{InputTokens: 1, TotalTokens: 1, TokenBreakdown: breakdown}})
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	events := service.UsageEvents()
	if len(events) != 1 || !events[0].RequestedAt.Equal(intentAt) {
		t.Fatalf("usage event requested_at = %#v, want intent time %s", events, intentAt)
	}
	results := service.CostResults()
	if len(results) != 1 || results[0].Cost.PriceVersionID != "price-before" {
		t.Fatalf("cost results = %#v, want price-before", results)
	}
}

func TestServiceDeduplicatesUsageAndSeparatesMissingZeroNotDispatched(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()

	intent := AttemptIntent{UpstreamAttemptID: "attempt-1", RequestID: "request-1", AuthID: "auth-1", DrainMode: "overdraft", DrainCycleID: "cycle-1", DrainRequestKind: "business"}
	if errIntent := service.RecordIntent(context.Background(), intent); errIntent != nil {
		t.Fatal(errIntent)
	}
	record := coreusage.Record{AuthID: "auth-1", Model: "gpt-test", UpstreamAttemptID: "attempt-1", RequestID: "request-1", DrainMode: "overdraft", DrainCycleID: "cycle-1", DrainRequestKind: "business", UsageReported: true, Detail: coreusage.Detail{InputTokens: 2, OutputTokens: 3, TotalTokens: 5}}
	service.HandleUsage(context.Background(), record)
	service.HandleUsage(context.Background(), record)
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	events := store.UsageEvents()
	if len(events) != 1 || events[0].Detail.TotalTokens != 5 {
		t.Fatalf("events = %#v", events)
	}

	zero := record
	zero.UpstreamAttemptID = "attempt-zero"
	zero.UsageReported = true
	zero.Detail = coreusage.Detail{}
	service.HandleUsage(context.Background(), zero)
	missing := record
	missing.UpstreamAttemptID = "attempt-missing"
	missing.UsageReported = false
	missing.Failed = true
	service.HandleUsage(context.Background(), missing)
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if events = store.UsageEvents(); len(events) != 3 {
		t.Fatalf("zero/missing events = %#v", events)
	}
	byID := make(map[string]UsageEvent, len(events))
	for _, event := range events {
		byID[event.UpstreamAttemptID] = event
	}
	if !byID["attempt-zero"].UsageReported || byID["attempt-zero"].Detail.TotalTokens != 0 || byID["attempt-missing"].UsageReported {
		t.Fatalf("zero/missing events = %#v", byID)
	}

	if errClose := service.RecordNotDispatched(context.Background(), "attempt-not-dispatched", "request-1", "auth-1"); errClose != nil {
		t.Fatal(errClose)
	}
	closures := store.Closures()
	if len(closures) != 1 || closures[0].FailureClass != FailureNotDispatched {
		t.Fatalf("closures = %#v", closures)
	}
}

func TestServiceKeepsIndependentUsagePartsForOneAttempt(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	base := coreusage.Record{AuthID: "auth-1", Model: "gpt-test", UpstreamAttemptID: "attempt-parts", UsagePart: "primary", UsageReported: true, Detail: coreusage.Detail{InputTokens: 2, TotalTokens: 2}}
	service.HandleUsage(context.Background(), base)
	additional := base
	additional.Model = "gpt-image-2"
	additional.UsagePart = "model:gpt-image-2"
	additional.Detail = coreusage.Detail{OutputTokens: 3, TotalTokens: 3}
	service.HandleUsage(context.Background(), additional)
	service.HandleUsage(context.Background(), additional)
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	events := store.UsageEvents()
	if len(events) != 2 || events[0].EventID == events[1].EventID {
		t.Fatalf("usage parts = %#v", events)
	}
}

func TestAggregateClassifiesNotDispatchedOnce(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	service.HandleUsage(context.Background(), coreusage.Record{
		AuthID: "auth-1", UpstreamAttemptID: "attempt-1", Failed: true,
		FailureClass: FailureNotDispatched,
	})
	if errClosure := service.RecordClosure(context.Background(), AttemptClosure{UpstreamAttemptID: "attempt-1", AuthID: "auth-1", FailureClass: FailureNotDispatched}); errClosure != nil {
		t.Fatal(errClosure)
	}
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	aggregate := service.Aggregate("auth-1", "")
	if aggregate.NotDispatched != 1 || aggregate.BusinessAttempts != 0 || aggregate.BusinessFailures != 0 {
		t.Fatalf("aggregate = %#v", aggregate)
	}
}

func TestAggregateSeparatesNormalAndOverdraftBusinessUsage(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()

	records := []coreusage.Record{
		{AuthID: "auth-1", UpstreamAttemptID: "normal-success", DrainMode: "normal", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 11}},
		{AuthID: "auth-1", UpstreamAttemptID: "normal-failure", DrainMode: "normal", UsageReported: true, Failed: true, Detail: coreusage.Detail{TotalTokens: 7}},
		{AuthID: "auth-1", UpstreamAttemptID: "overdraft-success", DrainMode: "overdraft", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 23}},
		{AuthID: "auth-1", UpstreamAttemptID: "probe", DrainMode: "overdraft", DrainRequestKind: "exhaustion_probe", UsageReported: true, Failed: true, Detail: coreusage.Detail{TotalTokens: 3}},
	}
	for _, record := range records {
		service.HandleUsage(context.Background(), record)
	}
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}

	aggregate := service.Aggregate("auth-1", "")
	if aggregate.NormalBusinessAttempts != 2 || aggregate.NormalBusinessSuccesses != 1 || aggregate.NormalBusinessFailures != 1 || aggregate.NormalBusinessTotalTokens != 18 {
		t.Fatalf("normal aggregate = %#v", aggregate)
	}
	if aggregate.OverdraftBusinessAttempts != 1 || aggregate.OverdraftBusinessSuccesses != 1 || aggregate.OverdraftBusinessFailures != 0 || aggregate.OverdraftBusinessTotalTokens != 23 {
		t.Fatalf("overdraft aggregate = %#v", aggregate)
	}
	if aggregate.BusinessAttempts != 3 || aggregate.BusinessTotalTokens != 41 || aggregate.ProbeAttempts != 1 || aggregate.ProbeTotalTokens != 3 {
		t.Fatalf("aggregate totals = %#v", aggregate)
	}
}

func TestAggregatesByAuthReturnsSortedIndependentTotals(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()

	for _, record := range []coreusage.Record{
		{AuthID: "auth-b", UpstreamAttemptID: "b-normal", DrainMode: "normal", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 5}},
		{AuthID: "auth-a", UpstreamAttemptID: "a-overdraft", DrainMode: "overdraft", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 9}},
	} {
		service.HandleUsage(context.Background(), record)
	}
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}

	aggregates := service.AggregatesByAuth()
	if len(aggregates) != 2 || aggregates[0].AuthID != "auth-a" || aggregates[1].AuthID != "auth-b" {
		t.Fatalf("aggregates = %#v", aggregates)
	}
	if aggregates[0].OverdraftBusinessTotalTokens != 9 || aggregates[0].NormalBusinessTotalTokens != 0 {
		t.Fatalf("auth-a aggregate = %#v", aggregates[0])
	}
	if aggregates[1].NormalBusinessTotalTokens != 5 || aggregates[1].OverdraftBusinessTotalTokens != 0 {
		t.Fatalf("auth-b aggregate = %#v", aggregates[1])
	}
}

func TestBoltStoreRestoresPricesAndCostResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounting.db")
	store, errOpen := OpenBoltStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	service := NewService(store, 8)
	requestedAt := time.Now().Add(-time.Minute).UTC()
	version, errPrice := service.PutPrice(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: requestedAt.Add(-time.Hour), InputNanoUSDPerMillion: 1_000_000})
	if errPrice != nil {
		t.Fatal(errPrice)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 2, Input: coreusage.TokenInputBreakdown{TotalTokens: 2, UncachedTokens: 2}}
	service.HandleUsage(context.Background(), coreusage.Record{AuthID: "auth", Model: "gpt-test", UpstreamAttemptID: "attempt", UsagePart: "primary", UsageReported: true, RequestedAt: requestedAt, Detail: coreusage.Detail{InputTokens: 2, TotalTokens: 2, TokenBreakdown: breakdown}})
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if errClose := service.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	reopenedStore, errReopen := OpenBoltStore(path)
	if errReopen != nil {
		t.Fatal(errReopen)
	}
	reopened := NewService(reopenedStore, 8)
	defer reopened.Close()
	if _, ok := reopened.Price(version.VersionID); !ok {
		t.Fatalf("price %q was not restored", version.VersionID)
	}
	if results := reopened.CostResults(); len(results) != 1 || results[0].Cost.TotalNanoUSD != 2 {
		t.Fatalf("cost results = %#v", results)
	}
}

func TestRepriceCreatesImmutableResultForEveryRun(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	requestedAt := time.Now().UTC()
	if _, errPrice := service.PutPrice(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: requestedAt.Add(-time.Hour), InputNanoUSDPerMillion: 1_000_000}); errPrice != nil {
		t.Fatal(errPrice)
	}
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 2, Input: coreusage.TokenInputBreakdown{TotalTokens: 2, UncachedTokens: 2}}
	service.HandleUsage(context.Background(), coreusage.Record{AuthID: "auth", Model: "gpt-test", UpstreamAttemptID: "attempt-reprice", UsageReported: true, RequestedAt: requestedAt, Detail: coreusage.Detail{TotalTokens: 2, TokenBreakdown: breakdown}})
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	before := service.CostResults()
	if len(before) != 1 {
		t.Fatalf("initial cost results = %#v", before)
	}
	runID, errReprice := service.Reprice()
	if errReprice != nil {
		t.Fatal(errReprice)
	}
	after := service.CostResults()
	if len(after) != 2 {
		t.Fatalf("repriced cost results = %#v", after)
	}
	if after[0].ResultID == after[1].ResultID {
		t.Fatalf("reprice reused result ID %q", after[0].ResultID)
	}
	foundRun := false
	for _, result := range after {
		if result.PricingRunID == runID {
			foundRun = true
		}
	}
	if !foundRun {
		t.Fatalf("pricing run %q missing from %#v", runID, after)
	}
}

func TestUnpricedUsagePersistsExplicitResult(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 1, Input: coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1}}
	service.HandleUsage(context.Background(), coreusage.Record{AuthID: "auth", Model: "unpriced-model", UpstreamAttemptID: "attempt-unpriced", UsageReported: true, Detail: coreusage.Detail{TotalTokens: 1, TokenBreakdown: breakdown}})
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	results := service.CostResults()
	raw, errJSON := json.Marshal(results)
	if errJSON != nil {
		t.Fatal(errJSON)
	}
	if len(results) != 1 || !strings.Contains(string(raw), `"cost_status":"UNPRICED"`) {
		t.Fatalf("unpriced cost results = %#v", results)
	}
}

func TestPricingRangesAreIndependentByProvider(t *testing.T) {
	from := time.Now().UTC().Format(time.RFC3339Nano)
	var codexPrice, openAIPrice PriceVersion
	if errDecode := json.Unmarshal([]byte(`{"provider":"codex","model":"gpt-test","tier":"default","effective_from":"`+from+`","input_nano_usd_per_million":1}`), &codexPrice); errDecode != nil {
		t.Fatal(errDecode)
	}
	if errDecode := json.Unmarshal([]byte(`{"provider":"openai","model":"gpt-test","tier":"default","effective_from":"`+from+`","input_nano_usd_per_million":2}`), &openAIPrice); errDecode != nil {
		t.Fatal(errDecode)
	}
	book := NewPricingBook()
	if _, errPut := book.Put(codexPrice); errPut != nil {
		t.Fatal(errPut)
	}
	if _, errPut := book.Put(openAIPrice); errPut != nil {
		t.Fatalf("provider-independent overlapping range rejected: %v", errPut)
	}
}

func TestStoresRejectConflictingImmutableDuplicate(t *testing.T) {
	stores := map[string]Store{"memory": NewMemoryStore()}
	boltStore, errOpen := OpenBoltStore(filepath.Join(t.TempDir(), "duplicates.db"))
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	stores["bolt"] = boltStore
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			first := AttemptIntent{UpstreamAttemptID: "attempt", RequestID: "request-a"}
			if errAppend := store.AppendIntent(first); errAppend != nil {
				t.Fatal(errAppend)
			}
			if errAppend := store.AppendIntent(first); errAppend != nil {
				t.Fatalf("identical duplicate error = %v", errAppend)
			}
			conflicting := first
			conflicting.RequestID = "request-b"
			if errAppend := store.AppendIntent(conflicting); errAppend == nil {
				t.Fatal("conflicting immutable duplicate error = nil")
			}
		})
	}
	if errClose := boltStore.Close(); errClose != nil {
		t.Fatal(errClose)
	}
}

func TestMemoryStoreDoesNotExposePriceEnabledPointer(t *testing.T) {
	store := NewMemoryStore()
	enabled := true
	version := PriceVersion{VersionID: "price-1", Model: "gpt-test", Tier: "default", Enabled: &enabled, EffectiveFrom: time.Unix(0, 0).UTC()}
	if errAppend := store.AppendPrice(version); errAppend != nil {
		t.Fatal(errAppend)
	}
	enabled = false
	prices := store.PriceVersions()
	if len(prices) != 1 || prices[0].Enabled == nil || !*prices[0].Enabled {
		t.Fatalf("stored price changed through AppendPrice() input: %#v", prices)
	}
	*prices[0].Enabled = false
	prices = store.PriceVersions()
	if len(prices) != 1 || prices[0].Enabled == nil || !*prices[0].Enabled {
		t.Fatalf("stored price changed through PriceVersions() result: %#v", prices)
	}
}

func TestStoresTreatUsageRedeliveryAsIdempotent(t *testing.T) {
	stores := map[string]Store{"memory": NewMemoryStore()}
	boltStore, errOpen := OpenBoltStore(filepath.Join(t.TempDir(), "usage-redelivery.db"))
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	stores["bolt"] = boltStore
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			first := UsageEvent{
				EventID:           "usage-1",
				UpstreamAttemptID: "attempt-1",
				UsagePart:         "primary",
				UsageReported:     true,
				ObservedAt:        time.Unix(100, 0),
				Detail:            coreusage.Detail{TotalTokens: 7},
			}
			if errAppend := store.AppendUsage(first); errAppend != nil {
				t.Fatal(errAppend)
			}
			redelivered := first
			redelivered.ObservedAt = time.Unix(200, 0)
			if errAppend := store.AppendUsage(redelivered); errAppend != nil {
				t.Fatalf("semantic redelivery error = %v", errAppend)
			}
			conflicting := redelivered
			conflicting.Detail.TotalTokens++
			if errAppend := store.AppendUsage(conflicting); errAppend == nil {
				t.Fatal("conflicting usage redelivery error = nil")
			}
		})
	}
	if errClose := boltStore.Close(); errClose != nil {
		t.Fatal(errClose)
	}
}

func TestServiceIgnoresNonCodexUsage(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 8)
	defer service.Close()
	service.HandleUsage(context.Background(), coreusage.Record{Provider: "claude", AuthID: "auth", UpstreamAttemptID: "attempt-claude", UsageReported: true})
	if errFlush := service.Flush(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if events := service.UsageEvents(); len(events) != 0 {
		t.Fatalf("non-Codex usage events = %#v", events)
	}
}

func TestServiceCloseIsIdempotent(t *testing.T) {
	service := NewService(NewMemoryStore(), 1)
	if errFirst := service.Close(); errFirst != nil {
		t.Fatal(errFirst)
	}
	if errSecond := service.Close(); errSecond != nil {
		t.Fatal(errSecond)
	}
}

func TestAccountingHealthReportsBoltReadFailure(t *testing.T) {
	store, errOpen := OpenBoltStore(filepath.Join(t.TempDir(), "health.db"))
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	service := NewService(store, 1)
	defer service.Close()
	if errClose := store.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	health := service.Health()
	if health.Healthy || health.LastError == "" {
		t.Fatalf("health = %#v, want unhealthy read error", health)
	}
}

func TestServiceReconcileRejectsCorruptAccountingStore(t *testing.T) {
	store, errOpen := OpenBoltStore(filepath.Join(t.TempDir(), "corrupt.db"))
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	if errWrite := store.db.Update(func(tx *bolt.Tx) error {
		root, errRoot := tx.CreateBucketIfNotExists(accountingBucket)
		if errRoot != nil {
			return errRoot
		}
		intents, errIntents := root.CreateBucketIfNotExists([]byte("intents"))
		if errIntents != nil {
			return errIntents
		}
		return intents.Put([]byte("attempt-corrupt"), []byte("{"))
	}); errWrite != nil {
		t.Fatal(errWrite)
	}
	service := NewService(store, 1)
	defer service.Close()
	if errReconcile := service.Reconcile(context.Background()); errReconcile == nil {
		t.Fatal("Reconcile() error = nil, want corrupt-store failure")
	}
}

func TestServiceReconcilesUnclosedIntentAsIndeterminate(t *testing.T) {
	store := NewMemoryStore()
	service := NewService(store, 2)
	if errIntent := service.RecordIntent(context.Background(), AttemptIntent{UpstreamAttemptID: "attempt-crash", RequestID: "request", AuthID: "auth"}); errIntent != nil {
		t.Fatal(errIntent)
	}
	if errReconcile := service.Reconcile(context.Background()); errReconcile != nil {
		t.Fatal(errReconcile)
	}
	closures := store.Closures()
	if len(closures) != 1 || closures[0].FailureClass != FailureIndeterminate {
		t.Fatalf("closures = %#v", closures)
	}
}
