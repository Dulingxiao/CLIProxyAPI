package accounting

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
)

// MemoryStore is a concurrency-safe store useful for tests and embedded hosts.
type MemoryStore struct {
	mu       sync.RWMutex
	intents  map[string]AttemptIntent
	usage    map[string]UsageEvent
	closures map[string]AttemptClosure
	prices   map[string]PriceVersion
	costs    map[string]UsageCostResult
}

// NewMemoryStore creates an empty accounting store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{intents: make(map[string]AttemptIntent), usage: make(map[string]UsageEvent), closures: make(map[string]AttemptClosure), prices: make(map[string]PriceVersion), costs: make(map[string]UsageCostResult)}
}

func (s *MemoryStore) AppendIntent(intent AttemptIntent) error {
	if intent.UpstreamAttemptID == "" {
		return fmt.Errorf("upstream attempt ID is required")
	}
	if intent.SchemaVersion == 0 {
		intent.SchemaVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.intents[intent.UpstreamAttemptID]; exists {
		return immutableDuplicateResult(existing, intent)
	}
	s.intents[intent.UpstreamAttemptID] = intent
	return nil
}

func (s *MemoryStore) AppendUsage(event UsageEvent) error {
	if event.UpstreamAttemptID == "" {
		return fmt.Errorf("upstream attempt ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.EventID == "" {
		return fmt.Errorf("usage event ID is required")
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 1
	}
	if existing, exists := s.usage[event.EventID]; exists {
		if equivalentUsageEvent(existing, event) {
			return nil
		}
		return fmt.Errorf("immutable accounting record conflicts with existing key")
	}
	s.usage[event.EventID] = event
	return nil
}

func equivalentUsageEvent(left, right UsageEvent) bool {
	left.RequestedAt = time.Time{}
	left.ObservedAt = time.Time{}
	right.RequestedAt = time.Time{}
	right.ObservedAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func (s *MemoryStore) AppendClosure(closure AttemptClosure) error {
	if closure.UpstreamAttemptID == "" {
		return fmt.Errorf("upstream attempt ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.closures[closure.UpstreamAttemptID]; exists {
		return immutableDuplicateResult(existing, closure)
	}
	s.closures[closure.UpstreamAttemptID] = closure
	return nil
}

func (s *MemoryStore) AppendPrice(version PriceVersion) error {
	if version.VersionID == "" {
		return fmt.Errorf("price version ID is required")
	}
	version = clonePriceVersion(version)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.prices[version.VersionID]; exists {
		return immutableDuplicateResult(existing, version)
	}
	s.prices[version.VersionID] = version
	return nil
}

func (s *MemoryStore) AppendCost(result UsageCostResult) error {
	if result.ResultID == "" {
		return fmt.Errorf("cost result ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.costs[result.ResultID]; exists {
		return immutableDuplicateResult(existing, result)
	}
	s.costs[result.ResultID] = result
	return nil
}

func (s *MemoryStore) Intent(attemptID string) (AttemptIntent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intent, ok := s.intents[attemptID]
	return intent, ok
}

func (s *MemoryStore) UsageEvents() []UsageEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := make([]UsageEvent, 0, len(s.usage))
	for _, event := range s.usage {
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].EventID < events[j].EventID })
	return events
}

func (s *MemoryStore) Closures() []AttemptClosure {
	s.mu.RLock()
	defer s.mu.RUnlock()
	closures := make([]AttemptClosure, 0, len(s.closures))
	for _, closure := range s.closures {
		closures = append(closures, closure)
	}
	sort.Slice(closures, func(i, j int) bool { return closures[i].UpstreamAttemptID < closures[j].UpstreamAttemptID })
	return closures
}

func (s *MemoryStore) PriceVersions() []PriceVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]PriceVersion, 0, len(s.prices))
	for _, value := range s.prices {
		values = append(values, clonePriceVersion(value))
	}
	sort.Slice(values, func(i, j int) bool { return values[i].VersionID < values[j].VersionID })
	return values
}

func (s *MemoryStore) CostResults() []UsageCostResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values := make([]UsageCostResult, 0, len(s.costs))
	for _, value := range s.costs {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].ResultID < values[j].ResultID })
	return values
}

func (s *MemoryStore) UnclosedIntents() []AttemptIntent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intents := make([]AttemptIntent, 0)
	for ID, intent := range s.intents {
		usageExists := false
		for _, event := range s.usage {
			if event.UpstreamAttemptID == ID {
				usageExists = true
				break
			}
		}
		if usageExists {
			continue
		}
		if _, closureExists := s.closures[ID]; closureExists {
			continue
		}
		intents = append(intents, intent)
	}
	return intents
}

func (s *MemoryStore) Close() error { return nil }

func immutableDuplicateResult(existing, incoming any) error {
	if reflect.DeepEqual(existing, incoming) {
		return nil
	}
	return fmt.Errorf("immutable accounting record conflicts with existing key")
}
