package accounting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var accountingBucket = []byte("accounting")

// BoltStore persists immutable accounting records in separate buckets.
type BoltStore struct {
	db *bolt.DB

	healthMu sync.RWMutex
	readErr  error
}

func OpenBoltStore(path string) (*BoltStore, error) {
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create accounting store directory: %w", errMkdir)
	}
	db, errOpen := bolt.Open(path, 0o600, nil)
	if errOpen != nil {
		return nil, fmt.Errorf("open accounting store: %w", errOpen)
	}
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) AppendIntent(intent AttemptIntent) error {
	if intent.SchemaVersion == 0 {
		intent.SchemaVersion = 1
	}
	return s.put("intents", intent.UpstreamAttemptID, intent)
}

func (s *BoltStore) AppendUsage(event UsageEvent) error {
	if event.SchemaVersion == 0 {
		event.SchemaVersion = 1
	}
	return s.putEquivalent("usage", event.EventID, event, func(existing []byte) bool {
		var stored UsageEvent
		return json.Unmarshal(existing, &stored) == nil && equivalentUsageEvent(stored, event)
	})
}

func (s *BoltStore) AppendClosure(closure AttemptClosure) error {
	return s.put("closures", closure.UpstreamAttemptID, closure)
}

func (s *BoltStore) AppendPrice(version PriceVersion) error {
	return s.put("prices", version.VersionID, version)
}

func (s *BoltStore) AppendCost(result UsageCostResult) error {
	return s.put("costs", result.ResultID, result)
}

func (s *BoltStore) put(bucketName, key string, value any) error {
	return s.putEquivalent(bucketName, key, value, nil)
}

func (s *BoltStore) putEquivalent(bucketName, key string, value any, equivalent func([]byte) bool) error {
	if key == "" {
		return fmt.Errorf("accounting key is required")
	}
	raw, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return errMarshal
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		root, errRoot := tx.CreateBucketIfNotExists(accountingBucket)
		if errRoot != nil {
			return errRoot
		}
		bucket, errBucket := root.CreateBucketIfNotExists([]byte(bucketName))
		if errBucket != nil {
			return errBucket
		}
		if existing := bucket.Get([]byte(key)); existing != nil {
			if bytes.Equal(existing, raw) || equivalent != nil && equivalent(existing) {
				return nil
			}
			return fmt.Errorf("immutable accounting record %q conflicts with existing key", key)
		}
		return bucket.Put([]byte(key), raw)
	})
}

func (s *BoltStore) Intent(id string) (AttemptIntent, bool) {
	var value AttemptIntent
	ok := s.get("intents", id, &value)
	return value, ok
}

func (s *BoltStore) UsageEvents() []UsageEvent {
	var values []UsageEvent
	s.list("usage", func(raw []byte) error {
		var value UsageEvent
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	})
	sort.Slice(values, func(i, j int) bool { return values[i].EventID < values[j].EventID })
	return values
}

func (s *BoltStore) Closures() []AttemptClosure {
	var values []AttemptClosure
	s.list("closures", func(raw []byte) error {
		var value AttemptClosure
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	})
	sort.Slice(values, func(i, j int) bool { return values[i].UpstreamAttemptID < values[j].UpstreamAttemptID })
	return values
}

func (s *BoltStore) PriceVersions() []PriceVersion {
	var values []PriceVersion
	s.list("prices", func(raw []byte) error {
		var value PriceVersion
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	})
	sort.Slice(values, func(i, j int) bool { return values[i].VersionID < values[j].VersionID })
	return values
}

func (s *BoltStore) CostResults() []UsageCostResult {
	var values []UsageCostResult
	s.list("costs", func(raw []byte) error {
		var value UsageCostResult
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		values = append(values, value)
		return nil
	})
	sort.Slice(values, func(i, j int) bool { return values[i].ResultID < values[j].ResultID })
	return values
}

func (s *BoltStore) UnclosedIntents() []AttemptIntent {
	all := make(map[string]AttemptIntent)
	s.list("intents", func(raw []byte) error {
		var value AttemptIntent
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		all[value.UpstreamAttemptID] = value
		return nil
	})
	for _, event := range s.UsageEvents() {
		delete(all, event.UpstreamAttemptID)
	}
	for _, closure := range s.Closures() {
		delete(all, closure.UpstreamAttemptID)
	}
	values := make([]AttemptIntent, 0, len(all))
	for _, value := range all {
		values = append(values, value)
	}
	return values
}

func (s *BoltStore) get(bucketName, key string, destination any) bool {
	if key == "" {
		return false
	}
	ok := false
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(accountingBucket)
		if root == nil {
			return nil
		}
		bucket := root.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}
		raw := bucket.Get([]byte(key))
		if len(raw) == 0 {
			return nil
		}
		if json.Unmarshal(raw, destination) == nil {
			ok = true
		}
		return nil
	})
	s.setReadError(errView)
	return ok
}

func (s *BoltStore) list(bucketName string, fn func([]byte) error) {
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(accountingBucket)
		if root == nil {
			return nil
		}
		bucket := root.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error { return fn(append([]byte(nil), value...)) })
	})
	s.setReadError(errView)
}

func (s *BoltStore) setReadError(err error) {
	if s == nil || err == nil {
		return
	}
	s.healthMu.Lock()
	s.readErr = err
	s.healthMu.Unlock()
}

// HealthError reports the latest durable read or decode failure.
func (s *BoltStore) HealthError() error {
	if s == nil {
		return fmt.Errorf("accounting store is not available")
	}
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.readErr
}

func (s *BoltStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
