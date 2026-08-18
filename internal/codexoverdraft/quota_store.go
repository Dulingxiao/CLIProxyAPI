package codexoverdraft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	bolt "go.etcd.io/bbolt"
)

var (
	quotaRootBucket         = []byte("codex_quota")
	quotaObservationsBucket = []byte("observations")
	quotaLatestBucket       = []byte("latest")
	quotaGenerationsBucket  = []byte("generations")
)

// QuotaStore persists immutable quota observations and the latest accepted snapshot.
type QuotaStore struct {
	db *bolt.DB
}

// SetGeneration advances the durable lifecycle fence for one Auth.ID.
func (s *QuotaStore) SetGeneration(authID string, generation uint64) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("quota store is not available")
	}
	if authID == "" || generation == 0 {
		return fmt.Errorf("quota auth ID and generation are required")
	}
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, generation)
	return s.db.Update(func(tx *bolt.Tx) error {
		root, errRoot := tx.CreateBucketIfNotExists(quotaRootBucket)
		if errRoot != nil {
			return fmt.Errorf("create quota root bucket: %w", errRoot)
		}
		generations, errGenerations := root.CreateBucketIfNotExists(quotaGenerationsBucket)
		if errGenerations != nil {
			return fmt.Errorf("create quota generations bucket: %w", errGenerations)
		}
		if current := generations.Get([]byte(authID)); len(current) == 8 {
			currentGeneration := binary.BigEndian.Uint64(current)
			if currentGeneration > generation {
				return fmt.Errorf("quota auth generation cannot move backward")
			}
			if currentGeneration == generation {
				return nil
			}
		}
		if errPut := generations.Put([]byte(authID), encoded); errPut != nil {
			return fmt.Errorf("write quota auth generation: %w", errPut)
		}
		return nil
	})
}

// Generations returns all durable Auth.ID lifecycle fences.
func (s *QuotaStore) Generations() (map[string]uint64, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("quota store is not available")
	}
	values := make(map[string]uint64)
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(quotaRootBucket)
		if root == nil {
			return nil
		}
		generations := root.Bucket(quotaGenerationsBucket)
		if generations == nil {
			return nil
		}
		return generations.ForEach(func(key, raw []byte) error {
			if len(raw) != 8 {
				return fmt.Errorf("decode quota auth generation %q", string(key))
			}
			values[string(key)] = binary.BigEndian.Uint64(raw)
			return nil
		})
	})
	return values, errView
}

// OpenQuotaStore opens or creates an independent durable quota store.
func OpenQuotaStore(path string) (*QuotaStore, error) {
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create quota store directory: %w", errMkdir)
	}
	db, errOpen := bolt.Open(path, 0o600, nil)
	if errOpen != nil {
		return nil, fmt.Errorf("open quota store: %w", errOpen)
	}
	return &QuotaStore{db: db}, nil
}

// Append stores one immutable observation and optionally advances the accepted latest value.
func (s *QuotaStore) Append(snapshot QuotaSnapshot, latest bool) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("quota store is not available")
	}
	if snapshot.AuthID == "" {
		return fmt.Errorf("quota auth ID is required")
	}
	if snapshot.Sequence == 0 {
		return fmt.Errorf("quota observation sequence is required")
	}
	raw, errMarshal := json.Marshal(snapshot)
	if errMarshal != nil {
		return fmt.Errorf("encode quota observation: %w", errMarshal)
	}
	sequenceKey := make([]byte, 8)
	binary.BigEndian.PutUint64(sequenceKey, snapshot.Sequence)
	return s.db.Update(func(tx *bolt.Tx) error {
		root, errRoot := tx.CreateBucketIfNotExists(quotaRootBucket)
		if errRoot != nil {
			return fmt.Errorf("create quota root bucket: %w", errRoot)
		}
		observations, errObservations := root.CreateBucketIfNotExists(quotaObservationsBucket)
		if errObservations != nil {
			return fmt.Errorf("create quota observations bucket: %w", errObservations)
		}
		if existing := observations.Get(sequenceKey); existing != nil {
			if !bytes.Equal(existing, raw) {
				return fmt.Errorf("immutable quota observation sequence %d conflicts with existing record", snapshot.Sequence)
			}
		} else if errPut := observations.Put(sequenceKey, raw); errPut != nil {
			return fmt.Errorf("write quota observation: %w", errPut)
		}
		if !latest {
			return nil
		}
		latestBucket, errLatest := root.CreateBucketIfNotExists(quotaLatestBucket)
		if errLatest != nil {
			return fmt.Errorf("create quota latest bucket: %w", errLatest)
		}
		if existing := latestBucket.Get([]byte(snapshot.AuthID)); existing != nil {
			var current QuotaSnapshot
			if errDecode := json.Unmarshal(existing, &current); errDecode != nil {
				return fmt.Errorf("decode latest quota snapshot: %w", errDecode)
			}
			if current.Sequence > snapshot.Sequence {
				return nil
			}
		}
		if errPut := latestBucket.Put([]byte(snapshot.AuthID), raw); errPut != nil {
			return fmt.Errorf("write latest quota snapshot: %w", errPut)
		}
		return nil
	})
}

// MaxSequence returns the largest durably assigned observation sequence.
func (s *QuotaStore) MaxSequence() (uint64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("quota store is not available")
	}
	var sequence uint64
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(quotaRootBucket)
		if root == nil {
			return nil
		}
		observations := root.Bucket(quotaObservationsBucket)
		if observations == nil {
			return nil
		}
		key, _ := observations.Cursor().Last()
		if len(key) == 8 {
			sequence = binary.BigEndian.Uint64(key)
		}
		return nil
	})
	return sequence, errView
}

// Latest returns the latest accepted snapshot for one Auth.ID.
func (s *QuotaStore) Latest(authID string) (QuotaSnapshot, bool, error) {
	if s == nil || s.db == nil {
		return QuotaSnapshot{}, false, fmt.Errorf("quota store is not available")
	}
	var snapshot QuotaSnapshot
	found := false
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(quotaRootBucket)
		if root == nil {
			return nil
		}
		latest := root.Bucket(quotaLatestBucket)
		if latest == nil {
			return nil
		}
		raw := latest.Get([]byte(authID))
		if len(raw) == 0 {
			return nil
		}
		if errDecode := json.Unmarshal(raw, &snapshot); errDecode != nil {
			return fmt.Errorf("decode latest quota snapshot: %w", errDecode)
		}
		found = true
		return nil
	})
	return snapshot, found, errView
}

// LatestSnapshots returns all latest accepted snapshots keyed by Auth.ID.
func (s *QuotaStore) LatestSnapshots() (map[string]QuotaSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("quota store is not available")
	}
	values := make(map[string]QuotaSnapshot)
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(quotaRootBucket)
		if root == nil {
			return nil
		}
		latest := root.Bucket(quotaLatestBucket)
		if latest == nil {
			return nil
		}
		return latest.ForEach(func(key, raw []byte) error {
			var snapshot QuotaSnapshot
			if errDecode := json.Unmarshal(raw, &snapshot); errDecode != nil {
				return fmt.Errorf("decode latest quota snapshot %q: %w", string(key), errDecode)
			}
			values[string(key)] = snapshot
			return nil
		})
	})
	return values, errView
}

// History returns all immutable observations ordered by sequence.
func (s *QuotaStore) History() ([]QuotaSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("quota store is not available")
	}
	var values []QuotaSnapshot
	errView := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(quotaRootBucket)
		if root == nil {
			return nil
		}
		observations := root.Bucket(quotaObservationsBucket)
		if observations == nil {
			return nil
		}
		return observations.ForEach(func(_, raw []byte) error {
			var snapshot QuotaSnapshot
			if errDecode := json.Unmarshal(raw, &snapshot); errDecode != nil {
				return fmt.Errorf("decode quota observation: %w", errDecode)
			}
			values = append(values, snapshot)
			return nil
		})
	})
	sort.Slice(values, func(i, j int) bool { return values[i].Sequence < values[j].Sequence })
	return values, errView
}

// Close flushes and closes the quota database.
func (s *QuotaStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
