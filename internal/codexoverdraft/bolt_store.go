package codexoverdraft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var (
	coordinatorBucket = []byte("codex_overdraft")
	coordinatorKey    = []byte("state")
)

// BoltStore persists overdraft state in a dedicated Bolt database.
type BoltStore struct {
	db *bolt.DB
}

// OpenBoltStore opens or creates a durable overdraft state store.
func OpenBoltStore(path string) (*BoltStore, error) {
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create overdraft store directory: %w", errMkdir)
	}
	db, errOpen := bolt.Open(path, 0o600, nil)
	if errOpen != nil {
		return nil, fmt.Errorf("open overdraft store: %w", errOpen)
	}
	return &BoltStore{db: db}, nil
}

// Load reads the latest complete coordinator state.
func (s *BoltStore) Load() (PersistentState, error) {
	var state PersistentState
	errView := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(coordinatorBucket)
		if bucket == nil {
			return nil
		}
		raw := bucket.Get(coordinatorKey)
		if len(raw) == 0 {
			return nil
		}
		if errUnmarshal := json.Unmarshal(raw, &state); errUnmarshal != nil {
			return fmt.Errorf("decode coordinator state: %w", errUnmarshal)
		}
		return nil
	})
	if errView != nil {
		return PersistentState{}, errView
	}
	return state, nil
}

// Save atomically replaces the coordinator state.
func (s *BoltStore) Save(state PersistentState) error {
	raw, errMarshal := json.Marshal(state)
	if errMarshal != nil {
		return fmt.Errorf("encode coordinator state: %w", errMarshal)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket, errBucket := tx.CreateBucketIfNotExists(coordinatorBucket)
		if errBucket != nil {
			return fmt.Errorf("create coordinator bucket: %w", errBucket)
		}
		if errPut := bucket.Put(coordinatorKey, raw); errPut != nil {
			return fmt.Errorf("write coordinator state: %w", errPut)
		}
		return nil
	})
}

// Close flushes and closes the database.
func (s *BoltStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
