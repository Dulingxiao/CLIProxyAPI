package codexoverdraft

import "time"

// Store persists coordinator state atomically.
type Store interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// PersistentState is the atomic durable coordinator snapshot.
type PersistentState struct {
	SchemaVersion          int               `json:"schema_version"`
	CoordinatorEpoch       uint64            `json:"coordinator_epoch"`
	ConfigRevision         uint64            `json:"config_revision"`
	CandidateSequence      uint64            `json:"candidate_sequence"`
	LastCandidateEnteredAt time.Time         `json:"last_candidate_entered_at,omitempty"`
	Owner                  string            `json:"owner,omitempty"`
	Records                map[string]Record `json:"records"`
}
