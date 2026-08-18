package codexoverdraft

import (
	"path/filepath"
	"testing"
	"time"
)

func TestQuotaStorePersistsHistoryLatestAndSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quota.db")
	store, errOpen := OpenQuotaStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	first := QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		ObservedAt:     time.Unix(100, 0),
		Sequence:       7,
		Windows:        []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 90_000_000}},
	}
	stale := first
	stale.Sequence = 8
	stale.ObservedAt = time.Unix(90, 0)
	stale.Windows = []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 80_000_000}}
	if errAppend := store.Append(first, true); errAppend != nil {
		t.Fatal(errAppend)
	}
	if errAppend := store.Append(stale, false); errAppend != nil {
		t.Fatal(errAppend)
	}
	if errClose := store.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	store, errOpen = OpenQuotaStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer store.Close()
	if got, errMax := store.MaxSequence(); errMax != nil || got != 8 {
		t.Fatalf("MaxSequence() = %d, %v; want 8, nil", got, errMax)
	}
	latest, okLatest, errLatest := store.Latest("auth-1")
	if errLatest != nil || !okLatest {
		t.Fatalf("Latest() = %#v, %t, %v", latest, okLatest, errLatest)
	}
	if latest.Sequence != first.Sequence || latest.ObservedAt != first.ObservedAt {
		t.Fatalf("latest = %#v, want first accepted snapshot", latest)
	}
	history, errHistory := store.History()
	if errHistory != nil {
		t.Fatal(errHistory)
	}
	if len(history) != 2 || history[0].Sequence != 7 || history[1].Sequence != 8 {
		t.Fatalf("history = %#v", history)
	}
}

func TestQuotaStoreRejectsConflictingSequence(t *testing.T) {
	store, errOpen := OpenQuotaStore(filepath.Join(t.TempDir(), "quota.db"))
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer store.Close()
	first := QuotaSnapshot{AuthID: "auth-1", AuthGeneration: 1, Sequence: 1, ObservedAt: time.Unix(1, 0)}
	if errAppend := store.Append(first, true); errAppend != nil {
		t.Fatal(errAppend)
	}
	if errAppend := store.Append(first, true); errAppend != nil {
		t.Fatalf("identical append error = %v", errAppend)
	}
	conflicting := first
	conflicting.AuthID = "auth-2"
	if errAppend := store.Append(conflicting, true); errAppend == nil {
		t.Fatal("conflicting sequence error = nil")
	}
}

func TestQuotaStorePersistsMonotonicAuthGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quota.db")
	store, errOpen := OpenQuotaStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	if errSet := store.SetGeneration("auth-1", 2); errSet != nil {
		t.Fatal(errSet)
	}
	if errSet := store.SetGeneration("auth-1", 1); errSet == nil {
		t.Fatal("generation rollback error = nil")
	}
	if errClose := store.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	store, errOpen = OpenQuotaStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer store.Close()
	generations, errGenerations := store.Generations()
	if errGenerations != nil {
		t.Fatal(errGenerations)
	}
	if got := generations["auth-1"]; got != 2 {
		t.Fatalf("generation = %d, want 2", got)
	}
}
