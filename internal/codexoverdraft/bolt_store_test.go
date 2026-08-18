package codexoverdraft

import (
	"path/filepath"
	"testing"
	"time"
)

func TestBoltStoreRestoresFIFOEpochAndProbeFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overdraft.db")
	store, errOpen := OpenBoltStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	clock := &fakeClock{now: time.Now()}
	c := enabledCoordinator(t, clock, store)
	_ = c.RegisterAuth("owner", 1, 0)
	_ = c.RegisterAuth("candidate", 1, 0)
	_ = c.ObserveThreshold("owner", 1, 99_000_000)
	clock.now = clock.now.Add(time.Second)
	_ = c.ObserveThreshold("candidate", 1, 99_000_000)
	lease, _ := c.AcquireBusiness("business")
	_ = c.BeginSend(lease)
	_ = c.BusinessUsageLimit(lease)
	_ = c.Release(lease)
	wantEpoch := c.Epoch()
	if errClose := store.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	reopened, errReopen := OpenBoltStore(path)
	if errReopen != nil {
		t.Fatal(errReopen)
	}
	defer reopened.Close()
	restored := enabledCoordinator(t, clock, reopened)
	if restored.Epoch() <= wantEpoch {
		t.Fatalf("restored epoch = %d, want > %d", restored.Epoch(), wantEpoch)
	}
	if got := restored.Record("owner"); got.State != StateActiveDrain || got.ProbeFailures != 1 {
		t.Fatalf("restored owner = %#v", got)
	} else if !got.CalibrationRequired {
		t.Fatal("restored owner was admitted without active quota calibration")
	}
	if got := restored.Candidates(); len(got) != 1 || got[0].AuthID != "candidate" {
		t.Fatalf("restored candidates = %#v", got)
	}
}

func TestRestartedActiveOwnerRequiresCalibrationBeforeAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overdraft.db")
	store, errOpen := OpenBoltStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	clock := &fakeClock{now: time.Now()}
	first := enabledCoordinator(t, clock, store)
	_ = first.RegisterAuth("auth-1", 1, 0)
	_ = first.ObserveThreshold("auth-1", 1, 99_000_000)

	restarted := enabledCoordinator(t, clock, store)
	if _, errAcquire := restarted.AcquireBusiness("before-calibration"); errAcquire != ErrNoActiveOwner {
		t.Fatalf("AcquireBusiness before calibration error = %v", errAcquire)
	}
	if errCalibration := restarted.ConfirmCalibration("auth-1", 1); errCalibration != nil {
		t.Fatal(errCalibration)
	}
	lease, errAcquire := restarted.AcquireBusiness("after-calibration")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	_ = restarted.Release(lease)
	_ = store.Close()
}

func TestDisabledCoordinatorClearsPersistedOverlay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overdraft.db")
	store, errOpen := OpenBoltStore(path)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	c := enabledCoordinator(t, &fakeClock{now: time.Now()}, store)
	_ = c.RegisterAuth("a", 1, 0)
	_ = c.ObserveThreshold("a", 1, 99_000_000)
	_ = store.Close()

	reopened, errReopen := OpenBoltStore(path)
	if errReopen != nil {
		t.Fatal(errReopen)
	}
	defer reopened.Close()
	disabled, errNew := NewCoordinator(CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}, reopened, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	if len(disabled.Records()) != 0 {
		t.Fatalf("disabled records = %#v", disabled.Records())
	}
}
