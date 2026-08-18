package codexoverdraft

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type failAfterStore struct {
	state     PersistentState
	saveCount int
	failAfter int
}

func (s *failAfterStore) Load() (PersistentState, error) { return s.state, nil }

func (s *failAfterStore) Save(state PersistentState) error {
	s.saveCount++
	if s.saveCount > s.failAfter {
		return errors.New("injected save failure")
	}
	s.state = state
	s.state.Records = make(map[string]Record, len(state.Records))
	for authID, record := range state.Records {
		s.state.Records[authID] = record
	}
	return nil
}

func enabledCoordinator(t *testing.T, clock Clock, store Store) *Coordinator {
	t.Helper()
	coordinator, errNew := NewCoordinator(CoordinatorConfig{
		Enabled:                 true,
		ThresholdMicropct:       98_000_000,
		MaxInFlight:             40,
		ExhaustionProbeFailures: 10,
	}, store, clock)
	if errNew != nil {
		t.Fatalf("NewCoordinator() error = %v", errNew)
	}
	return coordinator
}

func TestCoordinatorFIFOAndOwnerPromotion(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)}
	c := enabledCoordinator(t, clock, nil)
	for _, id := range []string{"a", "b", "c"} {
		if errRegister := c.RegisterAuth(id, 1, 0); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	if errObserve := c.ObserveThreshold("a", 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	if got := c.Record("a").State; got != StateActiveDrain {
		t.Fatalf("a state = %s, want ACTIVE_DRAIN", got)
	}

	clock.now = clock.now.Add(time.Second)
	if errObserve := c.ObserveThreshold("b", 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	clock.now = clock.now.Add(time.Second)
	if errObserve := c.ObserveThreshold("c", 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	if got := c.Candidates(); len(got) != 2 || got[0].AuthID != "b" || got[1].AuthID != "c" {
		t.Fatalf("Candidates() = %#v", got)
	}

	if errDisabled := c.SetDisabled("a", 1, true); errDisabled != nil {
		t.Fatal(errDisabled)
	}
	if got := c.Record("b").State; got != StateActiveDrain {
		t.Fatalf("b state = %s, want ACTIVE_DRAIN", got)
	}
	if errExhausted := c.ForceExhausted("b", 1); errExhausted != nil {
		t.Fatal(errExhausted)
	}
	if got := c.Record("c").State; got != StateActiveDrain {
		t.Fatalf("c state = %s, want ACTIVE_DRAIN", got)
	}
}

func TestCoordinatorPromotionSkipsCalibrationRequiredCandidates(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)}
	store := &failAfterStore{failAfter: 1 << 30}
	c := enabledCoordinator(t, clock, store)
	for _, id := range []string{"owner", "head", "tail"} {
		if errRegister := c.RegisterAuth(id, 1, 0); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	_ = c.ObserveThreshold("owner", 1, 99_000_000)
	clock.now = clock.now.Add(time.Second)
	_ = c.ObserveThreshold("head", 1, 99_000_000)
	clock.now = clock.now.Add(time.Second)
	_ = c.ObserveThreshold("tail", 1, 99_000_000)

	// Restarting marks every persisted non-normal record as calibration-required.
	restarted := enabledCoordinator(t, clock, store)
	if errRecover := restarted.ConfirmRecovery("owner", 1); errRecover != nil {
		t.Fatal(errRecover)
	}
	if got := restarted.Owner(); got != "" {
		t.Fatalf("owner = %q, want empty while all candidates need calibration", got)
	}
	if errCalibrate := restarted.ConfirmCalibration("tail", 1); errCalibrate != nil {
		t.Fatal(errCalibrate)
	}
	if got := restarted.Record("tail").State; got != StateActiveDrain {
		t.Fatalf("tail state = %s, want ACTIVE_DRAIN past uncalibrated head", got)
	}
	if head := restarted.Record("head"); head.State != StateCandidate || !head.CalibrationRequired {
		t.Fatalf("head record = %#v, want calibration-required candidate", head)
	}
}

func TestCoordinatorExposesCandidateAndDrainCycleMetrics(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)}
	c := enabledCoordinator(t, clock, nil)
	_ = c.RegisterAuth("owner", 1, 0)
	_ = c.RegisterAuth("queued", 1, 0)
	_ = c.ObserveThreshold("owner", 1, 98_500_000)
	_ = c.ObserveThreshold("queued", 1, 98_750_000)
	clock.now = clock.now.Add(15 * time.Second)
	queue := c.CandidateQueueMetrics()
	if queue.Length != 1 || queue.HeadWait != 15*time.Second || c.Record("queued").CandidateUsedMicropct != 98_750_000 {
		t.Fatalf("queue metrics = %#v record = %#v", queue, c.Record("queued"))
	}
	lease, errAcquire := c.AcquireBusiness("first")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	_ = c.BeginSend(lease)
	_ = c.BusinessUsageLimit(lease)
	record := c.Record("owner")
	if record.BusinessAttempts != 1 || !record.EnteredAlreadyFull || record.CandidateUsedMicropct != 98_500_000 {
		t.Fatalf("drain cycle metrics = %#v", record)
	}
	_ = c.Release(lease)
}

func TestCoordinatorCandidateClockRollbackPreservesFIFO(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)}
	c := enabledCoordinator(t, clock, nil)
	for _, id := range []string{"owner", "first", "second"} {
		if errRegister := c.RegisterAuth(id, 1, 0); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	_ = c.ObserveThreshold("owner", 1, 99_000_000)
	clock.now = clock.now.Add(time.Minute)
	_ = c.ObserveThreshold("first", 1, 99_000_000)
	clock.now = clock.now.Add(-time.Hour)
	_ = c.ObserveThreshold("second", 1, 99_000_000)

	candidates := c.Candidates()
	if len(candidates) != 2 || candidates[0].AuthID != "first" || candidates[1].AuthID != "second" {
		t.Fatalf("Candidates() = %#v", candidates)
	}
	if candidates[1].CandidateEnteredAt.Before(candidates[0].CandidateEnteredAt) || candidates[1].CandidateSequence <= candidates[0].CandidateSequence {
		t.Fatalf("candidate ordering fields = %#v", candidates)
	}
}

func TestCoordinatorLeaseFenceLimitAndSwitch(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}, nil, clock)
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = c.RegisterAuth("a", 1, 0)
	_ = c.ObserveThreshold("a", 1, 99_000_000)

	lease1, errLease1 := c.AcquireBusiness("dispatch-1")
	if errLease1 != nil {
		t.Fatal(errLease1)
	}
	lease2, errLease2 := c.AcquireBusiness("dispatch-2")
	if errLease2 != nil {
		t.Fatal(errLease2)
	}
	if _, errLease3 := c.AcquireBusiness("dispatch-3"); errLease3 != ErrCapacity {
		t.Fatalf("AcquireBusiness() error = %v, want ErrCapacity", errLease3)
	}
	if errBegin := c.BeginSend(lease1); errBegin != nil {
		t.Fatalf("BeginSend(lease1) error = %v", errBegin)
	}
	if errApply := c.ApplyConfig(2, CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}); errApply != nil {
		t.Fatal(errApply)
	}
	if errBegin := c.BeginSend(lease2); errBegin != ErrStaleLease {
		t.Fatalf("BeginSend(lease2) error = %v, want ErrStaleLease", errBegin)
	}
	if errRelease := c.Release(lease1); errRelease != nil {
		t.Fatal(errRelease)
	}
	if c.Enabled() || len(c.Records()) != 0 {
		t.Fatalf("disabled coordinator retains overlay: %#v", c.Records())
	}
}

func TestCoordinatorLeaseRevalidatesEveryActualSend(t *testing.T) {
	c := enabledCoordinator(t, &fakeClock{now: time.Now()}, nil)
	_ = c.RegisterAuth("auth", 1, 0)
	_ = c.ObserveThreshold("auth", 1, 99_000_000)
	lease, errAcquire := c.AcquireBusiness("dispatch")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	if errBegin := c.BeginSend(lease); errBegin != nil {
		t.Fatalf("BeginSend(first) error = %v", errBegin)
	}
	if errBegin := c.BeginSend(lease); errBegin != nil {
		t.Fatalf("BeginSend(retry) error = %v, want same fenced lease", errBegin)
	}
	if errDisable := c.ApplyConfig(2, CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}); errDisable != nil {
		t.Fatal(errDisable)
	}
	if errBegin := c.BeginSend(lease); errBegin != ErrStaleLease {
		t.Fatalf("BeginSend(after disable) error = %v, want ErrStaleLease", errBegin)
	}
}

func TestCoordinatorTenBusinessUsageLimitsExhaust(t *testing.T) {
	c := enabledCoordinator(t, &fakeClock{now: time.Now()}, nil)
	_ = c.RegisterAuth("a", 1, 0)
	_ = c.ObserveThreshold("a", 1, 99_000_000)
	for i := 1; i <= 9; i++ {
		lease, errLease := c.AcquireBusiness(fmt.Sprintf("dispatch-%d", i))
		if errLease != nil {
			t.Fatal(errLease)
		}
		if errBegin := c.BeginSend(lease); errBegin != nil {
			t.Fatal(errBegin)
		}
		if errLimit := c.BusinessUsageLimit(lease); errLimit != nil {
			t.Fatal(errLimit)
		}
		_ = c.Release(lease)
		if got := c.Record("a"); got.State != StateActiveDrain || got.ProbeFailures != i {
			t.Fatalf("after %d usage-limit 429s record = %#v", i, got)
		}
	}
	lease, errLease := c.AcquireBusiness("dispatch-10")
	if errLease != nil {
		t.Fatal(errLease)
	}
	_ = c.BeginSend(lease)
	_ = c.BusinessUsageLimit(lease)
	_ = c.Release(lease)
	if got := c.Record("a"); got.State != StateExhausted || got.ProbeFailures != 10 {
		t.Fatalf("exhausted record = %#v", got)
	}
	if c.Owner() != "" {
		t.Fatalf("Owner() = %q, want empty after exhaustion", c.Owner())
	}
	if errRecover := c.ConfirmRecovery("a", 1); errRecover != nil {
		t.Fatal(errRecover)
	}
	if got := c.Record("a"); got.State != StateNormal || got.ProbeFailures != 0 || got.DrainCycleID != "" {
		t.Fatalf("recovered record = %#v", got)
	}
}

func TestCoordinatorBusinessSuccessResetsUsageLimitStreak(t *testing.T) {
	c := enabledCoordinator(t, &fakeClock{now: time.Now()}, nil)
	_ = c.RegisterAuth("a", 1, 0)
	_ = c.ObserveThreshold("a", 1, 99_000_000)
	limitLease, _ := c.AcquireBusiness("limit")
	_ = c.BeginSend(limitLease)
	_ = c.BusinessUsageLimit(limitLease)
	_ = c.Release(limitLease)
	okLease, _ := c.AcquireBusiness("ok")
	_ = c.BeginSend(okLease)
	if errComplete := c.BusinessCompleted(okLease); errComplete != nil {
		t.Fatal(errComplete)
	}
	_ = c.Release(okLease)
	if got := c.Record("a"); got.State != StateActiveDrain || got.ProbeFailures != 0 {
		t.Fatalf("record = %#v", got)
	}
}

func TestCoordinatorHeadBlockingOnInheritedInflight(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}, nil, clock)
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = c.RegisterAuth("owner", 1, 0)
	_ = c.RegisterAuth("head", 1, 3)
	_ = c.RegisterAuth("tail", 1, 0)
	_ = c.ObserveThreshold("owner", 1, 99_000_000)
	_ = c.ObserveThreshold("head", 1, 99_000_000)
	_ = c.ObserveThreshold("tail", 1, 99_000_000)
	_ = c.SetDisabled("owner", 1, true)
	if c.Owner() != "" || c.Record("head").State != StateCandidate || c.Record("tail").State != StateCandidate {
		t.Fatalf("head blocking failed: owner=%q records=%#v", c.Owner(), c.Records())
	}
	if errInflight := c.UpdateInheritedInFlight("head", 1, 2); errInflight != nil {
		t.Fatal(errInflight)
	}
	if c.Owner() != "head" {
		t.Fatalf("Owner() = %q, want head", c.Owner())
	}
}

func TestCoordinatorOldOwnerInheritedInflightReleasesGlobalCapacity(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}, nil, clock)
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = c.RegisterAuth("old-owner", 1, 1)
	_ = c.RegisterAuth("next-owner", 1, 0)
	_ = c.ObserveThreshold("old-owner", 1, 99_000_000)
	_ = c.ObserveThreshold("next-owner", 1, 99_000_000)
	_ = c.ForceExhausted("old-owner", 1)

	first, errFirst := c.AcquireBusiness("first")
	if errFirst != nil {
		t.Fatalf("AcquireBusiness(first) error = %v", errFirst)
	}
	if _, errFull := c.AcquireBusiness("full"); errFull != ErrCapacity {
		t.Fatalf("AcquireBusiness(full) error = %v, want ErrCapacity", errFull)
	}
	if errUpdate := c.UpdateInheritedInFlight("old-owner", 1, 0); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	second, errSecond := c.AcquireBusiness("second")
	if errSecond != nil {
		t.Fatalf("AcquireBusiness(second) after inherited release error = %v", errSecond)
	}
	_ = c.Release(first)
	_ = c.Release(second)
}

func TestCoordinatorRemovedAuthInheritedRequestsKeepGlobalCapacity(t *testing.T) {
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 3, ExhaustionProbeFailures: 10}, nil, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = c.RegisterAuth("removed", 1, 2)
	_ = c.RegisterAuth("next", 1, 0)
	_ = c.ObserveThreshold("removed", 1, 99_000_000)
	_ = c.ObserveThreshold("next", 1, 99_000_000)
	if errRemove := c.RemoveAuth("removed", 1); errRemove != nil {
		t.Fatal(errRemove)
	}
	if got := c.Owner(); got != "next" {
		t.Fatalf("owner = %q, want next", got)
	}
	lease, errAcquire := c.AcquireBusiness("one-slot")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	if _, errAcquire := c.AcquireBusiness("over-capacity"); errAcquire != ErrCapacity {
		t.Fatalf("AcquireBusiness() error = %v, want ErrCapacity", errAcquire)
	}
	if errUpdate := c.UpdateRetiredInheritedInFlight("removed", 1, 0); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if _, errAcquire := c.AcquireBusiness("released-capacity"); errAcquire != nil {
		t.Fatalf("AcquireBusiness() after retired release error = %v", errAcquire)
	}
	_ = c.Release(lease)
}

func TestCoordinatorPersistFailureDoesNotPublishUncommittedTransition(t *testing.T) {
	store := &failAfterStore{failAfter: 2}
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}, store, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	if errRegister := c.RegisterAuth("auth", 1, 0); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errObserve := c.ObserveThreshold("auth", 1, 99_000_000); errObserve == nil {
		t.Fatal("ObserveThreshold() error = nil, want persistence failure")
	}
	if got := c.Record("auth").State; got != StateNormal {
		t.Fatalf("Record(auth).State = %s, want durable NORMAL", got)
	}
	if got := c.Owner(); got != "" {
		t.Fatalf("Owner() = %q, want empty durable owner", got)
	}
}

func TestCoordinatorDisableClosesMemoryGateWhenPersistenceFails(t *testing.T) {
	store := &failAfterStore{failAfter: 2}
	c, errNew := NewCoordinator(CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}, store, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	if errRegister := c.RegisterAuth("auth", 1, 0); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errDisable := c.ApplyConfig(2, CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 2, ExhaustionProbeFailures: 10}); errDisable == nil {
		t.Fatal("ApplyConfig(disabled) error = nil, want persistence failure")
	}
	if c.Enabled() {
		t.Fatal("memory admission gate remained enabled after disable persistence failure")
	}
	if _, errAcquire := c.AcquireBusiness("after-disable"); errAcquire != ErrNoActiveOwner {
		t.Fatalf("AcquireBusiness() error = %v, want ErrNoActiveOwner", errAcquire)
	}
	if records := c.Records(); len(records) != 0 {
		t.Fatalf("disabled memory overlay = %#v", records)
	}
}

func TestCoordinatorDisabledModeDoesNotRecreateOverlayOnAuthRegistration(t *testing.T) {
	c := enabledCoordinator(t, &fakeClock{now: time.Now()}, nil)
	if errDisable := c.ApplyConfig(2, CoordinatorConfig{Enabled: false, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}); errDisable != nil {
		t.Fatal(errDisable)
	}
	if errRegister := c.RegisterAuth("new-auth", 1, 3); errRegister != nil {
		t.Fatal(errRegister)
	}
	if records := c.Records(); len(records) != 0 {
		t.Fatalf("disabled coordinator records = %#v", records)
	}
}

func TestCoordinatorApplyIdenticalConfigPreservesLeaseFence(t *testing.T) {
	config := CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 40, ExhaustionProbeFailures: 10}
	coordinator, errNew := NewCoordinator(config, nil, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	if errRegister := coordinator.RegisterAuth("auth-1", 1, 0); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errObserve := coordinator.ObserveThreshold("auth-1", 1, 99_000_000); errObserve != nil {
		t.Fatal(errObserve)
	}
	lease, errAcquire := coordinator.AcquireBusiness("dispatch-1")
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	if errApply := coordinator.ApplyConfig(2, config); errApply != nil {
		t.Fatal(errApply)
	}
	if errBegin := coordinator.BeginSend(lease); errBegin != nil {
		t.Fatalf("identical config invalidated an admitted request: %v", errBegin)
	}
}

func TestCoordinatorPerAuthBusinessLimitIsIndependentFromGlobalLimit(t *testing.T) {
	config := CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 40, PerAuthMaxInFlight: 8, ExhaustionProbeFailures: 10}
	coordinator, errNew := NewCoordinator(config, nil, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = coordinator.RegisterAuth("auth", 1, 0)
	_ = coordinator.ObserveThreshold("auth", 1, 99_000_000)
	leases := make([]*DrainLease, 0, 8)
	for i := 0; i < 8; i++ {
		lease, errAcquire := coordinator.AcquireBusiness(fmt.Sprintf("dispatch-%d", i))
		if errAcquire != nil {
			t.Fatalf("AcquireBusiness(%d) error = %v", i, errAcquire)
		}
		leases = append(leases, lease)
	}
	if _, errAcquire := coordinator.AcquireBusiness("dispatch-9"); errAcquire != ErrCapacity {
		t.Fatalf("AcquireBusiness(9) error = %v, want ErrCapacity", errAcquire)
	}
	occupancy := coordinator.InFlightOccupancy()
	if occupancy.PerAuthInFlight["auth"] != 8 || occupancy.PerAuthMaxInFlight["auth"] != 8 || occupancy.TotalInFlight != 8 || occupancy.MaxInFlight != 40 {
		t.Fatalf("InFlightOccupancy() = %#v", occupancy)
	}
	for _, lease := range leases {
		_ = coordinator.Release(lease)
	}
}

func TestCoordinatorAuthLimitOverride(t *testing.T) {
	config := CoordinatorConfig{Enabled: true, ThresholdMicropct: 98_000_000, MaxInFlight: 10, PerAuthMaxInFlight: 8, ExhaustionProbeFailures: 10}
	coordinator, errNew := NewCoordinator(config, nil, &fakeClock{now: time.Now()})
	if errNew != nil {
		t.Fatal(errNew)
	}
	_ = coordinator.RegisterAuth("auth", 1, 0)
	if errLimit := coordinator.SetAuthMaxInFlight("auth", 1, 3); errLimit != nil {
		t.Fatal(errLimit)
	}
	_ = coordinator.ObserveThreshold("auth", 1, 99_000_000)
	leases := make([]*DrainLease, 0, 3)
	for i := 0; i < 3; i++ {
		lease, errAcquire := coordinator.AcquireBusiness(fmt.Sprintf("business-%d", i))
		if errAcquire != nil {
			t.Fatal(errAcquire)
		}
		leases = append(leases, lease)
	}
	if _, errAcquire := coordinator.AcquireBusiness("business-over-limit"); errAcquire != ErrCapacity {
		t.Fatalf("AcquireBusiness(over limit) error = %v, want ErrCapacity", errAcquire)
	}
	if errBegin := coordinator.BeginSend(leases[0]); errBegin != nil {
		t.Fatal(errBegin)
	}
	if errLimit := coordinator.BusinessUsageLimit(leases[0]); errLimit != nil {
		t.Fatal(errLimit)
	}
	if got := coordinator.Record("auth"); got.State != StateActiveDrain || got.ProbeFailures != 1 {
		t.Fatalf("record after usage-limit = %#v", got)
	}
	for _, lease := range leases {
		_ = coordinator.Release(lease)
	}
}
