package codexoverdraft

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParsePassiveQuotaHeaders(t *testing.T) {
	now := time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)
	headers := http.Header{
		"X-Codex-Primary-Used-Percent":        []string{"98.25"},
		"X-Codex-Primary-Window-Minutes":      []string{"300"},
		"X-Codex-Primary-Reset-After-Seconds": []string{"60"},
		"X-Codex-Secondary-Used-Percent":      []string{"75"},
		"X-Codex-Secondary-Window-Minutes":    []string{"10080"},
	}

	snapshot, errParse := ParsePassiveQuotaHeaders("auth-1", 3, "attempt-1", headers, now)
	if errParse != nil {
		t.Fatalf("ParsePassiveQuotaHeaders() error = %v", errParse)
	}
	if len(snapshot.Windows) != 2 {
		t.Fatalf("len(Windows) = %d, want 2", len(snapshot.Windows))
	}
	if got := snapshot.Windows[0]; got.Name != WindowPrimary || got.Kind != WindowShort || got.UsedMicropct != 98_250_000 || !got.ResetAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("primary window = %#v", got)
	}
	if got := snapshot.Windows[1]; got.Name != WindowSecondary || got.Kind != WindowLong || got.UsedMicropct != 75_000_000 {
		t.Fatalf("secondary window = %#v", got)
	}
	if snapshot.Source != QuotaSourcePassive || snapshot.AuthGeneration != 3 {
		t.Fatalf("snapshot identity = %#v", snapshot)
	}
}

func TestParseActiveQuotaPayloadSupportsSchemaVariantsAndCredits(t *testing.T) {
	now := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name          string
		fixture       string
		wantVariant   string
		wantBalance   string
		wantLimitType string
		wantPlan      string
		wantPrimary   int64
		wantSecondary int64
		wantCredits   bool
		wantUnlimited bool
	}{
		{name: "primary windows with top-level credits", fixture: "wham_usage_primary_windows.json", wantVariant: "primary_secondary+credits_top", wantBalance: "12.50", wantLimitType: "five_hour", wantPlan: "plus", wantPrimary: 98_500_000, wantSecondary: 70_000_000, wantCredits: true},
		{name: "named windows with nested credits", fixture: "wham_usage_named_windows.json", wantVariant: "five_hour_weekly+credits_rate_limit", wantBalance: "unlimited", wantLimitType: "weekly", wantPlan: "team", wantPrimary: 100_000_000, wantSecondary: 82_250_000, wantCredits: true, wantUnlimited: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body, errRead := os.ReadFile(filepath.Join("testdata", testCase.fixture))
			if errRead != nil {
				t.Fatal(errRead)
			}
			snapshot, errParse := ParseActiveQuotaPayload("auth-1", 2, "query-1", body, now)
			if errParse != nil {
				t.Fatalf("ParseActiveQuotaPayload() error = %v", errParse)
			}
			if snapshot.SchemaVariant != testCase.wantVariant || snapshot.HasCredits != testCase.wantCredits || snapshot.CreditsUnlimited != testCase.wantUnlimited || snapshot.CreditsBalance != testCase.wantBalance || snapshot.RateLimitReachedType != testCase.wantLimitType || snapshot.PlanType != testCase.wantPlan {
				t.Fatalf("snapshot metadata = %#v", snapshot)
			}
			if len(snapshot.Windows) != 2 || snapshot.Windows[0].UsedMicropct != testCase.wantPrimary || snapshot.Windows[1].UsedMicropct != testCase.wantSecondary {
				t.Fatalf("snapshot windows = %#v", snapshot.Windows)
			}
		})
	}
}

func TestParseActiveQuotaPayloadMarksUnknownSchema(t *testing.T) {
	snapshot, errParse := ParseActiveQuotaPayload("auth-unknown", 1, "query-unknown", []byte(`{"rate_limit":{"allowed":true,"mystery_window":{"consumed":42}},"opaque":"TOKEN"}`), time.Now())
	if !errors.Is(errParse, ErrUnknownQuotaSchema) {
		t.Fatalf("error = %v, want ErrUnknownQuotaSchema", errParse)
	}
	if !snapshot.UnknownSchema || snapshot.SchemaVariant != "unknown" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	keys := strings.Join(snapshot.UnknownKeys, ",")
	if !strings.Contains(keys, "rate_limit.mystery_window") || strings.Contains(keys, "TOKEN") {
		t.Fatalf("unknown keys = %q", keys)
	}
}

func TestParsePassiveQuotaHeadersRejectsInvalidPercent(t *testing.T) {
	for _, raw := range []string{"NaN", "Inf", "-1", "1001", "1.0000001"} {
		t.Run(raw, func(t *testing.T) {
			_, errParse := ParsePassiveQuotaHeaders("auth-1", 1, "attempt", http.Header{
				"X-Codex-Primary-Used-Percent": []string{raw},
			}, time.Now())
			if errParse == nil {
				t.Fatalf("ParsePassiveQuotaHeaders(%q) error = nil", raw)
			}
		})
	}
}

func TestParsePassiveQuotaHeadersRejectsNonFiniteResetValues(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		headers := http.Header{
			"X-Codex-Primary-Used-Percent":        []string{"98"},
			"X-Codex-Primary-Reset-After-Seconds": []string{value},
		}
		if _, errParse := ParsePassiveQuotaHeaders("auth-1", 1, "attempt-1", headers, time.Now()); errParse == nil {
			t.Fatalf("reset-after-seconds %q was accepted", value)
		}
	}
}

func TestParseActiveQuotaPayloadSupportsSnakeAndCamelCase(t *testing.T) {
	now := time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name string
		body string
	}{
		{
			name: "snake case",
			body: `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":99,"limit_window_seconds":18000,"reset_at":1786867260}}}`,
		},
		{
			name: "camel case",
			body: `{"rateLimit":{"allowed":true,"limitReached":false,"primaryWindow":{"usedPercent":99,"limitWindowSeconds":18000,"resetAt":1786867260}}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, errParse := ParseActiveQuotaPayload("auth-1", 2, "query-1", []byte(test.body), now)
			if errParse != nil {
				t.Fatalf("ParseActiveQuotaPayload() error = %v", errParse)
			}
			if !snapshot.Allowed || snapshot.LimitReached || len(snapshot.Windows) != 1 || snapshot.Windows[0].UsedMicropct != 99_000_000 {
				t.Fatalf("snapshot = %#v", snapshot)
			}
		})
	}
}

func TestQuotaStateFencesGenerationAndStaleCycles(t *testing.T) {
	state := NewQuotaState()
	newerReset := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	accepted := state.Merge(QuotaSnapshot{AuthID: "auth-1", AuthGeneration: 2, ObservedAt: newerReset.Add(-time.Hour), Sequence: 2, Windows: []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 90_000_000, ResetAt: newerReset}}}, 2)
	if !accepted {
		t.Fatal("current generation snapshot rejected")
	}
	if state.Merge(QuotaSnapshot{AuthID: "auth-1", AuthGeneration: 1, ObservedAt: newerReset, Sequence: 3, Windows: []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 99_000_000, ResetAt: newerReset}}}, 2) {
		t.Fatal("old generation snapshot accepted")
	}
	if state.Merge(QuotaSnapshot{AuthID: "auth-1", AuthGeneration: 2, ObservedAt: newerReset, Sequence: 4, Windows: []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 99_000_000, ResetAt: newerReset.Add(-time.Hour)}}}, 2) {
		t.Fatal("stale reset cycle accepted")
	}
	window, ok := state.Window("auth-1", WindowPrimary)
	if !ok || window.UsedMicropct != 90_000_000 {
		t.Fatalf("stored window = (%#v, %t)", window, ok)
	}
}

func TestQuotaStateDurableMergePublishesOnlyAfterPersistence(t *testing.T) {
	state := NewQuotaState()
	snapshot := QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		ObservedAt:     time.Unix(100, 0),
		Sequence:       1,
		Windows:        []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 98_000_000}},
	}
	persistErr := errors.New("injected quota persistence failure")
	accepted, errMerge := state.MergeDurable(snapshot, 1, func() error { return persistErr })
	if accepted || !errors.Is(errMerge, persistErr) {
		t.Fatalf("MergeDurable() = %t, %v", accepted, errMerge)
	}
	if _, exists := state.Window("auth-1", WindowPrimary); exists {
		t.Fatal("failed durable merge published quota state")
	}
	accepted, errMerge = state.MergeDurable(snapshot, 1, func() error { return nil })
	if !accepted || errMerge != nil {
		t.Fatalf("MergeDurable() = %t, %v; want true, nil", accepted, errMerge)
	}
	window, exists := state.Window("auth-1", WindowPrimary)
	if !exists || window.UsedMicropct != 98_000_000 {
		t.Fatalf("window = %#v, %t", window, exists)
	}
}

func TestPassiveQuotaMarksResetConflict(t *testing.T) {
	now := time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC)
	snapshot, errParse := ParsePassiveQuotaHeaders("auth-1", 1, "attempt", http.Header{
		"X-Codex-Primary-Used-Percent":        []string{"98"},
		"X-Codex-Primary-Reset-After-Seconds": []string{"60"},
		"X-Codex-Primary-Reset-At":            []string{"1786867800"},
	}, now)
	if errParse != nil {
		t.Fatalf("ParsePassiveQuotaHeaders() error = %v", errParse)
	}
	if !snapshot.Inconsistent {
		t.Fatal("Inconsistent = false, want true")
	}
}

func TestQuotaStateDoesNotFenceCalibrationWithInconsistentObservation(t *testing.T) {
	now := time.Now()
	state := NewQuotaState()
	inconsistent := QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		ObservedAt:     now,
		Sequence:       1,
		Inconsistent:   true,
		Windows:        []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 99_000_000, ResetAt: now.Add(time.Hour)}},
	}
	if state.Merge(inconsistent, 1) {
		t.Fatal("inconsistent observation was merged into current quota state")
	}
	calibration := QuotaSnapshot{
		AuthID:         "auth-1",
		AuthGeneration: 1,
		ObservedAt:     now.Add(time.Second),
		Sequence:       2,
		Windows:        []QuotaWindow{{Name: WindowPrimary, UsedMicropct: 10_000_000, ResetAt: now.Add(30 * time.Minute)}},
	}
	if !state.Merge(calibration, 1) {
		t.Fatal("valid calibration was rejected after inconsistent observation")
	}
}
