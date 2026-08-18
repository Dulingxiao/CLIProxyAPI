package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCodexOverdraftConfigDefaults(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte("{}"))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}

	if cfg.Codex.Quota != DefaultCodexQuotaConfig() {
		t.Fatalf("Codex.Quota = %#v, want %#v", cfg.Codex.Quota, DefaultCodexQuotaConfig())
	}
	if cfg.Codex.Overdraft != DefaultCodexOverdraftConfig() {
		t.Fatalf("Codex.Overdraft = %#v, want %#v", cfg.Codex.Overdraft, DefaultCodexOverdraftConfig())
	}
	if cfg.Accounting != DefaultAccountingConfig() {
		t.Fatalf("Accounting = %#v, want %#v", cfg.Accounting, DefaultAccountingConfig())
	}
	if cfg.Codex.Overdraft.Enabled {
		t.Fatal("Codex.Overdraft.Enabled = true, want false")
	}
	if cfg.Codex.TLSProfile != CodexTLSProfileChrome || !cfg.Codex.TLSReuseConnections {
		t.Fatalf("Codex TLS defaults = (%q, %t)", cfg.Codex.TLSProfile, cfg.Codex.TLSReuseConnections)
	}
	if !cfg.Codex.Quota.Enabled {
		t.Fatal("Codex.Quota.Enabled = false, want true")
	}
	if cfg.Codex.Quota.NearThresholdStaleAfter != "60s" || cfg.Codex.Quota.MinActiveInterval != "60s" || cfg.Codex.Overdraft.ArmThresholdPercent != "90" || cfg.Codex.Overdraft.PerAuthMaxInFlight != 0 || !cfg.Codex.Overdraft.LateAdmissionBypassCooldown || cfg.Codex.Overdraft.LateAdmissionMaxAttempts != 3 || cfg.Codex.Overdraft.AllowCreditSpend {
		t.Fatalf("new conservative defaults = quota %#v overdraft %#v", cfg.Codex.Quota, cfg.Codex.Overdraft)
	}
}

func TestCodexOverdraftConfigParsesEnabledConfiguration(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte(
		"codex:\n" +
			"  quota:\n" +
			"    enabled: true\n" +
			"    stale-after: 12m\n" +
			"    near-threshold-stale-after: 75s\n" +
			"    min-active-interval: 65s\n" +
			"    startup-jitter: 20s\n" +
			"    active-query-concurrency: 4\n" +
			"  overdraft:\n" +
			"    enabled: true\n" +
			"    quota-threshold-percent: \"98.25\"\n" +
			"    arm-threshold-percent: \"89.5\"\n" +
			"    max-in-flight: 24\n" +
			"    per-auth-max-in-flight: 8\n" +
			"    late-admission-bypass-cooldown: false\n" +
			"    late-admission-max-attempts: 5\n" +
			"    allow-credit-spend: true\n" +
			"    exhaustion-probe-failures: 10\n" +
			"    probe-min-interval: 1500ms\n" +
			"    probe-model: gpt-test\n" +
			"accounting:\n" +
			"  enabled: true\n" +
			"  storage-path: state/accounting.db\n",
	))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}

	if !cfg.Codex.Overdraft.Enabled || !cfg.Codex.Quota.Enabled || !cfg.Accounting.Enabled {
		t.Fatalf("enabled dependencies not preserved: %#v", cfg)
	}
	if got, errThreshold := cfg.Codex.Overdraft.ThresholdMicropct(); errThreshold != nil || got != 98_250_000 {
		t.Fatalf("ThresholdMicropct() = (%d, %v), want (98250000, nil)", got, errThreshold)
	}
	if got, errArm := cfg.Codex.Overdraft.ArmThresholdMicropct(); errArm != nil || got != 89_500_000 {
		t.Fatalf("ArmThresholdMicropct() = (%d, %v)", got, errArm)
	}
	near, minimum, errNear := cfg.Codex.Quota.ActiveDurations()
	if errNear != nil || near != 75*time.Second || minimum != 65*time.Second {
		t.Fatalf("ActiveDurations() = (%v, %v, %v)", near, minimum, errNear)
	}
	if cfg.Codex.Overdraft.PerAuthMaxInFlight != 8 || cfg.Codex.Overdraft.LateAdmissionBypassCooldown || cfg.Codex.Overdraft.LateAdmissionMaxAttempts != 5 || !cfg.Codex.Overdraft.AllowCreditSpend {
		t.Fatalf("parsed overdraft controls = %#v", cfg.Codex.Overdraft)
	}
	stale, jitter, errDurations := cfg.Codex.Quota.Durations()
	if errDurations != nil || stale != 12*time.Minute || jitter != 20*time.Second {
		t.Fatalf("Quota.Durations() = (%v, %v, %v)", stale, jitter, errDurations)
	}
	if got, errInterval := cfg.Codex.Overdraft.ProbeInterval(); errInterval != nil || got != 1500*time.Millisecond {
		t.Fatalf("ProbeInterval() = (%v, %v), want (1.5s, nil)", got, errInterval)
	}
}

func TestCodexOverdraftConfigRequiresQuotaAndAccounting(t *testing.T) {
	base := "codex:\n  quota:\n    enabled: %t\n  overdraft:\n    enabled: true\n    probe-model: gpt-test\naccounting:\n  enabled: %t\n"
	for _, test := range []struct {
		name       string
		quota      bool
		accounting bool
		want       string
	}{
		{name: "quota disabled", quota: false, accounting: true, want: "codex.quota.enabled"},
		{name: "accounting disabled", quota: true, accounting: false, want: "accounting.enabled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, errParse := ParseConfigBytes([]byte(fmt.Sprintf(base, test.quota, test.accounting)))
			if errParse == nil || !strings.Contains(errParse.Error(), test.want) {
				t.Fatalf("ParseConfigBytes() error = %v, want substring %q", errParse, test.want)
			}
		})
	}
}

func TestCodexOverdraftConfigRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "threshold", raw: "codex:\n  overdraft:\n    quota-threshold-percent: nan\n", want: "quota-threshold-percent"},
		{name: "threshold range", raw: "codex:\n  overdraft:\n    quota-threshold-percent: \"101\"\n", want: "quota-threshold-percent"},
		{name: "arm over threshold", raw: "codex:\n  overdraft:\n    arm-threshold-percent: \"99\"\n    quota-threshold-percent: \"98\"\n", want: "arm-threshold-percent"},
		{name: "max in flight", raw: "codex:\n  overdraft:\n    max-in-flight: 0\n", want: "max-in-flight"},
		{name: "per auth over global", raw: "codex:\n  overdraft:\n    max-in-flight: 8\n    per-auth-max-in-flight: 9\n", want: "per-auth-max-in-flight"},
		{name: "negative per auth", raw: "codex:\n  overdraft:\n    per-auth-max-in-flight: -1\n", want: "per-auth-max-in-flight"},
		{name: "late attempts", raw: "codex:\n  overdraft:\n    late-admission-max-attempts: 0\n", want: "late-admission-max-attempts"},
		{name: "probe failures", raw: "codex:\n  overdraft:\n    exhaustion-probe-failures: 0\n", want: "exhaustion-probe-failures"},
		{name: "probe interval", raw: "codex:\n  overdraft:\n    probe-min-interval: 0s\n", want: "probe-min-interval"},
		{name: "stale after", raw: "codex:\n  quota:\n    stale-after: 0s\n", want: "stale-after"},
		{name: "near threshold", raw: "codex:\n  quota:\n    near-threshold-stale-after: 0s\n", want: "near-threshold-stale-after"},
		{name: "minimum active", raw: "codex:\n  quota:\n    min-active-interval: 0s\n", want: "min-active-interval"},
		{name: "startup jitter", raw: "codex:\n  quota:\n    startup-jitter: -1s\n", want: "startup-jitter"},
		{name: "query concurrency", raw: "codex:\n  quota:\n    active-query-concurrency: 0\n", want: "active-query-concurrency"},
		{name: "TLS profile", raw: "codex:\n  tls-profile: native-rust\n", want: "tls-profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, errParse := ParseConfigBytes([]byte(test.raw))
			if errParse == nil || !strings.Contains(errParse.Error(), test.want) {
				t.Fatalf("ParseConfigBytes() error = %v, want substring %q", errParse, test.want)
			}
		})
	}
}

func TestCodexOverdraftConfigAllowsEmptyProbeModelWhenEnabled(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte(
		"codex:\n" +
			"  quota:\n" +
			"    enabled: true\n" +
			"  overdraft:\n" +
			"    enabled: true\n" +
			"accounting:\n" +
			"  enabled: true\n",
	))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}
	if !cfg.Codex.Overdraft.Enabled || strings.TrimSpace(cfg.Codex.Overdraft.ProbeModel) != "" {
		t.Fatalf("overdraft = %#v", cfg.Codex.Overdraft)
	}
}
