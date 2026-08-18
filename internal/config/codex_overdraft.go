package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const quotaPercentScale int64 = 1_000_000

// CodexQuotaConfig controls passive and active Codex quota collection.
type CodexQuotaConfig struct {
	Enabled                 bool   `yaml:"enabled,omitempty" json:"enabled"`
	StaleAfter              string `yaml:"stale-after,omitempty" json:"stale-after"`
	NearThresholdStaleAfter string `yaml:"near-threshold-stale-after,omitempty" json:"near-threshold-stale-after"`
	MinActiveInterval       string `yaml:"min-active-interval,omitempty" json:"min-active-interval"`
	StartupJitter           string `yaml:"startup-jitter,omitempty" json:"startup-jitter"`
	ActiveQueryConcurrency  int    `yaml:"active-query-concurrency,omitempty" json:"active-query-concurrency"`
}

// CodexOverdraftConfig controls Codex overdraft pool scheduling.
type CodexOverdraftConfig struct {
	Enabled                     bool   `yaml:"enabled,omitempty" json:"enabled"`
	QuotaThresholdPercent       string `yaml:"quota-threshold-percent,omitempty" json:"quota-threshold-percent"`
	ArmThresholdPercent         string `yaml:"arm-threshold-percent,omitempty" json:"arm-threshold-percent"`
	MaxInFlight                 int    `yaml:"max-in-flight,omitempty" json:"max-in-flight"`
	PerAuthMaxInFlight          int    `yaml:"per-auth-max-in-flight,omitempty" json:"per-auth-max-in-flight"`
	LateAdmissionBypassCooldown bool   `yaml:"late-admission-bypass-cooldown,omitempty" json:"late-admission-bypass-cooldown"`
	LateAdmissionMaxAttempts    int    `yaml:"late-admission-max-attempts,omitempty" json:"late-admission-max-attempts"`
	AllowCreditSpend            bool   `yaml:"allow-credit-spend,omitempty" json:"allow-credit-spend"`
	ExhaustionProbeFailures     int    `yaml:"exhaustion-probe-failures,omitempty" json:"exhaustion-probe-failures"`
	ProbeMinInterval            string `yaml:"probe-min-interval,omitempty" json:"probe-min-interval"`
	ProbeModel                  string `yaml:"probe-model,omitempty" json:"probe-model"`
}

// AccountingConfig controls durable per-auth usage accounting.
type AccountingConfig struct {
	Enabled     bool   `yaml:"enabled,omitempty" json:"enabled"`
	StoragePath string `yaml:"storage-path,omitempty" json:"storage-path"`
}

// DefaultCodexQuotaConfig returns the baseline quota collection settings.
func DefaultCodexQuotaConfig() CodexQuotaConfig {
	return CodexQuotaConfig{
		Enabled:                 true,
		StaleAfter:              "10m",
		NearThresholdStaleAfter: "60s",
		MinActiveInterval:       "60s",
		StartupJitter:           "30s",
		ActiveQueryConcurrency:  8,
	}
}

// DefaultCodexOverdraftConfig returns the baseline overdraft settings.
func DefaultCodexOverdraftConfig() CodexOverdraftConfig {
	return CodexOverdraftConfig{
		Enabled:                     false,
		QuotaThresholdPercent:       "98",
		ArmThresholdPercent:         "90",
		MaxInFlight:                 40,
		PerAuthMaxInFlight:          0,
		LateAdmissionBypassCooldown: true,
		LateAdmissionMaxAttempts:    3,
		AllowCreditSpend:            false,
		ExhaustionProbeFailures:     10,
		ProbeMinInterval:            "1s",
	}
}

// DefaultAccountingConfig returns the baseline durable accounting settings.
func DefaultAccountingConfig() AccountingConfig {
	return AccountingConfig{
		Enabled:     false,
		StoragePath: "AUTH_STATE_DIR/accounting.db",
	}
}

func newConfigWithDefaults() *Config {
	return &Config{
		CredentialInFlight: DefaultCredentialInFlightConfig(),
		Codex: CodexConfig{
			FingerprintMode:     DefaultCodexFingerprintMode,
			TLSProfile:          DefaultCodexTLSProfile,
			TLSReuseConnections: true,
			Quota:               DefaultCodexQuotaConfig(),
			Overdraft:           DefaultCodexOverdraftConfig(),
		},
		Accounting: DefaultAccountingConfig(),
	}
}

// Durations parses the quota freshness and startup jitter durations.
func (c CodexQuotaConfig) Durations() (staleAfter, startupJitter time.Duration, err error) {
	staleAfter, err = time.ParseDuration(strings.TrimSpace(c.StaleAfter))
	if err != nil || staleAfter <= 0 {
		return 0, 0, fmt.Errorf("codex.quota.stale-after must be a positive duration")
	}
	startupJitter, err = time.ParseDuration(strings.TrimSpace(c.StartupJitter))
	if err != nil || startupJitter < 0 {
		return 0, 0, fmt.Errorf("codex.quota.startup-jitter must be a non-negative duration")
	}
	return staleAfter, startupJitter, nil
}

// ActiveDurations parses near-threshold freshness and the per-auth request floor.
func (c CodexQuotaConfig) ActiveDurations() (nearThreshold, minActiveInterval time.Duration, err error) {
	nearThreshold, err = time.ParseDuration(strings.TrimSpace(c.NearThresholdStaleAfter))
	if err != nil || nearThreshold <= 0 {
		return 0, 0, fmt.Errorf("codex.quota.near-threshold-stale-after must be a positive duration")
	}
	minActiveInterval, err = time.ParseDuration(strings.TrimSpace(c.MinActiveInterval))
	if err != nil || minActiveInterval <= 0 {
		return 0, 0, fmt.Errorf("codex.quota.min-active-interval must be a positive duration")
	}
	return nearThreshold, minActiveInterval, nil
}

// Validate validates quota collection settings.
func (c CodexQuotaConfig) Validate() error {
	if _, _, errDurations := c.Durations(); errDurations != nil {
		return errDurations
	}
	if _, _, errDurations := c.ActiveDurations(); errDurations != nil {
		return errDurations
	}
	if c.ActiveQueryConcurrency <= 0 {
		return fmt.Errorf("codex.quota.active-query-concurrency must be positive")
	}
	return nil
}

// ThresholdMicropct converts a decimal percentage into an exact fixed-point value.
func (c CodexOverdraftConfig) ThresholdMicropct() (int64, error) {
	return parseQuotaThresholdMicropct(c.QuotaThresholdPercent, "quota-threshold-percent")
}

// ArmThresholdMicropct converts the accelerated polling threshold to fixed point.
func (c CodexOverdraftConfig) ArmThresholdMicropct() (int64, error) {
	return parseQuotaThresholdMicropct(c.ArmThresholdPercent, "arm-threshold-percent")
}

func parseQuotaThresholdMicropct(raw, field string) (int64, error) {
	raw = strings.TrimSpace(raw)
	prefix := "codex.overdraft." + field
	parts := strings.Split(raw, ".")
	if len(parts) > 2 || len(parts) == 0 || parts[0] == "" {
		return 0, fmt.Errorf("%s must be a decimal percentage", prefix)
	}
	whole, errWhole := strconv.ParseInt(parts[0], 10, 64)
	if errWhole != nil || whole < 0 {
		return 0, fmt.Errorf("%s must be a decimal percentage", prefix)
	}
	fraction := int64(0)
	if len(parts) == 2 {
		if parts[1] == "" || len(parts[1]) > 6 {
			return 0, fmt.Errorf("%s supports at most six decimal places", prefix)
		}
		fractionRaw := parts[1] + strings.Repeat("0", 6-len(parts[1]))
		var errFraction error
		fraction, errFraction = strconv.ParseInt(fractionRaw, 10, 64)
		if errFraction != nil {
			return 0, fmt.Errorf("%s must be a decimal percentage", prefix)
		}
	}
	if whole > 100 || (whole == 100 && fraction != 0) {
		return 0, fmt.Errorf("%s must be at most 100", prefix)
	}
	value := whole*quotaPercentScale + fraction
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", prefix)
	}
	return value, nil
}

// ProbeInterval parses the minimum interval between exhaustion probes.
func (c CodexOverdraftConfig) ProbeInterval() (time.Duration, error) {
	interval, errParse := time.ParseDuration(strings.TrimSpace(c.ProbeMinInterval))
	if errParse != nil || interval <= 0 {
		return 0, fmt.Errorf("codex.overdraft.probe-min-interval must be a positive duration")
	}
	return interval, nil
}

// Validate validates overdraft settings independently from its dependencies.
func (c CodexOverdraftConfig) Validate() error {
	threshold, errThreshold := c.ThresholdMicropct()
	if errThreshold != nil {
		return errThreshold
	}
	armThreshold, errArm := c.ArmThresholdMicropct()
	if errArm != nil {
		return errArm
	}
	if armThreshold > threshold {
		return fmt.Errorf("codex.overdraft.arm-threshold-percent must not exceed quota-threshold-percent")
	}
	if c.MaxInFlight <= 0 {
		return fmt.Errorf("codex.overdraft.max-in-flight must be positive")
	}
	if c.PerAuthMaxInFlight < 0 || c.PerAuthMaxInFlight > c.MaxInFlight {
		return fmt.Errorf("codex.overdraft.per-auth-max-in-flight must be zero or between 1 and max-in-flight")
	}
	if c.LateAdmissionMaxAttempts <= 0 {
		return fmt.Errorf("codex.overdraft.late-admission-max-attempts must be positive")
	}
	if c.ExhaustionProbeFailures <= 0 {
		return fmt.Errorf("codex.overdraft.exhaustion-probe-failures must be positive")
	}
	if _, errInterval := c.ProbeInterval(); errInterval != nil {
		return errInterval
	}
	return nil
}

// ValidateCodexOverdraft validates quota, overdraft, and accounting dependencies.
func (c *Config) ValidateCodexOverdraft() error {
	if c == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(c.Codex.TLSProfile)) {
	case "", CodexTLSProfileChrome, CodexTLSProfileSafariLike, CodexTLSProfileGoStandard:
	default:
		return fmt.Errorf("codex.tls-profile must be chrome, safari-like, or go-standard")
	}
	if errQuota := c.Codex.Quota.Validate(); errQuota != nil {
		return errQuota
	}
	if errOverdraft := c.Codex.Overdraft.Validate(); errOverdraft != nil {
		return errOverdraft
	}
	if c.Accounting.Enabled && strings.TrimSpace(c.Accounting.StoragePath) == "" {
		return fmt.Errorf("accounting.storage-path is required when accounting is enabled")
	}
	if !c.Codex.Overdraft.Enabled {
		return nil
	}
	if !c.Codex.Quota.Enabled {
		return fmt.Errorf("codex.quota.enabled must be true when overdraft is enabled")
	}
	if !c.Accounting.Enabled {
		return fmt.Errorf("accounting.enabled must be true when overdraft is enabled")
	}
	if strings.TrimSpace(c.Accounting.StoragePath) == "" {
		return fmt.Errorf("accounting.storage-path is required when overdraft is enabled")
	}
	return nil
}
