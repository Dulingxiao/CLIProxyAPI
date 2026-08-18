package config

import "testing"

func TestCodexOverdraftDefaultsAreAvailableThroughPublicSDK(t *testing.T) {
	quota := DefaultCodexQuotaConfig()
	overdraft := DefaultCodexOverdraftConfig()
	accounting := DefaultAccountingConfig()

	if !quota.Enabled || quota.ActiveQueryConcurrency <= 0 {
		t.Fatalf("quota defaults = %#v", quota)
	}
	if overdraft.Enabled || overdraft.MaxInFlight != 40 || overdraft.ExhaustionProbeFailures != 10 {
		t.Fatalf("overdraft defaults = %#v", overdraft)
	}
	if accounting.Enabled || accounting.StoragePath == "" {
		t.Fatalf("accounting defaults = %#v", accounting)
	}
}
