package accounting

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestPricingBookSkipsDisabledVersion(t *testing.T) {
	var version PriceVersion
	if errDecode := json.Unmarshal([]byte(`{"provider":"codex","model":"gpt-test","tier":"default","effective_from":"1970-01-01T00:00:00Z","enabled":false,"input_nano_usd_per_million":1000000}`), &version); errDecode != nil {
		t.Fatal(errDecode)
	}
	book := NewPricingBook()
	if _, errPut := book.Put(version); errPut != nil {
		t.Fatal(errPut)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 1, Input: coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1}}
	if _, errPrice := book.Price("codex", "gpt-test", "default", time.Unix(1, 0), breakdown); !errors.Is(errPrice, ErrUnpriced) {
		t.Fatalf("Price() error = %v, want ErrUnpriced", errPrice)
	}
}

func TestPricingBookRejectsOverlappingEffectiveRangesAndKeepsVersionsImmutable(t *testing.T) {
	book := NewPricingBook()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first, errPut := book.Put(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: start, EffectiveTo: start.Add(time.Hour), InputNanoUSDPerMillion: 1_000_000, OutputNanoUSDPerMillion: 2_000_000})
	if errPut != nil {
		t.Fatal(errPut)
	}
	if first.VersionID == "" {
		t.Fatal("version ID is empty")
	}
	if _, errOverlap := book.Put(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: start.Add(30 * time.Minute), EffectiveTo: start.Add(2 * time.Hour)}); errOverlap == nil {
		t.Fatal("overlapping price range accepted")
	}
	first.InputNanoUSDPerMillion = 99
	got, ok := book.Get(first.VersionID)
	if !ok || got.InputNanoUSDPerMillion != 1_000_000 {
		t.Fatalf("stored price mutated: %#v", got)
	}
}

func TestPricingBookDoesNotExposeEnabledPointer(t *testing.T) {
	book := NewPricingBook()
	enabled := true
	version, errPut := book.Put(PriceVersion{Model: "gpt-test", Tier: "default", Enabled: &enabled, EffectiveFrom: time.Unix(0, 0).UTC(), InputNanoUSDPerMillion: 1_000_000})
	if errPut != nil {
		t.Fatal(errPut)
	}
	enabled = false
	*version.Enabled = false
	loaded, ok := book.Get(version.VersionID)
	if !ok || loaded.Enabled == nil || !*loaded.Enabled {
		t.Fatalf("stored enabled state changed through Put() result: %#v", loaded)
	}
	*loaded.Enabled = false
	reloaded, ok := book.Get(version.VersionID)
	if !ok || reloaded.Enabled == nil || !*reloaded.Enabled {
		t.Fatalf("stored enabled state changed through Get() result: %#v", reloaded)
	}
}

func TestPricingBookRejectsEmptyEffectiveRange(t *testing.T) {
	book := NewPricingBook()
	now := time.Now()
	if _, errPut := book.Put(PriceVersion{Model: "model", Tier: "default", EffectiveFrom: now, EffectiveTo: now}); errPut == nil {
		t.Fatal("empty effective range was accepted")
	}
}

func TestPriceUsageUsesFiveBucketsAndHalfEvenRounding(t *testing.T) {
	book := NewPricingBook()
	version, errPut := book.Put(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: time.Unix(0, 0).UTC(), InputNanoUSDPerMillion: 1, CacheReadNanoUSDPerMillion: 3, CacheWriteNanoUSDPerMillion: 5, OutputNanoUSDPerMillion: 7, ReasoningNanoUSDPerMillion: 9})
	if errPut != nil {
		t.Fatal(errPut)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 5, Input: coreusage.TokenInputBreakdown{TotalTokens: 3, UncachedTokens: 1, CacheReadTokens: 1, CacheWriteTokens: 1}, Output: coreusage.TokenOutputBreakdown{TotalTokens: 2, NonReasoningTokens: 1, ReasoningTokens: 1}}
	cost, errCost := book.Price("codex", "gpt-test", "default", time.Unix(1, 0), breakdown)
	if errCost != nil {
		t.Fatal(errCost)
	}
	if cost.PriceVersionID != version.VersionID || cost.InputTokens != 1 || cost.CacheReadTokens != 1 || cost.CacheWriteTokens != 1 || cost.OutputTokens != 1 || cost.ReasoningTokens != 1 {
		t.Fatalf("cost buckets = %#v", cost)
	}
}

func TestPriceRoundsOnceAfterSummingAllBuckets(t *testing.T) {
	book := NewPricingBook()
	_, errPut := book.Put(PriceVersion{Model: "gpt-test", Tier: "default", EffectiveFrom: time.Unix(0, 0).UTC(), InputNanoUSDPerMillion: 500_000, OutputNanoUSDPerMillion: 500_000})
	if errPut != nil {
		t.Fatal(errPut)
	}
	breakdown := coreusage.TokenBreakdown{SchemaVersion: coreusage.TokenAccountingSchemaVersion, Quality: coreusage.TokenAccountingQualityComplete, TotalTokens: 2, Input: coreusage.TokenInputBreakdown{TotalTokens: 1, UncachedTokens: 1}, Output: coreusage.TokenOutputBreakdown{TotalTokens: 1, NonReasoningTokens: 1}}
	cost, errCost := book.Price("codex", "gpt-test", "default", time.Unix(1, 0), breakdown)
	if errCost != nil {
		t.Fatal(errCost)
	}
	if cost.TotalNanoUSD != 1 {
		t.Fatalf("total cost = %d, want 1", cost.TotalNanoUSD)
	}
}
