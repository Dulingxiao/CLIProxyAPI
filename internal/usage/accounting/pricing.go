package accounting

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

var ErrUnpriced = errors.New("usage is unpriced")

// PriceVersion is an immutable model/tier price range.
type PriceVersion struct {
	VersionID                   string    `json:"version_id"`
	Provider                    string    `json:"provider"`
	Model                       string    `json:"model"`
	Tier                        string    `json:"tier"`
	Version                     uint64    `json:"version"`
	Enabled                     *bool     `json:"enabled"`
	EffectiveFrom               time.Time `json:"effective_from"`
	EffectiveTo                 time.Time `json:"effective_to,omitempty"`
	InputNanoUSDPerMillion      int64     `json:"input_nano_usd_per_million"`
	CacheReadNanoUSDPerMillion  int64     `json:"cache_read_nano_usd_per_million"`
	CacheWriteNanoUSDPerMillion int64     `json:"cache_write_nano_usd_per_million"`
	OutputNanoUSDPerMillion     int64     `json:"output_nano_usd_per_million"`
	ReasoningNanoUSDPerMillion  int64     `json:"reasoning_nano_usd_per_million"`
}

// CostResult is an immutable price calculation result.
type CostResult struct {
	PriceVersionID   string `json:"price_version_id"`
	InputTokens      int64  `json:"input_tokens"`
	CacheReadTokens  int64  `json:"cache_read_tokens"`
	CacheWriteTokens int64  `json:"cache_write_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	ReasoningTokens  int64  `json:"reasoning_tokens"`
	TotalNanoUSD     int64  `json:"total_nano_usd"`
}

// PricingBook stores immutable non-overlapping effective price ranges.
type PricingBook struct {
	mu       sync.RWMutex
	versions map[string]PriceVersion
}

func NewPricingBook() *PricingBook { return &PricingBook{versions: make(map[string]PriceVersion)} }

func (b *PricingBook) Put(version PriceVersion) (PriceVersion, error) {
	if b == nil {
		return PriceVersion{}, fmt.Errorf("pricing book is nil")
	}
	version = clonePriceVersion(version)
	version.Model = strings.TrimSpace(version.Model)
	version.Provider = strings.ToLower(strings.TrimSpace(version.Provider))
	if version.Provider == "" {
		version.Provider = "codex"
	}
	version.Tier = strings.TrimSpace(version.Tier)
	version.VersionID = strings.TrimSpace(version.VersionID)
	if version.Model == "" || version.Tier == "" || version.EffectiveFrom.IsZero() || !version.EffectiveTo.IsZero() && !version.EffectiveTo.After(version.EffectiveFrom) {
		return PriceVersion{}, fmt.Errorf("invalid price version range")
	}
	if version.VersionID == "" {
		version.VersionID = "price_" + rand.Text()
	}
	if version.Version == 0 {
		version.Version = 1
	}
	if version.Enabled == nil {
		enabled := true
		version.Enabled = &enabled
	}
	for _, value := range []int64{version.InputNanoUSDPerMillion, version.CacheReadNanoUSDPerMillion, version.CacheWriteNanoUSDPerMillion, version.OutputNanoUSDPerMillion, version.ReasoningNanoUSDPerMillion} {
		if value < 0 {
			return PriceVersion{}, fmt.Errorf("price cannot be negative")
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.versions[version.VersionID]; exists {
		return PriceVersion{}, fmt.Errorf("price version %q already exists", version.VersionID)
	}
	for _, existing := range b.versions {
		if existing.Provider != version.Provider || existing.Model != version.Model || existing.Tier != version.Tier || !rangesOverlap(existing, version) {
			continue
		}
		return PriceVersion{}, fmt.Errorf("price range overlaps version %q", existing.VersionID)
	}
	b.versions[version.VersionID] = version
	return clonePriceVersion(version), nil
}

func (b *PricingBook) Get(versionID string) (PriceVersion, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	version, ok := b.versions[versionID]
	return clonePriceVersion(version), ok
}

func clonePriceVersion(version PriceVersion) PriceVersion {
	if version.Enabled != nil {
		enabled := *version.Enabled
		version.Enabled = &enabled
	}
	return version
}

func (b *PricingBook) remove(versionID string) {
	if b == nil || versionID == "" {
		return
	}
	b.mu.Lock()
	delete(b.versions, versionID)
	b.mu.Unlock()
}

func (b *PricingBook) Price(provider, model, tier string, at time.Time, breakdown coreusage.TokenBreakdown) (CostResult, error) {
	if !breakdown.Valid() {
		return CostResult{}, fmt.Errorf("invalid token breakdown")
	}
	if breakdown.Quality != coreusage.TokenAccountingQualityComplete || breakdown.UnclassifiedTokens != 0 {
		return CostResult{}, fmt.Errorf("token breakdown is not fully classified")
	}
	b.mu.RLock()
	versions := make([]PriceVersion, 0)
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "codex"
	}
	model = strings.TrimSpace(model)
	tier = strings.TrimSpace(tier)
	for _, version := range b.versions {
		if version.Enabled != nil && *version.Enabled && version.Provider == provider && version.Model == model && version.Tier == tier && containsTime(version, at) {
			versions = append(versions, version)
		}
	}
	b.mu.RUnlock()
	if len(versions) == 0 {
		return CostResult{}, ErrUnpriced
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].EffectiveFrom.After(versions[j].EffectiveFrom) })
	version := versions[0]
	result := CostResult{PriceVersionID: version.VersionID, InputTokens: breakdown.Input.UncachedTokens, CacheReadTokens: breakdown.Input.CacheReadTokens, CacheWriteTokens: breakdown.Input.CacheWriteTokens, OutputTokens: breakdown.Output.NonReasoningTokens, ReasoningTokens: breakdown.Output.ReasoningTokens}
	items := [][2]int64{{result.InputTokens, version.InputNanoUSDPerMillion}, {result.CacheReadTokens, version.CacheReadNanoUSDPerMillion}, {result.CacheWriteTokens, version.CacheWriteNanoUSDPerMillion}, {result.OutputTokens, version.OutputNanoUSDPerMillion}, {result.ReasoningTokens, version.ReasoningNanoUSDPerMillion}}
	var ok bool
	result.TotalNanoUSD, ok = sumProductsRoundHalfEven(items, 1_000_000)
	if !ok {
		return CostResult{}, fmt.Errorf("price calculation overflow")
	}
	return result, nil
}

func rangesOverlap(a, b PriceVersion) bool {
	aEnd, bEnd := a.EffectiveTo, b.EffectiveTo
	return (aEnd.IsZero() || b.EffectiveFrom.Before(aEnd)) && (bEnd.IsZero() || a.EffectiveFrom.Before(bEnd))
}

func containsTime(version PriceVersion, at time.Time) bool {
	return !at.Before(version.EffectiveFrom) && (version.EffectiveTo.IsZero() || at.Before(version.EffectiveTo))
}

func sumProductsRoundHalfEven(items [][2]int64, denominator int64) (int64, bool) {
	if denominator <= 0 {
		return 0, false
	}
	numerator := new(big.Int)
	for _, item := range items {
		if item[0] < 0 || item[1] < 0 {
			return 0, false
		}
		product := new(big.Int).Mul(big.NewInt(item[0]), big.NewInt(item[1]))
		numerator.Add(numerator, product)
	}
	denominatorBig := big.NewInt(denominator)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominatorBig, remainder)
	doubledRemainder := new(big.Int).Lsh(remainder, 1)
	comparison := doubledRemainder.Cmp(denominatorBig)
	if comparison > 0 || comparison == 0 && quotient.Bit(0) == 1 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, false
	}
	return quotient.Int64(), true
}
