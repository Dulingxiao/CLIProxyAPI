package management

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	usageaccounting "github.com/router-for-me/CLIProxyAPI/v7/internal/usage/accounting"
)

// GetCodexOverdraftSwitch returns the master switch and optimistic revision.
func (h *Handler) GetCodexOverdraftSwitch(c *gin.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	enabled := false
	if h.cfg != nil {
		enabled = h.cfg.Codex.Overdraft.Enabled
	}
	c.JSON(http.StatusOK, gin.H{"enabled": enabled, "revision": h.reloadGeneration})
}

// PatchCodexOverdraftSwitch atomically updates the master switch.
func (h *Handler) PatchCodexOverdraftSwitch(c *gin.Context) {
	var request struct {
		Enabled  bool   `json:"enabled"`
		Revision uint64 `json:"revision"`
	}
	if errBind := c.ShouldBindJSON(&request); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errBind.Error()})
		return
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "config is not loaded"})
		return
	}
	if request.Revision != h.reloadGeneration {
		current := h.reloadGeneration
		h.mu.Unlock()
		c.JSON(http.StatusConflict, gin.H{"error": "config revision conflict", "revision": current})
		return
	}
	previous := h.cfg.Codex.Overdraft.Enabled
	h.cfg.Codex.Overdraft.Enabled = request.Enabled
	if errValidate := h.cfg.ValidateCodexOverdraft(); errValidate != nil {
		h.cfg.Codex.Overdraft.Enabled = previous
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	var snapshot configReloadSnapshot
	if h.configFilePath != "" {
		var ok bool
		snapshot, ok = h.saveConfigAndSnapshotLocked(c)
		if !ok {
			h.cfg.Codex.Overdraft.Enabled = previous
			h.mu.Unlock()
			return
		}
	} else {
		snapshot = h.reloadSnapshotConfigLocked()
	}
	revision := snapshot.generation
	h.mu.Unlock()
	go h.reloadConfigAfterManagementSave(context.Background(), snapshot)
	c.JSON(http.StatusOK, gin.H{"enabled": request.Enabled, "revision": revision})
}

// GetCodexOverdraftStatus returns the current Auth.ID keyed scheduling overlay.
func (h *Handler) GetCodexOverdraftStatus(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "records": gin.H{}, "lease_in_flight": 0, "inherited_in_flight": 0, "total_in_flight": 0, "max_in_flight": 0, "per_auth_in_flight": gin.H{}, "per_auth_max_in_flight": gin.H{}})
		return
	}
	coordinator := h.authManager.CodexOverdraftCoordinator()
	if coordinator == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "records": gin.H{}, "lease_in_flight": 0, "inherited_in_flight": 0, "total_in_flight": 0, "max_in_flight": 0, "per_auth_in_flight": gin.H{}, "per_auth_max_in_flight": gin.H{}, "credit_protected_auths": h.authManager.CodexCreditProtectionCount()})
		return
	}
	healthy, lastError := coordinator.Health()
	occupancy := coordinator.InFlightOccupancy()
	allowCreditSpend := false
	h.mu.Lock()
	if h.cfg != nil {
		allowCreditSpend = h.cfg.Codex.Overdraft.AllowCreditSpend
	}
	h.mu.Unlock()
	records := coordinator.Records()
	drainMetrics := make(map[string]usageaccounting.Aggregate)
	if service := h.accountingService(); service != nil {
		for authID, record := range records {
			if record.DrainCycleID != "" {
				drainMetrics[record.DrainCycleID] = service.Aggregate(authID, record.DrainCycleID)
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"enabled": coordinator.Enabled(), "healthy": healthy, "last_error": lastError, "owner_auth_id": coordinator.Owner(), "coordinator_epoch": coordinator.Epoch(), "records": records, "candidates": coordinator.Candidates(), "candidate_queue": coordinator.CandidateQueueMetrics(), "drain_cycle_accounting": drainMetrics, "lease_in_flight": occupancy.LeaseInFlight, "inherited_in_flight": occupancy.InheritedInFlight, "total_in_flight": occupancy.TotalInFlight, "max_in_flight": occupancy.MaxInFlight, "per_auth_in_flight": occupancy.PerAuthInFlight, "per_auth_max_in_flight": occupancy.PerAuthMaxInFlight, "credit_protected_auths": h.authManager.CodexCreditProtectionCount(), "allow_credit_spend": allowCreditSpend})
}

// GetCodexQuotaSnapshots returns the latest active/passive snapshots.
func (h *Handler) GetCodexQuotaSnapshots(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusOK, gin.H{"snapshots": gin.H{}})
		return
	}
	if authID := strings.TrimSpace(c.Query("auth_id")); authID != "" {
		snapshot, ok := h.authManager.CodexQuotaSnapshot(authID)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "quota snapshot not found"})
			return
		}
		c.JSON(http.StatusOK, snapshot)
		return
	}
	c.JSON(http.StatusOK, gin.H{"snapshots": h.authManager.CodexQuotaSnapshots()})
}

// GetCodexQuotaHealth returns durable quota observation dependency health.
func (h *Handler) GetCodexQuotaHealth(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusOK, gin.H{"enabled": false, "healthy": true})
		return
	}
	c.JSON(http.StatusOK, h.authManager.CodexQuotaRuntimeHealth())
}

// GetCodexQuotaDebug returns the latest allowlisted /wham/usage body for one auth.
func (h *Handler) GetCodexQuotaDebug(c *gin.Context) {
	authID := strings.TrimSpace(c.Query("auth_id"))
	if authID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_id is required"})
		return
	}
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "quota debug payload not found"})
		return
	}
	snapshot, ok := h.authManager.CodexQuotaDebugPayload(authID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "quota debug payload not found"})
		return
	}
	c.JSON(http.StatusOK, snapshot)
}

// GetCodexUpstreamFailures returns bounded, sanitized per-attempt failure metadata.
func (h *Handler) GetCodexUpstreamFailures(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}})
		return
	}
	items := h.authManager.UpstreamFailures(strings.TrimSpace(c.Query("auth_id")), strings.TrimSpace(c.Query("model")))
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// PostCodexQuotaRefresh starts or joins one Auth.ID active quota query.
func (h *Handler) PostCodexQuotaRefresh(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota service is not active"})
		return
	}
	snapshot, errRefresh := h.authManager.RefreshCodexQuota(c.Request.Context(), strings.TrimSpace(c.Param("auth_id")))
	if errRefresh != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": errRefresh.Error()})
		return
	}
	c.JSON(http.StatusOK, snapshot)
}

// GetAccountingHealth returns durable sink availability and backlog-safe event counts.
func (h *Handler) GetAccountingHealth(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		required := false
		if h != nil {
			h.mu.Lock()
			required = h.cfg != nil && h.cfg.Accounting.Enabled
			h.mu.Unlock()
		}
		response := gin.H{"enabled": required, "healthy": !required, "events": 0, "cost_results": 0}
		if required {
			response["last_error"] = "accounting service is not active"
		}
		c.JSON(http.StatusOK, response)
		return
	}
	health := service.Health()
	c.JSON(http.StatusOK, gin.H{"enabled": true, "healthy": health.Healthy, "queue_depth": health.QueueDepth, "queue_capacity": health.QueueCapacity, "last_error": health.LastError, "last_persisted_at": health.LastPersistedAt, "events": health.UsageEvents, "cost_results": health.CostResults})
}

// GetAccountingCosts returns immutable pricing results.
func (h *Handler) GetAccountingCosts(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0, "offset": 0, "limit": 100})
		return
	}
	from, to, errRange := accountingTimeRange(c)
	if errRange != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errRange.Error()})
		return
	}
	eventsByID := make(map[string]usageaccounting.UsageEvent)
	for _, event := range service.UsageEvents() {
		eventsByID[event.EventID] = event
	}
	results := service.CostResults()
	costStatus := strings.TrimSpace(c.Query("cost_status"))
	priceVersionID := strings.TrimSpace(c.Query("price_version_id"))
	pricingRunID := strings.TrimSpace(c.Query("pricing_run_id"))
	filtered := make([]usageaccounting.UsageCostResult, 0, len(results))
	for _, result := range results {
		event, hasEvent := eventsByID[result.EventID]
		if costStatus != "" && !strings.EqualFold(result.CostStatus, costStatus) || priceVersionID != "" && result.Cost.PriceVersionID != priceVersionID || pricingRunID != "" && result.PricingRunID != pricingRunID {
			continue
		}
		if accountingEventFilterRequested(c) && (!hasEvent || !matchesAccountingEvent(c, event, from, to)) {
			continue
		}
		filtered = append(filtered, result)
	}
	offset, limit, end := accountingPage(c, len(filtered))
	c.JSON(http.StatusOK, gin.H{"items": filtered[offset:end], "total": len(filtered), "offset": offset, "limit": limit})
}

// GetAccountingEvents returns a bounded page of immutable usage events.
func (h *Handler) GetAccountingEvents(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0, "offset": 0, "limit": 100})
		return
	}
	from, to, errRange := accountingTimeRange(c)
	if errRange != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errRange.Error()})
		return
	}
	events := service.UsageEvents()
	filtered := make([]usageaccounting.UsageEvent, 0, len(events))
	for _, event := range events {
		if !matchesAccountingEvent(c, event, from, to) {
			continue
		}
		filtered = append(filtered, event)
	}
	offset, limit, end := accountingPage(c, len(filtered))
	c.JSON(http.StatusOK, gin.H{"items": filtered[offset:end], "total": len(filtered), "offset": offset, "limit": limit})
}

// GetAccountingAggregate rebuilds a filtered aggregate from immutable events.
func (h *Handler) GetAccountingAggregate(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusOK, usageaccounting.Aggregate{})
		return
	}
	c.JSON(http.StatusOK, service.Aggregate(strings.TrimSpace(c.Query("auth_id")), strings.TrimSpace(c.Query("drain_cycle_id"))))
}

// GetAccountingAggregates returns all per-auth aggregates in one storage pass.
func (h *Handler) GetAccountingAggregates(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0})
		return
	}
	items := service.AggregatesByAuth()
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
}

// PutModelPrice creates an immutable effective price version.
func (h *Handler) PutModelPrice(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "accounting service is not active"})
		return
	}
	var version usageaccounting.PriceVersion
	if errBind := c.ShouldBindJSON(&version); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errBind.Error()})
		return
	}
	stored, errPut := service.PutPrice(version)
	if errPut != nil {
		c.JSON(http.StatusConflict, gin.H{"error": errPut.Error()})
		return
	}
	c.JSON(http.StatusCreated, stored)
}

// ListModelPrices returns all immutable price versions.
func (h *Handler) ListModelPrices(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusOK, gin.H{"items": []any{}, "total": 0})
		return
	}
	versions := service.Prices()
	c.JSON(http.StatusOK, gin.H{"items": versions, "total": len(versions)})
}

// PostModelReprice runs an idempotent historical pricing pass.
func (h *Handler) PostModelReprice(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "accounting service is not active"})
		return
	}
	runID, errRun := service.Reprice()
	if errRun != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": errRun.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"pricing_run_id": runID})
}

// GetModelPrice returns one immutable price version.
func (h *Handler) GetModelPrice(c *gin.Context) {
	service := h.accountingService()
	if service == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "price not found"})
		return
	}
	version, ok := service.Price(strings.TrimSpace(c.Param("version_id")))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "price not found"})
		return
	}
	c.JSON(http.StatusOK, version)
}

// GetManagementCapabilities describes optional control-panel navigation entries.
func (h *Handler) GetManagementCapabilities(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"navigation": []gin.H{{"id": "model-pricing", "label": "Model Pricing", "path": "/model-pricing"}}, "codex_overdraft": true, "accounting": true})
}

func (h *Handler) accountingService() *usageaccounting.Service {
	if h == nil || h.authManager == nil {
		return nil
	}
	return h.authManager.AccountingService()
}

func nonNegativeQueryInt(c *gin.Context, key string, fallback int) int {
	value, errParse := strconv.Atoi(strings.TrimSpace(c.Query(key)))
	if errParse != nil || value < 0 {
		return fallback
	}
	return value
}

func accountingTimeRange(c *gin.Context) (time.Time, time.Time, error) {
	parse := func(key string) (time.Time, error) {
		raw := strings.TrimSpace(c.Query(key))
		if raw == "" {
			return time.Time{}, nil
		}
		value, errParse := time.Parse(time.RFC3339Nano, raw)
		if errParse != nil {
			return time.Time{}, fmt.Errorf("%s must be RFC3339", key)
		}
		return value, nil
	}
	from, errFrom := parse("from")
	if errFrom != nil {
		return time.Time{}, time.Time{}, errFrom
	}
	to, errTo := parse("to")
	if errTo != nil {
		return time.Time{}, time.Time{}, errTo
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return time.Time{}, time.Time{}, fmt.Errorf("to must not be before from")
	}
	return from, to, nil
}

func accountingEventFilterRequested(c *gin.Context) bool {
	for _, key := range []string{"auth_id", "provider", "model", "tier", "drain_cycle_id", "from", "to"} {
		if strings.TrimSpace(c.Query(key)) != "" {
			return true
		}
	}
	return false
}

func matchesAccountingEvent(c *gin.Context, event usageaccounting.UsageEvent, from, to time.Time) bool {
	authID := strings.TrimSpace(c.Query("auth_id"))
	provider := strings.TrimSpace(c.Query("provider"))
	model := strings.TrimSpace(c.Query("model"))
	tier := strings.TrimSpace(c.Query("tier"))
	cycleID := strings.TrimSpace(c.Query("drain_cycle_id"))
	if authID != "" && event.AuthID != authID || provider != "" && !strings.EqualFold(event.Provider, provider) || model != "" && event.Model != model || tier != "" && event.Tier != tier || cycleID != "" && event.DrainCycleID != cycleID {
		return false
	}
	at := event.RequestedAt
	if at.IsZero() {
		at = event.ObservedAt
	}
	return (from.IsZero() || !at.Before(from)) && (to.IsZero() || at.Before(to))
}

func accountingPage(c *gin.Context, total int) (int, int, int) {
	offset := nonNegativeQueryInt(c, "offset", 0)
	limit := nonNegativeQueryInt(c, "limit", 100)
	if limit > 500 {
		limit = 500
	}
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return offset, limit, end
}
