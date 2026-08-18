package auth

import (
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/codexoverdraft"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type codexProbeRun struct {
	cancel context.CancelFunc
}

var codexProbeIndeterminateBackoff = []time.Duration{5 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 5 * time.Minute}

func (m *Manager) startCodexOverdraftProbe(authID string) {
	if m == nil || authID == "" {
		return
	}
	m.codexProbeMu.Lock()
	if m.codexProbeRuns[authID] != nil {
		m.codexProbeMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &codexProbeRun{cancel: cancel}
	m.codexProbeRuns[authID] = run
	m.codexProbeMu.Unlock()
	go m.runCodexOverdraftProbe(ctx, authID, run)
}

func (m *Manager) runCodexOverdraftProbe(ctx context.Context, authID string, run *codexProbeRun) {
	defer func() {
		m.codexProbeMu.Lock()
		if m.codexProbeRuns[authID] == run {
			delete(m.codexProbeRuns, authID)
		}
		m.codexProbeMu.Unlock()
	}()
	coordinator := m.CodexOverdraftCoordinator()
	if coordinator == nil {
		return
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil {
		return
	}
	interval, errInterval := cfg.Codex.Overdraft.ProbeInterval()
	if errInterval != nil {
		return
	}
	model := strings.TrimSpace(cfg.Codex.Overdraft.ProbeModel)
	if model == "" {
		return
	}
	indeterminateStep := 0
	for {
		if errContext := ctx.Err(); errContext != nil {
			return
		}
		record := coordinator.Record(authID)
		if record.State != codexoverdraft.StateVerifying || record.VerificationPaused {
			return
		}
		lease, errAcquire := coordinator.AcquireProbe("probe_" + randomProbeID())
		if errAcquire != nil {
			if !waitProbeInterval(ctx, interval) {
				return
			}
			continue
		}
		execution := probeExecutionFromLease(coordinator, lease)
		payload := []byte(`{"model":"` + model + `","input":[{"type":"message","role":"user","content":"ping"}],"max_output_tokens":1}`)
		opts := attachCodexOverdraftExecution(cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FormatOpenAIResponse,
			OriginalRequest: payload,
		}, execution)

		m.mu.RLock()
		auth := m.auths[authID]
		if auth != nil {
			auth = auth.Clone()
		}
		executor := m.executors["codex"]
		m.mu.RUnlock()
		if auth == nil || executor == nil || auth.Disabled || auth.Status == StatusDisabled {
			execution.Release()
			return
		}
		opts = m.attachCodexQuotaObserver(opts, auth)
		req := cliproxyexecutor.Request{Model: model, Payload: payload, Format: sdktranslator.FormatOpenAIResponse}
		callOpts := m.attachAccountingAttempt(opts, auth, model)
		callCtx := accountingContextForExecution(ctx, callOpts)
		_, errExecute := executor.Execute(callCtx, auth, req, callOpts)
		resultError := resultErrorFromError(errExecute)
		if errExecute != nil && resultError != nil && resultError.HTTPStatus == 401 {
			if refreshed, errRefresh := m.refreshAuthForRequest(ctx, auth.ID, authAccessToken(auth)); errRefresh == nil && refreshed != nil {
				auth = refreshed
				callOpts = m.attachAccountingAttempt(opts, auth, model)
				callCtx = accountingContextForExecution(ctx, callOpts)
				_, errExecute = executor.Execute(callCtx, auth, req, callOpts)
				resultError = resultErrorFromError(errExecute)
			}
		}

		outcome := codexoverdraft.ProbeIndeterminate
		switch {
		case errExecute == nil:
			outcome = codexoverdraft.ProbeSuccess
		case isCodexUsageLimitError(resultError):
			outcome = codexoverdraft.ProbeUsageLimit
		case resultError != nil && (resultError.HTTPStatus == 400 || resultError.HTTPStatus == 404):
			_ = coordinator.PauseVerification(authID, record.AuthGeneration, "probe_request_invalid")
			execution.Release()
			return
		case resultError != nil && resultError.HTTPStatus == 401:
			m.MarkResult(ctx, Result{AuthID: authID, Provider: "codex", Model: model, Success: false, Error: resultError})
			_ = coordinator.PauseVerification(authID, record.AuthGeneration, "probe_auth_failed")
			execution.Release()
			return
		}
		if errResult := coordinator.ProbeResult(lease, outcome); errResult != nil {
			execution.Release()
			return
		}
		execution.Release()
		if outcome == codexoverdraft.ProbeSuccess || coordinator.Record(authID).State == codexoverdraft.StateExhausted {
			return
		}
		wait := interval
		if outcome == codexoverdraft.ProbeIndeterminate {
			step := indeterminateStep
			if step >= len(codexProbeIndeterminateBackoff) {
				step = len(codexProbeIndeterminateBackoff) - 1
			}
			if backoff := codexProbeIndeterminateBackoff[step]; backoff > wait {
				wait = backoff
			}
			if indeterminateStep < len(codexProbeIndeterminateBackoff)-1 {
				indeterminateStep++
			}
		} else {
			indeterminateStep = 0
		}
		if resultError != nil && resultError.HTTPStatus == 429 {
			if retryAfter := retryAfterFromError(errExecute); retryAfter != nil && *retryAfter > wait {
				wait = *retryAfter
			}
		}
		if !waitProbeInterval(ctx, wait) {
			return
		}
	}
}

func probeExecutionFromLease(coordinator *codexoverdraft.Coordinator, lease *codexoverdraft.DrainLease) *cliproxyexecutor.CodexOverdraftExecution {
	var releaseOnce sync.Once
	return cliproxyexecutor.NewCodexOverdraftExecution(
		lease.AuthID,
		lease.AuthGeneration,
		lease.DrainCycleID,
		lease.CoordinatorEpoch,
		lease.AuthStateEpoch,
		lease.DispatchID,
		cliproxyexecutor.CodexOverdraftProbe,
		func() error { return coordinator.BeginSend(lease) },
		func() { releaseOnce.Do(func() { _ = coordinator.Release(lease) }) },
		nil,
	)
}

func waitProbeInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func randomProbeID() string {
	return strings.ToLower(rand.Text())
}

func (m *Manager) stopAllCodexProbes() {
	if m == nil {
		return
	}
	m.codexProbeMu.Lock()
	runs := m.codexProbeRuns
	m.codexProbeRuns = make(map[string]*codexProbeRun)
	m.codexProbeMu.Unlock()
	for _, run := range runs {
		if run != nil && run.cancel != nil {
			run.cancel()
		}
	}
}
