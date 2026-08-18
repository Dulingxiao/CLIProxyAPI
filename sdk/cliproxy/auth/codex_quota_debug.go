package auth

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// CodexQuotaDebugSnapshot is an allowlisted view of one raw /wham/usage body.
type CodexQuotaDebugSnapshot struct {
	AuthID     string          `json:"auth_id"`
	CapturedAt time.Time       `json:"captured_at"`
	Payload    json.RawMessage `json:"payload"`
}

var quotaDebugScalarKeys = map[string]struct{}{
	"allowed": {}, "limit_reached": {}, "limitReached": {},
	"rate_limit_reached_type": {}, "rateLimitReachedType": {},
	"plan_type": {}, "planType": {},
}

var quotaDebugWindowKeys = map[string]struct{}{
	"used_percent": {}, "usedPercent": {},
	"limit_window_seconds": {}, "limitWindowSeconds": {},
	"reset_at": {}, "resetAt": {},
}

var quotaDebugCreditsKeys = map[string]struct{}{
	"has_credits": {}, "hasCredits": {}, "unlimited": {}, "balance": {},
}

func (m *Manager) storeCodexQuotaDebugPayload(authID string, body []byte, capturedAt time.Time) {
	if m == nil || strings.TrimSpace(authID) == "" {
		return
	}
	payload := allowlistedCodexQuotaDebugPayload(body)
	m.codexQuotaMu.Lock()
	m.codexQuotaDebug[authID] = CodexQuotaDebugSnapshot{AuthID: authID, CapturedAt: capturedAt.UTC(), Payload: payload}
	m.codexQuotaMu.Unlock()
}

func allowlistedCodexQuotaDebugPayload(body []byte) json.RawMessage {
	root := gjson.ParseBytes(body)
	out := make(map[string]any)
	copyQuotaDebugObjectFields(out, root, quotaDebugScalarKeys)
	for _, key := range []string{"credits"} {
		if value := quotaDebugObject(root.Get(key), quotaDebugCreditsKeys); value != nil {
			out[key] = value
		}
	}
	for _, rootKey := range []string{"rate_limit", "rateLimit"} {
		rateLimit := root.Get(rootKey)
		if !rateLimit.IsObject() {
			continue
		}
		allowed := make(map[string]any)
		copyQuotaDebugObjectFields(allowed, rateLimit, quotaDebugScalarKeys)
		if credits := quotaDebugObject(rateLimit.Get("credits"), quotaDebugCreditsKeys); credits != nil {
			allowed["credits"] = credits
		}
		for _, windowKey := range []string{"primary_window", "primaryWindow", "secondary_window", "secondaryWindow", "five_hour", "fiveHour", "weekly"} {
			if window := quotaDebugObject(rateLimit.Get(windowKey), quotaDebugWindowKeys); window != nil {
				allowed[windowKey] = window
			}
		}
		out[rootKey] = allowed
	}
	encoded, errMarshal := json.Marshal(out)
	if errMarshal != nil {
		return json.RawMessage(`{}`)
	}
	return encoded
}

func quotaDebugObject(source gjson.Result, allowedKeys map[string]struct{}) map[string]any {
	if !source.IsObject() {
		return nil
	}
	result := make(map[string]any)
	copyQuotaDebugObjectFields(result, source, allowedKeys)
	return result
}

func copyQuotaDebugObjectFields(target map[string]any, source gjson.Result, allowedKeys map[string]struct{}) {
	if !source.IsObject() {
		return
	}
	for key, value := range source.Map() {
		if _, allowed := allowedKeys[key]; !allowed {
			continue
		}
		var decoded any
		if errUnmarshal := json.Unmarshal([]byte(value.Raw), &decoded); errUnmarshal == nil {
			target[key] = decoded
		}
	}
}

// CodexQuotaDebugPayload returns a copy of the latest allowlisted body for an auth.
func (m *Manager) CodexQuotaDebugPayload(authID string) (CodexQuotaDebugSnapshot, bool) {
	if m == nil {
		return CodexQuotaDebugSnapshot{}, false
	}
	m.codexQuotaMu.RLock()
	snapshot, ok := m.codexQuotaDebug[strings.TrimSpace(authID)]
	m.codexQuotaMu.RUnlock()
	if ok {
		snapshot.Payload = append(json.RawMessage(nil), snapshot.Payload...)
	}
	return snapshot, ok
}
