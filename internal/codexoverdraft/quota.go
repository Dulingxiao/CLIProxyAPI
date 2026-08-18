package codexoverdraft

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	quotaScale             int64 = 1_000_000
	maxQuotaPercent        int64 = 1_000
	resetConflictTolerance       = 5 * time.Second
)

// ErrUnknownQuotaSchema reports a structurally valid payload whose quota field
// names are not recognized. Callers can retain the returned key-only metadata.
var ErrUnknownQuotaSchema = errors.New("unknown active quota schema")

var unknownQuotaSchemaWarnings sync.Map

// ParsePassiveQuotaHeaders parses the allowlisted Codex quota response headers.
func ParsePassiveQuotaHeaders(authID string, generation uint64, attemptID string, headers http.Header, observedAt time.Time) (QuotaSnapshot, error) {
	snapshot := QuotaSnapshot{
		AuthID:          authID,
		AuthGeneration:  generation,
		Source:          QuotaSourcePassive,
		SourceAttemptID: attemptID,
		ObservedAt:      observedAt,
		Allowed:         true,
	}
	for _, descriptor := range []struct {
		name   WindowName
		prefix string
	}{
		{name: WindowPrimary, prefix: "X-Codex-Primary-"},
		{name: WindowSecondary, prefix: "X-Codex-Secondary-"},
	} {
		rawUsed := strings.TrimSpace(headers.Get(descriptor.prefix + "Used-Percent"))
		if rawUsed == "" {
			continue
		}
		used, errUsed := parseMicropct(rawUsed)
		if errUsed != nil {
			return QuotaSnapshot{}, fmt.Errorf("parse %s used percent: %w", descriptor.name, errUsed)
		}
		window := QuotaWindow{Name: descriptor.name, Kind: WindowUnknown, UsedMicropct: used}
		if rawMinutes := strings.TrimSpace(headers.Get(descriptor.prefix + "Window-Minutes")); rawMinutes != "" {
			minutes, errMinutes := strconv.ParseInt(rawMinutes, 10, 64)
			if errMinutes != nil || minutes <= 0 || minutes > 10*365*24*60 {
				return QuotaSnapshot{}, fmt.Errorf("invalid %s window minutes", descriptor.name)
			}
			window.WindowSeconds = minutes * 60
			window.Kind = classifyWindow(window.WindowSeconds)
		}
		resetAfter, hasResetAfter, errAfter := parsePositiveSeconds(headers.Get(descriptor.prefix + "Reset-After-Seconds"))
		if errAfter != nil {
			return QuotaSnapshot{}, fmt.Errorf("invalid %s reset-after-seconds", descriptor.name)
		}
		resetAt, hasResetAt, errAt := parseResetAt(headers.Get(descriptor.prefix+"Reset-At"), observedAt)
		if errAt != nil {
			return QuotaSnapshot{}, fmt.Errorf("invalid %s reset-at", descriptor.name)
		}
		if hasResetAfter {
			fromAfter := observedAt.Add(resetAfter)
			window.ResetAt = fromAfter
			if hasResetAt {
				if durationAbs(fromAfter.Sub(resetAt)) > resetConflictTolerance {
					snapshot.Inconsistent = true
					if resetAt.Before(fromAfter) {
						snapshot.CalibrationAt = resetAt
					} else {
						snapshot.CalibrationAt = fromAfter
					}
				} else {
					window.ResetAt = resetAt
				}
			}
		} else if hasResetAt {
			window.ResetAt = resetAt
		}
		snapshot.Windows = append(snapshot.Windows, window)
	}
	if len(snapshot.Windows) == 0 {
		return QuotaSnapshot{}, fmt.Errorf("no Codex quota headers found")
	}
	return snapshot, nil
}

// ParseActiveQuotaPayload parses Codex usage responses using snake_case or camelCase keys.
func ParseActiveQuotaPayload(authID string, generation uint64, queryID string, body []byte, observedAt time.Time) (QuotaSnapshot, error) {
	payload := gjson.ParseBytes(body)
	snapshot := QuotaSnapshot{
		AuthID:          authID,
		AuthGeneration:  generation,
		Source:          QuotaSourceActive,
		SourceAttemptID: queryID,
		ObservedAt:      observedAt,
	}
	root := payload.Get("rate_limit")
	if !root.Exists() {
		root = payload.Get("rateLimit")
	}
	if !root.Exists() || !root.IsObject() {
		return markUnknownQuotaSchema(snapshot, payload, root, "active quota payload has no rate limit object")
	}
	snapshot.Allowed = boolValue(root, "allowed")
	snapshot.LimitReached = boolValue(root, "limit_reached", "limitReached")
	snapshot.PlanType = stringValue(payload, "plan_type", "planType")
	if snapshot.PlanType == "" {
		snapshot.PlanType = stringValue(root, "plan_type", "planType")
	}
	snapshot.RateLimitReachedType = stringValue(root, "rate_limit_reached_type", "rateLimitReachedType")
	if snapshot.RateLimitReachedType == "" {
		snapshot.RateLimitReachedType = stringValue(payload, "rate_limit_reached_type", "rateLimitReachedType")
	}
	credits := firstResult(payload, "credits")
	creditsLocation := ""
	if credits.Exists() && credits.IsObject() {
		creditsLocation = "credits_top"
	} else {
		credits = firstResult(root, "credits")
		if credits.Exists() && credits.IsObject() {
			creditsLocation = "credits_rate_limit"
		}
	}
	if creditsLocation != "" {
		snapshot.HasCredits = boolValue(credits, "has_credits", "hasCredits")
		snapshot.CreditsUnlimited = boolValue(credits, "unlimited")
		snapshot.CreditsBalance = stringValue(credits, "balance")
	}
	windowVariant := "five_hour_weekly"
	descriptors := []struct {
		name    WindowName
		keys    []string
		variant string
	}{
		{name: WindowPrimary, keys: []string{"five_hour", "fiveHour"}, variant: "five_hour_weekly"},
		{name: WindowSecondary, keys: []string{"weekly"}, variant: "five_hour_weekly"},
	}
	if firstResult(root, "primary_window", "primaryWindow", "secondary_window", "secondaryWindow").Exists() {
		windowVariant = "primary_secondary"
		descriptors = []struct {
			name    WindowName
			keys    []string
			variant string
		}{
			{name: WindowPrimary, keys: []string{"primary_window", "primaryWindow"}, variant: "primary_secondary"},
			{name: WindowSecondary, keys: []string{"secondary_window", "secondaryWindow"}, variant: "primary_secondary"},
		}
	}
	for _, descriptor := range descriptors {
		windowObject := firstResult(root, descriptor.keys...)
		if !windowObject.Exists() || !windowObject.IsObject() {
			continue
		}
		usedResult := firstResult(windowObject, "used_percent", "usedPercent")
		if !usedResult.Exists() {
			continue
		}
		used, errUsed := parseMicropct(usedResult.Raw)
		if errUsed != nil {
			return QuotaSnapshot{}, fmt.Errorf("parse %s used percent: %w", descriptor.name, errUsed)
		}
		window := QuotaWindow{Name: descriptor.name, Kind: WindowUnknown, UsedMicropct: used}
		secondsResult := firstResult(windowObject, "limit_window_seconds", "limitWindowSeconds")
		if secondsResult.Exists() {
			seconds := secondsResult.Int()
			if seconds <= 0 || seconds > 10*365*24*60*60 {
				return QuotaSnapshot{}, fmt.Errorf("invalid %s limit window seconds", descriptor.name)
			}
			window.WindowSeconds = seconds
			window.Kind = classifyWindow(seconds)
		}
		resetResult := firstResult(windowObject, "reset_at", "resetAt")
		if resetResult.Exists() {
			resetAt, _, errReset := parseResetAt(resetResult.Raw, observedAt)
			if errReset != nil {
				return QuotaSnapshot{}, fmt.Errorf("invalid %s reset at", descriptor.name)
			}
			window.ResetAt = resetAt
		}
		snapshot.Windows = append(snapshot.Windows, window)
	}
	if len(snapshot.Windows) == 0 {
		return markUnknownQuotaSchema(snapshot, payload, root, "active quota payload has no recognized windows")
	}
	snapshot.SchemaVariant = windowVariant
	if creditsLocation != "" {
		snapshot.SchemaVariant += "+" + creditsLocation
	}
	return snapshot, nil
}

func stringValue(parent gjson.Result, keys ...string) string {
	result := firstResult(parent, keys...)
	if !result.Exists() || result.Type == gjson.Null {
		return ""
	}
	return strings.TrimSpace(result.String())
}

func markUnknownQuotaSchema(snapshot QuotaSnapshot, payload, root gjson.Result, message string) (QuotaSnapshot, error) {
	snapshot.SchemaVariant = "unknown"
	snapshot.UnknownSchema = true
	keySet := make(map[string]struct{})
	if payload.IsObject() {
		for key := range payload.Map() {
			keySet[sanitizeSchemaKey(key)] = struct{}{}
		}
	}
	if root.IsObject() {
		for key := range root.Map() {
			keySet["rate_limit."+sanitizeSchemaKey(key)] = struct{}{}
		}
	}
	snapshot.UnknownKeys = make([]string, 0, len(keySet))
	for key := range keySet {
		snapshot.UnknownKeys = append(snapshot.UnknownKeys, key)
	}
	sort.Strings(snapshot.UnknownKeys)
	warningKey := strings.Join(snapshot.UnknownKeys, ",")
	if _, loaded := unknownQuotaSchemaWarnings.LoadOrStore(warningKey, struct{}{}); !loaded {
		log.WithField("keys", snapshot.UnknownKeys).Warn("unknown Codex quota response schema")
	}
	return snapshot, fmt.Errorf("%w: %s", ErrUnknownQuotaSchema, message)
}

func sanitizeSchemaKey(key string) string {
	key = strings.TrimSpace(key)
	if len(key) > 64 {
		key = key[:64]
	}
	var builder strings.Builder
	for _, character := range key {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('?')
		}
	}
	return builder.String()
}

func parseMicropct(raw string) (int64, error) {
	raw = strings.TrimSpace(strings.Trim(raw, `"`))
	parts := strings.Split(raw, ".")
	if len(parts) == 0 || len(parts) > 2 || parts[0] == "" {
		return 0, fmt.Errorf("invalid decimal percentage")
	}
	whole, errWhole := strconv.ParseInt(parts[0], 10, 64)
	if errWhole != nil || whole < 0 {
		return 0, fmt.Errorf("invalid decimal percentage")
	}
	fraction := int64(0)
	if len(parts) == 2 {
		if parts[1] == "" || len(parts[1]) > 6 {
			return 0, fmt.Errorf("percentage supports at most six decimal places")
		}
		fractionRaw := parts[1] + strings.Repeat("0", 6-len(parts[1]))
		var errFraction error
		fraction, errFraction = strconv.ParseInt(fractionRaw, 10, 64)
		if errFraction != nil {
			return 0, fmt.Errorf("invalid decimal percentage")
		}
	}
	if whole > maxQuotaPercent || (whole == maxQuotaPercent && fraction > 0) {
		return 0, fmt.Errorf("percentage exceeds upper bound")
	}
	return whole*quotaScale + fraction, nil
}

func classifyWindow(seconds int64) WindowKind {
	if seconds <= 0 {
		return WindowUnknown
	}
	if seconds <= 24*60*60 {
		return WindowShort
	}
	return WindowLong
}

func parsePositiveSeconds(raw string) (time.Duration, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	seconds, errSeconds := strconv.ParseFloat(raw, 64)
	if errSeconds != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 || seconds > float64(10*365*24*60*60) {
		return 0, false, fmt.Errorf("invalid seconds")
	}
	return time.Duration(seconds * float64(time.Second)), true, nil
}

func parseResetAt(raw string, observedAt time.Time) (time.Time, bool, error) {
	raw = strings.TrimSpace(strings.Trim(raw, `"`))
	if raw == "" {
		return time.Time{}, false, nil
	}
	if parsed, errRFC3339 := time.Parse(time.RFC3339, raw); errRFC3339 == nil {
		return parsed, true, validateResetAt(parsed, observedAt)
	}
	value, errNumber := strconv.ParseFloat(raw, 64)
	if errNumber != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return time.Time{}, false, fmt.Errorf("invalid reset timestamp")
	}
	if value > 1e12 {
		value /= 1000
	}
	seconds := int64(value)
	nanos := int64((value - float64(seconds)) * float64(time.Second))
	parsed := time.Unix(seconds, nanos)
	return parsed, true, validateResetAt(parsed, observedAt)
}

func validateResetAt(resetAt, observedAt time.Time) error {
	if resetAt.Before(observedAt.Add(-time.Minute)) || resetAt.After(observedAt.Add(10*365*24*time.Hour)) {
		return fmt.Errorf("reset timestamp outside accepted range")
	}
	return nil
}

func firstResult(parent gjson.Result, keys ...string) gjson.Result {
	for _, key := range keys {
		result := parent.Get(key)
		if result.Exists() {
			return result
		}
	}
	return gjson.Result{}
}

func boolValue(parent gjson.Result, keys ...string) bool {
	return firstResult(parent, keys...).Bool()
}

func durationAbs(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

type storedQuotaWindow struct {
	Window     QuotaWindow
	ObservedAt time.Time
	Sequence   uint64
}

// QuotaState merges observations with generation and reset-cycle fencing.
type QuotaState struct {
	mu          sync.RWMutex
	windows     map[string]map[WindowName]storedQuotaWindow
	generations map[string]uint64
}

// NewQuotaState creates an empty quota merge state.
func NewQuotaState() *QuotaState {
	return &QuotaState{windows: make(map[string]map[WindowName]storedQuotaWindow), generations: make(map[string]uint64)}
}

// Merge accepts current observations and rejects stale generation or reset cycles.
func (s *QuotaState) Merge(snapshot QuotaSnapshot, currentGeneration uint64) bool {
	accepted, _ := s.MergeDurable(snapshot, currentGeneration, nil)
	return accepted
}

// MergeDurable persists an accepted observation before publishing it to merge state.
func (s *QuotaState) MergeDurable(snapshot QuotaSnapshot, currentGeneration uint64, persist func() error) (bool, error) {
	if snapshot.AuthGeneration != currentGeneration || snapshot.Inconsistent {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existingGeneration := s.generations[snapshot.AuthID]
	byName := make(map[WindowName]storedQuotaWindow)
	if existingGeneration == 0 || existingGeneration == currentGeneration {
		for name, stored := range s.windows[snapshot.AuthID] {
			byName[name] = stored
		}
	}
	accepted := false
	for _, window := range snapshot.Windows {
		current, exists := byName[window.Name]
		if exists {
			if !current.Window.ResetAt.IsZero() && !window.ResetAt.IsZero() && window.ResetAt.Before(current.Window.ResetAt) {
				continue
			}
			sameCycle := current.Window.ResetAt.Equal(window.ResetAt) || current.Window.ResetAt.IsZero() || window.ResetAt.IsZero()
			if sameCycle && (snapshot.ObservedAt.Before(current.ObservedAt) || (snapshot.ObservedAt.Equal(current.ObservedAt) && snapshot.Sequence <= current.Sequence)) {
				continue
			}
		}
		byName[window.Name] = storedQuotaWindow{Window: window, ObservedAt: snapshot.ObservedAt, Sequence: snapshot.Sequence}
		accepted = true
	}
	if !accepted {
		return false, nil
	}
	if persist != nil {
		if errPersist := persist(); errPersist != nil {
			return false, errPersist
		}
	}
	s.generations[snapshot.AuthID] = currentGeneration
	s.windows[snapshot.AuthID] = byName
	return true, nil
}

// Remove clears current runtime quota merge state for an auth lifecycle.
func (s *QuotaState) Remove(authID string) {
	if s == nil || authID == "" {
		return
	}
	s.mu.Lock()
	delete(s.windows, authID)
	delete(s.generations, authID)
	s.mu.Unlock()
}

// Window returns the latest accepted window for one auth.
func (s *QuotaState) Window(authID string, name WindowName) (QuotaWindow, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	window, ok := s.windows[authID][name]
	return window.Window, ok
}
