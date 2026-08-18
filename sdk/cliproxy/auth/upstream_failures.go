package auth

import (
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const (
	upstreamFailureRingCapacity    = 20
	upstreamFailureModelLimit      = 32
	unknownUpstreamFailureModelKey = "*"
)

// UpstreamFailure is the sanitized diagnostic record retained for one failed
// upstream attempt. It deliberately excludes messages and response bodies.
type UpstreamFailure struct {
	AuthID        string    `json:"auth_id"`
	Model         string    `json:"model"`
	Code          string    `json:"code,omitempty"`
	Type          string    `json:"type,omitempty"`
	HTTPStatus    int       `json:"http_status,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	RequestScoped bool      `json:"request_scoped"`
}

type authUpstreamFailureHistory struct {
	byModel    map[string][]UpstreamFailure
	modelOrder []string
}

func (m *Manager) recordUpstreamFailureLocked(result Result, modelKey string, now time.Time) {
	if m == nil || result.Error == nil || strings.TrimSpace(result.AuthID) == "" {
		return
	}
	if modelKey == "" {
		modelKey = unknownUpstreamFailureModelKey
	}
	history := m.upstreamFailures[result.AuthID]
	if history == nil {
		history = &authUpstreamFailureHistory{byModel: make(map[string][]UpstreamFailure)}
		m.upstreamFailures[result.AuthID] = history
	}
	if _, exists := history.byModel[modelKey]; !exists {
		if len(history.modelOrder) >= upstreamFailureModelLimit {
			oldest := history.modelOrder[0]
			history.modelOrder = history.modelOrder[1:]
			delete(history.byModel, oldest)
		}
		history.modelOrder = append(history.modelOrder, modelKey)
	}
	failure := UpstreamFailure{
		AuthID:        result.AuthID,
		Model:         modelKey,
		Code:          strings.TrimSpace(result.Error.Code),
		Type:          upstreamFailureType(result.Error),
		HTTPStatus:    result.Error.StatusCode(),
		Timestamp:     now.UTC(),
		RequestScoped: result.Error.IsRequestScoped(),
	}
	ring := history.byModel[modelKey]
	if len(ring) >= upstreamFailureRingCapacity {
		copy(ring, ring[len(ring)-upstreamFailureRingCapacity+1:])
		ring = ring[:upstreamFailureRingCapacity-1]
	}
	history.byModel[modelKey] = append(ring, failure)
}

func upstreamFailureType(err *Error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Message)
	if gjson.Valid(message) {
		for _, path := range []string{"error.type", "type", "body.error.type"} {
			if value := strings.TrimSpace(gjson.Get(message, path).String()); value != "" {
				return value
			}
		}
	}
	return strings.TrimSpace(err.Code)
}

// UpstreamFailures returns a chronological copy of sanitized failure metadata.
// Empty filters match every auth or model respectively.
func (m *Manager) UpstreamFailures(authID, model string) []UpstreamFailure {
	if m == nil {
		return nil
	}
	authID = strings.TrimSpace(authID)
	model = canonicalModelKey(model)
	m.mu.RLock()
	result := make([]UpstreamFailure, 0)
	for currentAuthID, history := range m.upstreamFailures {
		if authID != "" && currentAuthID != authID || history == nil {
			continue
		}
		for currentModel, ring := range history.byModel {
			if model != "" && currentModel != model {
				continue
			}
			result = append(result, ring...)
		}
	}
	m.mu.RUnlock()
	sort.SliceStable(result, func(left, right int) bool {
		return result[left].Timestamp.Before(result[right].Timestamp)
	})
	return result
}
