package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexTurnStateHeader = "X-Codex-Turn-State"

type codexTurnStateOrigin struct {
	authID    string
	expiresAt time.Time
}

func codexTurnStateSeed(opts cliproxyexecutor.Options) string {
	callerScope := metadataString(opts.Metadata, cliproxyexecutor.CallerScopeMetadataKey)
	if callerScope == "" {
		return ""
	}
	sessionID := sessionHeaderValue(opts.Headers, "Session-Id")
	if sessionID == "" {
		sessionID = sessionHeaderValue(opts.Headers, "Session_id")
	}
	if sessionID == "" {
		return ""
	}
	return callerScope + "\x00" + sessionID
}

func codexTurnStateOriginKey(opts cliproxyexecutor.Options, state string) string {
	seed := codexTurnStateSeed(opts)
	state = strings.TrimSpace(state)
	if seed == "" || state == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(state))
	return seed + "\x00" + hex.EncodeToString(digest[:])
}

func (m *Manager) guardCodexTurnState(req cliproxyexecutor.Request, opts cliproxyexecutor.Options, auth *Auth) (cliproxyexecutor.Request, cliproxyexecutor.Options) {
	if m == nil || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") || !codexTurnStatePresent(req, opts) {
		return req, opts
	}
	foreign := false
	for _, state := range codexTurnStateValues(req, opts) {
		key := codexTurnStateOriginKey(opts, state)
		if key == "" {
			continue
		}
		rawOrigin, ok := m.codexTurnStateOrigins.Load(key)
		if !ok {
			continue
		}
		origin, ok := rawOrigin.(codexTurnStateOrigin)
		if !ok {
			m.codexTurnStateOrigins.Delete(key)
			continue
		}
		if !origin.expiresAt.IsZero() && time.Now().After(origin.expiresAt) {
			m.codexTurnStateOrigins.Delete(key)
			continue
		}
		if origin.authID != strings.TrimSpace(auth.ID) {
			foreign = true
			break
		}
	}
	if !foreign {
		return req, opts
	}

	opts.Headers = cloneRequestHeaders(opts.Headers)
	if opts.Headers != nil {
		opts.Headers.Del(codexTurnStateHeader)
	}
	opts.OriginalRequest = deleteCodexTurnStateFromPayload(opts.OriginalRequest)
	req.Payload = deleteCodexTurnStateFromPayload(req.Payload)
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	metadata[cliproxyexecutor.CodexTurnStateClearMetadataKey] = true
	opts.Metadata = metadata
	return req, opts
}

func codexTurnStatePresent(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) bool {
	return len(codexTurnStateValues(req, opts)) > 0
}

func codexTurnStateValues(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) []string {
	values := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	for _, value := range []string{opts.Headers.Get(codexTurnStateHeader), codexTurnStateFromPayload(req.Payload), codexTurnStateFromPayload(opts.OriginalRequest)} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values
}

func codexTurnStateFromPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	return strings.TrimSpace(gjson.GetBytes(payload, "client_metadata.x-codex-turn-state").String())
}

func deleteCodexTurnStateFromPayload(payload []byte) []byte {
	if codexTurnStateFromPayload(payload) == "" {
		return payload
	}
	updated, errDelete := sjson.DeleteBytes(payload, "client_metadata.x-codex-turn-state")
	if errDelete != nil {
		return payload
	}
	return updated
}

func (m *Manager) commitCodexTurnState(opts cliproxyexecutor.Options, auth *Auth, headers http.Header, bootstrap []cliproxyexecutor.StreamChunk) {
	if m == nil || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return
	}
	state := strings.TrimSpace(headers.Get(codexTurnStateHeader))
	if state == "" {
		state = codexTurnStateFromBootstrap(bootstrap)
	}
	if state == "" {
		return
	}
	key := codexTurnStateOriginKey(opts, state)
	if key == "" {
		return
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	m.codexTurnStateOrigins.Store(key, codexTurnStateOrigin{
		authID:    strings.TrimSpace(auth.ID),
		expiresAt: time.Now().Add(homeSessionAliasTTL(cfg)),
	})
	m.sweepCodexTurnStateOrigins()
}

func codexTurnStateFromBootstrap(chunks []cliproxyexecutor.StreamChunk) string {
	for _, chunk := range chunks {
		for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if bytes.HasPrefix(line, []byte("data:")) {
				line = bytes.TrimSpace(line[len("data:"):])
			}
			if strings.TrimSpace(gjson.GetBytes(line, "type").String()) != "response.metadata" {
				continue
			}
			if state := strings.TrimSpace(gjson.GetBytes(line, "headers.x-codex-turn-state").String()); state != "" {
				return state
			}
		}
	}
	return ""
}

func (m *Manager) sweepCodexTurnStateOrigins() {
	if m == nil || m.codexTurnStateWrites.Add(1)%256 != 0 {
		return
	}
	now := time.Now()
	m.codexTurnStateOrigins.Range(func(key, value any) bool {
		origin, ok := value.(codexTurnStateOrigin)
		if !ok || (!origin.expiresAt.IsZero() && now.After(origin.expiresAt)) {
			m.codexTurnStateOrigins.Delete(key)
		}
		return true
	})
}
