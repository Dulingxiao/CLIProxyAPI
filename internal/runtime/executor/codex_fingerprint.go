package executor

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/net/http/httpguts"
)

type codexFingerprintMode string

const (
	codexFingerprintOff     codexFingerprintMode = "off"
	codexFingerprintDevice  codexFingerprintMode = "device"
	codexFingerprintSession codexFingerprintMode = "session"
	codexFingerprintFull    codexFingerprintMode = "full"
)

type codexApplicationIdentity struct {
	enabled          bool
	mode             codexFingerprintMode
	profile          registry.CodexFingerprintProfile
	installationID   string
	sessionID        string
	threadID         string
	turnID           string
	windowID         string
	requestKind      string
	turnStartedAtMS  int64
	parentThreadID   string
	parentTurnID     string
	subagentKind     string
	turnMetadataJSON string
	userAgent        string
	attestation      string
	residency        string
	websocket        bool
}

func applyCodexOfficialApplicationIdentity(
	cfg *config.Config,
	auth *cliproxyauth.Auth,
	requestURL string,
	body []byte,
	clientHeaderSets ...http.Header,
) ([]byte, codexApplicationIdentity) {
	mode := codexFingerprintModeForRequest(cfg, auth)
	if !codexOfficialFingerprintScope(cfg, auth, requestURL) {
		return body, codexApplicationIdentity{}
	}
	profile := registry.GetCodexFingerprintProfile()
	if cfg != nil && cfg.Codex.DisableCodexCloaking {
		mode = codexFingerprintOff
	}
	var clientHeaders http.Header
	if len(clientHeaderSets) > 0 {
		clientHeaders = clientHeaderSets[0]
	}
	baseIdentity := codexApplicationIdentity{
		enabled:     true,
		mode:        mode,
		profile:     profile,
		userAgent:   codexUserAgentForAuth(profile, auth),
		attestation: strings.TrimSpace(headerValueCaseInsensitive(clientHeaders, profile.Headers.Attestation)),
		residency:   strings.TrimSpace(headerValueCaseInsensitive(clientHeaders, profile.Headers.Residency)),
		websocket:   codexApplicationIsWebsocket(requestURL),
	}
	if mode == codexFingerprintOff || len(body) == 0 {
		return body, baseIdentity
	}

	authIdentity := strings.TrimSpace(auth.ID)
	rawTurnMetadata := clientMetadataString(body, profile.Headers.TurnMetadata)
	turnMetadata := parseCodexTurnMetadata(body, profile)
	installationID := codexConvergedInstallationID(auth)
	body = setCodexClientMetadataString(body, profile.Headers.InstallationID, installationID)
	turnMetadata[profile.MetadataKeys.InstallationID] = installationID
	if mode == codexFingerprintDevice {
		turnMetadataJSON := ""
		if rawTurnMetadata != "" {
			turnMetadataJSONBytes, _ := json.Marshal(turnMetadata)
			turnMetadataJSON = string(turnMetadataJSONBytes)
			body = setCodexClientMetadataString(body, profile.Headers.TurnMetadata, turnMetadataJSON)
		}
		baseIdentity.installationID = installationID
		baseIdentity.turnMetadataJSON = turnMetadataJSON
		return body, baseIdentity
	}

	clientSessionSeed := codexFingerprintClientSessionID(clientHeaders)
	sessionID := stableCodexApplicationUUID(authIdentity, "session", "")
	threadID := sessionID
	if mode == codexFingerprintSession && clientSessionSeed != "" {
		threadID = stableCodexApplicationUUID(authIdentity, "thread", clientSessionSeed)
	}
	turnID := newCodexUUIDv7()
	windowID := threadID + ":0"

	parentThreadID := clientMetadataString(body, profile.Headers.ParentThreadID)
	if parentThreadID == "" {
		parentThreadID, _ = turnMetadata[profile.MetadataKeys.ParentThreadID].(string)
	}
	parentTurnID := clientMetadataString(body, profile.MetadataKeys.ParentTurnID)
	if parentTurnID == "" {
		parentTurnID, _ = turnMetadata[profile.MetadataKeys.ParentTurnID].(string)
	}
	subagentKind := clientMetadataString(body, profile.Headers.Subagent)
	if subagentKind == "" {
		subagentKind, _ = turnMetadata[profile.MetadataKeys.SubagentKind].(string)
	}

	requestKind := codexApplicationRequestKind(requestURL)
	turnStartedAtMS := time.Now().UnixMilli()
	if existing, ok := turnMetadata[profile.MetadataKeys.TurnStartedAtUnixMS].(float64); ok && existing > 0 {
		turnStartedAtMS = int64(existing)
	}

	turnMetadata[profile.MetadataKeys.SessionID] = sessionID
	turnMetadata[profile.MetadataKeys.ThreadID] = threadID
	turnMetadata[profile.MetadataKeys.TurnID] = turnID
	turnMetadata[profile.MetadataKeys.WindowID] = windowID
	turnMetadata[profile.MetadataKeys.RequestKind] = requestKind
	turnMetadata[profile.MetadataKeys.TurnStartedAtUnixMS] = turnStartedAtMS
	if parentThreadID != "" {
		turnMetadata[profile.MetadataKeys.ParentThreadID] = parentThreadID
	}
	if parentTurnID != "" {
		turnMetadata[profile.MetadataKeys.ParentTurnID] = parentTurnID
	}
	if subagentKind != "" {
		turnMetadata[profile.MetadataKeys.SubagentKind] = subagentKind
	}
	turnMetadataJSONBytes, _ := json.Marshal(turnMetadata)
	turnMetadataJSON := string(turnMetadataJSONBytes)

	body = setCodexClientMetadataString(body, profile.MetadataKeys.SessionID, sessionID)
	body = setCodexClientMetadataString(body, profile.MetadataKeys.ThreadID, threadID)
	body = setCodexClientMetadataString(body, profile.MetadataKeys.TurnID, turnID)
	body = setCodexClientMetadataString(body, profile.Headers.WindowID, windowID)
	body = setCodexClientMetadataString(body, profile.Headers.TurnMetadata, turnMetadataJSON)
	if parentThreadID != "" {
		body = setCodexClientMetadataString(body, profile.Headers.ParentThreadID, parentThreadID)
	}
	if parentTurnID != "" {
		body = setCodexClientMetadataString(body, profile.MetadataKeys.ParentTurnID, parentTurnID)
	}
	if subagentKind != "" {
		body = setCodexClientMetadataString(body, profile.Headers.Subagent, subagentKind)
	}

	return body, codexApplicationIdentity{
		enabled:          true,
		mode:             mode,
		profile:          profile,
		installationID:   installationID,
		sessionID:        sessionID,
		threadID:         threadID,
		turnID:           turnID,
		windowID:         windowID,
		requestKind:      requestKind,
		turnStartedAtMS:  turnStartedAtMS,
		parentThreadID:   parentThreadID,
		parentTurnID:     parentTurnID,
		subagentKind:     subagentKind,
		turnMetadataJSON: turnMetadataJSON,
		userAgent:        baseIdentity.userAgent,
		attestation:      baseIdentity.attestation,
		residency:        baseIdentity.residency,
		websocket:        codexApplicationIsWebsocket(requestURL),
	}
}

func applyCodexOfficialApplicationIdentityHeaders(headers http.Header, identity *codexApplicationIdentity) {
	if headers == nil || identity == nil || !identity.enabled {
		return
	}
	profile := identity.profile
	headers.Set("User-Agent", identity.userAgent)
	headers.Set("Originator", profile.Originator)
	headers.Set("Version", profile.Version)
	if identity.attestation != "" {
		headers.Set(profile.Headers.Attestation, identity.attestation)
	}
	if identity.residency != "" {
		headers.Set(profile.Headers.Residency, identity.residency)
	}
	if identity.websocket {
		headers.Set("OpenAI-Beta", profile.WebsocketBeta)
	}
	if identity.mode == codexFingerprintOff {
		return
	}
	if codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.InstallationID) {
		headers.Set(profile.Headers.InstallationID, identity.installationID)
	}
	if identity.mode == codexFingerprintDevice {
		if identity.turnMetadataJSON != "" && codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.TurnMetadata) {
			headers.Set(profile.Headers.TurnMetadata, identity.turnMetadataJSON)
		}
		return
	}
	if codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.SessionID) {
		setCodexDualIdentityHeaders(headers, "session", identity.sessionID)
	}
	if codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.ThreadID) {
		setCodexDualIdentityHeaders(headers, "thread", identity.threadID)
	}
	if identity.websocket && codexProfileIncludesHeader(profile, true, profile.Headers.ClientRequestID) {
		headers.Set(profile.Headers.ClientRequestID, identity.threadID)
	}
	if codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.WindowID) {
		headers.Set(profile.Headers.WindowID, identity.windowID)
	}
	if codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.TurnMetadata) {
		headers.Set(profile.Headers.TurnMetadata, identity.turnMetadataJSON)
	}
	if identity.parentThreadID != "" && codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.ParentThreadID) {
		headers.Set(profile.Headers.ParentThreadID, identity.parentThreadID)
	}
	if identity.subagentKind != "" && codexProfileIncludesHeader(profile, identity.websocket, profile.Headers.Subagent) {
		headers.Set(profile.Headers.Subagent, identity.subagentKind)
	}
}

func codexUserAgentForAuth(profile registry.CodexFingerprintProfile, auth *cliproxyauth.Auth) string {
	value := ""
	if auth != nil && auth.Attributes != nil {
		value = strings.TrimSpace(auth.Attributes["codex_user_agent"])
	}
	if value == "" && auth != nil && auth.Metadata != nil {
		value, _ = auth.Metadata["codex_user_agent"].(string)
		value = strings.TrimSpace(value)
	}
	if strings.HasPrefix(value, profile.Originator+"/") && httpguts.ValidHeaderFieldValue(value) {
		return value
	}
	return profile.UserAgent()
}

func codexProfileIncludesHeader(profile registry.CodexFingerprintProfile, websocket bool, name string) bool {
	names := profile.HTTPHeaderNames
	if websocket {
		names = profile.WebsocketHeaderNames
	}
	for _, candidate := range names {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

func setCodexDualIdentityHeaders(headers http.Header, kind, value string) {
	if headers == nil || strings.TrimSpace(value) == "" {
		return
	}
	underscore := strings.ToLower(strings.TrimSpace(kind)) + "_id"
	hyphen := strings.ToLower(strings.TrimSpace(kind)) + "-id"
	for key := range headers {
		if strings.EqualFold(key, underscore) || strings.EqualFold(key, hyphen) {
			delete(headers, key)
		}
	}
	headers[underscore] = []string{value}
	headers[hyphen] = []string{value}
}

func codexApplicationIsWebsocket(requestURL string) bool {
	parsed, errParse := url.Parse(strings.TrimSpace(requestURL))
	return errParse == nil && strings.EqualFold(parsed.Scheme, "wss")
}

func codexOfficialFingerprintScope(cfg *config.Config, auth *cliproxyauth.Auth, requestURL string) bool {
	if auth == nil || strings.TrimSpace(auth.ID) == "" {
		return false
	}
	_ = cfg
	return codexOfficialApplicationTarget(auth, requestURL)
}

func codexFingerprintModeForRequest(cfg *config.Config, auth *cliproxyauth.Auth) codexFingerprintMode {
	raw := ""
	if auth != nil && auth.Attributes != nil {
		raw = strings.TrimSpace(auth.Attributes["codex_fingerprint_mode"])
	}
	if raw == "" && auth != nil && auth.Metadata != nil {
		if value, ok := auth.Metadata["codex_fingerprint_mode"].(string); ok {
			raw = strings.TrimSpace(value)
		}
	}
	if raw == "" && cfg != nil {
		raw = strings.TrimSpace(cfg.Codex.FingerprintMode)
	}
	switch codexFingerprintMode(strings.ToLower(raw)) {
	case codexFingerprintDevice:
		return codexFingerprintDevice
	case codexFingerprintSession:
		return codexFingerprintSession
	case codexFingerprintFull:
		return codexFingerprintFull
	default:
		return codexFingerprintOff
	}
}

func codexOfficialApplicationTarget(auth *cliproxyauth.Auth, requestURL string) bool {
	if auth == nil {
		return false
	}
	if auth.Attributes != nil {
		if strings.TrimSpace(auth.Attributes["api_key"]) != "" ||
			strings.TrimSpace(auth.Attributes["base_url"]) != "" {
			return false
		}
	}
	parsed, errParse := url.Parse(strings.TrimSpace(requestURL))
	if errParse != nil || !strings.EqualFold(parsed.Hostname(), "chatgpt.com") {
		return false
	}
	path := strings.TrimSuffix(parsed.Path, "/")
	return path == "/backend-api/codex/responses" ||
		path == "/backend-api/codex/responses/compact"
}

func codexApplicationRequestKind(requestURL string) string {
	parsed, _ := url.Parse(strings.TrimSpace(requestURL))
	if strings.HasSuffix(strings.TrimSuffix(parsed.Path, "/"), "/responses/compact") {
		return "compaction"
	}
	return "turn"
}

func stableCodexApplicationUUID(authIdentity, kind, value string) string {
	value = strings.TrimSpace(value)
	name := strings.Join([]string{"cli-proxy-api", "codex", "application-identity", kind, authIdentity, value}, "\x00")
	sum := sha256.Sum256([]byte(name))
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}

func codexConvergedInstallationID(auth *cliproxyauth.Auth) string {
	if auth != nil {
		if auth.Attributes != nil {
			if value := strings.TrimSpace(auth.Attributes["openai_device_id"]); value != "" {
				return value
			}
		}
		if auth.Metadata != nil {
			if value, ok := auth.Metadata["openai_device_id"].(string); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	authIdentity := ""
	if auth != nil {
		authIdentity = strings.TrimSpace(auth.ID)
	}
	return stableCodexApplicationUUID(authIdentity, "installation", "")
}

func codexFingerprintClientSessionID(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if value := strings.TrimSpace(headers.Get("Session-Id")); value != "" {
		return value
	}
	return strings.TrimSpace(headers.Get("Session_id"))
}

func newCodexUUIDv7() string {
	value, errNew := uuid.NewV7()
	if errNew != nil {
		return uuid.NewString()
	}
	return value.String()
}

func parseCodexTurnMetadata(body []byte, profile registry.CodexFingerprintProfile) map[string]any {
	metadata := make(map[string]any)
	raw := clientMetadataString(body, profile.Headers.TurnMetadata)
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &metadata)
	}
	return metadata
}

func clientMetadataString(body []byte, key string) string {
	return strings.TrimSpace(gjson.GetBytes(body, codexClientMetadataPath(key)).String())
}

func setCodexClientMetadataString(body []byte, key, value string) []byte {
	updated, errSet := sjson.SetBytes(body, codexClientMetadataPath(key), value)
	if errSet != nil {
		return body
	}
	return updated
}

func codexClientMetadataPath(key string) string {
	escaped := strings.ReplaceAll(strings.TrimSpace(key), "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, ".", "\\.")
	return "client_metadata." + escaped
}
