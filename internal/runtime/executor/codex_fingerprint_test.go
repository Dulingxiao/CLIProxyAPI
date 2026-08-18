package executor

import (
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCodexFingerprintConvergenceIsOptIn(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "oauth-default-off", Metadata: map[string]any{"access_token": "oauth-token"}}
	input := []byte(`{"prompt_cache_key":"client-session","client_metadata":{"session_id":"client-session","thread_id":"client-thread"}}`)
	got, identity := applyCodexOfficialApplicationIdentity(&config.Config{}, auth, "https://chatgpt.com/backend-api/codex/responses", input)
	if !identity.enabled || identity.mode != codexFingerprintOff {
		t.Fatalf("official gating identity = %+v", identity)
	}
	if string(got) != string(input) {
		t.Fatalf("default-off request changed: %s", got)
	}
}

func TestCodexOfficialGatingHeadersRemainEnabledForOffModes(t *testing.T) {
	for _, testCase := range []struct {
		name string
		cfg  *config.Config
	}{
		{name: "default off", cfg: &config.Config{}},
		{name: "explicit off", cfg: &config.Config{Codex: config.CodexConfig{FingerprintMode: "off"}}},
		{name: "legacy cloaking disabled", cfg: &config.Config{Codex: config.CodexConfig{FingerprintMode: "full", DisableCodexCloaking: true}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.5","input":"hello"}`)
			got, identity := applyCodexOfficialApplicationIdentity(testCase.cfg, &cliproxyauth.Auth{ID: "oauth-gating", Metadata: map[string]any{"access_token": "oauth-token"}}, "https://chatgpt.com/backend-api/codex/responses", body)
			if string(got) != string(body) || !identity.enabled || identity.mode != codexFingerprintOff {
				t.Fatalf("identity = %#v body = %s", identity, got)
			}
			headers := make(http.Header)
			applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
			profile := registry.GetCodexFingerprintProfile()
			if !strings.HasPrefix(headers.Get("User-Agent"), "codex_cli_rs/") || headers.Get("Originator") != "codex_cli_rs" || headers.Get("Version") != profile.Version {
				t.Fatalf("gating headers = %#v", headers)
			}
			if headers.Get(profile.Headers.InstallationID) != "" || headers.Get(profile.Headers.TurnMetadata) != "" {
				t.Fatalf("off mode emitted convergence headers: %#v", headers)
			}
		})
	}
}

func TestCodexOfficialIdentityHeadersMatchGoldenContract(t *testing.T) {
	data, errRead := os.ReadFile("testdata/codex_official_identity_headers.json")
	if errRead != nil {
		t.Fatal(errRead)
	}
	var golden struct {
		SourceRevision string   `json:"source_revision"`
		HTTP           []string `json:"http"`
		Websocket      []string `json:"websocket"`
	}
	if errJSON := json.Unmarshal(data, &golden); errJSON != nil {
		t.Fatal(errJSON)
	}
	profile := registry.GetCodexFingerprintProfile()
	if golden.SourceRevision != profile.SourceRevision {
		t.Fatalf("golden source revision = %q, profile = %q", golden.SourceRevision, profile.SourceRevision)
	}
	auth := &cliproxyauth.Auth{ID: "oauth-golden", Metadata: map[string]any{"access_token": "oauth-token"}}
	body := []byte(`{"client_metadata":{"x-openai-subagent":"review"}}`)
	for _, test := range []struct {
		name string
		url  string
		want []string
	}{
		{name: "http", url: "https://chatgpt.com/backend-api/codex/responses", want: golden.HTTP},
		{name: "websocket", url: "wss://chatgpt.com/backend-api/codex/responses", want: golden.Websocket},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, identity := applyCodexOfficialApplicationIdentity(&config.Config{Codex: config.CodexConfig{FingerprintMode: "full"}}, auth, test.url, body)
			headers := make(http.Header)
			applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
			got := make([]string, 0, len(headers))
			for key := range headers {
				got = append(got, strings.ToLower(key))
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("header contract diff: got %#v want %#v", got, test.want)
			}
		})
	}
}

func sessionFingerprintConfig() *config.Config {
	return &config.Config{Codex: config.CodexConfig{FingerprintMode: "session"}}
}

func TestCodexFingerprintModesMatchOptInConvergenceStrength(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "oauth-modes", Metadata: map[string]any{"access_token": "oauth-token"}}
	requestURL := "https://chatgpt.com/backend-api/codex/responses"
	bodyA := []byte(`{"prompt_cache_key":"client-a","client_metadata":{"x-codex-installation-id":"install-a","session_id":"session-a","thread_id":"thread-a"}}`)
	bodyB := []byte(`{"prompt_cache_key":"client-b","client_metadata":{"x-codex-installation-id":"install-b","session_id":"session-b","thread_id":"thread-b"}}`)

	deviceBody, device := applyCodexOfficialApplicationIdentity(&config.Config{Codex: config.CodexConfig{FingerprintMode: "device"}}, auth, requestURL, bodyA)
	if !device.enabled || device.mode != codexFingerprintDevice {
		t.Fatalf("device identity = %+v", device)
	}
	if got := gjson.GetBytes(deviceBody, "client_metadata.session_id").String(); got != "session-a" {
		t.Fatalf("device mode session_id = %q, want passthrough", got)
	}
	if got := gjson.GetBytes(deviceBody, "client_metadata.thread_id").String(); got != "thread-a" {
		t.Fatalf("device mode thread_id = %q, want passthrough", got)
	}
	if got := gjson.GetBytes(deviceBody, "client_metadata.x-codex-installation-id").String(); got == "" || got == "install-a" {
		t.Fatalf("device mode installation_id = %q, want converged value", got)
	}

	sessionCfg := &config.Config{Codex: config.CodexConfig{FingerprintMode: "session"}}
	headersA := http.Header{"Session-Id": []string{"client-a"}}
	headersB := http.Header{"Session-Id": []string{"client-b"}}
	_, sessionA := applyCodexOfficialApplicationIdentity(sessionCfg, auth, requestURL, bodyA, headersA)
	_, sessionB := applyCodexOfficialApplicationIdentity(sessionCfg, auth, requestURL, bodyB, headersB)
	if sessionA.installationID != sessionB.installationID || sessionA.sessionID != sessionB.sessionID {
		t.Fatalf("session mode account identity diverged: A=%+v B=%+v", sessionA, sessionB)
	}
	if sessionA.threadID == sessionB.threadID {
		t.Fatalf("session mode collapsed distinct client sessions to one thread: %q", sessionA.threadID)
	}
	if sessionA.windowID != sessionA.threadID+":0" || sessionB.windowID != sessionB.threadID+":0" {
		t.Fatalf("session mode window IDs = %q, %q", sessionA.windowID, sessionB.windowID)
	}

	fullCfg := &config.Config{Codex: config.CodexConfig{FingerprintMode: "full"}}
	_, fullA := applyCodexOfficialApplicationIdentity(fullCfg, auth, requestURL, bodyA)
	_, fullB := applyCodexOfficialApplicationIdentity(fullCfg, auth, requestURL, bodyB)
	if fullA.installationID != fullB.installationID || fullA.sessionID != fullB.sessionID || fullA.threadID != fullB.threadID {
		t.Fatalf("full mode did not converge account identity: A=%+v B=%+v", fullA, fullB)
	}
}

func TestCodexFingerprintPerAuthModeOverridesGlobalMode(t *testing.T) {
	auth := &cliproxyauth.Auth{
		ID:         "oauth-override",
		Attributes: map[string]string{"codex_fingerprint_mode": "off"},
		Metadata:   map[string]any{"access_token": "oauth-token"},
	}
	input := []byte(`{"prompt_cache_key":"client-session"}`)
	got, identity := applyCodexOfficialApplicationIdentity(&config.Config{Codex: config.CodexConfig{FingerprintMode: "full"}}, auth, "https://chatgpt.com/backend-api/codex/responses", input)
	if !identity.enabled || identity.mode != codexFingerprintOff || string(got) != string(input) {
		t.Fatalf("per-auth off override identity=%+v body=%s", identity, got)
	}
}

func TestCodexFingerprintUsesPerAuthDeviceID(t *testing.T) {
	auth := &cliproxyauth.Auth{
		ID:         "oauth-device-id",
		Attributes: map[string]string{"openai_device_id": "configured-device-id"},
		Metadata:   map[string]any{"access_token": "oauth-token"},
	}
	body, identity := applyCodexOfficialApplicationIdentity(
		&config.Config{Codex: config.CodexConfig{FingerprintMode: "device"}},
		auth,
		"https://chatgpt.com/backend-api/codex/responses",
		[]byte(`{"client_metadata":{"x-codex-installation-id":"client-device"}}`),
	)
	if identity.installationID != "configured-device-id" {
		t.Fatalf("installation ID = %q, want configured-device-id", identity.installationID)
	}
	if got := gjson.GetBytes(body, "client_metadata.x-codex-installation-id").String(); got != "configured-device-id" {
		t.Fatalf("body installation ID = %q, want configured-device-id", got)
	}
}

func TestCodexApplicationIdentityProjectsCanonicalMetadata(t *testing.T) {
	cfg := sessionFingerprintConfig()
	auth := &cliproxyauth.Auth{
		ID:       "oauth-account-a",
		Metadata: map[string]any{"access_token": "oauth-token"},
	}
	body := []byte(`{
		"prompt_cache_key":"client-session-a",
		"client_metadata":{
			"custom-key":"preserved",
			"x-codex-parent-thread-id":"parent-thread-a",
			"parent_turn_id":"parent-turn-a",
			"x-openai-subagent":"collab_spawn"
		}
	}`)
	const requestURL = "https://chatgpt.com/backend-api/codex/responses"

	firstBody, first := applyCodexOfficialApplicationIdentity(cfg, auth, requestURL, body)
	if !first.enabled {
		t.Fatal("application identity is disabled for official OAuth request")
	}
	assertUUIDVersion(t, first.turnID, 7)
	if first.windowID != first.threadID+":0" {
		t.Fatalf("window ID = %q, want thread-scoped window", first.windowID)
	}
	if _, err := uuid.Parse(first.installationID); err != nil {
		t.Fatalf("installation ID %q is not a UUID: %v", first.installationID, err)
	}
	for name, value := range map[string]string{"installation": first.installationID, "session": first.sessionID, "thread": first.threadID} {
		parsed, errParse := uuid.Parse(value)
		if errParse != nil || parsed.Version() != 4 {
			t.Fatalf("%s ID = %q, want UUIDv4 (err=%v)", name, value, errParse)
		}
	}
	if got := gjson.GetBytes(firstBody, "client_metadata.custom-key").String(); got != "preserved" {
		t.Fatalf("custom client metadata = %q, want preserved", got)
	}

	profile := registry.GetCodexFingerprintProfile()
	if got := gjson.GetBytes(firstBody, "client_metadata."+profile.Headers.InstallationID).String(); got != first.installationID {
		t.Fatalf("body installation ID = %q, want %q", got, first.installationID)
	}
	if got := gjson.GetBytes(firstBody, "client_metadata."+profile.MetadataKeys.SessionID).String(); got != first.sessionID {
		t.Fatalf("body session ID = %q, want %q", got, first.sessionID)
	}
	if got := gjson.GetBytes(firstBody, "client_metadata."+profile.MetadataKeys.ThreadID).String(); got != first.threadID {
		t.Fatalf("body thread ID = %q, want %q", got, first.threadID)
	}
	if got := gjson.GetBytes(firstBody, "client_metadata."+profile.Headers.WindowID).String(); got != first.windowID {
		t.Fatalf("body window ID = %q, want %q", got, first.windowID)
	}

	turnMetadataRaw := gjson.GetBytes(firstBody, "client_metadata."+profile.Headers.TurnMetadata).String()
	var turnMetadata map[string]any
	if err := json.Unmarshal([]byte(turnMetadataRaw), &turnMetadata); err != nil {
		t.Fatalf("turn metadata is not JSON: %v", err)
	}
	for key, want := range map[string]string{
		profile.MetadataKeys.InstallationID: first.installationID,
		profile.MetadataKeys.SessionID:      first.sessionID,
		profile.MetadataKeys.ThreadID:       first.threadID,
		profile.MetadataKeys.TurnID:         first.turnID,
		profile.MetadataKeys.WindowID:       first.windowID,
		profile.MetadataKeys.RequestKind:    "turn",
		profile.MetadataKeys.ParentThreadID: "parent-thread-a",
		profile.MetadataKeys.ParentTurnID:   "parent-turn-a",
		profile.MetadataKeys.SubagentKind:   "collab_spawn",
	} {
		if got, _ := turnMetadata[key].(string); got != want {
			t.Fatalf("turn metadata %s = %q, want %q", key, got, want)
		}
	}
	if _, ok := turnMetadata[profile.MetadataKeys.TurnStartedAtUnixMS].(float64); !ok {
		t.Fatalf("turn metadata %s is missing or not numeric", profile.MetadataKeys.TurnStartedAtUnixMS)
	}

	headers := http.Header{}
	applyCodexOfficialApplicationIdentityHeaders(headers, &first)
	if got := headerValueCaseInsensitive(headers, profile.Headers.SessionID); got != first.sessionID {
		t.Fatalf("session header = %q, want %q", got, first.sessionID)
	}
	if got := headers.Get(profile.Headers.ClientRequestID); got != "" {
		t.Fatalf("HTTP client request header = %q, want empty", got)
	}
	if got := headers.Get(profile.Headers.TurnMetadata); got != turnMetadataRaw {
		t.Fatalf("turn metadata header differs from client_metadata: %q != %q", got, turnMetadataRaw)
	}
	if got := headers.Get(profile.Headers.ParentThreadID); got != "" {
		t.Fatalf("uncertain parent thread header = %q, want omitted", got)
	}
	if got := headers.Get(profile.Headers.Subagent); got != "collab_spawn" {
		t.Fatalf("subagent header = %q", got)
	}

	_, second := applyCodexOfficialApplicationIdentity(cfg, auth, requestURL, body)
	if second.installationID != first.installationID ||
		second.sessionID != first.sessionID ||
		second.threadID != first.threadID ||
		second.windowID != first.windowID {
		t.Fatalf("stable identity changed: first=%+v second=%+v", first, second)
	}
	if second.turnID == first.turnID {
		t.Fatalf("turn ID was reused: %q", second.turnID)
	}
}

func TestCodexApplicationIdentityProjectsCanonicalParentMetadata(t *testing.T) {
	profile := registry.GetCodexFingerprintProfile()
	rawMetadata := `{"parent_thread_id":"parent-thread-b","parent_turn_id":"parent-turn-b","subagent_kind":"review"}`
	body, identity := applyCodexOfficialApplicationIdentity(
		sessionFingerprintConfig(),
		&cliproxyauth.Auth{ID: "oauth-parent-projection", Metadata: map[string]any{"access_token": "oauth-token"}},
		"https://chatgpt.com/backend-api/codex/responses",
		[]byte(`{"prompt_cache_key":"parent-session","client_metadata":{"`+profile.Headers.TurnMetadata+`":`+strconv.Quote(rawMetadata)+`}}`),
	)
	if !identity.enabled {
		t.Fatal("application identity is disabled")
	}
	if got := gjson.GetBytes(body, "client_metadata."+profile.Headers.ParentThreadID).String(); got != "parent-thread-b" {
		t.Fatalf("parent thread projection = %q", got)
	}
	if got := gjson.GetBytes(body, "client_metadata."+profile.MetadataKeys.ParentTurnID).String(); got != "parent-turn-b" {
		t.Fatalf("parent turn projection = %q", got)
	}
	if got := gjson.GetBytes(body, "client_metadata."+profile.Headers.Subagent).String(); got != "review" {
		t.Fatalf("subagent projection = %q", got)
	}
}

func TestCodexApplicationIdentityMarksCompaction(t *testing.T) {
	cfg := sessionFingerprintConfig()
	auth := &cliproxyauth.Auth{ID: "oauth-compact", Metadata: map[string]any{"access_token": "oauth-token"}}
	body, identity := applyCodexOfficialApplicationIdentity(
		cfg,
		auth,
		"https://chatgpt.com/backend-api/codex/responses/compact",
		[]byte(`{"prompt_cache_key":"compact-session"}`),
	)
	if !identity.enabled {
		t.Fatal("compaction identity is disabled")
	}
	profile := registry.GetCodexFingerprintProfile()
	raw := gjson.GetBytes(body, "client_metadata."+profile.Headers.TurnMetadata).String()
	if got := gjson.Get(raw, profile.MetadataKeys.RequestKind).String(); got != "compaction" {
		t.Fatalf("request kind = %q, want compaction", got)
	}
	headers := http.Header{}
	applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
	if got := headers.Get(profile.Headers.InstallationID); got != "" {
		t.Fatalf("compact installation header = %q, want omitted by source-derived contract", got)
	}
}

func TestCodexOfficialIdentityPassesThroughGovernedHeadersAndDualIDs(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "oauth-governed", Metadata: map[string]any{"access_token": "oauth-token"}}
	inbound := http.Header{
		"X-Oai-Attestation":                 {"signed-attestation"},
		"X-Openai-Internal-Codex-Residency": {"us"},
	}
	_, identity := applyCodexOfficialApplicationIdentity(
		sessionFingerprintConfig(), auth, "https://chatgpt.com/backend-api/codex/responses", []byte(`{"input":"hello"}`), inbound,
	)
	headers := make(http.Header)
	applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
	if got := headers.Get("X-Oai-Attestation"); got != "signed-attestation" {
		t.Fatalf("attestation = %q", got)
	}
	if got := headers.Get("X-Openai-Internal-Codex-Residency"); got != "us" {
		t.Fatalf("residency = %q", got)
	}
	for _, key := range []string{"session_id", "session-id", "thread_id", "thread-id"} {
		if values := headers[key]; len(values) != 1 || values[0] == "" {
			t.Fatalf("dual identity header %q = %#v", key, values)
		}
	}
	if headers["session_id"][0] != headers["session-id"][0] || headers["thread_id"][0] != headers["thread-id"][0] {
		t.Fatalf("dual identity values diverged: %#v", headers)
	}
}

func TestCodexOfficialIdentityDoesNotSynthesizeGovernedHeaders(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "oauth-no-governed", Metadata: map[string]any{"access_token": "oauth-token"}}
	_, identity := applyCodexOfficialApplicationIdentity(sessionFingerprintConfig(), auth, "https://chatgpt.com/backend-api/codex/responses", []byte(`{"input":"hello"}`))
	headers := make(http.Header)
	applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
	if headers.Get("X-Oai-Attestation") != "" || headers.Get("X-Openai-Internal-Codex-Residency") != "" {
		t.Fatalf("governed headers were synthesized: %#v", headers)
	}
}

func TestCodexUserAgentAuthOverrideRequiresOfficialPrefix(t *testing.T) {
	for _, test := range []struct {
		name string
		ua   string
		want string
	}{
		{name: "valid", ua: "codex_cli_rs/0.200.0 (Mac OS 26.6; arm64)", want: "codex_cli_rs/0.200.0 (Mac OS 26.6; arm64)"},
		{name: "invalid prefix", ua: "browser/1.0", want: registry.GetCodexFingerprintProfile().UserAgent()},
		{name: "invalid value", ua: "codex_cli_rs/0.200.0\r\nInjected: yes", want: registry.GetCodexFingerprintProfile().UserAgent()},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth := &cliproxyauth.Auth{ID: "oauth-ua", Attributes: map[string]string{"codex_user_agent": test.ua}, Metadata: map[string]any{"access_token": "oauth-token"}}
			_, identity := applyCodexOfficialApplicationIdentity(&config.Config{}, auth, "https://chatgpt.com/backend-api/codex/responses", []byte(`{"input":"hello"}`))
			headers := make(http.Header)
			applyCodexOfficialApplicationIdentityHeaders(headers, &identity)
			if got := headers.Get("User-Agent"); got != test.want {
				t.Fatalf("User-Agent = %q, want %q", got, test.want)
			}
		})
	}
}

func TestApplyCodexOfficialFingerprintHeadersUsesProfile(t *testing.T) {
	cfg := sessionFingerprintConfig()
	auth := &cliproxyauth.Auth{ID: "oauth-software-profile", Metadata: map[string]any{"access_token": "oauth-token"}}
	_, identity := applyCodexOfficialApplicationIdentity(
		cfg,
		auth,
		"https://chatgpt.com/backend-api/codex/responses",
		[]byte(`{"prompt_cache_key":"software-profile-session"}`),
	)
	headers := http.Header{
		"User-Agent": {"stale-client/0.1.0"},
		"Originator": {"stale-client"},
		"Version":    {"0.1.0"},
	}
	applyCodexOfficialApplicationIdentityHeaders(headers, &identity)

	profile := registry.GetCodexFingerprintProfile()
	if got := headers.Get("User-Agent"); got != profile.UserAgent() {
		t.Fatalf("User-Agent = %q, want %q", got, profile.UserAgent())
	}
	if got := headers.Get("Originator"); got != profile.Originator {
		t.Fatalf("Originator = %q, want %q", got, profile.Originator)
	}
	if got := headers.Get("Version"); got != profile.Version {
		t.Fatalf("Version = %q, want %q", got, profile.Version)
	}
}

func TestApplyCodexWebsocketFingerprintUsesProfile(t *testing.T) {
	cfg := sessionFingerprintConfig()
	auth := &cliproxyauth.Auth{ID: "oauth-ws-profile", Metadata: map[string]any{"access_token": "oauth-token"}}
	_, identity := applyCodexOfficialApplicationIdentity(
		cfg,
		auth,
		"wss://chatgpt.com/backend-api/codex/responses",
		[]byte(`{"prompt_cache_key":"ws-profile-session"}`),
	)
	headers := http.Header{"OpenAI-Beta": {"responses_websockets=2000-01-01"}}
	applyCodexOfficialApplicationIdentityHeaders(headers, &identity)

	profile := registry.GetCodexFingerprintProfile()
	if got := headers.Get("OpenAI-Beta"); got != profile.WebsocketBeta {
		t.Fatalf("OpenAI-Beta = %q, want %q", got, profile.WebsocketBeta)
	}
	if got := headers.Get("Version"); got != profile.Version {
		t.Fatalf("Version = %q, want %q", got, profile.Version)
	}
	if got := headers.Get(profile.Headers.ClientRequestID); got != identity.threadID {
		t.Fatalf("websocket client request header = %q, want %q", got, identity.threadID)
	}
}

func TestCodexOfficialFingerprintScopeBypassesExcludedRequests(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *config.Config
		auth       *cliproxyauth.Auth
		requestURL string
	}{
		{
			name:       "api key",
			cfg:        sessionFingerprintConfig(),
			auth:       &cliproxyauth.Auth{ID: "api-key", Attributes: map[string]string{"api_key": "sk-test"}},
			requestURL: "https://chatgpt.com/backend-api/codex/responses",
		},
		{
			name:       "custom auth base URL",
			cfg:        sessionFingerprintConfig(),
			auth:       &cliproxyauth.Auth{ID: "custom", Attributes: map[string]string{"base_url": "https://gateway.example.com"}},
			requestURL: "https://gateway.example.com/responses",
		},
		{
			name:       "custom target",
			cfg:        sessionFingerprintConfig(),
			auth:       &cliproxyauth.Auth{ID: "oauth", Metadata: map[string]any{"access_token": "oauth-token"}},
			requestURL: "https://gateway.example.com/responses",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := []byte(`{"prompt_cache_key":"scope-session"}`)
			got, identity := applyCodexOfficialApplicationIdentity(tt.cfg, tt.auth, tt.requestURL, input)
			if identity.enabled {
				t.Fatalf("identity enabled for excluded scope: %+v", identity)
			}
			if string(got) != string(input) {
				t.Fatalf("excluded request body changed: %s", got)
			}
		})
	}
}

func assertUUIDVersion(t *testing.T, value string, want int) {
	t.Helper()
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("%q is not a UUID: %v", value, err)
	}
	if got := int(parsed.Version()); got != want {
		t.Fatalf("UUID %q version = %d, want %d", value, got, want)
	}
}
