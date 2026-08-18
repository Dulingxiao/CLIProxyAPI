package auth

import (
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func codexTurnStateTestOptions(state string) cliproxyexecutor.Options {
	headers := http.Header{}
	headers.Set("Session-Id", "client-session")
	if state != "" {
		headers.Set("X-Codex-Turn-State", state)
	}
	return cliproxyexecutor.Options{
		Headers: headers,
		Metadata: map[string]any{
			cliproxyexecutor.CallerScopeMetadataKey: "caller-scope",
		},
		OriginalRequest: []byte(`{"client_metadata":{"x-codex-turn-state":"state-a"}}`),
	}
}

func TestCodexTurnStateGuardKeepsSameAuthAndStripsForeignAuth(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	authA := &Auth{ID: "auth-a", Provider: "codex"}
	authB := &Auth{ID: "auth-b", Provider: "codex"}
	opts := codexTurnStateTestOptions("state-a")
	responseHeaders := http.Header{"X-Codex-Turn-State": []string{"state-a"}}
	manager.commitCodexTurnState(opts, authA, responseHeaders, nil)

	req := cliproxyexecutor.Request{Payload: []byte(`{"client_metadata":{"x-codex-turn-state":"state-a"}}`)}
	sameReq, sameOpts := manager.guardCodexTurnState(req, opts, authA)
	if got := sameOpts.Headers.Get("X-Codex-Turn-State"); got != "state-a" {
		t.Fatalf("same-auth header = %q, want state-a", got)
	}
	if got := gjson.GetBytes(sameReq.Payload, "client_metadata.x-codex-turn-state").String(); got != "state-a" {
		t.Fatalf("same-auth body state = %q, want state-a", got)
	}

	foreignReq, foreignOpts := manager.guardCodexTurnState(req, opts, authB)
	if got := foreignOpts.Headers.Get("X-Codex-Turn-State"); got != "" {
		t.Fatalf("foreign-auth header = %q, want empty", got)
	}
	if gjson.GetBytes(foreignReq.Payload, "client_metadata.x-codex-turn-state").Exists() {
		t.Fatalf("foreign-auth body retained turn state: %s", foreignReq.Payload)
	}
	if gjson.GetBytes(foreignOpts.OriginalRequest, "client_metadata.x-codex-turn-state").Exists() {
		t.Fatalf("foreign-auth original request retained turn state: %s", foreignOpts.OriginalRequest)
	}
	if !cliproxyexecutor.CodexTurnStateClearFromOptions(foreignOpts) {
		t.Fatal("foreign-auth options did not request transport-level turn-state removal")
	}
	if got := opts.Headers.Get("X-Codex-Turn-State"); got != "state-a" {
		t.Fatalf("guard mutated caller options: %q", got)
	}
}

func TestCodexTurnStateProvenanceDoesNotOverwriteEarlierClientState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	authA := &Auth{ID: "auth-a", Provider: "codex"}
	authB := &Auth{ID: "auth-b", Provider: "codex"}
	stateAOptions := codexTurnStateTestOptions("state-a")
	manager.commitCodexTurnState(stateAOptions, authA, http.Header{"X-Codex-Turn-State": []string{"state-a"}}, nil)
	stateBOptions := codexTurnStateTestOptions("state-b")
	manager.commitCodexTurnState(stateBOptions, authB, http.Header{"X-Codex-Turn-State": []string{"state-b"}}, nil)

	req := cliproxyexecutor.Request{Payload: []byte(`{"client_metadata":{"x-codex-turn-state":"state-a"}}`)}
	_, guarded := manager.guardCodexTurnState(req, stateAOptions, authB)
	if got := guarded.Headers.Get("X-Codex-Turn-State"); got != "" {
		t.Fatalf("earlier state provenance was overwritten; foreign header = %q", got)
	}
}

func TestCodexTurnStateGuardPreservesUnknownAndExpiredProvenance(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	opts := codexTurnStateTestOptions("state-a")
	req := cliproxyexecutor.Request{Payload: []byte(`{"client_metadata":{"x-codex-turn-state":"state-a"}}`)}

	_, unknown := manager.guardCodexTurnState(req, opts, &Auth{ID: "auth-b", Provider: "codex"})
	if got := unknown.Headers.Get("X-Codex-Turn-State"); got != "state-a" {
		t.Fatalf("unknown provenance header = %q, want state-a", got)
	}

	key := codexTurnStateOriginKey(opts, "state-a")
	manager.codexTurnStateOrigins.Store(key, codexTurnStateOrigin{authID: "auth-a", expiresAt: time.Now().Add(-time.Minute)})
	_, expired := manager.guardCodexTurnState(req, opts, &Auth{ID: "auth-b", Provider: "codex"})
	if got := expired.Headers.Get("X-Codex-Turn-State"); got != "state-a" {
		t.Fatalf("expired provenance header = %q, want state-a", got)
	}
	if _, ok := manager.codexTurnStateOrigins.Load(key); ok {
		t.Fatal("expired provenance was not pruned")
	}
}

func TestCodexTurnStateCommitRequiresClientVisibleState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	opts := codexTurnStateTestOptions("")
	auth := &Auth{ID: "auth-a", Provider: "codex"}

	manager.commitCodexTurnState(opts, auth, http.Header{}, nil)
	if count := codexTurnStateOriginCount(manager); count != 0 {
		t.Fatal("empty response state created provenance")
	}

	bootstrap := []cliproxyexecutor.StreamChunk{{Payload: []byte(`{"type":"response.metadata","headers":{"x-codex-turn-state":"ws-state"}}`)}}
	manager.commitCodexTurnState(opts, auth, nil, bootstrap)
	if _, ok := manager.codexTurnStateOrigins.Load(codexTurnStateOriginKey(opts, "ws-state")); !ok {
		t.Fatal("response.metadata state did not create provenance")
	}
}

func TestCodexTurnStateCommitObservesLateStreamMetadata(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	opts := codexTurnStateTestOptions("")
	auth := &Auth{ID: "auth-a", Provider: "codex"}
	remaining := make(chan cliproxyexecutor.StreamChunk, 2)
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"type":"response.output_text.delta","delta":"hello"}`)}
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"type":"response.metadata","headers":{"x-codex-turn-state":"late-state"}}`)}
	close(remaining)

	result := manager.wrapStreamResult(nil, auth, "codex", "model", nil, nil, remaining, OAuthModelAliasResult{}, false, opts)
	for range result.Chunks {
	}

	if _, ok := manager.codexTurnStateOrigins.Load(codexTurnStateOriginKey(opts, "late-state")); !ok {
		t.Fatal("late response.metadata state did not create provenance")
	}
}

func codexTurnStateOriginCount(manager *Manager) int {
	count := 0
	manager.codexTurnStateOrigins.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
