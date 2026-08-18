package handlers

import (
	"net/http"
	"testing"
)

func TestFilterUpstreamHeaders_RemovesConnectionScopedHeaders(t *testing.T) {
	src := http.Header{}
	src.Add("Connection", "keep-alive, x-hop-a, x-hop-b")
	src.Add("Connection", "x-hop-c")
	src.Set("Keep-Alive", "timeout=5")
	src.Set("X-Hop-A", "a")
	src.Set("X-Hop-B", "b")
	src.Set("X-Hop-C", "c")
	src.Set("X-Request-Id", "req-1")
	src.Set("Set-Cookie", "session=secret")
	src.Set("x-cpa-trace-id", "upstream-trace")
	src.Set("Access-Control-Expose-Headers", "upstream-header")

	filtered := FilterUpstreamHeaders(src)
	if filtered == nil {
		t.Fatalf("expected filtered headers, got nil")
	}

	requestID := filtered.Get("X-Request-Id")
	if requestID != "req-1" {
		t.Fatalf("expected X-Request-Id to be preserved, got %q", requestID)
	}

	blockedHeaderKeys := []string{
		"Connection",
		"Keep-Alive",
		"X-Hop-A",
		"X-Hop-B",
		"X-Hop-C",
		"Set-Cookie",
		"x-cpa-trace-id",
		"Access-Control-Expose-Headers",
	}
	for _, key := range blockedHeaderKeys {
		value := filtered.Get(key)
		if value != "" {
			t.Fatalf("expected %s to be removed, got %q", key, value)
		}
	}
}

func TestFilterUpstreamHeaders_ReturnsNilWhenAllHeadersBlocked(t *testing.T) {
	src := http.Header{}
	src.Add("Connection", "x-hop-a")
	src.Set("X-Hop-A", "a")
	src.Set("Set-Cookie", "session=secret")

	filtered := FilterUpstreamHeaders(src)
	if filtered != nil {
		t.Fatalf("expected nil when all headers are filtered, got %#v", filtered)
	}
}

func TestDownstreamHeadersAlwaysRelaysCodexTurnState(t *testing.T) {
	src := http.Header{}
	src.Set("X-Codex-Turn-State", "turn-state")
	src.Set("X-Request-Id", "request-id")

	filtered := downstreamHeadersFromExecutor(src, false)
	if got := filtered.Get("X-Codex-Turn-State"); got != "turn-state" {
		t.Fatalf("X-Codex-Turn-State = %q, want turn-state", got)
	}
	if got := filtered.Get("X-Request-Id"); got != "" {
		t.Fatalf("X-Request-Id = %q, want empty without passthrough", got)
	}
}

func TestDownstreamHeadersAfterNoopInterceptorKeepsCodexTurnState(t *testing.T) {
	base := http.Header{}
	base.Set("X-Codex-Turn-State", "turn-state")
	base.Set("X-Request-Id", "request-id")

	filtered := downstreamHeadersAfterInterceptors(base, base.Clone(), false)
	if got := filtered.Get("X-Codex-Turn-State"); got != "turn-state" {
		t.Fatalf("X-Codex-Turn-State = %q, want turn-state", got)
	}
	if got := filtered.Get("X-Request-Id"); got != "" {
		t.Fatalf("X-Request-Id = %q, want empty without passthrough", got)
	}
}

func TestDownstreamHeadersAfterInterceptorCanRemoveCodexTurnState(t *testing.T) {
	base := http.Header{"X-Codex-Turn-State": []string{"turn-state"}}
	filtered := downstreamHeadersAfterInterceptors(base, http.Header{}, false)
	if got := filtered.Get("X-Codex-Turn-State"); got != "" {
		t.Fatalf("X-Codex-Turn-State = %q, want interceptor removal", got)
	}
}
