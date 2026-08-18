package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestCodexExecutorPublishesAllowlistedQuotaHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		w.Header().Set("X-Codex-Primary-Used-Percent", "98")
		w.Header().Set("X-Codex-Primary-Reset-After-Seconds", "60")
		w.Header().Set("Authorization", "secret-must-not-copy")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	}))
	defer server.Close()

	var observation cliproxyexecutor.CodexQuotaHeadersObservation
	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.AccountingAttemptMetadataKey: &cliproxyexecutor.AccountingAttemptHooks{
				CurrentAttemptID: func() string { return "attempt-accounted" },
			},
			cliproxyexecutor.CodexQuotaObserverMetadataKey: cliproxyexecutor.CodexQuotaObserver(func(got cliproxyexecutor.CodexQuotaHeadersObservation) {
				observation = got
			}),
		},
	})
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if observation.AuthID != auth.ID || observation.Header.Get("X-Codex-Primary-Used-Percent") != "98" {
		t.Fatalf("observation = %#v", observation)
	}
	if observation.AttemptID != "attempt-accounted" {
		t.Fatalf("observation attempt ID = %q", observation.AttemptID)
	}
	if observation.Header.Get("Authorization") != "" || len(observation.Header) != 2 {
		t.Fatalf("non-allowlisted headers copied: %#v", observation.Header)
	}
}
