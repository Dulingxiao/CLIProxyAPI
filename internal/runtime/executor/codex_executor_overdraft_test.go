package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func overdraftOptions(begin func() error, release func()) cliproxyexecutor.Options {
	execution := cliproxyexecutor.NewCodexOverdraftExecution(
		"auth-1", 1, "cycle-1", 2, 3, "dispatch-1",
		cliproxyexecutor.CodexOverdraftBusiness,
		begin, release, func() error { return nil },
	)
	return cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.CodexOverdraftExecutionMetadataKey: execution,
		},
	}
}

func TestCodexExecutorInjectsOverdraftBeforeActualHTTPSend(t *testing.T) {
	var beginCalled atomic.Bool
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !beginCalled.Load() {
			t.Error("upstream request arrived before BeginSend")
		}
		var errRead error
		gotBody, errRead = io.ReadAll(request.Body)
		if errRead != nil {
			t.Errorf("read request body: %v", errRead)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	original := []byte(`{"model":"gpt-test","input":[{"type":"message","role":"user","content":"hello"}]}`)
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-test", Payload: original}, overdraftOptions(func() error {
		beginCalled.Store(true)
		return nil
	}, func() {}))
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := gjson.GetBytes(gotBody, "input.#").Int(); got != 3 {
		t.Fatalf("upstream input length = %d, body = %s", got, gotBody)
	}
	if gjson.GetBytes(gotBody, "input.1.arguments").Type != gjson.String || gjson.GetBytes(gotBody, "input.2.output").Type != gjson.String {
		t.Fatalf("injected fields are not strings: %s", gotBody)
	}
	if got := gjson.GetBytes(original, "input.#").Int(); got != 1 {
		t.Fatalf("original input length = %d, want 1", got)
	}
}

func TestCodexExecutorMovesCompactionTriggerToEnd(t *testing.T) {
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var errRead error
		gotBody, errRead = io.ReadAll(request.Body)
		if errRead != nil {
			t.Errorf("read request body: %v", errRead)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	payload := []byte(`{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"},{"type":"message","role":"user","content":"after"}]}`)
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.4", Payload: payload}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	input := gjson.GetBytes(gotBody, "input").Array()
	if len(input) != 3 {
		t.Fatalf("upstream input length = %d, body = %s", len(input), gotBody)
	}
	if got := input[2].Get("type").String(); got != "compaction_trigger" {
		t.Fatalf("final input item type = %q, want compaction_trigger; body=%s", got, gotBody)
	}
	if got := input[1].Get("content").String(); got != "after" {
		t.Fatalf("non-trigger item was not preserved before trigger: %s", gotBody)
	}
}

func TestCodexExecutorKeepsCompactionTriggerFinalWithOverdraft(t *testing.T) {
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var errRead error
		gotBody, errRead = io.ReadAll(request.Body)
		if errRead != nil {
			t.Errorf("read request body: %v", errRead)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[{\"type\":\"compaction\",\"encrypted_content\":\"opaque\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	payload := []byte(`{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`)
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.4", Payload: payload}, overdraftOptions(func() error { return nil }, func() {}))
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	input := gjson.GetBytes(gotBody, "input").Array()
	if len(input) == 0 {
		t.Fatalf("upstream body missing input: %s", gotBody)
	}
	if got := input[len(input)-1].Get("type").String(); got != "compaction_trigger" {
		t.Fatalf("final input item type = %q, want compaction_trigger; body=%s", got, gotBody)
	}
}

func TestCodexExecutorBeginSendFailureDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	var released atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"model":"gpt-test","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}, overdraftOptions(func() error { return errors.New("stale lease") }, func() { released.Store(true) }))
	if errExecute == nil || !cliproxyexecutor.IsUpstreamNotDispatched(errExecute) {
		t.Fatalf("Execute() error = %v, want not-dispatched error", errExecute)
	}
	if requests.Load() != 0 || !released.Load() {
		t.Fatalf("requests = %d, released = %t", requests.Load(), released.Load())
	}
}

func TestCodexExecutorWithoutOverdraftLeavesInputUninjected(t *testing.T) {
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		gotBody, _ = io.ReadAll(request.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	}))
	defer server.Close()
	executor := NewCodexExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	_, errExecute := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-test",
		Payload: []byte(`{"model":"gpt-test","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if got := gjson.GetBytes(gotBody, "input.#").Int(); got != 1 {
		t.Fatalf("input length = %d, want 1: %s", got, gotBody)
	}
}
