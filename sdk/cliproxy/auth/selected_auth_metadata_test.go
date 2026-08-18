package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type selectedAuthMetadataStreamExecutor struct {
	schedulerTestExecutor
}

func (selectedAuthMetadataStreamExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`{"ok":true}`)}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func TestManagerAfterAuthMetadataKeepsSelectedAuthWhenQuotaObserverCopiesOptions(t *testing.T) {
	const (
		authID = "auth-quota-copy"
		model  = "gpt-test"
	)
	manager := NewManager(nil, nil, nil)
	manager.codexQuotaEnabled.Store(true)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "codex"})
	registerSchedulerModels(t, "codex", model, authID)
	if _, errRegister := manager.Register(context.Background(), fileCodexAuth(authID)); errRegister != nil {
		t.Fatal(errRegister)
	}

	selectedAuthID := ""
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{"request_marker": "keep"},
		RequestAfterAuthInterceptor: func(_ context.Context, request cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
			selectedAuthID, _ = request.Metadata[cliproxyexecutor.SelectedAuthMetadataKey].(string)
			return cliproxyexecutor.RequestAfterAuthInterceptResponse{}
		},
	}
	if _, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, opts); errExecute != nil {
		t.Fatal(errExecute)
	}
	if selectedAuthID != authID {
		t.Fatalf("after-auth selected auth ID = %q, want %q", selectedAuthID, authID)
	}
}

func TestManagerStreamAfterAuthMetadataKeepsSelectedAuthWhenQuotaObserverCopiesOptions(t *testing.T) {
	const (
		authID = "auth-quota-stream-copy"
		model  = "gpt-test"
	)
	manager := NewManager(nil, nil, nil)
	manager.codexQuotaEnabled.Store(true)
	manager.RegisterExecutor(selectedAuthMetadataStreamExecutor{schedulerTestExecutor{provider: "codex"}})
	registerSchedulerModels(t, "codex", model, authID)
	if _, errRegister := manager.Register(context.Background(), fileCodexAuth(authID)); errRegister != nil {
		t.Fatal(errRegister)
	}

	selectedAuthID := ""
	opts := cliproxyexecutor.Options{
		Stream:   true,
		Metadata: map[string]any{"request_marker": "keep"},
		RequestAfterAuthInterceptor: func(_ context.Context, request cliproxyexecutor.RequestAfterAuthInterceptRequest) cliproxyexecutor.RequestAfterAuthInterceptResponse {
			selectedAuthID, _ = request.Metadata[cliproxyexecutor.SelectedAuthMetadataKey].(string)
			return cliproxyexecutor.RequestAfterAuthInterceptResponse{}
		},
	}
	stream, errExecute := manager.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, opts)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	for range stream.Chunks {
	}
	if selectedAuthID != authID {
		t.Fatalf("stream after-auth selected auth ID = %q, want %q", selectedAuthID, authID)
	}
}

func TestPublishSelectedAuthMetadataIncludesStableIndex(t *testing.T) {
	auth := &Auth{
		ID:       "auth-1",
		Provider: "codex",
		FileName: "auth-1.json",
	}
	selectedAuthID := ""
	selectedAuthIndex := ""
	meta := map[string]any{
		cliproxyexecutor.SelectedAuthCallbackMetadataKey: func(authID string) {
			selectedAuthID = authID
		},
		cliproxyexecutor.SelectedAuthIndexCallbackMetadataKey: func(authIndex string) {
			selectedAuthIndex = authIndex
		},
	}

	publishSelectedAuthMetadata(meta, auth)

	if selectedAuthID != auth.ID {
		t.Fatalf("selected auth ID = %q, want %q", selectedAuthID, auth.ID)
	}
	if selectedAuthIndex == "" || selectedAuthIndex != auth.Index {
		t.Fatalf("selected auth index = %q, want %q", selectedAuthIndex, auth.Index)
	}
	if got := meta[cliproxyexecutor.SelectedAuthMetadataKey]; got != auth.ID {
		t.Fatalf("selected auth metadata = %#v, want %q", got, auth.ID)
	}
	if got := meta[cliproxyexecutor.SelectedAuthIndexMetadataKey]; got != auth.Index {
		t.Fatalf("selected auth index metadata = %#v, want %q", got, auth.Index)
	}
}
