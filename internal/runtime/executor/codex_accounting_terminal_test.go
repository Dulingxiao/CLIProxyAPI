package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type codexTerminalUsageSink struct {
	mu      sync.Mutex
	records []coreusage.Record
}

func (s *codexTerminalUsageSink) HandleUsage(ctx context.Context, record coreusage.Record) {
	_ = s.HandleUsageDurable(ctx, record)
}

func (s *codexTerminalUsageSink) HandleUsageDurable(_ context.Context, record coreusage.Record) error {
	s.mu.Lock()
	s.records = append(s.records, record)
	s.mu.Unlock()
	return nil
}

func (s *codexTerminalUsageSink) snapshot() []coreusage.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]coreusage.Record(nil), s.records...)
}

func TestCodexExecutorClosesSuccessfulTerminalsWithoutReportedUsage(t *testing.T) {
	sink := &codexTerminalUsageSink{}
	coreusage.DefaultManager().SetBuiltinSink(sink)
	defer coreusage.DefaultManager().SetBuiltinSink(nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","status":"completed","service_tier":"default","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"hello"}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}
	if _, errExecute := executor.Execute(context.Background(), auth, req, opts); errExecute != nil {
		t.Fatal(errExecute)
	}
	opts.Stream = true
	result, errStream := executor.ExecuteStream(context.Background(), auth, req, opts)
	if errStream != nil {
		t.Fatal(errStream)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
	}

	records := sink.snapshot()
	if len(records) != 2 {
		t.Fatalf("terminal usage records = %#v, want 2", records)
	}
	for _, record := range records {
		if record.UsageReported || record.Failed || record.Provider != "codex" || record.AuthID != "auth-1" || record.ResponseServiceTier != "default" {
			t.Fatalf("terminal usage record = %#v", record)
		}
	}
}
