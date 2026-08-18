package executor

import (
	"net/http"
	"testing"
)

func TestCodexUsageLimitErrorPreservesTypedCode(t *testing.T) {
	err := newCodexStatusErr(http.StatusTooManyRequests, []byte(`{"error":{"type":"usage_limit_reached","message":"limit"}}`))
	if err.ProviderErrorCode() != "usage_limit_reached" {
		t.Fatalf("ProviderErrorCode() = %q", err.ProviderErrorCode())
	}
}
