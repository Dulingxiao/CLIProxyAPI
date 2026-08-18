package auth

import "testing"

type typedProviderError struct{}

func (typedProviderError) Error() string             { return "limit" }
func (typedProviderError) StatusCode() int           { return 429 }
func (typedProviderError) ProviderErrorCode() string { return "usage_limit_reached" }

func TestResultErrorPreservesProviderCode(t *testing.T) {
	result := resultErrorFromError(typedProviderError{})
	if result.Code != "usage_limit_reached" || result.HTTPStatus != 429 {
		t.Fatalf("result = %#v", result)
	}
}
