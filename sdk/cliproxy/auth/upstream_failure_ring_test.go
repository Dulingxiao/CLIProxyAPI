package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestUpstreamFailureRingKeepsRecentSanitizedFailures(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	authID := "codex-failure-ring"
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authID, Provider: "codex", Status: StatusActive}); errRegister != nil {
		t.Fatal(errRegister)
	}

	for index := 0; index < upstreamFailureRingCapacity+5; index++ {
		manager.MarkResult(context.Background(), Result{
			AuthID:   authID,
			Provider: "codex",
			Model:    "gpt-5.5(high)",
			Error: &Error{
				Code:       fmt.Sprintf("code-%02d", index),
				Message:    `{"error":{"type":"server_error","message":"TOKEN prompt secret"}}`,
				HTTPStatus: http.StatusInternalServerError,
			},
		})
	}

	failures := manager.UpstreamFailures(authID, "gpt-5.5")
	if len(failures) != upstreamFailureRingCapacity {
		t.Fatalf("failure count = %d, want %d", len(failures), upstreamFailureRingCapacity)
	}
	if failures[0].Code != "code-05" || failures[len(failures)-1].Code != "code-24" {
		t.Fatalf("retained codes = %q...%q", failures[0].Code, failures[len(failures)-1].Code)
	}
	if failures[0].Type != "server_error" || failures[0].HTTPStatus != http.StatusInternalServerError || failures[0].RequestScoped {
		t.Fatalf("unexpected sanitized failure: %#v", failures[0])
	}
	encoded, errMarshal := json.Marshal(failures)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	for _, forbidden := range []string{"TOKEN", "prompt", "secret", "message"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("failure history leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestUpstreamFailureRingSupportsConcurrentWritersAndFilters(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	for _, authID := range []string{"auth-a", "auth-b"} {
		if _, errRegister := manager.Register(context.Background(), &Auth{ID: authID, Provider: "codex", Status: StatusActive}); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	var writers sync.WaitGroup
	for index := 0; index < 64; index++ {
		writers.Add(1)
		go func(index int) {
			defer writers.Done()
			authID := "auth-a"
			model := "gpt-5.5"
			if index%2 == 0 {
				authID = "auth-b"
				model = "gpt-5.6"
			}
			manager.MarkResult(context.Background(), Result{
				AuthID: authID,
				Model:  model,
				Error:  &Error{Code: requestScopedErrorCode, HTTPStatus: http.StatusBadGateway},
			})
		}(index)
	}
	writers.Wait()

	for _, testCase := range []struct {
		authID string
		model  string
	}{
		{authID: "auth-a", model: "gpt-5.5"},
		{authID: "auth-b", model: "gpt-5.6"},
	} {
		failures := manager.UpstreamFailures(testCase.authID, testCase.model)
		if len(failures) != upstreamFailureRingCapacity {
			t.Fatalf("%s/%s failure count = %d", testCase.authID, testCase.model, len(failures))
		}
		for _, failure := range failures {
			if failure.AuthID != testCase.authID || failure.Model != testCase.model || !failure.RequestScoped {
				t.Fatalf("unexpected filtered failure: %#v", failure)
			}
		}
	}
}
