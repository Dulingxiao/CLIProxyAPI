package helps

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var codexQuotaHeaderAllowlist = []string{
	"X-Codex-Primary-Used-Percent",
	"X-Codex-Primary-Reset-After-Seconds",
	"X-Codex-Primary-Reset-At",
	"X-Codex-Primary-Window-Minutes",
	"X-Codex-Secondary-Used-Percent",
	"X-Codex-Secondary-Reset-After-Seconds",
	"X-Codex-Secondary-Reset-At",
	"X-Codex-Secondary-Window-Minutes",
}

type overdraftFunctionCall struct {
	Type      string `json:"type"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type overdraftFunctionOutput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

// InjectCodexOverdraft appends the fixed zz call pair to a valid non-empty input array.
func InjectCodexOverdraft(body []byte) ([]byte, error) {
	return injectCodexOverdraftWithReader(body, rand.Reader)
}

func injectCodexOverdraftWithReader(body []byte, random io.Reader) ([]byte, error) {
	input := gjson.GetBytes(body, "input")
	if !input.Exists() || !input.IsArray() {
		return nil, fmt.Errorf("Codex overdraft input must be a non-empty array")
	}
	callID := ""
	for attempts := 0; attempts < 32; attempts++ {
		var value [16]byte
		if _, errRead := io.ReadFull(random, value[:]); errRead != nil {
			return nil, fmt.Errorf("generate Codex overdraft call ID: %w", errRead)
		}
		candidate := "call_" + hex.EncodeToString(value[:])
		hasInput := false
		collision := false
		input.ForEach(func(_, item gjson.Result) bool {
			hasInput = true
			if strings.TrimSpace(item.Get("call_id").String()) == candidate {
				collision = true
				return false
			}
			return true
		})
		if !hasInput {
			return nil, fmt.Errorf("Codex overdraft input must be a non-empty array")
		}
		if !collision {
			callID = candidate
			break
		}
	}
	if callID == "" {
		return nil, fmt.Errorf("generate collision-free Codex overdraft call ID")
	}
	callRaw, errCall := json.Marshal(overdraftFunctionCall{Type: "function_call", CallID: callID, Name: "zz", Arguments: "{}"})
	if errCall != nil {
		return nil, fmt.Errorf("encode Codex overdraft call: %w", errCall)
	}
	outputRaw, errOutput := json.Marshal(overdraftFunctionOutput{Type: "function_call_output", CallID: callID, Output: "{}"})
	if errOutput != nil {
		return nil, fmt.Errorf("encode Codex overdraft output: %w", errOutput)
	}
	insertAt := input.Index + len(input.Raw) - 1
	if input.Index < 0 || insertAt < 0 || insertAt >= len(body) || body[insertAt] != ']' {
		return nil, fmt.Errorf("locate Codex overdraft input array")
	}
	injected := make([]byte, 0, len(body)+len(callRaw)+len(outputRaw)+2)
	injected = append(injected, body[:insertAt]...)
	injected = append(injected, ',')
	injected = append(injected, callRaw...)
	injected = append(injected, ',')
	injected = append(injected, outputRaw...)
	injected = append(injected, body[insertAt:]...)
	if errValidate := ValidateCodexOverdraftInjection(injected); errValidate != nil {
		return nil, errValidate
	}
	return injected, nil
}

// ValidateCodexOverdraftInjection verifies the exact adjacent terminal pair.
func ValidateCodexOverdraftInjection(body []byte) error {
	input := gjson.GetBytes(body, "input")
	if !input.Exists() || !input.IsArray() {
		return fmt.Errorf("Codex overdraft input is not an array")
	}
	itemCount := int(gjson.GetBytes(body, "input.#").Int())
	if itemCount < 2 {
		return fmt.Errorf("Codex overdraft input has no terminal call pair")
	}
	call := gjson.GetBytes(body, "input."+strconv.Itoa(itemCount-2))
	output := gjson.GetBytes(body, "input."+strconv.Itoa(itemCount-1))
	if call.Get("type").String() != "function_call" || call.Get("name").String() != "zz" {
		return fmt.Errorf("Codex overdraft function call is invalid")
	}
	arguments := call.Get("arguments")
	if arguments.Type != gjson.String || arguments.String() != "{}" {
		return fmt.Errorf("Codex overdraft arguments must be the string {}")
	}
	if output.Get("type").String() != "function_call_output" {
		return fmt.Errorf("Codex overdraft function output is invalid")
	}
	outputValue := output.Get("output")
	if outputValue.Type != gjson.String || outputValue.String() != "{}" {
		return fmt.Errorf("Codex overdraft output must be the string {}")
	}
	callID := strings.TrimSpace(call.Get("call_id").String())
	if callID == "" || output.Get("call_id").String() != callID {
		return fmt.Errorf("Codex overdraft call IDs must be non-empty and equal")
	}
	occurrences := 0
	input.ForEach(func(_, item gjson.Result) bool {
		if item.Get("call_id").String() == callID {
			occurrences++
		}
		return true
	})
	if occurrences != 2 {
		return fmt.Errorf("Codex overdraft call ID collides with existing input")
	}
	return nil
}

// PrepareCodexOverdraftBody injects the pair only when an overdraft execution is present.
func PrepareCodexOverdraftBody(body []byte, opts cliproxyexecutor.Options) ([]byte, error) {
	execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts)
	if execution == nil {
		return body, nil
	}
	// Remote compaction v2 places compaction_trigger last. Appending the zz pair
	// after it is rejected by upstream as "must be the final input item".
	if CodexInputHasItemType(body, codexCompactionTriggerType) {
		return body, nil
	}
	injected, errInject := InjectCodexOverdraft(body)
	if errInject != nil {
		execution.Release()
		return nil, cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("prepare Codex overdraft body: %w", errInject))
	}
	return injected, nil
}

// RefreshCodexOverdraftInjection replaces the terminal pair for an internal resend.
func RefreshCodexOverdraftInjection(body []byte, opts cliproxyexecutor.Options) ([]byte, error) {
	if cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts) == nil {
		return body, nil
	}
	if errValidate := ValidateCodexOverdraftInjection(body); errValidate != nil {
		return nil, cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("refresh Codex overdraft body: %w", errValidate))
	}
	itemCount := int(gjson.GetBytes(body, "input.#").Int())
	withoutOutput, errDeleteOutput := sjson.DeleteBytes(body, "input."+strconv.Itoa(itemCount-1))
	if errDeleteOutput != nil {
		return nil, cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("remove Codex overdraft output: %w", errDeleteOutput))
	}
	withoutPair, errDeleteCall := sjson.DeleteBytes(withoutOutput, "input."+strconv.Itoa(itemCount-2))
	if errDeleteCall != nil {
		return nil, cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("remove Codex overdraft call: %w", errDeleteCall))
	}
	refreshed, errInject := InjectCodexOverdraft(withoutPair)
	if errInject != nil {
		return nil, cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("refresh Codex overdraft injection: %w", errInject))
	}
	return refreshed, nil
}

// BeginCodexOverdraftSend checks the lease immediately before an actual network write.
func BeginCodexOverdraftSend(opts cliproxyexecutor.Options) error {
	accountingHooks := cliproxyexecutor.AccountingAttemptHooksFromOptions(opts)
	if accountingHooks != nil && accountingHooks.RecordIntent != nil {
		if errIntent := accountingHooks.RecordIntent(); errIntent != nil {
			if accountingHooks.RecordNotDispatched != nil {
				accountingHooks.RecordNotDispatched()
			}
			return cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("persist accounting attempt intent: %w", errIntent))
		}
	}
	execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts)
	if execution == nil {
		return nil
	}
	if errBegin := execution.BeginSend(); errBegin != nil {
		if accountingHooks != nil && accountingHooks.RecordNotDispatched != nil {
			accountingHooks.RecordNotDispatched()
		}
		execution.Release()
		return cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("begin Codex overdraft send: %w", errBegin))
	}
	return nil
}

// BeginCodexAccountingRetry persists a new intent for an internal websocket resend.
func BeginCodexAccountingRetry(opts cliproxyexecutor.Options) error {
	accountingHooks := cliproxyexecutor.AccountingAttemptHooksFromOptions(opts)
	if accountingHooks == nil || accountingHooks.RecordRetryIntent == nil {
		return nil
	}
	if errIntent := accountingHooks.RecordRetryIntent(); errIntent != nil {
		return cliproxyexecutor.NewUpstreamNotDispatchedError(fmt.Errorf("persist accounting retry intent: %w", errIntent))
	}
	return nil
}

// PublishCodexQuotaHeaders copies only quota headers into the request-scoped observer.
func PublishCodexQuotaHeaders(opts cliproxyexecutor.Options, authID string, headers http.Header) {
	if len(opts.Metadata) == 0 || len(headers) == 0 {
		return
	}
	observer, _ := opts.Metadata[cliproxyexecutor.CodexQuotaObserverMetadataKey].(cliproxyexecutor.CodexQuotaObserver)
	if observer == nil {
		return
	}
	allowed := make(http.Header)
	for _, name := range codexQuotaHeaderAllowlist {
		for _, value := range headers.Values(name) {
			allowed.Add(name, value)
		}
	}
	if len(allowed) == 0 {
		return
	}
	observation := cliproxyexecutor.CodexQuotaHeadersObservation{
		AuthID:     authID,
		AttemptID:  "attempt_" + strings.ToLower(rand.Text()),
		ObservedAt: time.Now(),
		Header:     allowed,
	}
	if execution := cliproxyexecutor.CodexOverdraftExecutionFromOptions(opts); execution != nil {
		observation.AuthGeneration = execution.AuthGeneration
		observation.AttemptID = execution.DispatchID
	}
	if hooks := cliproxyexecutor.AccountingAttemptHooksFromOptions(opts); hooks != nil && hooks.CurrentAttemptID != nil {
		if attemptID := strings.TrimSpace(hooks.CurrentAttemptID()); attemptID != "" {
			observation.AttemptID = attemptID
		}
	}
	observer(observation)
}
