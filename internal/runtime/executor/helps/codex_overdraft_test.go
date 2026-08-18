package helps

import (
	"bytes"
	"encoding/hex"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestInjectCodexOverdraftAppendsExactStringPair(t *testing.T) {
	original := []byte(`{"model":"gpt-test","unknown":{"keep":true},"input":[{"type":"message","role":"user","content":"hello"}]}`)
	wantOriginal := bytes.Clone(original)
	injected, errInject := InjectCodexOverdraft(original)
	if errInject != nil {
		t.Fatalf("InjectCodexOverdraft() error = %v", errInject)
	}
	if !bytes.Equal(original, wantOriginal) {
		t.Fatalf("original mutated:\n got %s\nwant %s", original, wantOriginal)
	}
	if !gjson.GetBytes(injected, "unknown.keep").Bool() {
		t.Fatal("unknown fields were not preserved")
	}
	input := gjson.GetBytes(injected, "input").Array()
	if len(input) != 3 {
		t.Fatalf("len(input) = %d, want 3", len(input))
	}
	call := input[1]
	output := input[2]
	if call.Get("type").String() != "function_call" || call.Get("name").String() != "zz" {
		t.Fatalf("call item = %s", call.Raw)
	}
	if call.Get("arguments").Type != gjson.String || call.Get("arguments").String() != "{}" {
		t.Fatalf("arguments = %#v, want string {}", call.Get("arguments"))
	}
	if output.Get("type").String() != "function_call_output" || output.Get("output").Type != gjson.String || output.Get("output").String() != "{}" {
		t.Fatalf("output item = %s", output.Raw)
	}
	callID := call.Get("call_id").String()
	if len(callID) != len("call_")+32 || output.Get("call_id").String() != callID {
		t.Fatalf("call IDs = (%q, %q)", callID, output.Get("call_id").String())
	}
	if errValidate := ValidateCodexOverdraftInjection(injected); errValidate != nil {
		t.Fatalf("ValidateCodexOverdraftInjection() error = %v", errValidate)
	}
}

func TestInjectCodexOverdraftRetriesCallIDCollision(t *testing.T) {
	zeroID := "call_" + hex.EncodeToString(make([]byte, 16))
	body := []byte(`{"input":[{"type":"function_call","call_id":"` + zeroID + `","name":"existing","arguments":"{}"}]}`)
	random := append(make([]byte, 16), bytes.Repeat([]byte{1}, 16)...)
	injected, errInject := injectCodexOverdraftWithReader(body, bytes.NewReader(random))
	if errInject != nil {
		t.Fatal(errInject)
	}
	callID := gjson.GetBytes(injected, "input.1.call_id").String()
	if callID == zeroID || callID != "call_"+hex.EncodeToString(bytes.Repeat([]byte{1}, 16)) {
		t.Fatalf("generated call ID = %q", callID)
	}
}

func TestInjectCodexOverdraftRejectsInvalidOrEmptyInput(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{}`),
		[]byte(`{"input":null}`),
		[]byte(`{"input":{}}`),
		[]byte(`{"input":[]}`),
	} {
		if _, errInject := InjectCodexOverdraft(body); errInject == nil {
			t.Fatalf("InjectCodexOverdraft(%s) error = nil", body)
		}
	}
}

func TestValidateCodexOverdraftInjectionRejectsNonStringFields(t *testing.T) {
	body := []byte(`{"input":[{"type":"message"},{"type":"function_call","call_id":"call_x","name":"zz","arguments":{}},{"type":"function_call_output","call_id":"call_x","output":{}}]}`)
	if errValidate := ValidateCodexOverdraftInjection(body); errValidate == nil {
		t.Fatal("ValidateCodexOverdraftInjection() error = nil")
	}
}

func TestPrepareCodexOverdraftBodySkipsCompactionTrigger(t *testing.T) {
	execution := cliproxyexecutor.NewCodexOverdraftExecution("auth", 1, "cycle", 1, 1, "dispatch", cliproxyexecutor.CodexOverdraftBusiness, nil, nil, nil)
	opts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.CodexOverdraftExecutionMetadataKey: execution}}
	original := []byte(`{"model":"gpt-test","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}`)

	got, errPrepare := PrepareCodexOverdraftBody(original, opts)
	if errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("compaction_trigger request was mutated:\n got %s\nwant %s", got, original)
	}
	input := gjson.GetBytes(got, "input").Array()
	if len(input) != 2 || input[len(input)-1].Get("type").String() != "compaction_trigger" {
		t.Fatalf("compaction_trigger is not the final input item: %s", got)
	}
}

func TestRefreshCodexOverdraftInjectionRotatesCallID(t *testing.T) {
	execution := cliproxyexecutor.NewCodexOverdraftExecution("auth", 1, "cycle", 1, 1, "dispatch", cliproxyexecutor.CodexOverdraftBusiness, nil, nil, nil)
	opts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.CodexOverdraftExecutionMetadataKey: execution}}
	first, errFirst := PrepareCodexOverdraftBody([]byte(`{"model":"gpt-test","input":[{"role":"user","content":"hello"}]}`), opts)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	firstID := gjson.GetBytes(first, "input.1.call_id").String()
	second, errSecond := RefreshCodexOverdraftInjection(first, opts)
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	secondID := gjson.GetBytes(second, "input.1.call_id").String()
	if firstID == "" || secondID == "" || firstID == secondID {
		t.Fatalf("call IDs were not rotated: first=%q second=%q", firstID, secondID)
	}
	if len(gjson.GetBytes(second, "input").Array()) != 3 {
		t.Fatalf("refreshed input = %s", gjson.GetBytes(second, "input").Raw)
	}
}

func BenchmarkInjectCodexOverdraftLargeRequest(b *testing.B) {
	item := []byte(`{"type":"message","role":"user","content":"abcdefghijklmnopqrstuvwxyz0123456789"}`)
	body := []byte(`{"model":"gpt-test","input":[`)
	for i := 0; i < 10_000; i++ {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, item...)
	}
	body = append(body, ']', '}')
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, errInject := InjectCodexOverdraft(body); errInject != nil {
			b.Fatal(errInject)
		}
	}
}
