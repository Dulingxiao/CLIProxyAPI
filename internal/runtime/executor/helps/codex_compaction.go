package helps

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexCompactionTriggerType = "compaction_trigger"

// CodexInputHasItemType reports whether any Responses input item has the given type.
func CodexInputHasItemType(body []byte, itemType string) bool {
	itemType = strings.TrimSpace(itemType)
	if len(body) == 0 || itemType == "" {
		return false
	}
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return false
	}
	for _, item := range input.Array() {
		if strings.EqualFold(strings.TrimSpace(item.Get("type").String()), itemType) {
			return true
		}
	}
	return false
}

// EnsureCodexCompactionTriggerIsFinal keeps at most one compaction_trigger and
// places it at the end of input. Official Codex / OpenAI Responses compact
// requests reject any item after the trigger.
func EnsureCodexCompactionTriggerIsFinal(body []byte) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}
	items := input.Array()
	if len(items) == 0 {
		return body
	}

	lastTrigger := -1
	triggerCount := 0
	for index, item := range items {
		if strings.TrimSpace(item.Get("type").String()) != codexCompactionTriggerType {
			continue
		}
		lastTrigger = index
		triggerCount++
	}
	if lastTrigger < 0 || (triggerCount == 1 && lastTrigger == len(items)-1) {
		return body
	}

	rebuilt := make([]string, 0, len(items))
	var triggerRaw string
	for _, item := range items {
		if strings.TrimSpace(item.Get("type").String()) == codexCompactionTriggerType {
			triggerRaw = item.Raw
			continue
		}
		rebuilt = append(rebuilt, item.Raw)
	}
	rebuilt = append(rebuilt, triggerRaw)
	updated, errSet := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(rebuilt, ",")+"]"))
	if errSet != nil {
		return body
	}
	return updated
}
