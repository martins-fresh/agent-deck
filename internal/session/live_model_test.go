package session

import (
	"os"
	"path/filepath"
	"testing"
)

func writeLiveModelJSONL(t *testing.T, lines ...string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "session.jsonl")
	var content string
	for _, l := range lines {
		content += l + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}
	return path
}

func TestTailModelID_ReadsLastNonSyntheticModel(t *testing.T) {
	path := writeLiveModelJSONL(t,
		`{"type":"assistant","message":{"model":"claude-opus-4-1-20250805"}}`,
		`{"type":"user","message":{}}`,
		`{"type":"assistant","message":{"model":"claude-sonnet-4-5-20250929"}}`,
		`{"type":"assistant","message":{"model":"<synthetic>"}}`,
	)
	model, ok := tailModelID(path)
	if !ok || model != "claude-sonnet-4-5-20250929" {
		t.Errorf("tailModelID = %q, %v; want claude-sonnet-4-5-20250929, true", model, ok)
	}
}

func TestTailModelID_NoAssistantModel(t *testing.T) {
	path := writeLiveModelJSONL(t, `{"type":"user","message":{}}`)
	model, ok := tailModelID(path)
	if ok || model != "" {
		t.Errorf("tailModelID = %q, %v; want \"\", false", model, ok)
	}
}

func TestTailModelID_MissingFile(t *testing.T) {
	model, ok := tailModelID(filepath.Join(t.TempDir(), "nope.jsonl"))
	if ok || model != "" {
		t.Errorf("tailModelID(missing) = %q, %v; want \"\", false", model, ok)
	}
}

func TestTailModelID_CacheInvalidatesOnChange(t *testing.T) {
	path := writeLiveModelJSONL(t, `{"type":"assistant","message":{"model":"claude-opus-4-1-20250805"}}`)
	model, ok := tailModelID(path)
	if !ok || model != "claude-opus-4-1-20250805" {
		t.Fatalf("initial read = %q, %v", model, ok)
	}

	// Overwrite with a different model. Sizes differ, so the mtime-equality
	// race some filesystems have on rapid rewrites doesn't matter here.
	if err := os.WriteFile(path, []byte(`{"type":"assistant","message":{"model":"claude-haiku-4-5-20251001"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	model, ok = tailModelID(path)
	if !ok || model != "claude-haiku-4-5-20251001" {
		t.Errorf("after rewrite = %q, %v; want claude-haiku-4-5-20251001, true", model, ok)
	}
}

func TestLiveModelInfo_FallsBackToLaunchOverride(t *testing.T) {
	inst := &Instance{Tool: "claude"}
	inst.SetClaudeOptions(&ClaudeOptions{Model: "opus"})
	info := inst.LiveModelInfo()
	if info.ModelID != "opus" {
		t.Errorf("LiveModelInfo().ModelID = %q, want %q (no JSONL path, should fall back)", info.ModelID, "opus")
	}
}

func TestLiveModelInfo_NilInstance(t *testing.T) {
	var inst *Instance
	if got := inst.LiveModelInfo(); got != (ModelInfo{}) {
		t.Errorf("LiveModelInfo() on nil = %+v, want zero value", got)
	}
}
