package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"sync"
)

// liveModelTailBytes bounds how much of a JSONL transcript tailModelID reads
// from the end of the file. Assistant message lines carrying a model field are
// typically well under 1KB even with large tool_use content blocks (Content
// here only captures type/name, not payloads), so 64KB comfortably covers
// several trailing lines without approaching a full-file scan.
const liveModelTailBytes = 64 * 1024

// liveModelCacheEntry pins a tail-read result to the file state it was read
// from, so a later call can tell "unchanged" from "needs re-read" without
// re-reading the file to check.
type liveModelCacheEntry struct {
	size    int64
	modTime int64
	modelID string
}

var (
	liveModelCacheMu sync.Mutex
	liveModelCache   = make(map[string]liveModelCacheEntry)
)

// LiveModelInfo reports the model actually in effect for this session right
// now, as distinct from LaunchModelInfo's "what override was requested."
//
// For Claude sessions with no explicit --model override, LaunchModelInfo
// returns empty (tool default) — accurate but unhelpful for a status display,
// since "tool default" resolves to whatever Claude Code itself picks. This
// reads that resolution back out of the session's own transcript: Claude Code
// stamps a model ID on every assistant message it writes, so the last one in
// the file is ground truth for what actually answered, not just what was
// asked for.
//
// Falls back to LaunchModelInfo when there's no JSONL to read (session not
// yet started, path unavailable) or the tail scan finds no model field. Other
// tools' Instance fields (GeminiModel, etc.) are already the live value with
// no file I/O needed, so this only special-cases Claude.
func (i *Instance) LiveModelInfo() ModelInfo {
	if i == nil {
		return ModelInfo{}
	}
	if IsClaudeCompatible(i.Tool) {
		if path := i.GetJSONLPath(); path != "" {
			if modelID, ok := tailModelID(path); ok {
				return ParseModelID(modelID)
			}
		}
	}
	return i.LaunchModelInfo()
}

// tailModelID returns the most recent non-empty, non-synthetic model field
// among assistant messages in the file's last liveModelTailBytes, without
// scanning the whole transcript. Cached per path, invalidated by size/mtime
// change, so repeated calls against an unchanged file (the common case
// between background refresh ticks) cost a single Stat.
func tailModelID(path string) (string, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return "", false
	}

	liveModelCacheMu.Lock()
	cached, ok := liveModelCache[path]
	liveModelCacheMu.Unlock()
	if ok && cached.size == info.Size() && cached.modTime == info.ModTime().UnixNano() {
		return cached.modelID, cached.modelID != ""
	}

	modelID := readTailModelID(path, info.Size())

	liveModelCacheMu.Lock()
	liveModelCache[path] = liveModelCacheEntry{
		size:    info.Size(),
		modTime: info.ModTime().UnixNano(),
		modelID: modelID,
	}
	liveModelCacheMu.Unlock()

	return modelID, modelID != ""
}

// readTailModelID does the actual seek-and-scan. A truncated first line (cut
// mid-record by the seek offset) is expected and simply skipped — later,
// complete lines are what we're after.
func readTailModelID(path string, size int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	offset := size - liveModelTailBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := f.Seek(offset, 0); err != nil {
		return ""
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var lastModel string
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.Contains(line, []byte(`"model"`)) {
			continue
		}
		var entry jsonlEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry.Type != "assistant" {
			continue
		}
		if entry.Message.Model != "" && entry.Message.Model != "<synthetic>" {
			lastModel = entry.Message.Model
		}
	}
	return lastModel
}
