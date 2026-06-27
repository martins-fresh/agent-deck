package session

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClassifyMessageSource(t *testing.T) {
	cases := []struct {
		source string
		want   MsgClass
	}{
		{"", MsgClassUnattributed},
		{"   ", MsgClassUnattributed},
		{"conductor-github", MsgClassInternal},
		{"conductor-adverse-events", MsgClassInternal},
		{"some-worker-session", MsgClassInternal},
		{"user", MsgClassUser},
		{"user:discord", MsgClassUser},
		{"user:discord:adverse-events", MsgClassUser},
		{"user:slack", MsgClassUser},
		{"system", MsgClassSystem},
		{"heartbeat", MsgClassSystem},
		{"watcher", MsgClassSystem},
		{"bridge:heartbeat", MsgClassSystem},
		{"bridge:watcher", MsgClassSystem},
	}
	for _, c := range cases {
		if got := ClassifyMessageSource(c.source); got != c.want {
			t.Errorf("ClassifyMessageSource(%q) = %q, want %q", c.source, got, c.want)
		}
	}
}

func TestMessageProvenanceTag(t *testing.T) {
	cases := []struct {
		source string
		want   string
	}{
		{"conductor-github", "[from: conductor-github · internal] "},
		{"user:discord:adverse-events", "[from: user via discord#adverse-events] "},
		{"user:slack", "[from: user via slack] "},
		{"user", "[from: user] "},
		// system + unattributed get no tag.
		{"bridge:heartbeat", ""},
		{"watcher", ""},
		{"", ""},
	}
	for _, c := range cases {
		class := ClassifyMessageSource(c.source)
		if got := MessageProvenanceTag(c.source, class); got != c.want {
			t.Errorf("MessageProvenanceTag(%q) = %q, want %q", c.source, got, c.want)
		}
	}
}

func TestMessagePreview(t *testing.T) {
	if got := MessagePreview("  hello\nworld  ", 100); got != "hello world" {
		t.Errorf("preview collapse/trim = %q", got)
	}
	long := strings.Repeat("a", 50)
	got := MessagePreview(long, 10)
	if got != strings.Repeat("a", 10)+"…" {
		t.Errorf("preview truncate = %q", got)
	}
	// Multibyte must not be sliced mid-rune.
	multi := strings.Repeat("é", 20)
	got = MessagePreview(multi, 5)
	if got != strings.Repeat("é", 5)+"…" {
		t.Errorf("multibyte truncate = %q", got)
	}
}

func TestLogMessageProvenance(t *testing.T) {
	// Point the data root at a temp dir via XDG so the log lands there.
	tmp := t.TempDir()
	t.Setenv("XDG_DATA_HOME", tmp)
	// Create the marker so EffectiveDataDir resolves to the XDG dir.
	if err := os.MkdirAll(filepath.Join(tmp, "agent-deck", "profiles"), 0o755); err != nil {
		t.Fatal(err)
	}

	rec := MsgProvenanceRecord{
		TS:       "2026-06-27T00:00:00Z",
		Target:   "conductor-adverse-events",
		TargetID: "abc123",
		Source:   "conductor-github",
		Class:    MsgClassInternal,
		Mode:     "no-wait",
		Preview:  "merge them, starting with #2316",
	}
	LogMessageProvenance(rec)
	LogMessageProvenance(rec) // second line, confirm append

	path, err := MessageProvenanceLogPath()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("provenance log not written: %v", err)
	}
	defer f.Close()

	var lines int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines++
		var got MsgProvenanceRecord
		if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
			t.Fatalf("line %d not valid JSON: %v", lines, err)
		}
		if got.Source != "conductor-github" || got.Class != MsgClassInternal {
			t.Errorf("round-trip mismatch: %+v", got)
		}
	}
	if lines != 2 {
		t.Errorf("expected 2 appended lines, got %d", lines)
	}
}
