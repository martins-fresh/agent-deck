package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/asheshgoplani/agent-deck/internal/agentpaths"
)

// Message provenance: who sent a message that lands in a session's pane.
//
// All pane-bound messages flow through `agent-deck session send`. Both the
// conductor bridge (relaying a human user's message) and peer conductors
// (messaging each other) use that one command — but historically with no
// attribution, so a receiving conductor could not tell a peer's instruction
// from the user's. This file classifies and records the source so the gap is
// visible (a JSONL log) and surfaced (an in-pane tag).

// MsgClass is the provenance bucket for a message source.
type MsgClass string

const (
	// MsgClassInternal: sent from within another session (peer conductor or
	// peer worker) — the source is that session's title via $AGENT_DECK_SESSION.
	MsgClassInternal MsgClass = "internal"
	// MsgClassUser: a human user's message relayed by the bridge from a
	// messaging channel (Discord/Slack/Telegram). Source like "user:discord".
	MsgClassUser MsgClass = "user"
	// MsgClassSystem: bridge-generated traffic (heartbeat triggers, watcher
	// events). Source like "bridge:heartbeat" / "bridge:watcher".
	MsgClassSystem MsgClass = "system"
	// MsgClassUnattributed: no source at all — a manual terminal invocation or
	// a script. The historically-dangerous bucket; logged loudly.
	MsgClassUnattributed MsgClass = "unattributed"
)

// ClassifyMessageSource maps a raw source string to a provenance class.
func ClassifyMessageSource(source string) MsgClass {
	s := strings.TrimSpace(source)
	switch {
	case s == "":
		return MsgClassUnattributed
	case s == "user" || strings.HasPrefix(s, "user:"):
		return MsgClassUser
	case s == "system" || s == "heartbeat" || s == "watcher" || strings.HasPrefix(s, "bridge:"):
		return MsgClassSystem
	default:
		// Any other non-empty source is session-originated (its own title via
		// $AGENT_DECK_SESSION) — a peer conductor or peer worker.
		return MsgClassInternal
	}
}

// MessageProvenanceTag returns the human-readable prefix to prepend to a
// delivered message, or "" when no tag should be added. Only internal (peer)
// and user (channel) messages are tagged; system traffic carries its own
// markers ([HEARTBEAT]/[WATCHER]) and unattributed terminal/script sends are
// left untouched to avoid noise on legitimate manual use.
func MessageProvenanceTag(source string, class MsgClass) string {
	switch class {
	case MsgClassInternal:
		return fmt.Sprintf("[from: %s · internal] ", source)
	case MsgClassUser:
		return fmt.Sprintf("[from: %s] ", renderUserSource(source))
	default:
		return ""
	}
}

// renderUserSource turns "user:discord:adverse-events" into the friendlier
// "user via discord#adverse-events"; "user:slack" into "user via slack";
// bare "user" stays "user".
func renderUserSource(source string) string {
	parts := strings.SplitN(source, ":", 3)
	if len(parts) == 0 || parts[0] != "user" {
		return source
	}
	switch len(parts) {
	case 1:
		return "user"
	case 2:
		return "user via " + parts[1]
	default:
		return fmt.Sprintf("user via %s#%s", parts[1], parts[2])
	}
}

// MessagePreview returns a single-line, length-capped preview of a message for
// the provenance log.
func MessagePreview(msg string, max int) string {
	p := strings.TrimSpace(msg)
	p = strings.ReplaceAll(p, "\n", " ")
	p = strings.ReplaceAll(p, "\r", " ")
	if max > 0 {
		// Count runes, not bytes, so multibyte input isn't sliced mid-rune.
		r := []rune(p)
		if len(r) > max {
			return string(r[:max]) + "…"
		}
	}
	return p
}

// MsgProvenanceRecord is one line in the provenance log.
type MsgProvenanceRecord struct {
	TS       string   `json:"ts"`
	Target   string   `json:"target"`
	TargetID string   `json:"target_id,omitempty"`
	Source   string   `json:"source"`
	Class    MsgClass `json:"class"`
	Mode     string   `json:"mode,omitempty"`
	// Decision records the send-policy outcome: "allow", "blocked-allowlist",
	// "blocked-elevated", or "warned-elevated". Empty for plain allow.
	Decision string `json:"decision,omitempty"`
	// Reason is a human-readable explanation when Decision is a block/warn.
	Reason  string `json:"reason,omitempty"`
	Preview string `json:"preview"`
}

// MessageProvenanceLogPath is the JSONL provenance log, resolved against the
// agent-deck data root (XDG-first with legacy fallback) so it sits next to the
// bridge log and per-profile state.
func MessageProvenanceLogPath() (string, error) {
	dir, err := agentpaths.EffectiveDataDir("profiles", "state.json", "conductor")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "message-provenance.jsonl"), nil
}

// LogMessageProvenance appends one JSONL record. Best-effort by design: any
// failure is swallowed so provenance logging can never block or fail a send.
func LogMessageProvenance(rec MsgProvenanceRecord) {
	path, err := MessageProvenanceLogPath()
	if err != nil {
		return
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	// Single append write per record; preview is length-capped by the caller so
	// lines stay small and O_APPEND keeps concurrent writers from interleaving.
	_, _ = f.Write(append(line, '\n'))
}
