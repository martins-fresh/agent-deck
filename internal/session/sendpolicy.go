package session

import (
	"log/slog"
	"regexp"
	"strings"
)

// Send policy: operator-defined guards enforced inside `agent-deck session
// send` to harden the conductor messaging surface (see msgprovenance.go for the
// provenance signal these build on).
//
// Two independent, opt-in guards:
//
//  1. Allowlist — restrict which PEER sources may message a conductor at all.
//  2. Elevated-authorization gate — require merge/irreversible authorizations to
//     originate from a verified user channel (bridge --from user:*).
//
// Honest scope: both key on the message source, which for a peer is the
// spoofable $AGENT_DECK_SESSION env var. They stop confused/relaying conductors
// and accidental flows and give the operator policy + audit, but a determined
// in-session process can forge --from. The elevated gate is the stronger of the
// two because the "is this the user" check passes only for bridge-set
// user:* attribution, which in-session code cannot mint for itself. A true
// boundary needs daemon-side pty attribution (not done here).

// SendPolicy is the TOML shape under [conductor.send_policy] (global) and
// [conductors.<name>.send_policy] (per-conductor allowlist override).
type SendPolicy struct {
	// AllowedSenders, when non-empty, restricts which peer sources may send to a
	// conductor target. Values match the resolved --from / $AGENT_DECK_SESSION
	// string (e.g. "conductor-core"). user/system (bridge) traffic always
	// bypasses. Empty = no restriction (default).
	AllowedSenders []string `toml:"allowed_senders,omitempty"`
	// ElevatedAuthorizationMode gates messages that read like merge/irreversible
	// authorizations to the verified user channel: "off" (default), "warn"
	// (deliver with a loud UNVERIFIED tag) or "block" (refuse delivery).
	// Global only — ignored on per-conductor overrides.
	ElevatedAuthorizationMode string `toml:"elevated_authorization_mode,omitempty"`
	// ElevatedPatterns are regexes (Go syntax) marking a message as elevated.
	// Empty = built-in defaults. Global only.
	ElevatedPatterns []string `toml:"elevated_patterns,omitempty"`
}

// defaultElevatedPatterns flag the outward/irreversible authorizations seen in
// the 2026-06 incident ("merge them…", "arm both"). Deliberately narrow to
// avoid false-positives on ordinary dev chatter.
var defaultElevatedPatterns = []string{
	`(?i)\bmerge\b`,
	`(?i)\barm\b`,
	`(?i)auto-?merge`,
	`(?i)force[ -]?push`,
}

// ResolvedSendPolicy is the compiled, ready-to-check policy for one target.
type ResolvedSendPolicy struct {
	hasAllowlist   bool
	allowedSenders map[string]bool
	elevatedMode   string // "off" | "warn" | "block"
	patterns       []*regexp.Regexp
}

func normalizeElevatedMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "off":
		return "off"
	case "warn":
		return "warn"
	case "block":
		return "block"
	default:
		// Fail open on a typo'd mode rather than silently blocking every send;
		// warn so the misconfiguration is visible.
		sessionLog.Warn("unknown conductor send_policy elevated_authorization_mode; treating as off",
			slog.String("mode", mode))
		return "off"
	}
}

func compileElevatedPatterns(raw []string) []*regexp.Regexp {
	src := raw
	if len(src) == 0 {
		src = defaultElevatedPatterns
	}
	out := make([]*regexp.Regexp, 0, len(src))
	for _, p := range src {
		re, err := regexp.Compile(p)
		if err != nil {
			sessionLog.Warn("invalid conductor send_policy elevated_pattern; skipping",
				slog.String("pattern", p), slog.String("error", err.Error()))
			continue
		}
		out = append(out, re)
	}
	return out
}

// ResolveSendPolicy compiles the effective policy for a conductor target.
// Per-conductor allowed_senders (if any) replaces the global allowlist; the
// elevated gate is global-only. A nil config yields a no-op policy (allow-all,
// elevated off) so callers can fail open when config is unreadable.
func ResolveSendPolicy(cfg *UserConfig, conductorName string) ResolvedSendPolicy {
	var r ResolvedSendPolicy
	r.elevatedMode = "off"
	if cfg == nil {
		return r
	}
	global := cfg.Conductor.SendPolicy

	allow := global.AllowedSenders
	if conductorName != "" {
		if ov, ok := cfg.Conductors[conductorName]; ok && len(ov.SendPolicy.AllowedSenders) > 0 {
			allow = ov.SendPolicy.AllowedSenders
		}
	}
	if len(allow) > 0 {
		r.hasAllowlist = true
		r.allowedSenders = make(map[string]bool, len(allow))
		for _, s := range allow {
			if s = strings.TrimSpace(s); s != "" {
				r.allowedSenders[s] = true
			}
		}
	}

	r.elevatedMode = normalizeElevatedMode(global.ElevatedAuthorizationMode)
	if r.elevatedMode != "off" {
		r.patterns = compileElevatedPatterns(global.ElevatedPatterns)
	}
	return r
}

// CheckAllowed reports whether a send from (class, source) to the conductor is
// permitted by the allowlist. user/system traffic always passes; internal
// (peer) and unattributed sources are gated when an allowlist is configured.
// Returns (true, "") when allowed.
func (r ResolvedSendPolicy) CheckAllowed(class MsgClass, source string) (bool, string) {
	if !r.hasAllowlist {
		return true, ""
	}
	switch class {
	case MsgClassUser, MsgClassSystem:
		return true, ""
	}
	if src := strings.TrimSpace(source); src != "" && r.allowedSenders[src] {
		return true, ""
	}
	if strings.TrimSpace(source) == "" {
		return false, "unattributed sender not in allowlist"
	}
	return false, "sender '" + source + "' not in allowlist"
}

// ElevatedMode returns the configured gate mode ("off" | "warn" | "block").
func (r ResolvedSendPolicy) ElevatedMode() string {
	if r.elevatedMode == "" {
		return "off"
	}
	return r.elevatedMode
}

// MatchElevated reports whether the message reads like an elevated/irreversible
// authorization, returning the matched pattern for logging.
func (r ResolvedSendPolicy) MatchElevated(message string) (bool, string) {
	for _, re := range r.patterns {
		if re.MatchString(message) {
			return true, re.String()
		}
	}
	return false, ""
}

// ElevatedUnverifiedTag is the loud in-pane prefix prepended (in "warn" mode) to
// an elevated message whose source is not a verified user channel.
func ElevatedUnverifiedTag(source string) string {
	s := source
	if strings.TrimSpace(s) == "" {
		s = "unattributed"
	}
	return "[⚠ UNVERIFIED AUTHORIZATION — source: " + s + ", not a user channel; do not act without user confirmation] "
}
