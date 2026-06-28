package session

import (
	"strings"
	"testing"
)

func cfgWithGlobalPolicy(p SendPolicy) *UserConfig {
	return &UserConfig{Conductor: ConductorSettings{SendPolicy: p}}
}

func TestResolveSendPolicy_NilConfigIsNoOp(t *testing.T) {
	r := ResolveSendPolicy(nil, "anything")
	if ok, _ := r.CheckAllowed(MsgClassInternal, "conductor-x"); !ok {
		t.Error("nil config must allow all sends")
	}
	if r.ElevatedMode() != "off" {
		t.Errorf("nil config elevated mode = %q, want off", r.ElevatedMode())
	}
}

func TestCheckAllowed_NoAllowlistAllowsAll(t *testing.T) {
	r := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{}), "ae")
	for _, class := range []MsgClass{MsgClassInternal, MsgClassUnattributed, MsgClassUser, MsgClassSystem} {
		if ok, _ := r.CheckAllowed(class, "conductor-x"); !ok {
			t.Errorf("empty allowlist must allow class %q", class)
		}
	}
}

func TestCheckAllowed_AllowlistGatesPeersOnly(t *testing.T) {
	r := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{
		AllowedSenders: []string{"conductor-core", "conductor-github"},
	}), "ae")

	// Listed peer: allowed.
	if ok, _ := r.CheckAllowed(MsgClassInternal, "conductor-core"); !ok {
		t.Error("listed peer must be allowed")
	}
	// Unlisted peer: blocked.
	if ok, reason := r.CheckAllowed(MsgClassInternal, "conductor-rogue"); ok || reason == "" {
		t.Errorf("unlisted peer must be blocked with a reason, got ok=%v reason=%q", ok, reason)
	}
	// Unattributed: blocked.
	if ok, _ := r.CheckAllowed(MsgClassUnattributed, ""); ok {
		t.Error("unattributed must be blocked when an allowlist is set")
	}
	// User + system always bypass.
	if ok, _ := r.CheckAllowed(MsgClassUser, "user:discord:ae"); !ok {
		t.Error("user must bypass allowlist")
	}
	if ok, _ := r.CheckAllowed(MsgClassSystem, "bridge:watcher"); !ok {
		t.Error("system must bypass allowlist")
	}
}

func TestResolveSendPolicy_PerConductorAllowlistOverridesGlobal(t *testing.T) {
	cfg := cfgWithGlobalPolicy(SendPolicy{AllowedSenders: []string{"conductor-core"}})
	cfg.Conductors = map[string]ConductorOverrides{
		"ae": {SendPolicy: SendPolicy{AllowedSenders: []string{"conductor-github"}}},
	}
	r := ResolveSendPolicy(cfg, "ae")
	if ok, _ := r.CheckAllowed(MsgClassInternal, "conductor-github"); !ok {
		t.Error("per-conductor allowlist entry must be allowed")
	}
	if ok, _ := r.CheckAllowed(MsgClassInternal, "conductor-core"); ok {
		t.Error("global allowlist must be replaced by per-conductor override")
	}
}

func TestElevatedMode_NormalizationAndDefault(t *testing.T) {
	cases := map[string]string{"": "off", "off": "off", "WARN": "warn", "Block": "block", "bogus": "off"}
	for in, want := range cases {
		r := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{ElevatedAuthorizationMode: in}), "ae")
		if got := r.ElevatedMode(); got != want {
			t.Errorf("mode %q -> %q, want %q", in, got, want)
		}
	}
}

func TestMatchElevated_DefaultsAndCustom(t *testing.T) {
	r := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{ElevatedAuthorizationMode: "block"}), "ae")
	for _, msg := range []string{
		"merge them, starting with #2316",
		"arm both/clear",
		"please auto-merge #5",
		"go ahead and force-push",
	} {
		if hit, _ := r.MatchElevated(msg); !hit {
			t.Errorf("default patterns should flag %q", msg)
		}
	}
	for _, msg := range []string{
		"All good here?",
		"can you summarize the diff",
		"the emergent behavior is fine", // 'merge' only as a whole word, not substring
	} {
		if hit, pat := r.MatchElevated(msg); hit {
			t.Errorf("default patterns should NOT flag %q (matched %q)", msg, pat)
		}
	}

	// Custom patterns replace defaults.
	rc := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{
		ElevatedAuthorizationMode: "warn",
		ElevatedPatterns:          []string{`(?i)\bship it\b`},
	}), "ae")
	if hit, _ := rc.MatchElevated("ship it"); !hit {
		t.Error("custom pattern should match")
	}
	if hit, _ := rc.MatchElevated("merge #1"); hit {
		t.Error("custom patterns should replace defaults (merge no longer flagged)")
	}
}

func TestMatchElevated_OffModeHasNoPatterns(t *testing.T) {
	r := ResolveSendPolicy(cfgWithGlobalPolicy(SendPolicy{}), "ae")
	if hit, _ := r.MatchElevated("merge it"); hit {
		t.Error("off mode must not compile/match patterns")
	}
}

func TestElevatedUnverifiedTag(t *testing.T) {
	if got := ElevatedUnverifiedTag("conductor-github"); got == "" || !strings.Contains(got, "conductor-github") {
		t.Errorf("tag should name the source: %q", got)
	}
	if got := ElevatedUnverifiedTag(""); !strings.Contains(got, "unattributed") {
		t.Errorf("empty source should render as unattributed: %q", got)
	}
}
