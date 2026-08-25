package session

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A finished one-off worker sitting around forever looks identical to a
// genuinely stuck session to anything counting "needs attention" — notably
// the conductor heartbeat's `session children --attention` gate. These tests
// pin emitDoneSignals' auto-archive side effect: a worker (non-empty
// ParentSessionID) is archived the moment its done sentinel is processed; a
// top-level/conductor session (empty ParentSessionID) never is; and the
// config toggle disables the behavior entirely.

// writeAutoArchiveConfig writes a config.toml under home enabling/disabling
// auto-archive, then clears the config cache so the next GetNotificationsSettings
// call re-reads it.
func writeAutoArchiveConfig(t *testing.T, home string, enabled *bool) {
	t.Helper()
	body := "[notifications]\nenabled = true\n"
	if enabled != nil {
		if *enabled {
			body += "auto_archive_completed_children = true\n"
		} else {
			body += "auto_archive_completed_children = false\n"
		}
	}
	configDir := filepath.Join(home, ".agent-deck")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(body), 0o600); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}
	ClearUserConfigCache()
}

func TestDaemon_EmitDoneSignals_AutoArchivesCompletedWorker(t *testing.T) {
	profile := "_test-auto-archive-worker"
	parentID, childID := seedDoneParentChild(t, profile)
	writeAutoArchiveConfig(t, os.Getenv("HOME"), nil) // default (nil = enabled)

	d := NewTransitionDaemon()
	t.Cleanup(d.notifier.Close)

	storage, err := NewStorageWithProfile(profile)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer storage.Close()
	instances, _, err := storage.LoadWithGroups()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	byID := map[string]*Instance{}
	for _, inst := range instances {
		byID[inst.ID] = inst
	}

	hookStatuses := map[string]*HookStatus{
		childID: {
			Status:      "waiting",
			Event:       "Stop",
			DoneStatus:  "ok",
			DoneSummary: "worker finished",
			UpdatedAt:   time.Now(),
		},
	}
	d.emitDoneSignals(profile, byID, hookStatuses)
	d.notifier.Flush()

	// Reload from the DB — autoArchiveCompletedChild persists via a targeted
	// UPDATE, not the in-memory byID map, so this proves it actually landed.
	reloaded, _, err := storage.LoadWithGroups()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	var child *Instance
	for _, inst := range reloaded {
		if inst.ID == childID {
			child = inst
		}
	}
	if child == nil {
		t.Fatalf("child %s not found after reload", childID)
	}
	if !child.IsArchived() {
		t.Error("worker should be auto-archived after emitting its done sentinel")
	}
	_ = parentID
}

func TestDaemon_EmitDoneSignals_NeverArchivesTopLevelSession(t *testing.T) {
	profile := "_test-auto-archive-toplevel"
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENT_DECK_HOME", "")
	t.Setenv("AGENT_DECK_PROFILE", "")
	ClearUserConfigCache()
	ResetInboxFingerprintCacheForTest()
	t.Cleanup(func() {
		ClearUserConfigCache()
		ResetInboxFingerprintCacheForTest()
	})
	if err := os.MkdirAll(home+"/.agent-deck", 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeAutoArchiveConfig(t, home, nil)

	storage, err := NewStorageWithProfile(profile)
	if err != nil {
		t.Fatalf("NewStorageWithProfile: %v", err)
	}
	// Top-level session: no ParentSessionID, so it must never be auto-archived
	// even though it emits a done sentinel — a conductor finishing its own
	// turn is not "worker done" in this sense.
	top := &Instance{
		ID:          "top-level-no-parent",
		Title:       "conductor-solo",
		ProjectPath: "/tmp/top-level",
		GroupPath:   DefaultGroupPath,
		Tool:        "claude",
		Status:      StatusWaiting,
		CreatedAt:   time.Now(),
	}
	if err := storage.SaveWithGroups([]*Instance{top}, nil); err != nil {
		t.Fatalf("save: %v", err)
	}
	storage.Close()

	d := NewTransitionDaemon()
	t.Cleanup(d.notifier.Close)

	storage2, err := NewStorageWithProfile(profile)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer storage2.Close()
	instances, _, err := storage2.LoadWithGroups()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	byID := map[string]*Instance{}
	for _, inst := range instances {
		byID[inst.ID] = inst
	}

	hookStatuses := map[string]*HookStatus{
		top.ID: {
			Status:      "waiting",
			Event:       "Stop",
			DoneStatus:  "ok",
			DoneSummary: "top-level turn ended",
			UpdatedAt:   time.Now(),
		},
	}
	d.emitDoneSignals(profile, byID, hookStatuses)
	d.notifier.Flush()

	reloaded, _, err := storage2.LoadWithGroups()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	for _, inst := range reloaded {
		if inst.ID == top.ID && inst.IsArchived() {
			t.Error("top-level session (no ParentSessionID) must never be auto-archived")
		}
	}
}

func TestDaemon_EmitDoneSignals_AutoArchiveDisabledByConfig(t *testing.T) {
	profile := "_test-auto-archive-disabled"
	parentID, childID := seedDoneParentChild(t, profile)
	disabled := false
	writeAutoArchiveConfig(t, os.Getenv("HOME"), &disabled)

	d := NewTransitionDaemon()
	t.Cleanup(d.notifier.Close)

	storage, err := NewStorageWithProfile(profile)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer storage.Close()
	instances, _, err := storage.LoadWithGroups()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	byID := map[string]*Instance{}
	for _, inst := range instances {
		byID[inst.ID] = inst
	}

	hookStatuses := map[string]*HookStatus{
		childID: {
			Status:      "waiting",
			Event:       "Stop",
			DoneStatus:  "ok",
			DoneSummary: "worker finished, but auto-archive is off",
			UpdatedAt:   time.Now(),
		},
	}
	d.emitDoneSignals(profile, byID, hookStatuses)
	d.notifier.Flush()

	reloaded, _, err := storage.LoadWithGroups()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	for _, inst := range reloaded {
		if inst.ID == childID && inst.IsArchived() {
			t.Error("worker must not be auto-archived when auto_archive_completed_children=false")
		}
	}
	_ = parentID
}
