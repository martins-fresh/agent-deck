package ui

import (
	"strings"
	"testing"

	"github.com/asheshgoplani/agent-deck/internal/session"
	"github.com/stretchr/testify/require"
)

// renderTitleRow is a focused harness for the overview row renderer: it renders
// one session row whose snapshot carries a live model, then returns the line
// (styled, so the plain-text assertions below match inside the ANSI runs). The
// model badge is secondary status; the session name is how a user tells rows
// apart.
func renderTitleRow(t *testing.T, width int, title, model, account string) string {
	t.Helper()
	h := NewHome()
	h.width, h.height = width, 40
	inst := &session.Instance{ID: "title-row", Title: title, Tool: "claude", Status: session.StatusIdle, Account: account}
	state := sessionRenderState{
		status:         session.StatusIdle,
		tool:           "claude",
		title:          title,
		account:        account,
		accountDisplay: newAccountPresentation(account),
		model:          model,
	}
	var b strings.Builder
	h.renderSessionItem(&b, session.Item{Type: session.ItemTypeSession, Session: inst, Level: 1, Path: "work", IsLastInGroup: true}, false, map[string]sessionRenderState{inst.ID: state}, width)
	return strings.TrimSuffix(b.String(), "\n")
}

// Regression: the " · <model>" badge used to consume the fixed width
// reservation before the title got a cell, so a narrow SESSIONS pane rendered
// every row in a group as "…" and the names became indistinguishable. The badge
// is secondary and must yield to the name.
func TestSessionModelBadgeYieldsToTitle(t *testing.T) {
	cases := []struct {
		name    string
		width   int
		account string
	}{
		{"empty-slot-narrow", 30, ""},
		{"named-slot-54", 54, "work"},
		{"named-slot-56", 56, "work"},
		{"named-slot-58", 58, "work"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := renderTitleRow(t, tc.width, "conductor-call-queueing", "claude-opus-5", tc.account)
			require.LessOrEqual(t, cellWidth(row), tc.width, "row must not overflow the pane")
			require.Contains(t, row, "conductor", "session name must survive the model badge")
			require.NotContains(t, row, "· claude-opus-5", "model badge must yield when it would starve the title")
		})
	}
}

// The badge still renders when there is room — dropping it under pressure must
// not silently delete the feature on normal-width panes.
func TestSessionModelBadgeRendersWhenRoomy(t *testing.T) {
	row := renderTitleRow(t, 140, "conductor-call-queueing", "claude-opus-5", "work")
	require.Contains(t, row, "conductor-call-queueing")
	require.Contains(t, row, "· claude-opus-5")
}

// An unset account slot is the ambient/default login, not stored metadata: it
// renders no badge so the row does not repeat "[account:inherited]" everywhere.
// A named slot still must appear.
func TestEmptyAccountSlotRendersNoBadge(t *testing.T) {
	empty := renderTitleRow(t, 120, "conductor-core", "", "")
	require.NotContains(t, empty, "[account:")
	require.NotContains(t, empty, "inherited")

	named := renderTitleRow(t, 120, "conductor-core", "", "work")
	require.Contains(t, named, `[account:"work"]`)
}
