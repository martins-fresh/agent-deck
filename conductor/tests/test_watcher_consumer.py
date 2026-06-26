"""Tests for the gh-watcher event consumer.

Covers the pure helpers (subject parsing, session matching, filters,
rate-limit window) plus an end-to-end run of the consume loop against
an in-memory SQLite database that mirrors the production schema.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import bridge  # noqa: E402
from bridge import (  # noqa: E402
    WatcherConsumerState,
    consume_watcher_events_loop,
    extract_match_keys,
    find_target_session,
    load_watcher_consumer_config,
    should_coalesce_pr_labeled,
    should_drop_orca_review,
)


# ---------------------------------------------------------------------------
# extract_match_keys
# ---------------------------------------------------------------------------


class TestExtractMatchKeys:
    def test_pr_only(self):
        out = extract_match_keys(
            "[PR synchronize] #1832: feat(appointments): something",
        )
        assert out == {"pr": "1832"}

    def test_cor_in_brackets(self):
        out = extract_match_keys(
            "[PR synchronize] #1832: feat [COR-3227]",
        )
        assert out["pr"] == "1832"
        assert out["cor"] == "3227"

    def test_cor_inline_lowercase(self):
        out = extract_match_keys("commit on cor-3236 branch")
        assert out["cor"] == "3236"

    def test_branch_slug(self):
        out = extract_match_keys(
            "[push] feature/cor-3227-consent-form-enforcement: 1 commit(s)",
        )
        assert out["branch"].startswith("feature/cor-3227")
        assert out["cor"] == "3227"

    def test_all_three_present(self):
        out = extract_match_keys(
            "[PR synchronize] #1832 feature/cor-3227-x [COR-3227]",
        )
        assert out == {
            "pr": "1832",
            "cor": "3227",
            "branch": "feature/cor-3227-x",
        }

    def test_no_signals(self):
        out = extract_match_keys(
            "[pull_request_review] event from Fresh-Clinics/core-platform",
        )
        assert out == {}


# ---------------------------------------------------------------------------
# find_target_session
# ---------------------------------------------------------------------------


@pytest.fixture
def sessions():
    """Mirror real production session shape."""
    return [
        {"title": "1831", "path": "/home/martins/work/infra/.worktrees/feature-1831"},
        {"title": "1832", "path": "/home/martins/work/infra/.worktrees/feature-1832"},
        {"title": "1830", "path": "/home/martins/work/infra/.worktrees/feature-1830"},
        {"title": "impl-cor-3225", "path": "/home/martins/work/core-platform-cor-3225"},
        {
            "title": "impl-cor-3227-consent",
            "path": "/home/martins/work/core-platform/.worktrees/feature/cor-3227-consent-form-enforcement",
        },
        {"title": "ad-bridge-multichannel", "path": "/home/martins/code/agent-deck"},
        {"title": "conductor-core", "path": "/home/martins/.agent-deck/conductor/core"},
    ]


class TestFindTargetSession:
    def test_pr_exact_title_match(self, sessions):
        target = find_target_session({"pr": "1831"}, sessions)
        assert target is not None
        assert target["title"] == "1831"

    def test_pr_path_match(self, sessions):
        # PR number with no exact title — falls to path match.
        custom = [
            {"title": "work-1822", "path": "/home/martins/work/infra/.worktrees/feature-1822"},
        ]
        target = find_target_session({"pr": "1822"}, custom)
        assert target is not None
        assert target["title"] == "work-1822"

    def test_cor_path_match(self, sessions):
        target = find_target_session({"cor": "3227"}, sessions)
        assert target is not None
        assert "3227" in target["path"]

    def test_branch_slug_match(self, sessions):
        target = find_target_session(
            {"branch": "feature/cor-3227-consent-form-enforcement"},
            sessions,
        )
        assert target is not None
        assert "3227" in target["path"]

    def test_pr_wins_over_cor_when_both_present(self, sessions):
        # PR 1832 doesn't have a session with COR-3227 in path, but
        # there's a separate session with both. PR priority should
        # pick the PR-matching one.
        target = find_target_session(
            {"pr": "1832", "cor": "3225"}, sessions,
        )
        assert target["title"] == "1832"

    def test_no_match_returns_none(self, sessions):
        target = find_target_session({"pr": "9999"}, sessions)
        assert target is None

    def test_empty_keys_returns_none(self, sessions):
        target = find_target_session({}, sessions)
        assert target is None

    def test_longest_match_wins_on_tie(self):
        # Two sessions both contain "cor-3" in path; the one with the
        # longer path string wins.
        sessions = [
            {"title": "short", "path": "/cor-3"},
            {"title": "long", "path": "/very-long-path-cor-3-something-else"},
        ]
        target = find_target_session({"cor": "3"}, sessions)
        # Longest path overlap → "long".
        assert target["title"] == "long"


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


class TestDropOrcaReview:
    def test_orca_review_dropped(self):
        assert should_drop_orca_review(
            "orca-security-au[bot]@github.com",
            "[pull_request_review] event from Fresh-Clinics/core-platform",
        ) is True

    def test_non_orca_passes(self):
        assert should_drop_orca_review(
            "davemcpherson-fc@github.com",
            "[pull_request_review] event from Fresh-Clinics/core-platform",
        ) is False

    def test_orca_non_review_passes(self):
        # If orca ever sent a non-review event, we wouldn't drop it.
        assert should_drop_orca_review(
            "orca-security-au[bot]@github.com",
            "[issue_comment] event",
        ) is False


class TestDropBarePrReview:
    """Sender-agnostic filter for `[pull_request_review]` events that
    carry no PR# and no verdict. The conductor can't act on these."""

    def test_bare_dave_review_dropped(self):
        from bridge import should_drop_bare_pr_review
        assert should_drop_bare_pr_review(
            "[pull_request_review] event from Fresh-Clinics/core-platform"
        ) is True

    def test_sender_agnostic_any_reviewer_dropped(self):
        # Filter doesn't take sender; works for any reviewer.
        from bridge import should_drop_bare_pr_review
        for subject in [
            "[pull_request_review] event from Fresh-Clinics/core-platform",
            "[pull_request_review] event from martins-fresh/agent-deck",
            "[pull_request_review] something",
        ]:
            assert should_drop_bare_pr_review(subject) is True, subject

    def test_review_with_pr_number_kept(self):
        # If a PR# is present, the conductor has something to look up.
        from bridge import should_drop_bare_pr_review
        assert should_drop_bare_pr_review(
            "[pull_request_review] #1832: approved"
        ) is False

    def test_review_with_verdict_kept(self):
        # Verdict-bearing reviews are actionable even without PR#.
        from bridge import should_drop_bare_pr_review
        for verdict in (
            "approved", "changes_requested", "commented", "dismissed",
        ):
            subject = f"[pull_request_review] {verdict} on something"
            assert should_drop_bare_pr_review(subject) is False, subject

    def test_verdict_case_insensitive(self):
        from bridge import should_drop_bare_pr_review
        assert should_drop_bare_pr_review(
            "[pull_request_review] CHANGES_REQUESTED on PR"
        ) is False

    def test_non_review_event_kept(self):
        from bridge import should_drop_bare_pr_review
        for subject in [
            "[push] feature/cor-3227: 1 commit(s)",
            "[PR opened] #1842: title here",
            "[PR synchronize] #1832",
            "[issue_comment] event",
            "[PR labeled] #1831",
        ]:
            assert should_drop_bare_pr_review(subject) is False, subject

    def test_config_toggle_off_bypasses_filter(self):
        """When drop_bare_pr_review is False in state.json, the
        deliver-path must not invoke the filter. Verified indirectly
        through load_watcher_consumer_config."""
        # The function itself is pure — the toggle lives at the
        # caller. Verify the config loader honors a False value.
        import bridge
        import json
        import tempfile
        from pathlib import Path
        tmp = Path(tempfile.mkdtemp())
        (tmp / "state.json").write_text(
            json.dumps({"watcher_consumer": {"drop_bare_pr_review": False}})
        )
        orig = bridge.AGENT_DECK_DIR
        try:
            bridge.AGENT_DECK_DIR = tmp
            cfg = bridge.load_watcher_consumer_config()
            assert cfg["drop_bare_pr_review"] is False
        finally:
            bridge.AGENT_DECK_DIR = orig

    def test_default_config_drops_bare(self):
        import bridge
        import tempfile
        from pathlib import Path
        tmp = Path(tempfile.mkdtemp())
        orig = bridge.AGENT_DECK_DIR
        try:
            bridge.AGENT_DECK_DIR = tmp  # no state.json at all
            cfg = bridge.load_watcher_consumer_config()
            assert cfg["drop_bare_pr_review"] is True
        finally:
            bridge.AGENT_DECK_DIR = orig


class TestDropBarePrEdit:
    """Sender-agnostic filter for `[PR edited]` events. The github
    watcher emits these for every title/description change; Linear's
    sync-bot can flood a single PR with 9+ identical edits. No commit,
    no state change — pure noise."""

    def test_pr_edit_dropped(self):
        from bridge import should_drop_bare_pr_edit
        assert should_drop_bare_pr_edit(
            "[PR edited] #2052: fix(billing): Change Plan button stays disabled [COR-3693]"
        ) is True

    def test_pr_edit_sender_agnostic(self):
        # Filter takes only the subject; works for any editor —
        # Linear bot, human, anyone.
        from bridge import should_drop_bare_pr_edit
        for subject in [
            "[PR edited] #2029: feat(payouts): something [COR-3263]",
            "[PR edited] #2052: title here",
            "[PR edited] #999: bare title",
        ]:
            assert should_drop_bare_pr_edit(subject) is True, subject

    def test_non_edit_events_kept(self):
        from bridge import should_drop_bare_pr_edit
        for subject in [
            "[PR opened] #1842: title",
            "[PR synchronize] #1832: stuff",
            "[PR closed] #1822: ...",
            "[PR labeled] #1831",
            "[push] feature/cor-3227: 1 commit(s)",
            "[pull_request_review] event from Fresh-Clinics/core-platform",
            "[issue_comment] event",
        ]:
            assert should_drop_bare_pr_edit(subject) is False, subject

    def test_edit_in_body_text_not_at_token_boundary_kept(self):
        # We anchor on the literal "[PR edited]" token; the word
        # "edited" appearing elsewhere in a non-edit subject must not
        # trigger the filter.
        from bridge import should_drop_bare_pr_edit
        assert should_drop_bare_pr_edit(
            "[PR opened] #100: edited template still applies"
        ) is False

    def test_config_toggle_off_bypasses_filter(self):
        import bridge, json, tempfile
        from pathlib import Path
        tmp = Path(tempfile.mkdtemp())
        (tmp / "state.json").write_text(
            json.dumps({"watcher_consumer": {"drop_bare_pr_edit": False}})
        )
        orig = bridge.AGENT_DECK_DIR
        try:
            bridge.AGENT_DECK_DIR = tmp
            cfg = bridge.load_watcher_consumer_config()
            assert cfg["drop_bare_pr_edit"] is False
        finally:
            bridge.AGENT_DECK_DIR = orig

    def test_default_config_drops_pr_edit(self):
        import bridge, tempfile
        from pathlib import Path
        tmp = Path(tempfile.mkdtemp())
        orig = bridge.AGENT_DECK_DIR
        try:
            bridge.AGENT_DECK_DIR = tmp  # no state.json
            cfg = bridge.load_watcher_consumer_config()
            assert cfg["drop_bare_pr_edit"] is True
        finally:
            bridge.AGENT_DECK_DIR = orig


class TestCoalescePrLabeled:
    def test_first_labeled_event_not_coalesced(self):
        state = WatcherConsumerState()
        out = should_coalesce_pr_labeled(
            "[PR labeled] #1831: test", state, now=100.0, window_s=10,
        )
        assert out is False
        assert "1831" in state.pr_labeled_seen

    def test_second_within_window_coalesced(self):
        state = WatcherConsumerState()
        should_coalesce_pr_labeled(
            "[PR labeled] #1831: test", state, now=100.0, window_s=10,
        )
        out = should_coalesce_pr_labeled(
            "[PR labeled] #1831: test", state, now=105.0, window_s=10,
        )
        assert out is True

    def test_second_outside_window_passes(self):
        state = WatcherConsumerState()
        should_coalesce_pr_labeled(
            "[PR labeled] #1831: test", state, now=100.0, window_s=10,
        )
        out = should_coalesce_pr_labeled(
            "[PR labeled] #1831: test", state, now=115.0, window_s=10,
        )
        assert out is False

    def test_different_prs_dont_collide(self):
        state = WatcherConsumerState()
        should_coalesce_pr_labeled(
            "[PR labeled] #1831", state, now=100.0, window_s=10,
        )
        out = should_coalesce_pr_labeled(
            "[PR labeled] #1832", state, now=101.0, window_s=10,
        )
        assert out is False

    def test_non_labeled_event_passes(self):
        state = WatcherConsumerState()
        out = should_coalesce_pr_labeled(
            "[PR synchronize] #1831", state, now=100.0, window_s=10,
        )
        assert out is False


# ---------------------------------------------------------------------------
# Rate limit
# ---------------------------------------------------------------------------


class TestRateLimit:
    def test_first_n_pass_then_drop(self):
        state = WatcherConsumerState()
        now = 1000.0
        # 6 should pass; 7th must drop.
        for i in range(6):
            assert state.can_deliver("core", now + i * 0.1, 6) is True
            state.record_delivery("core", now + i * 0.1)
        # 7th in the same minute
        assert state.can_deliver("core", now + 0.7, 6) is False

    def test_window_slides(self):
        state = WatcherConsumerState()
        now = 1000.0
        for i in range(6):
            assert state.can_deliver("core", now + i * 0.1, 6) is True
            state.record_delivery("core", now + i * 0.1)
        # 60s later — window has slid, deliveries allowed again.
        assert state.can_deliver("core", now + 61.0, 6) is True

    def test_per_conductor_isolation(self):
        state = WatcherConsumerState()
        for i in range(6):
            state.record_delivery("core", 1000.0 + i)
        # core is full
        assert state.can_deliver("core", 1006.0, 6) is False
        # imgproxy is fresh
        assert state.can_deliver("imgproxy", 1006.0, 6) is True


# ---------------------------------------------------------------------------
# load_watcher_consumer_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", tmp_path)
        cfg = load_watcher_consumer_config()
        assert cfg["poll_interval_seconds"] == 10
        assert cfg["rate_limit_per_minute"] == 6
        assert cfg["coalesce_pr_labeled_window_s"] == 10
        assert cfg["fallback_conductor"] == "core"

    def test_overrides(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", tmp_path)
        (tmp_path / "state.json").write_text(
            '{"watcher_consumer": {'
            '"poll_interval_seconds": 5,'
            '"rate_limit_per_minute": 12,'
            '"fallback_conductor": "github"'
            "}}"
        )
        cfg = load_watcher_consumer_config()
        assert cfg["poll_interval_seconds"] == 5
        assert cfg["rate_limit_per_minute"] == 12
        assert cfg["fallback_conductor"] == "github"

    def test_malformed_state_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", tmp_path)
        (tmp_path / "state.json").write_text("not json {")
        cfg = load_watcher_consumer_config()
        assert cfg["poll_interval_seconds"] == 10  # default


# ---------------------------------------------------------------------------
# Consume loop end-to-end
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_db(tmp_path):
    """Build a watcher_events DB matching the production schema."""
    db = tmp_path / "state.db"
    con = sqlite3.connect(str(db))
    con.executescript(
        """
        CREATE TABLE watcher_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watcher_id TEXT NOT NULL,
            dedup_key TEXT NOT NULL,
            sender TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL DEFAULT '',
            routed_to TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            triage_session_id TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        );
        """
    )
    rows = [
        ("w1", "k1", "martin.s@freshclinics.com",
         "[PR synchronize] #1832: feat [COR-3227]", "janitor", 1000),
        ("w1", "k2", "orca-security-au[bot]@github.com",
         "[pull_request_review] event from Fresh-Clinics/core-platform", "janitor", 1001),
        ("w1", "k3", "greg.m@freshclinics.com",
         "[push] feature/cor-2364-x: 1 commit(s)", "janitor", 1002),
        ("w1", "k4", "user@example.com",
         "[issue_comment] event with no signals at all", "janitor", 1003),
    ]
    for r in rows:
        con.execute(
            "INSERT INTO watcher_events (watcher_id, dedup_key, sender, "
            "subject, routed_to, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            r,
        )
    con.commit()
    con.close()
    return db


class TestConsumeLoop:
    @pytest.mark.asyncio
    async def test_full_cycle_routes_and_filters(
        self, populated_db, tmp_path, monkeypatch,
    ):
        # Point AGENT_DECK_DIR at tmp_path with state.db under
        # profiles/default/state.db (production layout).
        deck = tmp_path
        (deck / "profiles" / "default").mkdir(parents=True)
        populated_db.rename(deck / "profiles" / "default" / "state.db")
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", deck)

        sessions = [
            {
                "title": "1832",
                "path": "/work/feature-1832",
                "profile": "default",
            },
            {
                "title": "fix-cor-2364",
                "path": "/work/core-platform-cor-2364",
                "profile": "default",
            },
            {
                "title": "conductor-core",
                "path": "/.agent-deck/conductor/core",
                "profile": "default",
            },
        ]
        send_mock = MagicMock(return_value=(True, ""))

        # poll_interval=0 + cancel after one cycle so test is fast.
        (deck / "state.json").write_text(
            '{"watcher_consumer": {"poll_interval_seconds": 0}}'
        )

        with patch("bridge._list_all_sessions", return_value=sessions), \
             patch("bridge.send_to_conductor", send_mock), \
             patch("bridge._init_watcher_cursor", return_value=0):
            import asyncio
            state = WatcherConsumerState()
            task = asyncio.create_task(
                consume_watcher_events_loop({"routes": None}, state)
            )
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Should have delivered:
        #   k1 → session 1832 (PR# match)
        #   k3 → session fix-cor-2364 (COR + branch match)
        #   k4 → conductor-core fallback
        # Should have dropped: k2 (orca bot review)
        targets = [c.args[0] for c in send_mock.call_args_list]
        assert "1832" in targets
        assert "fix-cor-2364" in targets
        assert "conductor-core" in targets
        # Exactly 3 deliveries, orca dropped.
        assert len(targets) == 3

    @pytest.mark.asyncio
    async def test_init_cursor_skips_history(
        self, populated_db, tmp_path, monkeypatch,
    ):
        """Initial cursor = max(id) so historical rows are NOT re-delivered."""
        deck = tmp_path
        (deck / "profiles" / "default").mkdir(parents=True)
        populated_db.rename(deck / "profiles" / "default" / "state.db")
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", deck)

        send_mock = MagicMock(return_value=(True, ""))
        (deck / "state.json").write_text(
            '{"watcher_consumer": {"poll_interval_seconds": 0}}'
        )

        with patch("bridge._list_all_sessions", return_value=[]), \
             patch("bridge.send_to_conductor", send_mock):
            import asyncio
            state = WatcherConsumerState()
            task = asyncio.create_task(
                consume_watcher_events_loop({"routes": None}, state)
            )
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Cursor advanced to max(id) at init, no fresh rows → no deliveries.
        send_mock.assert_not_called()
        assert state.last_seen_id == 4

    @pytest.mark.asyncio
    async def test_missing_db_returns_without_crash(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(bridge, "AGENT_DECK_DIR", tmp_path)
        state = WatcherConsumerState()
        # state.db doesn't exist → consumer logs and returns immediately.
        await consume_watcher_events_loop({"routes": None}, state)
