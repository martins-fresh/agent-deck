"""Tests for the Discord attachment-fix on inbound bridge messages.

Image-only messages were dropped by the `if not text: return` guard in
`on_message`. The fix downloads attachments to local disk and appends
`[ATTACHED:/path]` markers to the message text so the conductor sees
both the text (if any) and the local file paths.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import bridge  # noqa: E402
from bridge import (  # noqa: E402
    _augment_text_with_attachments,
    _save_discord_attachments,
)


class TestAugmentText:
    def test_no_attachments_returns_text_unchanged(self):
        assert _augment_text_with_attachments("hello", []) == "hello"

    def test_empty_text_with_attachments_yields_attached_only(self):
        out = _augment_text_with_attachments("", ["/tmp/a.png"])
        assert out == "[ATTACHED:/tmp/a.png]"

    def test_whitespace_text_with_attachments_drops_whitespace(self):
        out = _augment_text_with_attachments("   ", ["/tmp/a.png"])
        assert out == "[ATTACHED:/tmp/a.png]"

    def test_text_plus_attachment_joined_with_space(self):
        out = _augment_text_with_attachments("look at this", ["/tmp/a.png"])
        assert out == "look at this [ATTACHED:/tmp/a.png]"

    def test_multiple_attachments_space_separated(self):
        out = _augment_text_with_attachments(
            "", ["/tmp/a.png", "/tmp/b.jpg"],
        )
        assert out == "[ATTACHED:/tmp/a.png] [ATTACHED:/tmp/b.jpg]"


class TestSaveDiscordAttachments:
    @pytest.fixture
    def message_with_attachments(self):
        """A fake discord.Message with two attachments."""
        msg = MagicMock()
        msg.channel.id = 1234567890
        msg.id = 9876543210
        att1 = MagicMock()
        att1.id = 111
        att1.filename = "photo.png"
        att1.save = AsyncMock()
        att2 = MagicMock()
        att2.id = 222
        att2.filename = "doc.pdf"
        att2.save = AsyncMock()
        msg.attachments = [att1, att2]
        return msg, [att1, att2]

    @pytest.mark.asyncio
    async def test_no_attachments_returns_empty(self):
        msg = MagicMock()
        msg.attachments = []
        result = await _save_discord_attachments(msg)
        assert result == []

    @pytest.mark.asyncio
    async def test_saves_each_attachment_under_channel_msg_path(
        self, tmp_path, monkeypatch, message_with_attachments,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg, atts = message_with_attachments

        result = await _save_discord_attachments(msg)

        expected_dir = tmp_path / "attachments" / "1234567890" / "9876543210"
        assert expected_dir.exists()
        assert len(result) == 2
        assert result[0] == str(expected_dir / "photo.png")
        assert result[1] == str(expected_dir / "doc.pdf")
        atts[0].save.assert_awaited_once_with(str(expected_dir / "photo.png"))
        atts[1].save.assert_awaited_once_with(str(expected_dir / "doc.pdf"))

    @pytest.mark.asyncio
    async def test_path_traversal_filename_stripped(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 99
        att.filename = "../../escape.png"
        att.save = AsyncMock()
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)

        # basename strips the traversal — the file lands inside the
        # message dir, not somewhere up the tree.
        assert len(result) == 1
        assert result[0].endswith("/1/2/escape.png")
        assert ".." not in result[0]

    @pytest.mark.asyncio
    async def test_empty_filename_falls_back_to_id(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 4242
        att.filename = ""
        att.save = AsyncMock()
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        assert result[0].endswith("attachment-4242")

    @pytest.mark.asyncio
    async def test_concurrent_attachments_all_saved_in_order(
        self, tmp_path, monkeypatch,
    ):
        """Multiple attachments arriving on the same message — all
        should land at the right paths, ordering preserved, no race
        on directory creation."""
        import asyncio
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2

        # Five attachments whose .save() awaits varying durations to
        # surface any ordering / race issues. Order of completion is
        # NOT the order they were enumerated.
        atts = []
        durations = [0.03, 0.001, 0.02, 0.005, 0.01]
        for i, d in enumerate(durations):
            a = MagicMock()
            a.id = i
            a.filename = f"f{i}.bin"

            async def _save(path, delay=d):
                await asyncio.sleep(delay)
            a.save = AsyncMock(side_effect=_save)
            atts.append(a)
        msg.attachments = atts

        result = await _save_discord_attachments(msg)

        # All 5 saved.
        assert len(result) == 5
        # Order in the returned list matches the order in
        # message.attachments, NOT the order they happened to finish.
        for i, path in enumerate(result):
            assert path.endswith(f"/f{i}.bin")
        # Each .save() invoked exactly once with the right path.
        for i, a in enumerate(atts):
            a.save.assert_awaited_once_with(
                str(tmp_path / "attachments" / "1" / "2" / f"f{i}.bin"),
            )

    @pytest.mark.asyncio
    async def test_save_failure_does_not_abort_batch(
        self, tmp_path, monkeypatch,
    ):
        """If one attachment's .save() raises, the others must still
        succeed. Partial download beats dropping the whole message."""
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2

        good = MagicMock()
        good.id = 1
        good.filename = "good.png"
        good.save = AsyncMock()

        bad = MagicMock()
        bad.id = 2
        bad.filename = "bad.jpg"
        bad.save = AsyncMock(side_effect=RuntimeError("network died"))

        msg.attachments = [bad, good]
        result = await _save_discord_attachments(msg)

        # Only `good.png` was saved.
        assert len(result) == 1
        assert result[0].endswith("/good.png")


class TestMalformedMessages:
    """Adversarial cases the helpers must survive without crashing."""

    @pytest.mark.asyncio
    async def test_message_with_no_attachments_attribute(self):
        """Discord messages where `attachments` is missing entirely
        (delete events, partial fetches) must not crash."""
        msg = MagicMock(spec=["channel", "id"])
        msg.channel = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        # No .attachments at all.
        result = await _save_discord_attachments(msg)
        assert result == []

    @pytest.mark.asyncio
    async def test_attachment_with_none_filename(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 777
        att.filename = None  # None, not empty string
        att.save = AsyncMock()
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        # Falls back to "attachment-<id>" rather than crashing.
        assert len(result) == 1
        assert result[0].endswith("attachment-777")

    @pytest.mark.asyncio
    async def test_attachment_http_404_logged_and_skipped(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 1
        att.filename = "gone.png"
        # discord.py raises NotFound on 404 — we wrap in a generic
        # Exception to avoid coupling tests to discord.py internals.
        att.save = AsyncMock(side_effect=Exception("404 Not Found"))
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        # Failed download is skipped, not crashed.
        assert result == []

    @pytest.mark.asyncio
    async def test_attachment_5xx_error_skipped(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 1
        att.filename = "server_down.png"
        att.save = AsyncMock(
            side_effect=Exception("503 Service Unavailable"),
        )
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        assert result == []

    @pytest.mark.asyncio
    async def test_oversized_attachment_oserror_does_not_crash(
        self, tmp_path, monkeypatch,
    ):
        """Disk write failure (out of space, permission denied, file
        too large for FS) on one attachment must not take down the
        whole message."""
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", tmp_path / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 1
        att.filename = "huge.bin"
        att.save = AsyncMock(side_effect=OSError("File too large"))
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        assert result == []

    @pytest.mark.asyncio
    async def test_target_dir_creation_failure_returns_empty(
        self, tmp_path, monkeypatch,
    ):
        """If mkdir itself raises (read-only FS, permission denied),
        we log and bail rather than half-creating state."""
        bad_root = MagicMock()
        # Pretend mkdir always fails.
        bad_root_dir = tmp_path / "no_write"
        bad_root_dir.mkdir(mode=0o500)  # read+exec only, no write
        monkeypatch.setattr(
            bridge, "BRIDGE_ATTACHMENTS_DIR", bad_root_dir / "attachments",
        )
        msg = MagicMock()
        msg.channel.id = 1
        msg.id = 2
        att = MagicMock()
        att.id = 1
        att.filename = "x.png"
        att.save = AsyncMock()
        msg.attachments = [att]

        result = await _save_discord_attachments(msg)
        assert result == []
        # Save should never have been attempted because target_dir
        # creation failed.
        att.save.assert_not_called()

    def test_augment_with_none_text(self):
        """Some discord events provide `content=None` rather than ''."""
        # The helper should treat None like empty.
        # We can't pass None directly (signature is str), but the
        # caller in on_message uses `message.content` which can be
        # the empty string when missing. Verify the empty-string path.
        out = _augment_text_with_attachments("", ["/tmp/a.png"])
        assert out == "[ATTACHED:/tmp/a.png]"
