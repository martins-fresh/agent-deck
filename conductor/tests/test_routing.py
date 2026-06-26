"""Tests for the per-conductor-per-channel routing primitive.

Covers Routes lookup behavior and _build_routes parsing of the new
[conductor.<name>.<platform>] config sections. Does not exercise the
Discord/Slack handler code paths — those are integration-level and
covered separately.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from bridge import Routes, _build_routes  # noqa: E402


def _cfg(d):
    """Make a fake [conductor] section dict for _build_routes."""
    return d


class TestBuildRoutes:
    def test_empty_config_yields_empty_routes(self):
        routes = _build_routes(_cfg({}), legacy_channels={"discord": "", "slack": "", "telegram": ""})
        assert routes.by_channel["discord"] == {}
        assert routes.by_conductor["discord"] == {}

    def test_single_per_conductor_binding(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 12345}}}),
            legacy_channels={"discord": "999"},
        )
        assert routes.by_channel["discord"] == {"12345": "imgproxy"}
        assert routes.by_conductor["discord"] == {"imgproxy": "12345"}

    def test_multiple_conductors_across_platforms(self):
        routes = _build_routes(
            _cfg({
                "imgproxy": {
                    "discord": {"channel_id": 111},
                    "slack": {"channel_id": "C111"},
                },
                "ahpra": {"discord": {"channel_id": 222}},
            }),
            legacy_channels={"discord": "", "slack": ""},
        )
        assert routes.by_channel["discord"] == {"111": "imgproxy", "222": "ahpra"}
        assert routes.by_channel["slack"] == {"C111": "imgproxy"}
        assert routes.by_conductor["discord"]["imgproxy"] == "111"
        assert routes.by_conductor["discord"]["ahpra"] == "222"

    def test_reserved_keys_are_skipped(self):
        # Top-level [conductor.discord] etc. must not be treated as a
        # conductor named "discord".
        routes = _build_routes(
            _cfg({
                "discord": {"channel_id": 999, "bot_token": "x"},
                "slack": {"channel_id": "C999"},
                "telegram": {"token": "y"},
                "enabled": True,
                "heartbeat_interval": 15,
                "imgproxy": {"discord": {"channel_id": 111}},
            }),
            legacy_channels={"discord": "999", "slack": "C999"},
        )
        assert routes.by_channel["discord"] == {"111": "imgproxy"}
        assert "discord" not in routes.by_conductor["discord"]

    def test_conflicting_channel_warns_and_keeps_first(self, caplog):
        # Two conductors claim the same channel — first wins, warning logged.
        with caplog.at_level("WARNING"):
            routes = _build_routes(
                _cfg({
                    "imgproxy": {"discord": {"channel_id": 111}},
                    "ahpra": {"discord": {"channel_id": 111}},
                }),
                legacy_channels={"discord": ""},
            )
        assert routes.by_channel["discord"] == {"111": "imgproxy"}
        # ahpra didn't get a binding on discord
        assert "ahpra" not in routes.by_conductor["discord"]
        assert any("Routing conflict" in r.message for r in caplog.records)

    def test_legacy_channel_collision_warns(self, caplog):
        with caplog.at_level("WARNING"):
            _build_routes(
                _cfg({"imgproxy": {"discord": {"channel_id": 999}}}),
                legacy_channels={"discord": "999"},
            )
        assert any(
            "also appears as a per-conductor binding" in r.message
            for r in caplog.records
        )

    def test_empty_channel_id_skipped(self):
        routes = _build_routes(
            _cfg({
                "imgproxy": {"discord": {"channel_id": 0}},
                "ahpra": {"discord": {"channel_id": ""}},
                "core": {"discord": {"channel_id": None}},
            }),
            legacy_channels={"discord": ""},
        )
        assert routes.by_channel["discord"] == {}


class TestRoutesInbound:
    def test_bound_channel_returns_target_and_is_bound(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        target, is_bound = routes.inbound("discord", 111)
        assert target == "imgproxy"
        assert is_bound is True

    def test_legacy_channel_returns_none_not_bound(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        target, is_bound = routes.inbound("discord", 999)
        assert target is None
        assert is_bound is False

    def test_string_and_int_channel_ids_both_resolve(self):
        # Discord uses int channel IDs; Slack uses string. The lookup
        # stringifies on both sides so either works.
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": ""},
        )
        assert routes.inbound("discord", 111)[0] == "imgproxy"
        assert routes.inbound("discord", "111")[0] == "imgproxy"

    def test_unknown_channel_returns_none(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        target, is_bound = routes.inbound("discord", 7777)
        assert target is None
        assert is_bound is False

    def test_is_known_channel(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        assert routes.is_known_channel("discord", 111) is True
        assert routes.is_known_channel("discord", 999) is True
        assert routes.is_known_channel("discord", 7777) is False


class TestRoutesOutbound:
    def test_bound_conductor_returns_channel_and_is_bound(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        ch, is_bound = routes.outbound("discord", "imgproxy")
        assert ch == "111"
        assert is_bound is True

    def test_unbound_conductor_falls_back_to_legacy(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )
        ch, is_bound = routes.outbound("discord", "ahpra")
        assert ch == "999"
        assert is_bound is False

    def test_unbound_conductor_with_no_legacy_returns_none(self):
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": ""},
        )
        ch, is_bound = routes.outbound("discord", "ahpra")
        assert ch is None
        assert is_bound is False

    def test_per_platform_isolation(self):
        # A discord binding does NOT make the conductor reachable on slack.
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999", "slack": ""},
        )
        assert routes.outbound("slack", "imgproxy") == (None, False)


class TestFormatAlertBody:
    """The body-formatter is a small piece, but it's the one place where
    'channel implies conductor' becomes user-visible. Lock it down."""

    def test_bound_channel_drops_prefix_even_in_multi(self):
        from bridge import _format_alert_body
        body = _format_alert_body(
            "imgproxy", "NEED: foo", is_bound=True, multi=True,
        )
        assert body == "Conductor alert:\nNEED: foo"
        assert "[imgproxy]" not in body

    def test_unbound_multi_keeps_prefix(self):
        from bridge import _format_alert_body
        body = _format_alert_body(
            "imgproxy", "NEED: foo", is_bound=False, multi=True,
        )
        assert body == "[imgproxy] Conductor alert:\nNEED: foo"

    def test_unbound_single_conductor_drops_prefix(self):
        from bridge import _format_alert_body
        body = _format_alert_body(
            "solo", "NEED: foo", is_bound=False, multi=False,
        )
        assert body == "Conductor alert:\nNEED: foo"


class TestPostDiscordAlert:
    """Tests for the proactive-send outbound path.

    The three cases the user explicitly asked for:
      1. Bound conductor → send_discord_output called with the bound channel.
      2. Unbound conductor with legacy → send_discord_output called with
         the legacy channel; body carries the [name] prefix.
      3. Unbound conductor, no legacy → send_discord_output NOT called;
         debug log emitted.
    """

    @pytest.fixture
    def mock_send(self, monkeypatch):
        """Patch bridge.send_discord_output so we can observe calls."""
        from unittest.mock import AsyncMock
        import bridge
        mock = AsyncMock()
        monkeypatch.setattr(bridge, "send_discord_output", mock)
        return mock

    @pytest.fixture
    def mock_discord_bot(self):
        """A fake discord.py client whose .get_channel returns a sentinel."""
        from unittest.mock import MagicMock
        bot = MagicMock()
        # Default: any channel id resolves to a non-None sentinel object.
        bot.get_channel = MagicMock(side_effect=lambda cid: f"<channel:{cid}>")
        return bot

    @pytest.mark.asyncio
    async def test_bound_conductor_posts_to_dedicated_channel(
        self, mock_send, mock_discord_bot,
    ):
        from bridge import _post_discord_alert
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )

        await _post_discord_alert(
            mock_discord_bot, routes, "imgproxy",
            response="NEED: foo", multi=True,
        )

        mock_discord_bot.get_channel.assert_called_once_with(111)
        mock_send.assert_called_once()
        channel_arg, body_arg = mock_send.call_args.args
        assert channel_arg == "<channel:111>"
        # Bound channel → no [name] prefix in body.
        assert "[imgproxy]" not in body_arg
        assert body_arg == "Conductor alert:\nNEED: foo"

    @pytest.mark.asyncio
    async def test_unbound_with_legacy_posts_to_legacy_with_prefix(
        self, mock_send, mock_discord_bot,
    ):
        from bridge import _post_discord_alert
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": "999"},
        )

        # `ahpra` has no per-conductor binding → falls back to legacy 999
        # and (because multi=True) the body should carry the [ahpra] prefix.
        await _post_discord_alert(
            mock_discord_bot, routes, "ahpra",
            response="NEED: bar", multi=True,
        )

        mock_discord_bot.get_channel.assert_called_once_with(999)
        mock_send.assert_called_once()
        channel_arg, body_arg = mock_send.call_args.args
        assert channel_arg == "<channel:999>"
        assert body_arg.startswith("[ahpra] ")
        assert body_arg == "[ahpra] Conductor alert:\nNEED: bar"

    @pytest.mark.asyncio
    async def test_unbound_no_legacy_skips_and_logs_debug(
        self, mock_send, mock_discord_bot, caplog,
    ):
        from bridge import _post_discord_alert
        # No bindings, no legacy → unbound conductor has no destination.
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": ""},
        )

        with caplog.at_level("DEBUG", logger="conductor-bridge"):
            await _post_discord_alert(
                mock_discord_bot, routes, "ahpra",
                response="NEED: baz", multi=True,
            )

        mock_send.assert_not_called()
        mock_discord_bot.get_channel.assert_not_called()
        assert any(
            "Skipping Discord alert for ahpra" in r.message
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_channel_not_resolvable_logs_warning_and_skips_send(
        self, mock_send, caplog,
    ):
        """Edge case: routing resolves a channel but discord.py's
        get_channel returns None (bot not in that channel). We must
        warn rather than call send_discord_output with None."""
        from unittest.mock import MagicMock
        from bridge import _post_discord_alert
        routes = _build_routes(
            _cfg({"imgproxy": {"discord": {"channel_id": 111}}}),
            legacy_channels={"discord": ""},
        )
        bot = MagicMock()
        bot.get_channel = MagicMock(return_value=None)

        with caplog.at_level("WARNING", logger="conductor-bridge"):
            await _post_discord_alert(
                bot, routes, "imgproxy",
                response="NEED: qux", multi=True,
            )

        mock_send.assert_not_called()
        bot.get_channel.assert_called_once_with(111)
        assert any(
            "not found by client" in r.message for r in caplog.records
        )
