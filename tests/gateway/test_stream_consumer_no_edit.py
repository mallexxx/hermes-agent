"""Tests for GatewayStreamConsumer no-edit (webhook) mode — segment send bug."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig


# ── No-edit (webhook) mode tests ────────────────────────────────────────
# The bug: adapter_supports_edit=False mode uses `"\\\\n" in self._accumulated`
# which searches for literal ``\\n`` (two chars: backslash + n) instead of
# the real newline character ``\n`` (one char).  This prevents intermediate
# text between SEGMENT_BREAKs from being sent — it accumulates until got_done.
# Fixed by removing the bogus \\n check and using self._accumulated directly.


class TestNoEditModeSegmentSends:
    """Verify that in no-edit (webhook) mode, intermediate text between
    segment breaks is sent as separate messages, not accumulated until done."""

    @pytest.mark.asyncio
    async def test_no_edit_sends_text_across_segment_breaks(self):
        """Red-green test for the \\n-with-backslash bug: with
        adapter_supports_edit=False, text between SEGMENT_BREAKs must
        be sent independently (one send call per segment), not all at
        the end."""
        adapter = MagicMock()
        send_result_1 = SimpleNamespace(success=True, message_id="msg_1")
        send_result_2 = SimpleNamespace(success=True, message_id="msg_2")
        send_result_3 = SimpleNamespace(success=True, message_id="msg_3")
        adapter.send = AsyncMock(
            side_effect=[send_result_1, send_result_2, send_result_3]
        )
        adapter.MAX_MESSAGE_LENGTH = 4096

        config = StreamConsumerConfig(
            edit_interval=0.01,
            buffer_threshold=5,
            adapter_supports_edit=False,
        )
        consumer = GatewayStreamConsumer(adapter, "chat_123", config)

        # First segment: text before tool call
        consumer.on_delta("посмотреть лог\n")
        consumer.on_delta(None)  # SEGMENT_BREAK (tool call)

        # Second segment: text after tool call returns
        consumer.on_delta("Результат терминала #1\n")
        consumer.on_delta(None)  # SEGMENT_BREAK (another tool call)

        # Third segment: final text
        consumer.on_delta("Итог по итерациям\n")
        consumer.finish()

        await consumer.run()

        # Must have sent 3 separate messages (one per segment)
        assert adapter.send.call_count == 3, (
            f"Expected 3 sends (one per segment break), got {adapter.send.call_count}"
        )
        first_text = adapter.send.call_args_list[0][1]["content"]
        second_text = adapter.send.call_args_list[1][1]["content"]
        third_text = adapter.send.call_args_list[2][1]["content"]
        assert "посмотреть лог" in first_text
        assert "Результат терминала #1" in second_text
        assert "Итог по итерациям" in third_text

    @pytest.mark.asyncio
    async def test_no_edit_sends_text_without_newline_on_segment_break(self):
        """Even text without \\n must be sent when a segment break arrives.
        The old code checked for \\n-containing backslash which made this
        impossible — text accumulated until got_done."""
        adapter = MagicMock()
        send_result_1 = SimpleNamespace(success=True, message_id="msg_1")
        send_result_2 = SimpleNamespace(success=True, message_id="msg_2")
        adapter.send = AsyncMock(
            side_effect=[send_result_1, send_result_2]
        )
        adapter.MAX_MESSAGE_LENGTH = 4096

        config = StreamConsumerConfig(
            edit_interval=0.01,
            buffer_threshold=5,
            adapter_supports_edit=False,
        )
        consumer = GatewayStreamConsumer(adapter, "chat_123", config)

        # Text without newline, then segment break
        consumer.on_delta("Intermediate thought")
        consumer.on_delta(None)  # SEGMENT_BREAK
        consumer.on_delta("Final answer")
        consumer.finish()

        await consumer.run()

        # Both segments must have been delivered
        assert adapter.send.call_count == 2
        first_text = adapter.send.call_args_list[0][1]["content"]
        second_text = adapter.send.call_args_list[1][1]["content"]
        assert "Intermediate" in first_text
        assert "Final" in second_text

    @pytest.mark.asyncio
    async def test_no_edit_multiple_segments_are_separate_messages(self):
        """Multiple tool boundaries in no-edit mode create multiple
        separate sends, not one big accumulated message."""
        adapter = MagicMock()
        msg_counter = iter(["msg_1", "msg_2", "msg_3", "msg_4"])
        adapter.send = AsyncMock(
            side_effect=lambda **kw: SimpleNamespace(
                success=True, message_id=next(msg_counter)
            )
        )
        adapter.MAX_MESSAGE_LENGTH = 4096

        config = StreamConsumerConfig(
            edit_interval=0.01,
            buffer_threshold=5,
            adapter_supports_edit=False,
        )
        consumer = GatewayStreamConsumer(adapter, "chat_123", config)

        consumer.on_delta("Phase 1\n")
        consumer.on_delta(None)
        consumer.on_delta("Phase 2\n")
        consumer.on_delta(None)
        consumer.on_delta("Phase 3\n")
        consumer.on_delta(None)
        consumer.on_delta("Phase 4\n")
        consumer.finish()

        await consumer.run()

        assert adapter.send.call_count == 4, (
            f"Expected 4 sends (4 segment breaks), got {adapter.send.call_count}"
        )
