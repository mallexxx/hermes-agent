"""Zulip platform adapter.

Connects to a self-hosted (or cloud) Zulip instance via its REST API
and event-queue long-polling for real-time message delivery.
No external Zulip library required — uses aiohttp (already a Hermes dep).

Authentication: Zulip uses HTTP Basic Auth with bot email + API key.

Environment variables:
    ZULIP_URL               Server URL (e.g. https://zulip.local or https://yourorg.zulipchat.com)
    ZULIP_BOT_EMAIL         Bot email (e.g. eagle-bot@zulip.local)
    ZULIP_API_KEY           Bot API key
    ZULIP_ALLOWED_USERS     Comma-separated user IDs (numeric) or emails
    ZULIP_HOME_CHANNEL      Home stream::topic (e.g. "master::hermes")
    ZULIP_DEFAULT_TOPIC     Default topic when not specified (default: "general")
    ZULIP_REQUIRE_MENTION   Require @mention in stream messages (default: true)
    ZULIP_FREE_STREAMS      Comma-separated stream names where bot responds freely

chat_id format: "stream_name" or "stream_name::topic"
  - If "stream_name::topic", both stream and topic are explicit.
  - If "stream_name" only, ZULIP_DEFAULT_TOPIC is used (default: "general").
  - thread_id (from executor) becomes the topic for threaded conversations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

# Zulip message size limit (server default is 10000 chars; use 4000 for safety).
MAX_MESSAGE_LENGTH = 4000

# Reconnect parameters (exponential backoff for event queue errors).
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# Zulip event queue long-poll timeout in seconds.
_POLL_TIMEOUT = 90


def _parse_chat_id(chat_id: str) -> tuple[str, str | None]:
    """Parse a chat_id into (stream_name, topic_or_None).

    Supported formats:
      "stream_name"          → (stream_name, None)
      "stream_name::topic"   → (stream_name, topic)
    """
    if "::" in chat_id:
        stream, _, topic = chat_id.partition("::")
        return stream.strip(), topic.strip() or None
    return chat_id.strip(), None


def check_zulip_requirements() -> bool:
    """Return True if the Zulip adapter can be used."""
    email = os.getenv("ZULIP_BOT_EMAIL", "")
    api_key = os.getenv("ZULIP_API_KEY", "")
    url = os.getenv("ZULIP_URL", "")
    if not email:
        logger.debug("Zulip: ZULIP_BOT_EMAIL not set")
        return False
    if not api_key:
        logger.debug("Zulip: ZULIP_API_KEY not set")
        return False
    if not url:
        logger.warning("Zulip: ZULIP_URL not set")
        return False
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Zulip: aiohttp not installed")
        return False


class ZulipAdapter(BasePlatformAdapter):
    """Gateway adapter for Zulip (self-hosted or cloud)."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.ZULIP)

        self._base_url: str = (
            config.extra.get("url", "")
            or os.getenv("ZULIP_URL", "")
        ).rstrip("/")
        self._bot_email: str = (
            config.extra.get("bot_email", "")
            or os.getenv("ZULIP_BOT_EMAIL", "")
        )
        self._api_key: str = config.token or os.getenv("ZULIP_API_KEY", "")
        self._default_topic: str = (
            config.extra.get("default_topic", "")
            or os.getenv("ZULIP_DEFAULT_TOPIC", "general")
        )

        self._bot_user_id: int = 0
        self._bot_name: str = ""

        # aiohttp session
        self._session: Any = None  # aiohttp.ClientSession
        self._poll_task: Optional[asyncio.Task] = None
        self._closing = False

        # Event queue state
        self._queue_id: str = ""
        self._last_event_id: int = -1

        # Dedup cache
        self._dedup = MessageDeduplicator()

        # Pending reaction-based approvals: message_id → session_key
        self._pending_approvals: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _auth(self) -> tuple[str, str]:
        """Return (email, api_key) tuple for HTTP Basic Auth."""
        return (self._bot_email, self._api_key)

    async def _subscribe_to_all_streams(self) -> None:
        """Subscribe the bot to all public streams.

        Zulip only delivers reaction events for streams the bot is subscribed to.
        This is called once at connect() time to ensure full reaction coverage.
        """
        data = await self._api_get("streams", {"include_public": "true", "include_subscribed": "false"})
        streams = data.get("streams", [])
        if not streams:
            logger.debug("Zulip: no unsubscribed public streams found")
            return
        subs = [{"name": s["name"]} for s in streams]
        result = await self._api_post(
            "users/me/subscriptions",
            {"subscriptions": json.dumps(subs)},
        )
        subscribed = list(result.get("subscribed", {}).values())
        already = list(result.get("already_subscribed", {}).values())
        count_new = sum(len(v) for v in subscribed)
        count_existing = sum(len(v) for v in already)
        logger.info(
            "Zulip: stream subscriptions — %d new, %d already subscribed",
            count_new, count_existing,
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _api_get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        """GET /api/v1/{path}."""
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.get(
                url, params=params or {},
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=_POLL_TIMEOUT + 10),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Zulip GET %s → %s: %s", path, resp.status, body[:200])
                    return {}
                data = await resp.json()
                if data.get("result") != "success":
                    logger.error("Zulip GET %s error: %s", path, data.get("msg", "unknown"))
                    return {}
                return data
        except aiohttp.ClientError as exc:
            logger.error("Zulip GET %s network error: %s", path, exc)
            return {}

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /api/v1/{path} with form-encoded body."""
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.post(
                url, data=payload,
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Zulip POST %s → %s: %s", path, resp.status, body[:200])
                    return {}
                data = await resp.json()
                if data.get("result") != "success":
                    logger.error("Zulip POST %s error: %s", path, data.get("msg", "unknown"))
                    return {}
                return data
        except aiohttp.ClientError as exc:
            logger.error("Zulip POST %s network error: %s", path, exc)
            return {}

    async def _api_delete(self, path: str, params: Dict[str, Any] | None = None) -> None:
        """DELETE /api/v1/{path}."""
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.delete(
                url, params=params or {},
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.debug("Zulip DELETE %s → %s: %s", path, resp.status, body[:100])
        except aiohttp.ClientError as exc:
            logger.debug("Zulip DELETE %s error: %s", path, exc)

    async def _api_patch(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """PATCH /api/v1/{path} with form-encoded body."""
        import aiohttp
        url = f"{self._base_url}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.patch(
                url, data=payload,
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Zulip PATCH %s → %s: %s", path, resp.status, body)
                    return {}
                data = await resp.json()
                if data.get("result") != "success":
                    logger.error("Zulip PATCH %s error: %s", path, data.get("msg", "unknown"))
                    return {}
                return data
        except aiohttp.ClientError as exc:
            logger.error("Zulip PATCH %s network error: %s", path, exc)
            return {}

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to Zulip and start the event-queue polling loop."""
        import aiohttp

        if not self._base_url or not self._bot_email or not self._api_key:
            logger.error("Zulip: URL, bot email, or API key not configured")
            return False

        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        self._closing = False

        # Verify credentials + fetch bot identity.
        me = await self._api_get("users/me")
        if not me or "user_id" not in me:
            logger.error("Zulip: failed to authenticate — check ZULIP_URL, ZULIP_BOT_EMAIL, ZULIP_API_KEY")
            await self._session.close()
            return False

        self._bot_user_id = me["user_id"]
        self._bot_name = me.get("full_name", self._bot_email.split("@")[0])
        logger.info(
            "Zulip: authenticated as %s (id=%s) on %s",
            self._bot_name, self._bot_user_id, self._base_url,
        )

        # Subscribe the bot to all public streams so reaction events are received.
        # (Zulip only delivers reaction events for streams the bot is subscribed to.)
        await self._subscribe_to_all_streams()

        # Register event queue and start polling.
        if not await self._register_queue():
            await self._session.close()
            return False

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Disconnect from Zulip."""
        self._closing = True

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass

        # Deregister event queue.
        if self._queue_id:
            await self._api_delete("events", {"queue_id": self._queue_id})
            self._queue_id = ""

        if self._session and not self._session.closed:
            await self._session.close()

        logger.info("Zulip: disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a stream (and topic) or DM.

        chat_id can be:
          - "stream_name"          → posts to stream with default topic
          - "stream_name::topic"   → posts to stream with specific topic
          - "@user_id" or "@email" → sends a PM

        metadata may contain:
          - "topic": override the topic for this message
          - "type": "direct" to send a PM
        """
        if not content:
            return SendResult(success=True)

        metadata = metadata or {}
        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, MAX_MESSAGE_LENGTH)

        last_id = None
        for chunk in chunks:
            result = await self._send_chunk(chat_id, chunk, metadata, reply_to)
            if not result.success:
                return result
            last_id = result.message_id

        return SendResult(success=True, message_id=last_id)

    async def _send_chunk(
        self,
        chat_id: str,
        chunk: str,
        metadata: Dict[str, Any],
        reply_to: Optional[str],
    ) -> SendResult:
        """Send a single chunk to Zulip."""
        # DM support: chat_id = "@user_id" or "@email"
        msg_type = metadata.get("type", "")
        if msg_type == "direct" or chat_id.startswith("@"):
            to = chat_id.lstrip("@")
            payload = {
                "type": "direct",
                "to": json.dumps([to]),
                "content": chunk,
            }
        else:
            stream_name, topic = _parse_chat_id(chat_id)
            # topic priority: metadata > thread_id > parsed > default
            final_topic = (
                metadata.get("topic")
                or metadata.get("thread_id")
                or topic
                or self._default_topic
            )
            payload = {
                "type": "stream",
                "to": stream_name,
                "topic": final_topic,
                "content": chunk,
            }

        data = await self._api_post("messages", payload)
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to post message")
        return SendResult(success=True, message_id=str(data["id"]))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return stream name and type."""
        if chat_id.startswith("@"):
            return {"name": chat_id, "type": "dm"}
        stream_name, topic = _parse_chat_id(chat_id)
        display = f"{stream_name}" + (f"/{topic}" if topic else "")
        return {"name": display, "type": "channel", "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send a typing indicator (Zulip supports this for DMs only; no-op for streams)."""
        # Zulip's /api/v1/typing only works for direct messages.
        # For stream messages there's no typing indicator, so we skip silently.
        pass

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit an existing message."""
        formatted = self.format_message(content)
        data = await self._api_patch(f"messages/{message_id}", {"content": formatted})
        if not data:
            return SendResult(success=False, error="Failed to edit message")
        return SendResult(success=True, message_id=message_id)

    def format_message(self, content: str) -> str:
        """Zulip uses its own flavour of Markdown — mostly compatible.

        Notable differences:
          - Zulip renders standard Markdown (bold, italic, code blocks, tables).
          - Images: Zulip auto-previews URLs that look like images; img markdown works.
          - No need to strip/transform anything for basic text.
        """
        return content

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send a typing indicator (Zulip supports this for DMs only; no-op for streams)."""
        # Zulip's /api/v1/typing only works for direct messages.
        # For stream messages there's no typing indicator, so we skip silently.
        pass

    # ------------------------------------------------------------------
    # Event queue
    # ------------------------------------------------------------------

    async def _register_queue(self) -> bool:
        """Register a Zulip event queue for message and reaction events."""
        import aiohttp
        url = f"{self._base_url}/api/v1/register"
        payload = {
            "event_types": json.dumps(["message", "reaction"]),
        }
        try:
            async with self._session.post(
                url, data=payload,
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if data.get("result") != "success":
                    logger.error("Zulip: failed to register event queue: %s", data.get("msg"))
                    return False
                self._queue_id = data["queue_id"]
                self._last_event_id = data.get("last_event_id", -1)
                logger.info("Zulip: registered event queue %s", self._queue_id)
                return True
        except Exception as exc:
            logger.error("Zulip: error registering event queue: %s", exc)
            return False

    async def _poll_loop(self) -> None:
        """Long-poll the Zulip event queue in a loop, reconnecting on error."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._poll_once()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except _QueueExpiredError:
                # Queue expired — re-register.
                if self._closing:
                    return
                logger.info("Zulip: event queue expired, re-registering")
                self._queue_id = ""
                self._last_event_id = -1
                if not await self._register_queue():
                    logger.error("Zulip: failed to re-register queue — retrying in %ss", delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _RECONNECT_MAX_DELAY)
                else:
                    delay = _RECONNECT_BASE_DELAY
            except Exception as exc:
                if self._closing:
                    return
                err_str = str(exc).lower()
                if "401" in err_str or "403" in err_str or "unauthorized" in err_str:
                    logger.error("Zulip: auth error in poll loop — stopping: %s", exc)
                    return
                logger.warning("Zulip: poll error: %s — retrying in %.0fs", exc, delay)
                import random
                jitter = delay * _RECONNECT_JITTER * random.random()
                await asyncio.sleep(delay + jitter)
                delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _poll_once(self) -> None:
        """Perform one long-poll request and dispatch events."""
        import aiohttp
        url = f"{self._base_url}/api/v1/events"
        params = {
            "queue_id": self._queue_id,
            "last_event_id": str(self._last_event_id),
            "dont_block": "false",
        }
        try:
            async with self._session.get(
                url, params=params,
                auth=aiohttp.BasicAuth(*self._auth()),
                timeout=aiohttp.ClientTimeout(total=_POLL_TIMEOUT + 15),
            ) as resp:
                if resp.status == 400:
                    body = await resp.json()
                    if body.get("code") == "BAD_EVENT_QUEUE_ID":
                        raise _QueueExpiredError()
                    logger.warning("Zulip: events API 400: %s", body.get("msg"))
                    return
                if resp.status >= 400:
                    body = await resp.text()
                    logger.warning("Zulip: events API %s: %s", resp.status, body[:200])
                    return
                data = await resp.json()
        except _QueueExpiredError:
            raise
        except asyncio.TimeoutError:
            # Normal timeout — just retry.
            return
        except aiohttp.ClientError as exc:
            raise exc

        if data.get("result") != "success":
            logger.warning("Zulip: events response not success: %s", data.get("msg"))
            return

        events = data.get("events", [])
        for event in events:
            eid = event.get("id", self._last_event_id)
            self._last_event_id = max(self._last_event_id, eid)
            if event.get("type") == "message":
                await self._handle_message_event(event)
            elif event.get("type") == "reaction":
                await self._handle_reaction_event(event)

    async def _handle_message_event(self, event: Dict[str, Any]) -> None:
        """Process a single Zulip message event."""
        msg = event.get("message", {})
        if not msg:
            return

        msg_id = str(msg.get("id", ""))
        if not msg_id:
            return

        # Ignore own messages.
        if msg.get("sender_id") == self._bot_user_id:
            return

        # Dedup.
        if self._dedup.is_duplicate(msg_id):
            return

        msg_type = msg.get("type", "stream")  # "stream" or "private"
        content = msg.get("content", "")
        sender_id = str(msg.get("sender_id", ""))
        sender_email = msg.get("sender_email", "")
        sender_name = msg.get("sender_full_name", sender_email)

        if msg_type == "stream":
            stream_name = msg.get("display_recipient", "")
            topic = msg.get("subject", self._default_topic)  # Zulip uses "subject" for topic
            chat_id = f"{stream_name}::{topic}"
            chat_type = "channel"

            # Mention-gating for stream messages.
            _require_mention_raw = os.getenv("ZULIP_REQUIRE_MENTION", "false")
            logger.debug("Zulip: ZULIP_REQUIRE_MENTION=%r", _require_mention_raw)
            require_mention = _require_mention_raw.lower() not in ("false", "0", "no")

            free_streams_raw = os.getenv("ZULIP_FREE_STREAMS", "")
            free_streams = {s.strip().lower() for s in free_streams_raw.split(",") if s.strip()}
            is_free_stream = stream_name.lower() in free_streams

            # Check for @mention (Zulip wraps mentions as @**Bot Name**)
            has_mention = (
                f"@**{self._bot_name}**" in content
                or f"@{self._bot_email}" in content
                or f"@**{self._bot_email}**" in content
            )

            if require_mention and not is_free_stream and not has_mention:
                logger.warning(
                    "Zulip: skipping stream message without @mention (stream=%s, topic=%s)",
                    stream_name, topic,
                )
                return

            # Strip @mention from content.
            if has_mention:
                content = re.sub(
                    re.escape(f"@**{self._bot_name}**"), "", content, flags=re.IGNORECASE
                ).strip()
                content = re.sub(
                    re.escape(f"@**{self._bot_email}**"), "", content, flags=re.IGNORECASE
                ).strip()

            # thread_id = topic (so executor can route to the right topic)
            thread_id: Optional[str] = topic

        else:
            # Private/DM message
            chat_id = f"@{sender_id}"
            chat_type = "dm"
            thread_id = None

        hermes_type = MessageType.TEXT
        if content.startswith("/"):
            hermes_type = MessageType.COMMAND

        source = self.build_source(
            chat_id=chat_id,
            chat_type=chat_type,
            user_id=sender_id or sender_email,
            user_name=sender_name,
            thread_id=thread_id,
        )

        from gateway.platforms.base import resolve_channel_prompt
        _channel_prompt = resolve_channel_prompt(self.config.extra, chat_id, None)

        msg_event = MessageEvent(
            text=content,
            message_type=hermes_type,
            source=source,
            raw_message=msg,
            message_id=msg_id,
            channel_prompt=_channel_prompt,
        )

        await self.handle_message(msg_event)


    async def _handle_reaction_event(self, event: Dict[str, Any]) -> None:
        """Handle a Zulip reaction event for interactive approvals.

        Emoji map:
          👍  (+1 / thumbs_up) → allow once
          🔒  (lock)          → allow session
          ♾️  (infinity)       → allow always
          👎  (-1 / thumbs_down) → deny
        """
        # Only handle "add" reactions (not removes)
        if event.get("op") != "add":
            return

        # Ignore reactions from the bot itself
        user_id = str(event.get("user_id", ""))
        if user_id and int(user_id) == self._bot_user_id:
            return

        msg_id = str(event.get("message_id", ""))

        logger.info(
            "Zulip reaction: msg_id=%s emoji=%r op=%s user=%s pending_keys=%s",
            msg_id, event.get("emoji_name"), event.get("op"), user_id,
            list(self._pending_approvals.keys()),
        )

        if not msg_id or msg_id not in self._pending_approvals:
            return

        emoji_name = event.get("emoji_name", "").lower()
        # Normalize common aliases
        choice_map = {
            "+1": "once",
            "thumbs_up": "once",
            "check_mark": "once",
            "heavy_check_mark": "once",
            "white_check_mark": "once",
            "check": "once",
            "lock": "session",
            "infinity": "always",
            "cross_mark": "deny",
            "x": "deny",
            "-1": "deny",
            "thumbs_down": "deny",
        }
        choice = choice_map.get(emoji_name)
        if not choice:
            return

        session_key = self._pending_approvals.pop(msg_id)
        try:
            from tools.approval import resolve_gateway_approval
            count = resolve_gateway_approval(session_key, choice)
            logger.info(
                "Zulip reaction resolved %d approval(s) for session %s (choice=%s, user=%s)",
                count, session_key, choice, user_id,
            )
        except Exception as exc:
            logger.error("Failed to resolve gateway approval from Zulip reaction: %s", exc)

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a reaction-based exec approval prompt.

        Posts a formatted message and waits for the user to react:
          ✅ — allow once
          🔒 — allow session
          ♾️  — allow always
          ❌ — deny
        """
        cmd_preview = command if len(command) <= 800 else command[:797] + "..."
        msg = (
            f"⚠️ **Command Approval Required**\n\n"
            f"**Reason:** {description}\n\n"
            f"```\n{cmd_preview}\n```\n\n"
            f"React to approve or deny:\n"
            f"- 👍 — allow once\n"
            f"- 🔒 — allow for this session\n"
            f"- ♾️ — always allow this pattern\n"
            f"- 👎 — deny"
        )
        result = await self.send(chat_id, msg, metadata=metadata)
        if result.success and result.message_id:
            self._pending_approvals[result.message_id] = session_key
        return result

class _QueueExpiredError(Exception):
    """Raised when Zulip reports BAD_EVENT_QUEUE_ID."""
