"""Zulip thread history tool.

Lets the agent read the Zulip thread it is currently operating in,
or any specified stream+topic, directly from the Zulip REST API.

This is the preferred way to restore session context when running inside
a Zulip thread — the Zulip message store is the authoritative history for
that thread, more complete than the local SQLite session files.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _get_session_env(name: str, default: str = "") -> str:
    """Read a session contextvar (gateway) or fall back to os.environ (CLI/cron)."""
    try:
        from gateway.session_context import get_session_env
        return get_session_env(name, default)
    except Exception:
        return os.environ.get(name, default)


def _zulip_api_get(
    base_url: str,
    bot_email: str,
    api_key: str,
    path: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Synchronous GET /api/v1/{path} via urllib (safe to call from a thread executor)."""
    url = f"{base_url.rstrip('/')}/api/v1/{path.lstrip('/')}"
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    credentials = base64.b64encode(f"{bot_email}:{api_key}".encode()).decode()
    req = urllib.request.Request(
        full_url,
        headers={
            "Authorization": f"Basic {credentials}",
            "User-Agent": "ZulipPython/0.9.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:300]
        return {"result": "error", "msg": f"HTTP {exc.code}: {body}"}
    except Exception as exc:
        return {"result": "error", "msg": str(exc)}
    return data


def zulip_read_thread(
    stream: str = "",
    topic: str = "",
    limit: int = 50,
) -> str:
    """Fetch messages from a Zulip stream+topic and return them as structured history.

    When called without arguments the tool reads the stream and topic from the
    current session context (the thread the agent is operating in right now).
    Pass explicit ``stream``/``topic`` to read a different thread.

    Args:
        stream: Zulip stream (channel) name. Defaults to the current session stream.
        topic:  Zulip topic name. Defaults to the current session topic.
        limit:  Maximum number of messages to fetch (1–100, default 50).
    """
    base_url = os.getenv("ZULIP_URL", "").rstrip("/")
    bot_email = os.getenv("ZULIP_BOT_EMAIL", "")
    api_key = os.getenv("ZULIP_API_KEY", "")

    if not base_url or not bot_email or not api_key:
        return json.dumps({
            "error": "Zulip not configured — ZULIP_URL, ZULIP_BOT_EMAIL, ZULIP_API_KEY required",
            "success": False,
        })

    # Resolve stream + topic from session context if not provided.
    if not stream or not topic:
        chat_id = _get_session_env("HERMES_SESSION_CHAT_ID", "")
        thread_id = _get_session_env("HERMES_SESSION_THREAD_ID", "")
        if "::" in chat_id:
            _s, _, _t = chat_id.partition("::")
            stream = stream or _s.strip()
            topic = topic or _t.strip() or thread_id
        else:
            stream = stream or chat_id.strip()
            topic = topic or thread_id

    if not stream:
        return json.dumps({"error": "Could not determine Zulip stream — pass stream= explicitly", "success": False})
    if not topic:
        return json.dumps({"error": "Could not determine Zulip topic — pass topic= explicitly", "success": False})

    limit = max(1, min(int(limit), 100))

    narrow = json.dumps([
        {"operator": "stream", "operand": stream},
        {"operator": "topic", "operand": topic},
    ])
    params: Dict[str, Any] = {
        "narrow": narrow,
        "anchor": "newest",
        "num_before": limit,
        "num_after": 0,
        "include_anchor": "true",
        "apply_markdown": "false",
    }

    data = _zulip_api_get(base_url, bot_email, api_key, "messages", params)
    if data.get("result") != "success":
        return json.dumps({"error": data.get("msg", "Zulip API error"), "success": False})

    raw_msgs: List[Dict[str, Any]] = data.get("messages", [])

    # Resolve the bot's own user ID so we can label its messages as "assistant".
    bot_user_id: Optional[int] = None
    try:
        me = _zulip_api_get(base_url, bot_email, api_key, "users/me", {})
        if me.get("result") == "success":
            bot_user_id = me.get("user", {}).get("user_id")
    except Exception:
        pass

    messages: List[Dict[str, str]] = []
    for m in raw_msgs:
        content = (m.get("content") or "").strip()
        if not content:
            continue
        sender_id = m.get("sender_id")
        sender_name = m.get("sender_full_name") or m.get("sender_email", "unknown")
        role = "assistant" if (bot_user_id is not None and sender_id == bot_user_id) else "user"
        messages.append({
            "role": role,
            "sender": sender_name,
            "content": content,
            "id": str(m.get("id", "")),
        })

    return json.dumps({
        "success": True,
        "stream": stream,
        "topic": topic,
        "count": len(messages),
        "messages": messages,
    }, ensure_ascii=False)


ZULIP_READ_THREAD_SCHEMA = {
    "name": "zulip_read_thread",
    "description": (
        "Read message history from the current (or a specified) Zulip stream+topic "
        "directly from the Zulip server. Use this to restore session context when "
        "starting or resuming a conversation in a Zulip thread — the Zulip thread "
        "is the authoritative source of what was said, more complete than local "
        "session files. Call with no arguments to read the thread you are in right now."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "stream": {
                "type": "string",
                "description": "Zulip stream (channel) name. Omit to use the current session stream.",
            },
            "topic": {
                "type": "string",
                "description": "Zulip topic name. Omit to use the current session topic.",
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of messages to fetch (1–100, default 50).",
                "default": 50,
            },
        },
        "required": [],
    },
}


def check_zulip_read_thread_requirements() -> bool:
    """Available when ZULIP_URL + ZULIP_BOT_EMAIL + ZULIP_API_KEY are set."""
    return bool(
        os.getenv("ZULIP_URL")
        and os.getenv("ZULIP_BOT_EMAIL")
        and os.getenv("ZULIP_API_KEY")
    )


from tools.registry import registry  # noqa: E402

registry.register(
    name="zulip_read_thread",
    toolset="zulip",
    schema=ZULIP_READ_THREAD_SCHEMA,
    handler=lambda args, **kw: zulip_read_thread(
        stream=args.get("stream", ""),
        topic=args.get("topic", ""),
        limit=args.get("limit", 50),
    ),
    check_fn=check_zulip_read_thread_requirements,
    emoji="💬",
)
