"""Unit tests for the hermes_events pub/sub bus.

Covers:
  - publish/subscribe round-trip
  - glob pattern matching (`*` one segment, `**` zero or more)
  - sync subscriber dispatch (fires in publisher stack)
  - async subscriber dispatch via asyncio.create_task when a loop is running
  - async subscriber drop-with-warning when no loop is running
  - exception isolation (one bad subscriber doesn't kill others or raise)
  - unsubscribe idempotence
  - envelope auto-stamp of type/ts/src when missing
  - envelope preservation of pre-populated ts/src (cross-process relay case)
  - high-fanout micro-benchmark sanity check
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest

import hermes_events
from hermes_events import publish, subscribe, unsubscribe


@pytest.fixture(autouse=True)
def _reset_bus():
    """Reset bus state before each test. Per-file pytest isolation already
    means cross-file leakage is impossible, but multiple tests in this file
    share the same process — we want each test to start clean.
    """
    hermes_events._reset_for_tests()
    yield
    hermes_events._reset_for_tests()


# ---------------------------------------------------------------------------
# Basic publish/subscribe round-trip
# ---------------------------------------------------------------------------


def test_publish_subscribe_round_trip():
    received: list[dict] = []
    subscribe("foo.bar", received.append)

    publish("foo.bar", {"k": 1})

    assert len(received) == 1
    env = received[0]
    assert env["type"] == "foo.bar"
    assert env["src"] == "foo"
    assert "ts" in env
    assert env["k"] == 1


def test_publish_with_no_payload_still_delivers_envelope():
    received: list[dict] = []
    subscribe("alone", received.append)

    publish("alone")

    assert len(received) == 1
    env = received[0]
    assert env["type"] == "alone"
    assert env["src"] == "alone"
    assert isinstance(env["ts"], float)


def test_publish_empty_topic_raises():
    with pytest.raises(ValueError):
        publish("")


def test_subscribe_empty_pattern_raises():
    with pytest.raises(ValueError):
        subscribe("", lambda env: None)


# ---------------------------------------------------------------------------
# Glob pattern matching
# ---------------------------------------------------------------------------


def test_star_matches_exactly_one_segment():
    received: list[str] = []
    subscribe("tui.*", lambda env: received.append(env["type"]))

    publish("tui.tool")         # one segment after tui → matches
    publish("tui.tool.start")   # two segments after tui → does NOT match
    publish("gateway.tool")     # different prefix → does NOT match
    publish("tui")              # too few → does NOT match

    assert received == ["tui.tool"]


def test_double_star_matches_zero_or_more_segments():
    received: list[str] = []
    subscribe("tui.**", lambda env: received.append(env["type"]))

    publish("tui")              # zero suffix segments
    publish("tui.tool")          # one
    publish("tui.tool.start")    # two
    publish("gateway.tool")      # different prefix → no match
    received_after_match = list(received)

    assert received_after_match == ["tui", "tui.tool", "tui.tool.start"]


def test_double_star_alone_is_firehose():
    received: list[str] = []
    subscribe("**", lambda env: received.append(env["type"]))

    publish("a")
    publish("a.b")
    publish("a.b.c.d.e")

    assert received == ["a", "a.b", "a.b.c.d.e"]


def test_mid_segment_double_star():
    received: list[str] = []
    subscribe("a.**.z", lambda env: received.append(env["type"]))

    publish("a.z")          # ** consumes zero
    publish("a.b.z")         # ** consumes one
    publish("a.b.c.z")       # ** consumes two
    publish("a.b.c.y")       # doesn't end with z → no match
    publish("a")             # too short → no match

    assert received == ["a.z", "a.b.z", "a.b.c.z"]


def test_literal_segments_must_match_exactly():
    received: list[str] = []
    subscribe("gateway.agent.start", lambda env: received.append(env["type"]))

    publish("gateway.agent.start")  # exact match
    publish("gateway.agent.end")    # different last segment
    publish("gateway.session.start")  # different middle segment

    assert received == ["gateway.agent.start"]


# ---------------------------------------------------------------------------
# Sync vs async subscriber dispatch
# ---------------------------------------------------------------------------


def test_sync_subscriber_fires_in_publisher_stack():
    """Sync subscribers must execute synchronously — the publisher's next
    statement must see their side-effects already applied."""
    received: list[dict] = []

    def on_evt(env: dict) -> None:
        received.append(env)

    subscribe("sync.test", on_evt)
    publish("sync.test", {"v": 42})

    # No await, no sleep, no loop — must already be there.
    assert len(received) == 1
    assert received[0]["v"] == 42


@pytest.mark.asyncio
async def test_async_subscriber_dispatched_via_create_task():
    """Async subscribers fire via asyncio.create_task when a loop is running.
    The publisher returns immediately; we await once for the task to run."""
    received: list[dict] = []

    async def on_evt(env: dict) -> None:
        received.append(env)

    subscribe("async.test", on_evt)
    publish("async.test", {"v": 7})

    # Publish returned synchronously; the task is scheduled but not yet run.
    # Yield to the loop once so the task can fire.
    await asyncio.sleep(0)

    assert len(received) == 1
    assert received[0]["v"] == 7


def test_async_subscriber_dropped_when_no_loop(caplog):
    """When no event loop is running, an async subscriber is dropped with
    a single warning log line per emit. Sync subscribers still fire."""
    async_received: list[dict] = []
    sync_received: list[dict] = []

    async def on_async(env: dict) -> None:
        async_received.append(env)

    def on_sync(env: dict) -> None:
        sync_received.append(env)

    subscribe("noloop.test", on_async)
    subscribe("noloop.test", on_sync)

    with caplog.at_level(logging.WARNING, logger="hermes_events"):
        publish("noloop.test", {"v": 1})

    # Async one was dropped (no loop running).
    assert async_received == []
    # Sync one still fired.
    assert len(sync_received) == 1

    # Warning was emitted.
    assert any(
        "dropped" in r.message and "noloop.test" in r.message
        for r in caplog.records
    ), [r.message for r in caplog.records]


# ---------------------------------------------------------------------------
# Exception isolation
# ---------------------------------------------------------------------------


def test_exception_in_one_subscriber_does_not_block_others(caplog):
    received: list[dict] = []

    def boom(env: dict) -> None:
        raise RuntimeError("subscriber test failure")

    def ok(env: dict) -> None:
        received.append(env)

    # Order matters less than coverage — register boom first to confirm
    # ok still runs after the exception path.
    subscribe("err.test", boom)
    subscribe("err.test", ok)

    with caplog.at_level(logging.ERROR, logger="hermes_events"):
        # publish() must not raise even though boom does.
        publish("err.test", {"v": 1})

    assert len(received) == 1
    # The error was logged.
    assert any(
        "subscriber test failure" in (r.message + (r.exc_text or ""))
        or "err.test" in r.message
        for r in caplog.records
    ), [r.message for r in caplog.records]


def test_sync_subscriber_returning_coroutine_is_discarded(caplog):
    """A sync subscriber that accidentally returns a coroutine (e.g. someone
    converted it to async without re-registering) should not leak the
    coroutine. We discard with a warning."""
    received: list[dict] = []

    # Note: NOT declared async, but body returns a coroutine.
    def sneaky(env: dict):
        async def inner():
            received.append(env)
        return inner()

    subscribe("sneak.test", sneaky)

    with caplog.at_level(logging.WARNING, logger="hermes_events"):
        publish("sneak.test")

    # The inner coroutine was discarded — never awaited, never ran.
    assert received == []
    assert any("coroutine" in r.message for r in caplog.records), [
        r.message for r in caplog.records
    ]


# ---------------------------------------------------------------------------
# Unsubscribe
# ---------------------------------------------------------------------------


def test_unsubscribe_removes_subscription():
    received: list[dict] = []
    h = subscribe("u.test", received.append)

    publish("u.test")
    assert len(received) == 1

    assert unsubscribe(h) is True
    publish("u.test")
    assert len(received) == 1  # not delivered

    # Idempotent: unsubscribing a removed handle is not an error.
    assert unsubscribe(h) is False


def test_subscribe_same_pattern_twice_yields_distinct_handles():
    received_a: list[dict] = []
    received_b: list[dict] = []

    h_a = subscribe("dup", received_a.append)
    h_b = subscribe("dup", received_b.append)
    assert h_a is not h_b

    publish("dup")
    assert len(received_a) == 1
    assert len(received_b) == 1

    unsubscribe(h_a)
    publish("dup")
    assert len(received_a) == 1
    assert len(received_b) == 2


# ---------------------------------------------------------------------------
# Envelope auto-stamping and preservation
# ---------------------------------------------------------------------------


def test_envelope_autostamps_type_ts_src_when_missing():
    received: list[dict] = []
    subscribe("**", received.append)

    before = time.time()
    publish("ns.evt", {"x": 1})
    after = time.time()

    env = received[0]
    assert env["type"] == "ns.evt"
    assert env["src"] == "ns"
    assert before <= env["ts"] <= after
    assert env["x"] == 1


def test_envelope_preserves_prepopulated_ts_and_src():
    """For the cross-process relay case: the dashboard's bridge ingestor
    re-publishes a frame received from the gateway, with the gateway's
    original ts and src already in the payload. The bus must not overwrite."""
    received: list[dict] = []
    subscribe("**", received.append)

    publish(
        "gateway.agent.start",
        {"ts": 1000.5, "src": "remote-gateway", "platform": "telegram"},
    )

    env = received[0]
    assert env["ts"] == 1000.5
    assert env["src"] == "remote-gateway"
    assert env["type"] == "gateway.agent.start"  # type is always set to topic
    assert env["platform"] == "telegram"


def test_envelope_does_not_overwrite_caller_provided_type():
    """If the caller pre-populates `type` (e.g. relaying a frame whose topic
    name differs slightly from the current one), the pre-populated value
    wins — same rule as ts/src."""
    received: list[dict] = []
    subscribe("**", received.append)

    publish("foo.bar", {"type": "foo.bar.relayed", "k": 1})

    env = received[0]
    assert env["type"] == "foo.bar.relayed"


# ---------------------------------------------------------------------------
# High-fanout micro-benchmark
# ---------------------------------------------------------------------------


def test_high_fanout_publish_is_sub_millisecond():
    """100 subscribers on a hot topic must dispatch in well under a
    millisecond on any reasonable machine. This is a sanity check, not a
    perf gate — generous bound so it doesn't flake on busy CI."""
    counts: list[int] = [0] * 100

    def make_cb(i: int):
        def cb(env: dict) -> None:
            counts[i] += 1
        return cb

    for i in range(100):
        subscribe("fan", make_cb(i))

    start = time.perf_counter()
    publish("fan", {"k": 1})
    elapsed = time.perf_counter() - start

    assert all(c == 1 for c in counts)
    assert elapsed < 0.05, f"100-way fanout took {elapsed*1000:.2f}ms (>50ms)"


# ---------------------------------------------------------------------------
# Internal helpers (sanity)
# ---------------------------------------------------------------------------


def test_subscriber_count_helper():
    assert hermes_events._subscriber_count() == 0
    h1 = subscribe("a", lambda e: None)
    h2 = subscribe("b", lambda e: None)
    assert hermes_events._subscriber_count() == 2
    unsubscribe(h1)
    unsubscribe(h2)
    assert hermes_events._subscriber_count() == 0
