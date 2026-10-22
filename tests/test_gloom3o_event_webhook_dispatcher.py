"""EventWebhookDispatcherEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.event_webhook_dispatcher import EventWebhookDispatcherEngine


# ===== Fake subscribers =====

def sub_ok(*, event, payload):
    return {"got": event, "n": payload.get("n")}


async def async_sub_ok(*, event, payload):
    return {"got": event, "async": True}


def sub_raises(*, event, payload):
    raise ValueError("sub error")


def _make_engine(**overrides):
    defaults = dict(
        subscribers=[sub_ok],
        trigger={"on_signal": True},
        config={},
        name="test-dispatcher",
    )
    defaults.update(overrides)
    return EventWebhookDispatcherEngine(**defaults)


# ===== Tests =====

def test_event_webhook_dispatcher_registered():
    assert "event_webhook_dispatcher" in list_skeletons()


def test_injection_points_keys():
    pts = EventWebhookDispatcherEngine.injection_points
    assert "subscribers" in pts


def test_injection_point_subscribers_kind():
    pt = EventWebhookDispatcherEngine.injection_points["subscribers"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1..n"


def test_trigger_mode_on_signal():
    assert EventWebhookDispatcherEngine.trigger_mode == "on_signal"


def test_instantiates_with_single_subscriber():
    engine = _make_engine(subscribers=sub_ok)  # single callable, not list
    assert len(engine.subscriber_list) == 1


def test_run_is_non_blocking_and_sets_running():
    engine = _make_engine()
    engine.run()  # must return immediately
    assert engine._running is True


def test_dispatch_returns_completed_with_event():
    engine = _make_engine()
    result = asyncio.run(engine.dispatch("order.created", {"n": 1}))
    assert result["status"] == "completed"
    assert result["event"] == "order.created"


def test_dispatch_fans_out_to_all_subscribers():
    engine = _make_engine(subscribers=[sub_ok, sub_ok, sub_ok])
    result = asyncio.run(engine.dispatch("e", {"n": 5}))
    assert len(result["results"]) == 3
    assert all(r["n"] == 5 for r in result["results"])


def test_dispatch_async_subscriber():
    engine = _make_engine(subscribers=[async_sub_ok])
    result = asyncio.run(engine.dispatch("e", {}))
    assert result["results"][0]["async"] is True


def test_dispatch_mixed_sync_async():
    engine = _make_engine(subscribers=[sub_ok, async_sub_ok])
    result = asyncio.run(engine.dispatch("e", {"n": 1}))
    assert len(result["results"]) == 2


def test_subscriber_exception_captured_not_raised():
    engine = _make_engine(subscribers=[sub_ok, sub_raises])
    result = asyncio.run(engine.dispatch("e", {"n": 1}))
    assert result["status"] == "completed"
    assert len(result["results"]) == 1  # the healthy one
    assert len(result["errors"]) == 1
    assert result["errors"][0]["subscriber"] == "sub_raises"


def test_all_subscribers_fail_still_completed():
    engine = _make_engine(subscribers=[sub_raises, sub_raises])
    result = asyncio.run(engine.dispatch("e", {}))
    assert result["status"] == "completed"
    assert result["results"] == []
    assert len(result["errors"]) == 2


def test_dispatch_fans_out_concurrently():
    active = 0
    max_active = 0

    async def slow_sub(*, event, payload):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return "done"

    engine = _make_engine(subscribers=[slow_sub, slow_sub, slow_sub])
    result = asyncio.run(engine.dispatch("e", {}))
    assert max_active >= 2  # overlap proves concurrent fan-out
    assert len(result["results"]) == 3


def test_on_step_callback_per_subscriber():
    called = []
    engine = _make_engine(subscribers=[sub_ok, sub_raises])
    asyncio.run(engine.dispatch("e", {}, on_step=called.append))
    assert len(called) == 2
    assert {c["ok"] for c in called} == {True, False}


def test_health_reports_counts():
    engine = _make_engine(subscribers=[sub_ok, sub_ok])
    engine.run()
    asyncio.run(engine.dispatch("e", {}))
    h = engine.health()
    assert h["status"] == "healthy"
    assert h["details"]["subscribers_count"] == 2
    assert h["details"]["dispatch_count"] == 1


def test_stop_clears_running_and_sets_event():
    engine = _make_engine()
    engine.run()
    engine.stop()
    assert engine._running is False
    assert engine._stop_event.is_set()
