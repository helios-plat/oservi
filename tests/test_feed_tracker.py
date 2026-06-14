"""FeedTrackerEngine tests — ≥10 tests (rewritten engine)."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.feed_tracker import FeedTrackerEngine


# ===== Fake injectables =====

def fake_fetch_event(*, config):
    return [
        {"id": "ev1", "url": "https://example.com/feed1"},
        {"id": "ev2", "url": "https://example.com/feed2"},
    ]


async def async_fetch_event(*, config):
    return [{"id": "async_ev1", "url": "https://async.example.com/feed"}]


def fake_fetch_empty(*, config):
    return []


def fake_subscription(*, event):
    return {"status": "updated", "event_id": event.get("id")}


processed_events = []


def fake_ingest(*, event):
    processed_events.append(event)
    return {"ingested": event.get("id")}


async def async_ingest(*, event):
    return {"ingested_async": event.get("id")}


def _make_engine(**overrides):
    processed_events.clear()
    defaults = dict(
        fetch_event=fake_fetch_event,
        subscription=fake_subscription,
        ingest=None,
        trigger={"on_interval": 5},
        config={},
        name="test-feed-tracker",
    )
    defaults.update(overrides)
    return FeedTrackerEngine(**defaults)


# ===== Tests =====

def test_feed_tracker_registered():
    assert "feed_tracker" in list_skeletons()


def test_injection_points_keys():
    pts = FeedTrackerEngine.injection_points
    assert "fetch_event" in pts
    assert "subscription" in pts
    assert "ingest" in pts


def test_injection_point_fetch_event():
    pt = FeedTrackerEngine.injection_points["fetch_event"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_subscription():
    pt = FeedTrackerEngine.injection_points["subscription"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "1"


def test_injection_point_ingest():
    pt = FeedTrackerEngine.injection_points["ingest"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "0..1"


def test_trigger_mode_on_interval():
    assert FeedTrackerEngine.trigger_mode == "on_interval"


def test_instantiates():
    engine = _make_engine()
    assert engine.name == "test-feed-tracker"
    assert engine.interval_seconds == 5


def test_interval_from_trigger():
    engine = _make_engine(trigger={"on_interval": 30})
    assert engine.interval_seconds == 30


def test_ingest_none_by_default():
    engine = _make_engine()
    assert engine.ingest is None


def test_fetch_events_sync():
    engine = _make_engine()
    events = asyncio.run(engine._fetch_events())
    assert len(events) == 2
    assert events[0]["id"] == "ev1"


def test_fetch_events_async():
    engine = _make_engine(fetch_event=async_fetch_event)
    events = asyncio.run(engine._fetch_events())
    assert len(events) == 1
    assert events[0]["id"] == "async_ev1"


def test_fetch_events_empty():
    engine = _make_engine(fetch_event=fake_fetch_empty)
    events = asyncio.run(engine._fetch_events())
    assert events == []


def test_process_event_calls_subscription():
    seen = []
    def sub(*, event):
        seen.append(event)
    engine = _make_engine(subscription=sub)
    asyncio.run(engine._process_event({"id": "e1"}))
    assert len(seen) == 1
    assert seen[0]["id"] == "e1"


def test_process_event_calls_ingest_when_set():
    engine = _make_engine(ingest=fake_ingest)
    asyncio.run(engine._process_event({"id": "ingest_me"}))
    assert any(e["id"] == "ingest_me" for e in processed_events)


def test_process_event_skips_ingest_when_none():
    engine = _make_engine(ingest=None)
    # should not raise
    asyncio.run(engine._process_event({"id": "no_ingest"}))


def test_process_event_async_ingest():
    engine = _make_engine(ingest=async_ingest)
    # should not raise
    asyncio.run(engine._process_event({"id": "async_ev"}))


def test_health_stopped():
    engine = _make_engine()
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["running"] is False
    assert h["details"]["tick_count"] == 0


def test_health_has_ingest_false():
    engine = _make_engine()
    assert engine.health()["details"]["has_ingest"] is False


def test_health_has_ingest_true():
    engine = _make_engine(ingest=fake_ingest)
    assert engine.health()["details"]["has_ingest"] is True


def test_stop_sets_stop_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
    assert engine._running is False


def test_fetch_error_returns_empty_list():
    def bad_fetch(*, config):
        raise RuntimeError("network error")
    engine = _make_engine(fetch_event=bad_fetch)
    events = asyncio.run(engine._fetch_events())
    assert events == []
    assert engine._last_error is None  # error logged, not stored in _fetch_events
