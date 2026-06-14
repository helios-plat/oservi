"""TriageEngine tests — ≥10 tests (updated for new injection_points)."""

from __future__ import annotations

import asyncio
import pytest

from oservi import TriageEngine, list_skeletons


# ===== Fake injectables =====

def fake_llm_caller(*, config, max_events=50, **kw):
    return [
        {"entity_id": "e1", "message": "CPU spike", "priority_score": 80},
        {"entity_id": "e2", "message": "disk warning", "priority_score": 40},
    ]


def fake_llm_score(*, config, events, mode="score", **kw):
    return [{"priority_score": 75, **e} for e in events]


def fake_llm_empty(*, config, **kw):
    return []


def filter_keep_all(*, event):
    return True


def filter_drop_all(*, event):
    return False


def filter_high_only(*, event):
    return event.get("priority_score", 0) >= 70


def _make_engine(**overrides):
    defaults = dict(
        llm_caller=fake_llm_caller,
        filters=None,
        trigger={"on_signal": True},
        config={},
        name="test-triage",
        on_triage_result=None,
    )
    defaults.update(overrides)
    return TriageEngine(**defaults)


# ===== Tests =====

def test_triage_registered_as_skeleton():
    assert "triage" in list_skeletons()


def test_triage_injection_points_keys():
    pts = TriageEngine.injection_points
    assert "llm_caller" in pts
    assert "filters" in pts
    assert len(pts) == 2


def test_injection_point_llm_caller_kind():
    pt = TriageEngine.injection_points["llm_caller"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_filters_kind():
    pt = TriageEngine.injection_points["filters"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "0..n"


def test_trigger_mode_is_on_signal():
    assert TriageEngine.trigger_mode == "on_signal"


def test_triage_engine_instantiates():
    engine = _make_engine()
    assert engine.name == "test-triage"
    assert engine._running is False


def test_triage_requires_valid_trigger_key():
    with pytest.raises(ValueError, match="trigger must contain"):
        TriageEngine(
            llm_caller=fake_llm_caller,
            trigger={"no_valid_key": True},
            config={},
            name="bad",
        )


def test_triage_no_trigger_raises():
    with pytest.raises(ValueError, match="on_signal"):
        TriageEngine(
            llm_caller=fake_llm_caller,
            trigger={},
            config={},
            name="bad",
        )


def test_triage_filters_default_empty():
    engine = _make_engine()
    assert engine.filters == []


def test_triage_filters_assigned():
    engine = _make_engine(filters=[filter_keep_all, filter_high_only])
    assert len(engine.filters) == 2


def test_triage_health_stopped():
    engine = _make_engine()
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["running"] is False


def test_triage_apply_filters_keep_all():
    engine = _make_engine(filters=[filter_keep_all])
    events = [{"entity_id": "x", "priority_score": 10}]
    assert engine._apply_filters(events) == events


def test_triage_apply_filters_drop_all():
    engine = _make_engine(filters=[filter_drop_all])
    events = [{"entity_id": "x", "priority_score": 10}]
    assert engine._apply_filters(events) == []


def test_triage_iterate_once_calls_callback():
    results = []
    engine = _make_engine(
        trigger={"on_interval": 10},
        config={"dedup_bucket_seconds": 0},
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    assert len(results) > 0


def test_triage_dedup_suppresses_duplicate():
    results = []
    engine = _make_engine(
        trigger={"on_interval": 10},
        config={"dedup_bucket_seconds": 9999},
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    first_count = len(results)
    asyncio.run(engine._iterate_once())
    assert len(results) == first_count


def test_triage_min_score_filter():
    results = []
    engine = _make_engine(
        trigger={"on_interval": 10},
        config={"min_score": 60, "dedup_bucket_seconds": 0},
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    for r in results:
        assert r.get("priority_score", 0) >= 60


def test_triage_empty_provider_no_callback():
    results = []
    engine = _make_engine(
        llm_caller=fake_llm_empty,
        trigger={"on_interval": 10},
        config={},
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    assert results == []


def test_triage_stop_sets_running_false():
    engine = _make_engine()
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_triage_run_non_blocking_on_signal():
    engine = _make_engine(trigger={"on_signal": True})
    engine.run()
    assert engine._ready is True
    assert engine._running is False


@pytest.mark.asyncio
async def test_triage_process_requires_run_first():
    engine = _make_engine(trigger={"on_signal": True})
    with pytest.raises(RuntimeError, match="not started"):
        await engine.process({"entity_id": "s1"})


@pytest.mark.asyncio
async def test_triage_process_returns_scored_event():
    engine = _make_engine(trigger={"on_signal": True})
    engine.run()
    result = await engine.process({"entity_id": "s1", "message": "test", "priority_score": 60})
    assert result is not None
    assert "priority_score" in result


@pytest.mark.asyncio
async def test_triage_process_filter_drops_event():
    engine = _make_engine(trigger={"on_signal": True}, filters=[filter_drop_all])
    engine.run()
    result = await engine.process({"entity_id": "s1", "priority_score": 90})
    assert result is None


def test_triage_assemble_validates_required_llm_caller():
    from oservi import assemble
    from oservi.manifest import ServiceManifest, ManifestValidationError

    m = ServiceManifest(
        name="t-no-llm",
        skeleton="triage",
        inject={"filters": []},
        trigger={"on_signal": True},
        config={},
    )
    with pytest.raises(ManifestValidationError, match="llm_caller"):
        assemble(m)


def test_triage_on_signal_assemble():
    from oservi import assemble
    from oservi.manifest import ServiceManifest

    def _oprim_llm(*, config, **kw):
        return []
    _oprim_llm.__module__ = "oprim.test_utils"

    m = ServiceManifest(
        name="t-signal",
        skeleton="triage",
        inject={"llm_caller": [_oprim_llm], "filters": []},
        trigger={"on_signal": True},
        config={},
    )
    engine = assemble(m)
    assert isinstance(engine, TriageEngine)
