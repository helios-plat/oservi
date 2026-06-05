"""TriageEngine 测试 — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock

from oservice import TriageEngine, list_skeletons


# ===== Fake injectables =====


def fake_llm_fetch(*, config, max_events=50, **kw):
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


# ===== Tests =====


def test_triage_registered_as_skeleton():
    assert "triage" in list_skeletons()


def test_triage_engine_instantiates():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 60},
        config={},
        name="test-triage",
    )
    assert engine.name == "test-triage"
    assert engine._running is False


def test_triage_requires_trigger_key():
    with pytest.raises(ValueError, match="trigger must contain"):
        TriageEngine(
            llm_provider=fake_llm_fetch,
            trigger={"no_valid_key": True},
            config={},
            name="bad",
        )


def test_triage_filters_default_empty():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    assert engine.filters == []


def test_triage_filters_assigned():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        filters=[filter_keep_all, filter_high_only],
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    assert len(engine.filters) == 2


def test_triage_health_stopped():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["running"] is False


def test_triage_apply_filters_keep_all():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        filters=[filter_keep_all],
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    events = [{"entity_id": "x", "priority_score": 10}]
    assert engine._apply_filters(events) == events


def test_triage_apply_filters_drop_all():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        filters=[filter_drop_all],
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    events = [{"entity_id": "x", "priority_score": 10}]
    assert engine._apply_filters(events) == []


def test_triage_iterate_once_calls_callback():
    results = []
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={"dedup_bucket_seconds": 0},
        name="t",
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    assert len(results) > 0


def test_triage_dedup_suppresses_duplicate():
    results = []
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={"dedup_bucket_seconds": 9999},
        name="t",
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    first_count = len(results)
    asyncio.run(engine._iterate_once())
    # dedup_bucket_seconds=9999 → second iteration suppressed
    assert len(results) == first_count


def test_triage_min_score_filter():
    results = []
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={"min_score": 60, "dedup_bucket_seconds": 0},
        name="t",
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    for r in results:
        assert r.get("priority_score", 0) >= 60


def test_triage_empty_provider_no_callback():
    results = []
    engine = TriageEngine(
        llm_provider=fake_llm_empty,
        trigger={"on_interval": 10},
        config={},
        name="t",
        on_triage_result=results.extend,
    )
    asyncio.run(engine._iterate_once())
    assert results == []


def test_triage_stop_sets_running_false():
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_interval": 10},
        config={},
        name="t",
    )
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_triage_injection_points_declared():
    assert "llm_provider" in TriageEngine.injection_points
    assert "filters" in TriageEngine.injection_points
    assert TriageEngine.injection_points["llm_provider"].kind == "obase"
    assert TriageEngine.injection_points["filters"].kind == "layer4"
    assert TriageEngine.injection_points["filters"].cardinality == "0..n"


# ===== on_signal 触发模式测试 =====

fake_signal = {"entity_id": "s1", "message": "test signal", "priority_score": 60}


def test_triage_on_signal_trigger_accepted():
    """on_signal 触发模式应被装配器接受."""
    from oservice import assemble
    from oservice.manifest import ServiceManifest

    # assembler 校验 kind="obase" — 需把 __module__ 伪装成 obase.*
    def _obase_fake_llm(*, config, **kw):
        return []

    _obase_fake_llm.__module__ = "obase.test_utils"

    m = ServiceManifest(
        name="t-signal",
        skeleton="triage",
        inject={"llm_provider": [_obase_fake_llm], "filters": []},
        trigger={"on_signal": True},
        config={},
    )
    engine = assemble(m)
    assert isinstance(engine, TriageEngine)


def test_triage_run_non_blocking():
    """on_signal 模式 run() 应立即返回, _ready=True."""
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_signal": True},
        config={},
        name="t-signal",
    )
    engine.run()
    assert engine._ready is True


@pytest.mark.asyncio
async def test_triage_process_requires_run_first():
    """process() 调用前必须 run(), 否则 RuntimeError."""
    engine = TriageEngine(
        llm_provider=fake_llm_fetch,
        trigger={"on_signal": True},
        config={},
        name="t-signal",
    )
    with pytest.raises(RuntimeError, match="not started"):
        await engine.process(fake_signal)


def test_triage_no_valid_trigger_raises():
    """缺有效 trigger key → raise, 错误信息含 on_signal."""
    with pytest.raises(ValueError, match="on_signal"):
        TriageEngine(
            llm_provider=fake_llm_fetch,
            trigger={},
            config={},
            name="bad",
        )
