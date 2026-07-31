"""SagaComposerEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.saga_composer import SagaComposerEngine


# ===== Fake steps =====

def step_ok(*, input_data, step_no):
    return {**input_data, f"step_{step_no}": True}


async def async_step_ok(*, input_data, step_no):
    return {**input_data, f"async_step_{step_no}": True}


def step_failed_status(*, input_data, step_no):
    return {"status": "failed", "step_no": step_no}


def step_raises(*, input_data, step_no):
    raise ValueError("step error")


# ===== Fake compensations =====

def comp_a(*, input_data, compensation_no, reason):
    return {"comp": "a", "no": compensation_no}


def comp_b(*, input_data, compensation_no, reason):
    return {"comp": "b", "no": compensation_no}


async def async_comp(*, input_data, compensation_no, reason):
    return {"comp": "async", "no": compensation_no}


def comp_raises(*, input_data, compensation_no, reason):
    raise RuntimeError("comp error")


def _make_engine(**overrides):
    defaults = dict(
        steps=[step_ok],
        compensations=None,
        trigger={"on_demand": True},
        config={},
        name="test-saga",
    )
    defaults.update(overrides)
    return SagaComposerEngine(**defaults)


# ===== Tests =====

def test_saga_composer_registered():
    assert "saga_composer" in list_skeletons()


def test_injection_points_keys():
    pts = SagaComposerEngine.injection_points
    assert "steps" in pts
    assert "compensations" in pts


def test_injection_point_steps_kind():
    pt = SagaComposerEngine.injection_points["steps"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1..n"


def test_injection_point_compensations_kind():
    pt = SagaComposerEngine.injection_points["compensations"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "0..n"


def test_trigger_mode_on_demand():
    assert SagaComposerEngine.trigger_mode == "on_demand"


def test_instantiates_with_single_step():
    engine = _make_engine(steps=step_ok)  # single callable, not list
    assert len(engine.step_list) == 1


def test_instantiates_with_no_compensations():
    engine = _make_engine(compensations=None)
    assert engine.compensation_list == []


def test_run_all_steps_completed():
    engine = _make_engine(steps=[step_ok, step_ok])
    result = asyncio.run(engine.run(input_data={"x": 1}))
    assert result["status"] == "completed"
    assert len(result["results"]) == 2
    assert result["compensations_run"] == []


def test_run_async_step():
    engine = _make_engine(steps=[async_step_ok])
    result = asyncio.run(engine.run(input_data={"v": 42}))
    assert result["status"] == "completed"
    assert "async_step_0" in result["results"][0]


def test_run_mixed_sync_async_steps():
    engine = _make_engine(steps=[step_ok, async_step_ok])
    result = asyncio.run(engine.run(input_data={}))
    assert len(result["results"]) == 2
    assert "step_0" in result["results"][0]
    assert "async_step_1" in result["results"][1]


def test_run_failed_status_stops_forward_and_compensates():
    engine = _make_engine(
        steps=[step_ok, step_failed_status, step_ok],
        compensations=[comp_a],
    )
    result = asyncio.run(engine.run(input_data={}))
    assert result["status"] == "compensated"
    # forward stopped at the failing step → only 2 forward results
    assert len(result["results"]) == 2
    assert len(result["compensations_run"]) == 1


def test_run_step_raises_triggers_compensation():
    engine = _make_engine(steps=[step_raises], compensations=[comp_a])
    result = asyncio.run(engine.run(input_data={}))
    assert result["status"] == "compensated"
    assert result["results"][0]["status"] == "failed"
    assert len(result["compensations_run"]) == 1


def test_compensations_run_in_reverse_order():
    engine = _make_engine(
        steps=[step_failed_status],
        compensations=[comp_a, comp_b],
    )
    result = asyncio.run(engine.run(input_data={}))
    comps = result["compensations_run"]
    assert [c["comp"] for c in comps] == ["b", "a"]
    assert [c["no"] for c in comps] == [0, 1]


def test_async_compensation_supported():
    engine = _make_engine(steps=[step_failed_status], compensations=[async_comp])
    result = asyncio.run(engine.run(input_data={}))
    assert result["status"] == "compensated"
    assert result["compensations_run"][0]["comp"] == "async"


def test_compensation_error_captured_not_masking():
    engine = _make_engine(
        steps=[step_failed_status],
        compensations=[comp_a, comp_raises],
    )
    result = asyncio.run(engine.run(input_data={}))
    # reversed → comp_raises (no=0) errors, comp_a (no=1) ok
    assert result["status"] == "compensated"
    comps = result["compensations_run"]
    assert len(comps) == 2
    assert "error" in comps[0]
    assert comps[1]["comp"] == "a"


def test_failure_with_no_compensations_returns_failed():
    engine = _make_engine(steps=[step_raises], compensations=None)
    result = asyncio.run(engine.run(input_data={}))
    assert result["status"] == "failed"
    assert result["compensations_run"] == []


def test_on_step_callback_called_per_forward_step():
    called = []
    engine = _make_engine(steps=[step_ok, step_ok])
    asyncio.run(engine.run(input_data={}, on_step=called.append))
    assert len(called) == 2
    assert called[0]["step_no"] == 0
    assert called[1]["step_no"] == 1


def test_health_reports_counts():
    engine = _make_engine(steps=[step_ok, step_ok], compensations=[comp_a])
    h = engine.health()
    assert h["details"]["steps_count"] == 2
    assert h["details"]["compensations_count"] == 1


def test_stop_sets_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
