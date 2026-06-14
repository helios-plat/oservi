"""SequentialComposerEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.sequential_composer import SequentialComposerEngine


# ===== Fake steps =====

def step_add_key(*, input_data, step_no):
    return {**input_data, f"step_{step_no}": True}


async def async_step(*, input_data, step_no):
    return {**input_data, f"async_step_{step_no}": True}


def step_raises(*, input_data, step_no):
    raise ValueError("step error")


def _make_engine(**overrides):
    defaults = dict(
        steps=[step_add_key],
        router=None,
        trigger={"on_demand": True},
        config={},
        name="test-composer",
    )
    defaults.update(overrides)
    return SequentialComposerEngine(**defaults)


# ===== Tests =====

def test_sequential_composer_registered():
    assert "sequential_composer" in list_skeletons()


def test_injection_points_keys():
    pts = SequentialComposerEngine.injection_points
    assert "steps" in pts
    assert "router" in pts


def test_injection_point_steps_kind():
    pt = SequentialComposerEngine.injection_points["steps"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1..n"


def test_injection_point_router_kind():
    pt = SequentialComposerEngine.injection_points["router"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "0..1"


def test_trigger_mode_on_demand():
    assert SequentialComposerEngine.trigger_mode == "on_demand"


def test_instantiates_with_single_step():
    engine = _make_engine(steps=step_add_key)  # single callable, not list
    assert len(engine.step_list) == 1


def test_instantiates_with_step_list():
    engine = _make_engine(steps=[step_add_key, step_add_key])
    assert len(engine.step_list) == 2


def test_run_single_step_returns_completed():
    engine = _make_engine()
    result = asyncio.run(engine.run(input_data={"x": 1}))
    assert result["status"] == "completed"
    assert len(result["results"]) == 1


def test_run_multiple_steps_all_executed():
    engine = _make_engine(steps=[step_add_key, step_add_key, step_add_key])
    result = asyncio.run(engine.run(input_data={}))
    assert len(result["results"]) == 3


def test_run_async_step():
    engine = _make_engine(steps=[async_step])
    result = asyncio.run(engine.run(input_data={"val": 42}))
    assert result["status"] == "completed"
    assert "async_step_0" in result["results"][0]


def test_run_mixed_sync_async_steps():
    engine = _make_engine(steps=[step_add_key, async_step])
    result = asyncio.run(engine.run(input_data={}))
    assert len(result["results"]) == 2
    assert "step_0" in result["results"][0]
    assert "async_step_1" in result["results"][1]


def test_run_step_error_captured_not_raised():
    engine = _make_engine(steps=[step_raises])
    result = asyncio.run(engine.run(input_data={}))
    assert result["status"] == "completed"
    assert "error" in result["results"][0]


def test_run_error_step_continues_remaining():
    engine = _make_engine(steps=[step_raises, step_add_key])
    result = asyncio.run(engine.run(input_data={}))
    assert len(result["results"]) == 2
    assert "error" in result["results"][0]
    assert "step_1" in result["results"][1]


def test_on_step_callback_called_per_step():
    called = []
    engine = _make_engine(steps=[step_add_key, step_add_key])
    asyncio.run(engine.run(input_data={}, on_step=called.append))
    assert len(called) == 2
    assert called[0]["step_no"] == 0
    assert called[1]["step_no"] == 1


def test_on_step_callback_none_does_not_raise():
    engine = _make_engine()
    result = asyncio.run(engine.run(input_data={}, on_step=None))
    assert result["status"] == "completed"


def test_run_empty_input_data():
    engine = _make_engine()
    result = asyncio.run(engine.run())
    assert result["status"] == "completed"


def test_router_stored():
    router = lambda: None  # noqa: E731
    engine = _make_engine(router=router)
    assert engine.router is router


def test_health_reports_steps_count():
    engine = _make_engine(steps=[step_add_key, step_add_key])
    h = engine.health()
    assert h["details"]["steps_count"] == 2
    assert h["details"]["has_router"] is False


def test_health_has_router_true():
    engine = _make_engine(router=lambda: None)
    assert engine.health()["details"]["has_router"] is True


def test_stop_sets_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
