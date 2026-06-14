"""AgenticLoopEngine tests — ≥10 tests (updated for new injection_points)."""

from __future__ import annotations

import asyncio
import inspect
import pytest

from oservi import AgenticLoopEngine, list_skeletons


# ===== Fake injectables =====

def fake_llm_caller(*, messages, tools, config):
    return {"content": "done", "cost_usd": 0.01}


async def async_llm_caller(*, messages, tools, config):
    return {"content": "async done", "cost_usd": 0.02}


def fake_turn_handler(*, messages, context):
    return {"messages": messages, "processed": True}


async def async_turn_handler(*, messages, context):
    return {"messages": messages, "processed": True, "async": True}


def fake_tool_a(*, query=""):
    return {"result": f"tool_a({query})"}


def fake_tool_b(*, query=""):
    return {"result": f"tool_b({query})"}


def fake_retrieval(*, query):
    return "retrieved context"


def _make_engine(**overrides):
    defaults = dict(
        llm_caller=fake_llm_caller,
        tools=[fake_tool_a],
        turn_handler=fake_turn_handler,
        retrieval=None,
        layer4_ui=None,
        trigger={"on_demand": True},
        config={},
        name="test-loop",
    )
    defaults.update(overrides)
    return AgenticLoopEngine(**defaults)


# ===== Tests =====

def test_agentic_loop_registered():
    assert "agentic_loop" in list_skeletons()


def test_agentic_loop_injection_points_keys():
    pts = AgenticLoopEngine.injection_points
    assert set(pts.keys()) == {"llm_caller", "tools", "retrieval", "turn_handler", "layer4_ui"}


def test_injection_point_llm_caller():
    pt = AgenticLoopEngine.injection_points["llm_caller"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_tools():
    pt = AgenticLoopEngine.injection_points["tools"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1..n"


def test_injection_point_retrieval():
    pt = AgenticLoopEngine.injection_points["retrieval"]
    assert pt.kind == "oskill"
    assert pt.cardinality == "0..1"


def test_injection_point_turn_handler():
    pt = AgenticLoopEngine.injection_points["turn_handler"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1"


def test_injection_point_layer4_ui():
    pt = AgenticLoopEngine.injection_points["layer4_ui"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "0..1"


def test_trigger_mode_is_on_demand():
    assert AgenticLoopEngine.trigger_mode == "on_demand"


def test_instantiates_with_required_args():
    engine = _make_engine()
    assert engine.name == "test-loop"
    assert engine.llm_caller is fake_llm_caller
    assert engine.turn_handler is fake_turn_handler


def test_tools_stored_by_name():
    engine = _make_engine(tools=[fake_tool_a, fake_tool_b])
    assert "fake_tool_a" in engine.tools
    assert "fake_tool_b" in engine.tools


def test_retrieval_none_by_default():
    engine = _make_engine()
    assert engine.retrieval is None


def test_layer4_ui_none_by_default():
    engine = _make_engine()
    assert engine.layer4_ui is None


def test_run_turn_sync_handlers():
    engine = _make_engine()
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "hi"}]))
    assert result["status"] == "completed"
    assert "turn_result" in result
    assert isinstance(result["cost_usd"], float)


def test_run_turn_returns_cost_usd():
    engine = _make_engine()
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "cost?"}]))
    assert result["cost_usd"] == pytest.approx(0.01)


def test_run_turn_with_async_llm_caller():
    engine = _make_engine(llm_caller=async_llm_caller)
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "async"}]))
    assert result["status"] == "completed"
    assert result["cost_usd"] == pytest.approx(0.02)


def test_run_turn_with_async_turn_handler():
    engine = _make_engine(turn_handler=async_turn_handler)
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "async handler"}]))
    assert result["status"] == "completed"


def test_run_turn_with_context():
    received_context = {}

    def handler_captures_context(*, messages, context):
        received_context.update(context)
        return {"messages": messages}

    engine = _make_engine(turn_handler=handler_captures_context)
    asyncio.run(engine.run_turn(
        messages=[{"role": "user", "content": "ctx"}],
        context={"session_id": "abc"},
    ))
    assert received_context.get("session_id") == "abc"


def test_run_turn_with_retrieval_set():
    engine = _make_engine(retrieval=fake_retrieval)
    assert engine.retrieval is fake_retrieval
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "retrieve"}]))
    assert result["status"] == "completed"


def test_health_stopped():
    engine = _make_engine()
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["running"] is False


def test_health_has_retrieval_false():
    engine = _make_engine()
    assert engine.health()["details"]["has_retrieval"] is False


def test_health_has_retrieval_true():
    engine = _make_engine(retrieval=fake_retrieval)
    assert engine.health()["details"]["has_retrieval"] is True


def test_health_has_layer4_ui():
    engine = _make_engine(layer4_ui=lambda: None)
    assert engine.health()["details"]["has_layer4_ui"] is True


def test_submit_task_enqueues():
    engine = _make_engine()
    engine.submit_task({"goal": "test"})
    assert engine._task_queue.qsize() == 1


def test_stop_sets_not_running():
    engine = _make_engine()
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_run_turn_llm_error_still_returns_completed():
    def bad_llm(*, messages, tools, config):
        raise RuntimeError("LLM down")

    engine = _make_engine(llm_caller=bad_llm)
    result = asyncio.run(engine.run_turn(messages=[{"role": "user", "content": "hi"}]))
    assert result["status"] == "completed"
    assert "error" in result["turn_result"]


def test_run_turn_turn_handler_error_uses_original_messages():
    def bad_handler(*, messages, context):
        raise ValueError("handler broken")

    engine = _make_engine(turn_handler=bad_handler)
    msgs = [{"role": "user", "content": "original"}]
    result = asyncio.run(engine.run_turn(messages=msgs))
    # Should still complete, llm_caller receives original messages
    assert result["status"] == "completed"
