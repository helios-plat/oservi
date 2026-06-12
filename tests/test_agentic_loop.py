"""AgenticLoopEngine 测试 — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import MagicMock

from oservi import AgenticLoopEngine, list_skeletons


# ===== Fake injectables =====


def fake_llm_final(*, task, context, history):
    return {"thought": "done", "final_answer": "task complete", "tool_name": ""}


def fake_llm_use_tool(*, task, context, history):
    if len(history) == 0:
        return {
            "thought": "check",
            "action": "inspect",
            "tool_name": "inspect_tool",
            "tool_args": {"name": "c1"},
        }
    return {"thought": "done", "final_answer": "inspection complete"}


def fake_llm_always_tool(*, task, context, history):
    return {"thought": "loop", "action": "tool", "tool_name": "inspect_tool", "tool_args": {}}


def fake_inspect_tool(*, name="x"):
    return {"container": name, "status": "running"}


def fake_knowledge(*, query):
    return "Runbook: restart the service if memory > 90%"


# ===== Tests =====


def test_agentic_loop_registered():
    assert "agentic_loop" in list_skeletons()


def test_agentic_loop_instantiates():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={},
        name="test-loop",
    )
    assert engine.name == "test-loop"
    assert "fake_inspect_tool" in engine.tools


def test_agentic_loop_injection_points():
    pts = AgenticLoopEngine.injection_points
    assert pts["llm_provider"].kind == "obase"
    assert pts["tools"].kind == "oprim"
    assert pts["tools"].cardinality == "1..n"
    assert pts["knowledge_retrieval"].kind == "oskill"
    assert pts["knowledge_retrieval"].cardinality == "0..1"


def test_execute_task_final_answer_immediate():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={"max_steps": 10},
        name="t",
    )
    result = asyncio.run(engine._execute_task({"goal": "check system"}))
    assert result["status"] == "completed"
    assert result["final_answer"] == "task complete"
    assert result["steps"] == 1


def test_execute_task_uses_tool():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_use_tool,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={"max_steps": 5},
        name="t",
    )
    result = asyncio.run(engine._execute_task({"goal": "inspect c1"}))
    assert result["status"] == "completed"
    assert result["steps"] >= 2
    assert any(h["tool_name"] == "inspect_tool" for h in result["history"])


def test_execute_task_max_steps_reached():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_always_tool,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={"max_steps": 3},
        name="t",
    )
    result = asyncio.run(engine._execute_task({"goal": "loop"}))
    assert result["status"] == "max_steps_reached"
    assert result["steps"] == 3


def test_execute_task_unknown_tool_returns_error_observation():
    def llm_bad_tool(*, task, context, history):
        if not history:
            return {"tool_name": "nonexistent_tool", "tool_args": {}}
        return {"final_answer": "done"}

    engine = AgenticLoopEngine(
        llm_provider=llm_bad_tool,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={"max_steps": 5},
        name="t",
    )
    result = asyncio.run(engine._execute_task({}))
    assert any("ERROR" in str(h.get("observation", "")) for h in result["history"])


def test_execute_task_with_knowledge_retrieval():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        knowledge_retrieval=fake_knowledge,
        trigger={"on_interval": 0},
        config={"max_steps": 5},
        name="t",
    )
    result = asyncio.run(engine._execute_task({"goal": "runbook lookup"}))
    assert result["status"] == "completed"


def test_health_stopped():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["has_knowledge_retrieval"] is False


def test_health_with_knowledge():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        knowledge_retrieval=fake_knowledge,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    assert engine.health()["details"]["has_knowledge_retrieval"] is True


def test_submit_task_enqueues():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    engine.submit_task({"goal": "test"})
    assert engine._task_queue.qsize() == 1


def test_stop_sets_not_running():
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_on_task_done_callback_called():
    done_results = []
    engine = AgenticLoopEngine(
        llm_provider=fake_llm_final,
        tools=[fake_inspect_tool],
        trigger={"on_interval": 0},
        config={"max_steps": 5},
        name="t",
        on_task_done=done_results.append,
    )
    result = asyncio.run(engine._execute_task({"goal": "x"}))
    # Manually invoke callback as run() is blocking
    engine.on_task_done(result)
    assert len(done_results) == 1
    assert done_results[0]["status"] == "completed"
