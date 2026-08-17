"""ResilientAgenticLoop injection + L2 intervene + halt."""

from __future__ import annotations

import pytest
from omodul.execution_health_monitor import execution_health_monitor

from oservi import ResilientAgenticLoop, list_skeletons


def _echo_tool(*, text=""):
    return f"echo:{text}"


class _ScriptedLLM:
    def __init__(self, replies: list[dict]) -> None:
        self.replies = list(replies)

    def __call__(self, *, messages):
        if not self.replies:
            return {"final_answer": "done"}
        return self.replies.pop(0)


def _engine(llm, **kwargs) -> ResilientAgenticLoop:
    defaults = {
        "llm_caller": llm,
        "tools": [_echo_tool],
        "health_monitor": execution_health_monitor,
        "trigger": {"on_demand": True},
        "config": {"monitor": {"max_consecutive_errors": 99, "max_steps_per_turn": 25}},
        "name": "test-resilient",
    }
    defaults.update(kwargs)
    return ResilientAgenticLoop(**defaults)


def test_registered() -> None:
    assert "resilient_agentic_loop" in list_skeletons()


def test_injection_points() -> None:
    pts = ResilientAgenticLoop.injection_points
    assert set(pts) == {"llm_caller", "tools", "health_monitor", "context_compactor"}
    assert pts["health_monitor"].kind == "omodul"
    assert pts["context_compactor"].cardinality == "0..1"


@pytest.mark.asyncio
async def test_completes_without_tools() -> None:
    engine = _engine(_ScriptedLLM([{"final_answer": "ok"}]))
    rec = await engine.run_loop("hi")
    assert rec["status"] == "completed"
    assert rec["final"] == "ok"


@pytest.mark.asyncio
async def test_runs_tool_then_finishes() -> None:
    engine = _engine(
        _ScriptedLLM(
            [
                {"tool_name": "_echo_tool", "tool_args": {"text": "x"}},
                {"final_answer": "done"},
            ]
        )
    )
    rec = await engine.run_loop("hi")
    assert rec["status"] == "completed"
    assert any(m.get("role") == "tool" and "echo:x" in str(m.get("content")) for m in rec["messages"])


@pytest.mark.asyncio
async def test_l2_intervene_skips_tool() -> None:
    calls = {"n": 0}

    def counting_echo(*, text=""):
        calls["n"] += 1
        return text

    engine = _engine(
        _ScriptedLLM(
            [
                {"tool_name": "counting_echo", "tool_args": {"text": "a"}},
                {"tool_name": "counting_echo", "tool_args": {"text": "a"}},
                {"tool_name": "counting_echo", "tool_args": {"text": "a"}},
                {"tool_name": "counting_echo", "tool_args": {"text": "a"}},
                {"final_answer": "stopped-loop"},
            ]
        ),
        tools=[counting_echo],
    )
    rec = await engine.run_loop("hi")
    assert rec["status"] == "completed"
    assert calls["n"] < 4
    assert any("SYSTEM SHIELD" in str(m.get("content")) for m in rec["messages"])


@pytest.mark.asyncio
async def test_l3_halt(tmp_path) -> None:
    engine = _engine(
        _ScriptedLLM(
            [
                {"tool_name": "_echo_tool", "tool_args": {"text": "1"}},
                {"tool_name": "_echo_tool", "tool_args": {"text": "2"}},
                {"tool_name": "_echo_tool", "tool_args": {"text": "3"}},
            ]
        ),
        config={"monitor": {"max_steps_per_turn": 2, "max_consecutive_errors": 99}},
        output_dir=tmp_path,
    )
    rec = await engine.run_loop("hi")
    assert rec["status"] == "halted_by_breaker"
