"""Tests for oservi.engines.graph_skills_agentic_loop."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from oservi import GraphSkillsAgenticLoopEngine, list_skeletons

# ============================================================================
# Fake injectables
# ============================================================================

def fake_llm_caller(prompt, *, temperature=0.0, **kwargs):
    return f"Response to: {str(prompt)[:50]}"


async def async_llm_caller(prompt, *, temperature=0.0, **kwargs):
    return f"Async response to: {str(prompt)[:50]}"


def fake_skills_registry():
    reg = MagicMock()
    reg.match_rules.return_value = ["Rule A: Follow PEP8.", "Rule B: Use type hints."]
    return reg


def fake_graph_parser(code, *, file_path=""):
    return {
        "status": "success",
        "nodes": [
            {"id": f"{file_path}::main", "type": "function", "name": "main"},
        ],
        "edges": [],
    }


def _make_engine(**overrides):
    defaults = dict(
        llm_caller=[fake_llm_caller],
        skills_registry=[fake_skills_registry()],
        graph_parse_op=None,
        trigger={"on_demand": True},
        config={},
        name="test-graph-skills",
    )
    defaults.update(overrides)
    return GraphSkillsAgenticLoopEngine(**defaults)


# ============================================================================
# Tests
# ============================================================================

class TestGraphSkillsAgenticLoop:
    def test_registered(self):
        assert "graph_skills_agentic_loop" in list_skeletons()

    def test_injection_points_keys(self):
        pts = GraphSkillsAgenticLoopEngine.injection_points
        assert set(pts.keys()) == {"llm_caller", "skills_registry", "graph_parse_op"}

    def test_injection_point_llm_caller(self):
        pt = GraphSkillsAgenticLoopEngine.injection_points["llm_caller"]
        assert pt.kind == "oprim"
        assert pt.cardinality == "1"

    def test_injection_point_skills_registry(self):
        pt = GraphSkillsAgenticLoopEngine.injection_points["skills_registry"]
        assert pt.kind == "obase"
        assert pt.cardinality == "1"

    def test_injection_point_graph_parse(self):
        pt = GraphSkillsAgenticLoopEngine.injection_points["graph_parse_op"]
        assert pt.kind == "oprim"
        assert pt.cardinality == "0..1"

    def test_trigger_mode(self):
        assert GraphSkillsAgenticLoopEngine.trigger_mode == "on_demand"

    def test_instantiates_with_required_args(self):
        engine = _make_engine()
        assert engine.name == "test-graph-skills"
        assert engine.llm_caller is fake_llm_caller

    def test_run_guided_loop_injects_rules(self):
        engine = _make_engine()
        result = asyncio.run(
            engine.run_guided_loop("Write a function", "main.py")
        )
        assert result["status"] == "completed"
        assert result["injected_rules_count"] == 2
        assert "Response to:" in result["response"]

    def test_run_guided_loop_with_async_llm(self):
        engine = _make_engine(llm_caller=[async_llm_caller])
        result = asyncio.run(
            engine.run_guided_loop("Async task", "lib.rs")
        )
        assert result["status"] == "completed"

    def test_run_guided_loop_with_graph_parser(self):
        engine = _make_engine(graph_parse_op=[fake_graph_parser])
        result = asyncio.run(
            engine.run_guided_loop("Analyze this code", "src/main.py")
        )
        assert result["status"] == "completed"
        assert result["graph_nodes"] >= 1

    def test_no_rules_matched(self):
        reg = MagicMock()
        reg.match_rules.return_value = []
        engine = _make_engine(skills_registry=[reg])
        result = asyncio.run(
            engine.run_guided_loop("task", "unknown.xyz")
        )
        assert result["injected_rules_count"] == 0

    def test_rules_in_prompt(self):
        engine = _make_engine()
        result = asyncio.run(
            engine.run_guided_loop("Add types", "contract.rs")
        )
        # The response should contain info based on the prompt
        assert isinstance(result["response"], str)
        assert len(result["response"]) > 0

    def test_llm_failure_still_completes(self):
        def broken_llm(prompt, **kwargs):
            raise RuntimeError("LLM crash")

        engine = _make_engine(llm_caller=[broken_llm])
        result = asyncio.run(
            engine.run_guided_loop("fail task", "test.py")
        )
        assert result["status"] == "completed"
        assert "LLM call failed" in result["response"]

    def test_health_stopped(self):
        engine = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"

    def test_health_has_details(self):
        engine = _make_engine(graph_parse_op=[fake_graph_parser])
        h = engine.health()
        assert h["details"]["has_graph_parser"] is True
        assert h["details"]["has_skills_registry"] is True

    def test_submit_task_enqueues(self):
        engine = _make_engine()
        engine.submit_task({"task": "test", "context_file": "main.py"})
        assert engine._task_queue.qsize() == 1

    def test_stop_sets_not_running(self):
        engine = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False

    def test_run_raises_if_already_running(self):
        engine = _make_engine()
        engine._running = True
        with pytest.raises(RuntimeError, match="already running"):
            engine.run()

    def test_graph_parser_none_by_default(self):
        engine = _make_engine()
        assert engine.graph_parse_op is None

    def test_response_is_string(self):
        engine = _make_engine()
        result = asyncio.run(
            engine.run_guided_loop("hello", "world.py")
        )
        assert isinstance(result["response"], str)
