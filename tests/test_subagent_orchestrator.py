"""SubagentOrchestratorEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine


# ===== Fake injectables =====

def fake_subagent_runner(*, task, config):
    return {"done": True, "task_id": task.get("id"), "cost_usd": 0.05}


async def async_subagent_runner(*, task, config):
    return {"done": True, "task_id": task.get("id"), "cost_usd": 0.10, "async": True}


def failing_subagent_runner(*, task, config):
    raise RuntimeError("subagent crashed")


def fake_llm_caller(*, messages, tools=None, config=None):
    return {"content": "orchestrated", "cost_usd": 0.01}


def fake_scheduler(*, tasks):
    return {"scheduled": len(tasks)}


def _make_engine(**overrides):
    defaults = dict(
        subagent_runner=fake_subagent_runner,
        llm_caller=fake_llm_caller,
        scheduler=None,
        trigger={"on_demand": True},
        config={},
        name="test-orchestrator",
    )
    defaults.update(overrides)
    return SubagentOrchestratorEngine(**defaults)


# ===== Tests =====

def test_subagent_orchestrator_registered():
    assert "subagent_orchestrator" in list_skeletons()


def test_injection_points_keys():
    pts = SubagentOrchestratorEngine.injection_points
    assert "subagent_runner" in pts
    assert "llm_caller" in pts
    assert "scheduler" in pts


def test_injection_point_subagent_runner():
    pt = SubagentOrchestratorEngine.injection_points["subagent_runner"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1"


def test_injection_point_llm_caller():
    pt = SubagentOrchestratorEngine.injection_points["llm_caller"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_scheduler():
    pt = SubagentOrchestratorEngine.injection_points["scheduler"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "0..1"


def test_trigger_mode_on_demand():
    assert SubagentOrchestratorEngine.trigger_mode == "on_demand"


def test_instantiates():
    engine = _make_engine()
    assert engine.name == "test-orchestrator"
    assert engine.scheduler is None


def test_scheduler_stored():
    engine = _make_engine(scheduler=fake_scheduler)
    assert engine.scheduler is fake_scheduler


def test_run_marks_running():
    engine = _make_engine()
    engine.run()
    assert engine._running is True


def test_stop_marks_not_running():
    engine = _make_engine()
    engine.run()
    engine.stop()
    assert engine._running is False


def test_orchestrate_single_task():
    engine = _make_engine()
    result = asyncio.run(engine.orchestrate([{"id": "t1"}]))
    assert result["status"] == "completed"
    assert len(result["results"]) == 1
    assert result["results"][0]["task_id"] == "t1"


def test_orchestrate_multiple_tasks_sequential():
    engine = _make_engine()
    tasks = [{"id": f"t{i}"} for i in range(3)]
    result = asyncio.run(engine.orchestrate(tasks))
    assert result["status"] == "completed"
    assert len(result["results"]) == 3


def test_orchestrate_cost_propagation():
    engine = _make_engine()
    tasks = [{"id": "t1"}, {"id": "t2"}]
    result = asyncio.run(engine.orchestrate(tasks))
    assert result["total_cost_usd"] == pytest.approx(0.10)  # 2 × 0.05


def test_orchestrate_parallel():
    engine = _make_engine(config={"max_parallel": 2})
    tasks = [{"id": f"t{i}"} for i in range(4)]
    result = asyncio.run(engine.orchestrate(tasks, parallel=True))
    assert result["status"] == "completed"
    assert len(result["results"]) == 4


def test_orchestrate_async_runner():
    engine = _make_engine(subagent_runner=async_subagent_runner)
    result = asyncio.run(engine.orchestrate([{"id": "async_t1"}]))
    assert result["results"][0].get("async") is True


def test_orchestrate_failing_runner_captured():
    engine = _make_engine(subagent_runner=failing_subagent_runner)
    result = asyncio.run(engine.orchestrate([{"id": "fail"}]))
    assert result["status"] == "completed"
    assert "error" in result["results"][0]


def test_task_count_increments():
    engine = _make_engine()
    asyncio.run(engine.orchestrate([{"id": "t1"}, {"id": "t2"}]))
    assert engine._task_count == 2


def test_total_cost_accumulates_across_calls():
    engine = _make_engine()
    asyncio.run(engine.orchestrate([{"id": "t1"}]))
    asyncio.run(engine.orchestrate([{"id": "t2"}]))
    assert engine._total_cost_usd == pytest.approx(0.10)


def test_health_reports():
    engine = _make_engine()
    engine.run()
    h = engine.health()
    assert h["status"] == "healthy"
    assert h["details"]["running"] is True
    assert h["details"]["has_scheduler"] is False
