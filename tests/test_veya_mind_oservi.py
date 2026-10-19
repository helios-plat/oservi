"""Tests for Veya 终极前沿认知能力 — oservi 层 (1 个).

Covers: world_simulation_loop（System-2 世界模型推演引擎）.
"""

from __future__ import annotations


def _planner(task_goal, *, llm_caller, num_simulations=3, rollout_depth=3):
    return {
        "best_plan": ["step1", "step2"],
        "confidence": 0.9,
        "evaluated_paths": num_simulations,
    }


async def _executor(action):
    return {"executed": action}


class _FakeLLM:
    def __call__(self, prompt, *, temperature=0.7):
        return "{}"


def _engine(**kw):
    from oservi.engines import WorldSimulationLoopEngine

    defaults = dict(
        mcts_planner=_planner,
        real_executor=_executor,
        trigger={"on_demand": True},
        config={"num_simulations": 3},
        name="sys2-test",
    )
    defaults.update(kw)
    return WorldSimulationLoopEngine(**defaults)


class TestWorldSimulationLoop:
    async def test_registered(self):
        from oservi.engines import list_skeletons

        assert "world_simulation_loop" in list_skeletons()

    async def test_simulate_then_execute(self):
        engine = _engine()
        r = await engine.run_simulated_turn("发布新版本", _FakeLLM())
        assert r["status"] == "completed"
        assert r["evaluated_simulations"] == 3
        assert r["executed_plan"] == ["step1", "step2"]
        assert len(r["execution_results"]) == 2
        assert r["execution_results"][0] == {"executed": "step1"}

    async def test_run_entrypoint(self):
        engine = _engine()
        r = await engine.run(input_data={"task_goal": "t", "llm_caller": _FakeLLM()})
        assert r["status"] == "completed"
        assert r["turn"]["executed_plan"] == ["step1", "step2"]
        assert r["turns"] == 1

    async def test_missing_task_goal(self):
        engine = _engine()
        r = await engine.run(input_data={})
        assert r["status"] == "failed" and "task_goal" in r["error"]

    async def test_executor_error_isolated(self):
        async def broken(action):
            raise RuntimeError("exec boom")

        engine = _engine(real_executor=broken)
        r = await engine.run_simulated_turn("x", _FakeLLM())
        assert r["status"] == "completed"
        assert all("boom" in str(res["error"]) for res in r["execution_results"])

    async def test_stop(self):
        engine = _engine()
        engine.stop()
        r = await engine.run_simulated_turn("x", _FakeLLM())
        assert r["status"] == "failed" and "stopped" in r["error"]

    def test_health(self):
        engine = _engine()
        h = engine.health()
        assert h["status"] == "healthy"
        assert h["details"]["has_mcts_planner"] is True
        assert h["details"]["has_real_executor"] is True

    def test_injection_points(self):
        engine = _engine()
        assert engine.injection_points["mcts_planner"].kind == "oskill"
        assert engine.injection_points["real_executor"].kind == "oprim"
