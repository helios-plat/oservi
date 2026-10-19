"""Tests for the Veya/3O 42-element inventory — oservi layer (3 engines).

Covers: dag_orchestrator (new), sequential_composer (enhanced: omodul 标准三件套),
agentic_loop (已有，注册复验).
"""

from __future__ import annotations

import asyncio


class TestDagOrchestrator:
    async def test_diamond_topological_order(self):
        from oservi.engines import DagOrchestratorEngine

        order: list[str] = []

        async def make(node: str):
            async def _run(node_id, input_data):
                await asyncio.sleep(0.01)
                order.append(node_id)
                return f"{node_id}:{input_data.get('x')}"

            return _run

        engine = DagOrchestratorEngine(
            nodes={
                "a": await make("a"),
                "b": await make("b"),
                "c": await make("c"),
                "d": await make("d"),
            },
            edges=[
                {"from": "a", "to": "b"},
                {"from": "a", "to": "c"},
                {"from": "b", "to": "d"},
                {"from": "c", "to": "d"},
            ],
            trigger={"on_demand": True},
            config={"max_parallel": 2},
            name="diamond",
        )
        r = await engine.run(input_data={"x": 1}, output_dir="/tmp")
        assert r["status"] == "completed"
        assert r["levels"] == 3
        assert r["results"]["d"] == "d:1"
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    async def test_cycle_rejected(self):
        from oservi.engines import DagOrchestratorEngine

        async def _run(node_id, input_data):
            return node_id

        engine = DagOrchestratorEngine(
            nodes={"a": _run, "b": _run},
            edges=[{"from": "a", "to": "b"}, {"from": "b", "to": "a"}],
            trigger={"on_demand": True},
            config={},
            name="cycle",
        )
        r = await engine.run()
        assert r["status"] == "failed"
        assert "cycle" in r["error"].lower()

    async def test_unknown_edge_node_rejected(self):
        from oservi.engines import DagOrchestratorEngine

        async def _run(node_id, input_data):
            return node_id

        engine = DagOrchestratorEngine(
            nodes={"a": _run},
            edges=[{"from": "a", "to": "ghost"}],
            trigger={"on_demand": True},
            config={},
            name="ghost",
        )
        r = await engine.run()
        assert r["status"] == "failed"

    async def test_omodul_standard_node(self):
        """节点以 omodul 标准三件套 (config, input_data, output_dir) 注入。"""
        from oservi.engines import DagOrchestratorEngine

        def omodul_step(config, input_data, output_dir):
            return {"status": "completed", "node": input_data.get("node_id")}

        engine = DagOrchestratorEngine(
            nodes={"s1": omodul_step},
            edges=[],
            trigger={"on_demand": True},
            config={},
            name="omodul-node",
        )
        r = await engine.run(input_data={})
        assert r["results"]["s1"]["node"] == "s1"

    async def test_error_isolation(self):
        from oservi.engines import DagOrchestratorEngine

        async def ok_node(node_id, input_data):
            return "ok"

        async def bad_node(node_id, input_data):
            raise RuntimeError("boom")

        engine = DagOrchestratorEngine(
            nodes={"good": ok_node, "bad": bad_node},
            edges=[],
            trigger={"on_demand": True},
            config={"max_parallel": 2},
            name="isolate",
        )
        r = await engine.run()
        assert r["status"] == "completed_with_errors"
        assert r["results"]["good"] == "ok"
        assert "boom" in r["errors"]["bad"]

    def test_health(self):
        from oservi.engines import DagOrchestratorEngine

        engine = DagOrchestratorEngine(
            nodes={"a": lambda: 1},
            edges=[],
            trigger={"on_demand": True},
            config={},
            name="h",
        )
        assert engine.health()["status"] == "healthy"
        assert engine.health()["details"]["nodes"] == 1

    def test_stop(self):
        from oservi.engines import DagOrchestratorEngine

        engine = DagOrchestratorEngine(
            nodes={"a": lambda: 1},
            edges=[],
            trigger={"on_demand": True},
            config={},
            name="s",
        )
        engine.stop()  # on_demand 引擎 stop 为 no-op，不应抛


class TestSequentialComposerEnhanced:
    async def test_omodul_standard_steps(self):
        """步骤以 omodul 标准三件套注入 —— 编排 omodul 协作的正道。"""
        from oservi.engines import SequentialComposerEngine

        seen: list[str] = []

        def std_step(config, input_data, output_dir):
            seen.append(input_data.get("step"))
            return {"status": "completed", "step": input_data.get("step")}

        async def std_step2(config, input_data, output_dir):
            seen.append(input_data.get("step"))
            return {"status": "completed", "step": input_data.get("step")}

        engine = SequentialComposerEngine(
            steps=[std_step, std_step2],
            router=None,
            trigger={"on_demand": True},
            config={"cfg": 1},
            name="std-omodul",
        )
        r = await engine.run(input_data={"step": "A"}, output_dir="/tmp")
        assert r["status"] == "completed"
        assert [x["step"] for x in r["results"]] == ["A", "A"]
        assert seen == ["A", "A"]

    async def test_legacy_steps_still_work(self):
        from oservi.engines import SequentialComposerEngine

        async def legacy_step(input_data=None, step_no=None):
            return {"status": "completed", "step_no": step_no}

        engine = SequentialComposerEngine(
            steps=[legacy_step],
            router=None,
            trigger={"on_demand": True},
            config={},
            name="legacy",
        )
        r = await engine.run(input_data={}, output_dir="/tmp")
        assert r["results"][0]["step_no"] == 0

    async def test_step_error_captured(self):
        from oservi.engines import SequentialComposerEngine

        def broken_step(config, input_data, output_dir):
            raise ValueError("kaput")

        engine = SequentialComposerEngine(
            steps=[broken_step],
            router=None,
            trigger={"on_demand": True},
            config={},
            name="broken",
        )
        r = await engine.run(input_data={}, output_dir="/tmp")
        assert r["status"] == "completed"  # 错误被捕获进结果
        assert "kaput" in str(r["results"][0]["error"])


class TestEngineRegistry:
    def test_all_three_registered(self):
        from oservi.engines import get_skeleton, list_skeletons

        names = list_skeletons()
        assert "dag_orchestrator" in names
        assert "sequential_composer" in names
        assert "agentic_loop" in names
        assert get_skeleton("dag_orchestrator").__name__ == "DagOrchestratorEngine"

    def test_agentic_loop_trigger_modes(self):
        from oservi.engines.agentic_loop import AgenticLoopEngine

        assert AgenticLoopEngine.trigger_mode == "on_demand"
        assert "tools" in AgenticLoopEngine.injection_points
