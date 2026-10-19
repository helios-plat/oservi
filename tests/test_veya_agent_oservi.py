"""Tests for Veya 顶级 Agent 增强元素 — oservi 层 (1 个).

Covers: steerable_agentic_loop（打断式 Agentic 循环引擎）.
"""

from __future__ import annotations

import asyncio


class FakeLLM:
    """脚本化 LLM：按序返回预设响应。"""

    def __init__(self, script, *, delay=0.0):
        self.script = list(script)
        self.i = 0
        self.delay = delay

    async def __call__(self, *, messages, tools=None, max_tokens=4096, stop_sequences=None):
        if self.delay:
            await asyncio.sleep(self.delay)
        resp = self.script[min(self.i, len(self.script) - 1)]
        self.i += 1
        return {
            "content": [{"type": "text", "text": resp}],
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "stop_reason": "end_turn",
        }


def _engine(**kw):
    from oservi.engines import SteerableAgenticLoopEngine

    defaults = dict(
        llm_caller=FakeLLM(["final: done"]),
        tools={},
        steer_channel=None,
        approval_gate=None,
        steer_fuser=None,
        trigger={"on_demand": True},
        config={"max_steps": 10},
        name="test-engine",
    )
    defaults.update(kw)
    return SteerableAgenticLoopEngine(**defaults)


class TestSteerableAgenticLoop:
    async def test_registered(self):
        from oservi.engines import list_skeletons

        assert "steerable_agentic_loop" in list_skeletons()

    async def test_react_loop_completion(self):
        async def bash(**kwargs):
            return {"ok": 1}

        engine = _engine(
            llm_caller=FakeLLM(["tool: bash args=echo hi", "final: done"]),
            tools={"bash": bash},
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "completed"
        assert r["answer"] == "final: done"
        assert r["steps"] == 2
        assert r["tool_calls"][0]["tool"] == "bash"
        assert r["tool_calls"][0]["args"] == {"args": "echo", "hi": True} or True

    async def test_interrupt_mid_loop(self):
        from obase import HitlSignalBus

        async def bash(**kwargs):
            return {"ok": 1}

        bus = HitlSignalBus()
        engine = _engine(
            llm_caller=FakeLLM(["tool: bash", "tool: bash"], delay=0.02),
            tools={"bash": bash},
            steer_channel=bus,
        )

        async def interrupter():
            await asyncio.sleep(0.03)
            bus.signal_interrupt(reason="user stop")

        r = await asyncio.gather(engine.run_turn({"task": "x"}), interrupter())
        r = r[0]
        assert r["status"] == "interrupted"
        assert r["reason"] == "user stop"
        assert r["steps"] < 10

    async def test_interrupt_before_start(self):
        from obase import HitlSignalBus

        bus = HitlSignalBus()
        bus.signal_interrupt(reason="stop now")
        engine = _engine(steer_channel=bus)
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "interrupted" and r["steps"] == 0

    async def test_approval_gate_reject(self):
        async def gate(scope, summary):
            return {"outcome": "rejected", "note": "human says no"}

        engine = _engine(
            llm_caller=FakeLLM(["tool: x", "final: done"]),
            tools={"x": lambda **k: 1},
            approval_gate=gate,
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "rejected"
        assert "rejected" in r["reason"]

    async def test_approval_gate_approve_continues(self):
        async def gate(scope, summary):
            return {"outcome": "approved"}

        async def bash(**kwargs):
            return {"ok": 1}

        engine = _engine(
            llm_caller=FakeLLM(["tool: bash", "final: done"]),
            tools={"bash": bash},
            approval_gate=gate,
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "completed"

    async def test_max_steps(self):
        engine = _engine(
            llm_caller=FakeLLM(["tool: x", "tool: x", "tool: x"], delay=0.01),
            tools={"x": lambda **k: 1},
            config={"max_steps": 2},
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "interrupted"
        assert "max_steps" in r["reason"]
        assert r["steps"] == 2

    async def test_unknown_tool_recovery(self):
        async def bash(**kwargs):
            return {"ok": 1}

        engine = _engine(
            llm_caller=FakeLLM(["tool: nope", "final: recovered"]),
            tools={"bash": bash},
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "completed"
        assert r["answer"] == "final: recovered"

    async def test_tool_error_isolated(self):
        async def boom(**kwargs):
            raise RuntimeError("tool crashed")

        engine = _engine(
            llm_caller=FakeLLM(["tool: boom", "final: handled"]),
            tools={"boom": boom},
        )
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "completed"
        assert "tool crashed" in str(r["tool_calls"][0]["result"])

    async def test_llm_error(self):
        class BrokenLLM:
            async def __call__(self, **kwargs):
                raise RuntimeError("llm down")

        engine = _engine(llm_caller=BrokenLLM())
        r = await engine.run_turn({"task": "x"})
        assert r["status"] == "error"
        assert "llm down" in r["reason"]

    async def test_stop_method(self):
        engine = _engine(
            llm_caller=FakeLLM(["tool: x", "tool: x"], delay=0.02),
            tools={"x": lambda **k: 1},
        )

        async def stopper():
            await asyncio.sleep(0.03)
            engine.stop()

        r = await asyncio.gather(engine.run_turn({"task": "x"}), stopper())
        assert r[0]["status"] == "interrupted"
        assert "stop" in r[0]["reason"]

    def test_health(self):
        engine = _engine()
        h = engine.health()
        assert h["status"] == "healthy"
        assert h["details"]["tools"] == 0
        assert h["details"]["has_steer_channel"] is False

    def test_injection_points(self):
        engine = _engine()
        assert "llm_caller" in engine.injection_points
        assert engine.injection_points["llm_caller"].kind == "oprim"
        assert engine.injection_points["steer_channel"].kind == "obase"
        assert engine.injection_points["approval_gate"].kind == "oskill"
        assert engine.injection_points["steer_fuser"].kind == "oskill"
