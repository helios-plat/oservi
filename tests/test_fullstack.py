"""3O 范式全栈 — oservi 引擎测试 (元素 25/26)."""

from __future__ import annotations

import asyncio

import pytest

from obase.daemon_bus import InProcessDaemonBus
from oservi import DaemonEngine
from oservi.agentic_loop import AgenticLoopEngine as MinimalAgenticLoopEngine
from oservi.engine_manifest import InjectionKind, ServiceManifest, TriggerMode


def test_engine_manifest_reexports():
    """engine_manifest 提供规范引用入口 (单源收敛 oservi.manifest)。"""
    assert ServiceManifest is not None
    assert TriggerMode is not None
    assert InjectionKind is not None
    assert "oprim" in InjectionKind.__args__  # type: ignore[attr-defined]
    assert "layer4" in InjectionKind.__args__  # type: ignore[attr-defined]


def test_minimal_engine_requires_injections():
    manifest = ServiceManifest(name="demo", skeleton="agentic_loop", inject={}, trigger={})
    with pytest.raises(ValueError, match="Missing required injections"):
        MinimalAgenticLoopEngine(manifest)


class _FakeLLM:
    """按 LLMCaller 形状的假 LLM: 第一轮给工具意图, 第二轮给 final_answer。"""

    def __init__(self):
        self.turn = 0

    async def __call__(self, *, messages, tools=None, max_tokens=4096):
        self.turn += 1
        if self.turn == 1:
            return {"content": '{"tool_name": "shell_exec", "args": {"command": "echo hi"}}'}
        return {"content": '{"tool_name": "final_answer"}'}


class _FakePipeline:
    """假工具管线 (adapter 形态: 引擎只传 raw_text, 三件套由注入方绑定)。"""

    def __init__(self):
        self.calls: list[str] = []

    async def __call__(self, raw_text: str) -> dict:
        self.calls.append(raw_text)
        return {"findings": {"exit_code": 0, "stdout": "hi"}}


def _stop_evaluator(raw_text: str, turn: int) -> bool:
    return "final_answer" in raw_text


async def test_minimal_agentic_loop_full_run():
    fake_llm = _FakeLLM()
    fake_pipeline = _FakePipeline()
    manifest = ServiceManifest(
        name="demo-loop",
        skeleton="agentic_loop",
        inject={
            "llm_caller": [fake_llm],
            "tool_pipeline": [fake_pipeline],
            "stop_evaluator": [_stop_evaluator],
        },
        trigger={},
        config={"max_turns": 10},
    )
    engine = MinimalAgenticLoopEngine(manifest)
    result = await engine.run("audit this repo")
    history = result["final_history"]
    roles = [m["role"] for m in history]
    assert roles == ["user", "assistant", "tool", "assistant"]
    assert len(fake_pipeline.calls) == 1
    assert history[-1]["content"] == '{"tool_name": "final_answer"}'


async def test_daemon_engine_handle_once():
    bus = InProcessDaemonBus()
    manifest = ServiceManifest(name="demo-daemon", skeleton="x", inject={}, trigger={})
    engine = DaemonEngine(manifest, bus=bus, worker=lambda p: _async_echo(p))

    results = await bus.subscribe(engine.result_topic)
    out = await engine.handle_once({"n": 1})
    assert out == {"echo": {"n": 1}}
    assert await results.receive() == {"echo": {"n": 1}}


async def _async_echo(payload: dict) -> dict:
    return {"echo": payload}


async def test_daemon_engine_serve_and_stop():
    bus = InProcessDaemonBus()
    manifest = ServiceManifest(name="demo-daemon2", skeleton="x", inject={}, trigger={})
    engine = DaemonEngine(manifest, bus=bus, worker=_async_echo)

    results = await bus.subscribe(engine.result_topic)
    task = asyncio.create_task(engine.serve())
    await asyncio.sleep(0.02)  # 等待订阅建立 (队列总线, 消息不丢失)

    await bus.publish(engine.trigger_topic, {"n": 1})
    assert await results.receive() == {"echo": {"n": 1}}

    await engine.stop()
    await bus.publish(engine.trigger_topic, {"n": 2})  # 唤醒循环, 随后退出
    await asyncio.wait_for(task, timeout=2)
    assert engine._running is False
