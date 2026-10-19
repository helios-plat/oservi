"""Tests for Veya P1/P2 顶级 Agent 功能复现 — oservi 层 (2 个).

Covers: swarm_orchestrator, realtime_media_loop.
"""

from __future__ import annotations

import asyncio


class TestSwarmOrchestrator:
    async def test_registered(self):
        from oservi.engines import list_skeletons

        assert "swarm_orchestrator" in list_skeletons()

    async def test_round_robin_dispatch(self):
        from oservi.engines import SwarmOrchestratorEngine

        async def w1(task_id, payload):
            return f"w1:{task_id}"

        async def w2(task_id, payload):
            return f"w2:{task_id}"

        engine = SwarmOrchestratorEngine(
            workers={"w1": w1, "w2": w2},
            trigger={"on_demand": True},
            config={"max_parallel": 2},
            name="swarm1",
        )
        r = await engine.run(input_data={"tasks": [{"id": "a"}, {"id": "b"}, {"id": "c"}]})
        assert r["status"] == "completed"
        assert len(r["results"]) == 3
        assert r["assigned"] == {"a": "w1", "b": "w2", "c": "w1"}

    async def test_dispatch_policy(self):
        from oservi.engines import SwarmOrchestratorEngine

        async def w(task_id, payload):
            return task_id

        async def policy(tasks, workers):
            return {t["id"]: "w2" for t in tasks}

        engine = SwarmOrchestratorEngine(
            workers={"w1": w, "w2": w},
            dispatch_policy=policy,
            trigger={"on_demand": True},
            config={},
            name="swarm2",
        )
        r = await engine.run(input_data={"tasks": [{"id": "a"}, {"id": "b"}]})
        assert all(v == "w2" for v in r["assigned"].values())

    async def test_error_isolation(self):
        from oservi.engines import SwarmOrchestratorEngine

        async def good(task_id, payload):
            return "ok"

        async def boom(task_id, payload):
            raise RuntimeError("x")

        engine = SwarmOrchestratorEngine(
            workers={"good": good, "bad": boom},
            trigger={"on_demand": True},
            config={},
            name="swarm3",
        )
        r = await engine.run(input_data={"tasks": [{"id": "a"}, {"id": "b"}]})
        assert r["status"] == "completed_with_errors"
        assert len(r["results"]) == 1
        assert any("x" in e for e in r["errors"].values())

    async def test_event_sink(self):
        from oservi.engines import SwarmOrchestratorEngine

        async def w(task_id, payload):
            return "ok"

        events: list[str] = []

        async def sink(event, payload):
            events.append(event)

        engine = SwarmOrchestratorEngine(
            workers={"w": w},
            event_sink=sink,
            trigger={"on_demand": True},
            config={},
            name="swarm4",
        )
        await engine.run(input_data={"tasks": [{"id": "a"}]})
        assert "swarm.dispatch" in events and "swarm.task_done" in events

    async def test_stop_before_run(self):
        from oservi.engines import SwarmOrchestratorEngine

        async def w(task_id, payload):
            return "ok"

        engine = SwarmOrchestratorEngine(
            workers={"w": w}, trigger={"on_demand": True}, config={}, name="swarm5"
        )
        engine.stop()
        r = await engine.run(input_data={"tasks": [{"id": "a"}]})
        assert "stopped" in r["errors"]["a"]

    async def test_no_tasks(self):
        from oservi.engines import SwarmOrchestratorEngine

        engine = SwarmOrchestratorEngine(
            workers={"w": lambda t, p: 1},
            trigger={"on_demand": True}, config={}, name="swarm6",
        )
        r = await engine.run(input_data={"tasks": []})
        assert r["status"] == "failed"

    def test_health(self):
        from oservi.engines import SwarmOrchestratorEngine

        engine = SwarmOrchestratorEngine(
            workers={"w": lambda t, p: 1},
            trigger={"on_demand": True}, config={}, name="swarm7",
        )
        assert engine.health()["status"] == "healthy"
        assert engine.health()["details"]["workers"] == 1


class _FrameSource:
    def __init__(self, frames, *, delay=0.0):
        self.frames = list(frames)
        self.delay = delay

    async def __aiter__(self):
        for f in self.frames:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield f


class TestRealtimeMediaLoop:
    async def test_registered(self):
        from oservi.engines import list_skeletons

        assert "realtime_media_loop" in list_skeletons()

    async def test_full_loop(self):
        from oservi.engines import RealtimeMediaLoopEngine

        async def processor(frame, ctx):
            return {"frame": frame, "iteration": ctx["iteration"]} if frame != "quiet" else None

        async def responder(event, ctx):
            return f"reply-{event['frame']}"

        outputs: list[str] = []

        async def sink(output, ctx):
            outputs.append(output)

        engine = RealtimeMediaLoopEngine(
            media_source=_FrameSource(["a", "quiet", "b"]),
            frame_processor=processor,
            responder=responder,
            media_sink=sink,
            trigger={"on_interval": 0.01},
            config={},
            name="media1",
        )
        r = await engine.run()
        assert r["status"] == "completed"
        assert r["iterations"] == 3
        assert outputs == ["reply-a", "reply-b"]

    async def test_stop_mid_loop(self):
        from oservi.engines import RealtimeMediaLoopEngine

        async def processor(frame, ctx):
            return {"frame": frame}

        engine = RealtimeMediaLoopEngine(
            media_source=_FrameSource([f"f{i}" for i in range(100)], delay=0.01),
            frame_processor=processor,
            trigger={"on_interval": 0.01},
            config={},
            name="media2",
        )

        async def stopper():
            await asyncio.sleep(0.03)
            engine.stop()

        r = await asyncio.gather(engine.run(), stopper())
        assert r[0]["status"] == "completed"
        assert r[0]["reason"] == "stopped"
        assert 0 < r[0]["iterations"] < 100

    async def test_max_iterations(self):
        from oservi.engines import RealtimeMediaLoopEngine

        async def processor(frame, ctx):
            return {"frame": frame}

        engine = RealtimeMediaLoopEngine(
            media_source=_FrameSource([f"f{i}" for i in range(10)]),
            frame_processor=processor,
            trigger={"on_interval": 0.001},
            config={"max_iterations": 3},
            name="media3",
        )
        r = await engine.run()
        assert r["iterations"] == 3

    async def test_next_frame_adapter(self):
        from oservi.engines import RealtimeMediaLoopEngine

        class SyncSource:
            def __init__(self, frames):
                self.frames = list(frames)

            def next_frame(self):
                return self.frames.pop(0) if self.frames else None

        async def processor(frame, ctx):
            return {"frame": frame}

        engine = RealtimeMediaLoopEngine(
            media_source=SyncSource(["x", "y"]),
            frame_processor=processor,
            trigger={"on_interval": 0.001},
            config={},
            name="media4",
        )
        r = await engine.run()
        assert r["iterations"] == 2

    async def test_source_error(self):
        from oservi.engines import RealtimeMediaLoopEngine

        class BrokenSource:
            async def __aiter__(self):
                yield "f1"
                raise RuntimeError("source died")

        async def processor(frame, ctx):
            return {"frame": frame}

        engine = RealtimeMediaLoopEngine(
            media_source=BrokenSource(),
            frame_processor=processor,
            trigger={"on_interval": 0.001},
            config={},
            name="media5",
        )
        r = await engine.run()
        assert r["status"] == "error" and "source" in r["reason"]

    async def test_invalid_source(self):
        from oservi.engines import RealtimeMediaLoopEngine

        async def processor(frame, ctx):
            return {}

        engine = RealtimeMediaLoopEngine(
            media_source="not-a-source",
            frame_processor=processor,
            trigger={"on_interval": 0.001},
            config={},
            name="media6",
        )
        with pytest_raises(TypeError):
            await engine.run()

    def test_health(self):
        from oservi.engines import RealtimeMediaLoopEngine

        async def processor(frame, ctx):
            return {}

        engine = RealtimeMediaLoopEngine(
            media_source=_FrameSource([]),
            frame_processor=processor,
            trigger={"on_interval": 0.001},
            config={},
            name="media7",
        )
        h = engine.health()
        assert h["status"] == "idle"
        assert h["details"]["has_responder"] is False


def pytest_raises(exc):
    import pytest

    return pytest.raises(exc)
