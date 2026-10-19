"""Swarm Orchestrator Engine Skeleton — 群智编排引擎.

机制 (固化):
- 持有 worker 引擎表（注入），按策略分发任务
- dispatch_policy 注入（leader_worker 语义），缺省轮询
- 每 worker 执行结果聚合 + 失败隔离
- on_demand 触发；stop() 优雅停止

业务 (注入):
- workers: 1..n 个 worker 引擎 callable（async (task, payload) -> Any）
- dispatch_policy: 0..1 个分发策略 callable（async (tasks, workers) -> assignments）
- event_sink: 0..1 个事件汇（async (event, payload) -> None）

红线对照:
- 红线 2/3/4/5: 业务全靠注入；无状态骨架；不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class SwarmOrchestratorEngine(EngineSkeleton):
    """群智编排引擎骨架.

    Example::

        engine = SwarmOrchestratorEngine(
            workers={"research": research_worker, "write": write_worker},
            dispatch_policy=my_policy,      # async (tasks, worker_names) -> {task: worker}
            event_sink=log_sink,
            trigger={"on_demand": True},
            config={"max_parallel": 4, "timeout_seconds": 120},
            name="content-swarm",
        )
        result = asyncio.run(engine.run(input_data={"tasks": [...]}))
    """

    injection_points: ClassVar[dict] = {
        "workers": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Worker 引擎表（name -> async callable）",
        ),
        "dispatch_policy": Injection(
            kind="oskill",
            cardinality="0..1",
            description="分发策略：async (tasks, workers) -> assignments",
        ),
        "event_sink": Injection(
            kind="obase",
            cardinality="0..1",
            description="事件汇：async (event, payload) -> None",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        workers: dict[str, Callable[..., Any]],
        dispatch_policy: Callable[..., Any] | None = None,
        event_sink: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.workers = dict(workers)
        self.dispatch_policy = dispatch_policy
        self.event_sink = event_sink
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 事件
    # ------------------------------------------------------------------

    async def _emit(self, event: str, payload: dict[str, Any]) -> None:
        if self.event_sink is None:
            return
        try:
            result = self.event_sink(event, payload)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:  # pragma: no cover - 事件汇不致命
            logger.warning("swarm event sink failed: %s", exc)

    # ------------------------------------------------------------------
    # 分发
    # ------------------------------------------------------------------

    async def _assign(
        self, tasks: list[dict[str, Any]], payloads: dict[str, dict[str, Any]]
    ) -> dict[str, str]:
        worker_names = list(self.workers)
        if self.dispatch_policy is not None:
            try:
                assignments = await self.dispatch_policy(tasks, worker_names)
                if isinstance(assignments, dict):
                    return {
                        str(t.get("id")): str(assignments[t.get("id")])
                        for t in tasks
                        if t.get("id") in assignments
                    }
            except Exception as exc:  # pragma: no cover - 策略失败回退轮询
                logger.warning("dispatch_policy failed, falling back: %s", exc)
        return {
            str(t.get("id", f"task{i}")): worker_names[i % len(worker_names)]
            for i, t in enumerate(tasks)
        }

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """分发并执行任务批次。

        input_data: {"tasks": [{"id", "payload"}], "max_parallel": int|None}

        Returns:
            {"status": "completed"|"completed_with_errors",
             "assigned": {task: worker}, "results": {task: Any},
             "errors": {task: str}}
        """
        input_data = input_data or {}
        tasks = input_data.get("tasks") or []
        if not tasks:
            return {"status": "failed", "error": "no tasks", "assigned": {}, "results": {}, "errors": {}}
        if not self.workers:
            return {"status": "failed", "error": "no workers", "assigned": {}, "results": {}, "errors": {}}

        payloads = {
            str(t.get("id", f"task{i}")): t.get("payload", {})
            for i, t in enumerate(tasks)
        }
        assignments = await self._assign(tasks, payloads)
        await self._emit("swarm.dispatch", {"assignments": assignments})

        max_parallel = max(1, int(input_data.get("max_parallel") or self.config.get("max_parallel", 4)))
        timeout_seconds = self.config.get("timeout_seconds")
        sem = asyncio.Semaphore(max_parallel)
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}

        async def run_task(task_id: str, worker_name: str) -> None:
            async with sem:
                if self._stop_event.is_set():
                    errors[task_id] = "stopped"
                    return
                worker = self.workers[worker_name]
                try:
                    if on_step:
                        on_step({"task": task_id, "worker": worker_name, "status": "running"})
                    coro = worker(task_id, payloads.get(task_id, {}))
                    if inspect.isawaitable(coro):
                        if timeout_seconds:
                            coro = asyncio.wait_for(coro, timeout=timeout_seconds)
                        result = await coro
                    else:
                        result = coro
                    results[task_id] = result
                    await self._emit("swarm.task_done", {"task": task_id, "worker": worker_name})
                except Exception as exc:  # noqa: BLE001 - 错误隔离
                    errors[task_id] = f"{type(exc).__name__}: {exc}"
                    logger.warning("swarm task %s on %s failed: %s", task_id, worker_name, exc)

        await asyncio.gather(
            *(run_task(tid, worker) for tid, worker in assignments.items())
        )

        status = "completed" if not errors else "completed_with_errors"
        return {
            "status": status,
            "assigned": assignments,
            "results": results,
            "errors": errors,
        }

    def stop(self) -> None:
        """请求停止：后续任务批次不再分发（进行中的任务完成）。"""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "workers": len(self.workers),
                "has_dispatch_policy": self.dispatch_policy is not None,
                "has_event_sink": self.event_sink is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("swarm_orchestrator", SwarmOrchestratorEngine)
