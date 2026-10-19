"""DAG Orchestrator Engine Skeleton.

机制 (固化):
- 按注入的 DAG（nodes + edges）做拓扑排序
- 同层节点并发执行（asyncio.gather，受 max_parallel 信号量限制）
- 检测环 → 拒绝启动并返回 failed
- 每节点结果按 node id 汇总，失败不中断同层其余节点（错误隔离）

业务 (注入):
- nodes: 1..n 个 task callable（omodul 标准签名或 (node_id, task) callable）
- edges: 0..n 条依赖边 {from, to}
- 节点执行语义：接收 (node_id, task_meta, context) → Any

红线对照:
- 红线 2 (机制/业务分离): 节点业务全靠注入
- 红线 3 (注入契约): nodes=omodul(1..n) / edges=layer4(0..n) / concurrency=config
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
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


class DagOrchestratorEngine(EngineSkeleton):
    """基于拓扑排序的并发 DAG 任务调度引擎.

    Example::

        engine = DagOrchestratorEngine(
            nodes={"a": step_a, "b": step_b, "c": step_c},
            edges=[{"from": "a", "to": "b"}, {"from": "a", "to": "c"}],
            trigger={"on_demand": True},
            config={"max_parallel": 2, "timeout_seconds": 120},
            name="multi-agent-pipeline",
        )
        result = asyncio.run(engine.run(input_data={"task": "..."}, output_dir="/tmp"))
    """

    injection_points: ClassVar[dict] = {
        "nodes": Injection(
            kind="omodul",
            cardinality="1..n",
            description="DAG 节点 callable（按 node id 键控）",
        ),
        "edges": Injection(
            kind="layer4",
            cardinality="0..n",
            description="依赖边 [{from, to}]",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        nodes: dict[str, Callable[..., Any]],
        edges: list[dict[str, str]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.nodes = dict(nodes)
        self.edges = list(edges or [])
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 拓扑排序（Kahn）
    # ------------------------------------------------------------------

    def _toposort(self) -> list[list[str]]:
        """返回按层分组的拓扑顺序（每层可并行）。检测到环抛 ValueError。"""
        node_ids = list(self.nodes)
        indeg = {n: 0 for n in node_ids}
        adj: dict[str, list[str]] = {n: [] for n in node_ids}
        for e in self.edges:
            frm, to = e.get("from", ""), e.get("to", "")
            if frm not in self.nodes or to not in self.nodes:
                raise ValueError(f"DAG edge references unknown node: {e}")
            if frm == to:
                raise ValueError(f"DAG self-loop edge: {e}")
            adj[frm].append(to)
            indeg[to] += 1

        levels: list[list[str]] = []
        remaining = set(node_ids)
        while remaining:
            ready = sorted(n for n in remaining if indeg[n] == 0)
            if not ready:
                raise ValueError(
                    f"DAG cycle detected among nodes: {sorted(remaining)[:10]}"
                )
            levels.append(ready)
            for n in ready:
                remaining.discard(n)
                for m in adj[n]:
                    indeg[m] -= 1
        return levels

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
        """按拓扑分层并发执行全部节点。

        Returns:
            {
                "status": "completed" | "failed",
                "levels": int,
                "results": {node_id: result},
                "errors": {node_id: error_str},
            }
        """
        input_data = input_data or {}
        max_parallel = max(1, int(self.config.get("max_parallel", 4)))
        timeout_seconds = self.config.get("timeout_seconds")

        try:
            levels = self._toposort()
        except ValueError as exc:
            self._last_error = str(exc)
            return {"status": "failed", "error": str(exc), "levels": 0, "results": {}, "errors": {}}

        results: dict[str, Any] = {}
        errors: dict[str, Any] = {}
        sem = asyncio.Semaphore(max_parallel)

        async def run_node(node_id: str) -> None:
            async with sem:
                task = self.nodes[node_id]
                try:
                    res = self._invoke(task, node_id, input_data, output_dir)
                    if inspect.isawaitable(res):
                        if timeout_seconds:
                            res = await asyncio.wait_for(res, timeout=timeout_seconds)
                        else:
                            res = await res
                    results[node_id] = res
                except Exception as exc:  # noqa: BLE001 - 错误隔离
                    errors[node_id] = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        "DagOrchestratorEngine '%s' node %s failed: %s",
                        self.name, node_id, errors[node_id],
                    )

        for level_no, level in enumerate(levels):
            if on_step:
                try:
                    on_step({"stage": "level", "level_no": level_no, "nodes": level})
                except Exception as cb_err:  # pragma: no cover
                    logger.warning("on_step callback failed: %s", cb_err)
            await asyncio.gather(*(run_node(n) for n in level))

        status = "completed" if not errors else "completed_with_errors"
        return {
            "status": status,
            "levels": len(levels),
            "results": results,
            "errors": errors,
        }

    def stop(self) -> None:
        """Signal the engine to stop (no-op for on_demand engines)."""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "nodes": len(self.nodes),
                "edges": len(self.edges),
                "last_error": self._last_error,
            },
        }

    @staticmethod
    def _invoke(
        task: Callable[..., Any],
        node_id: str,
        input_data: dict[str, Any],
        output_dir: Any,
    ) -> Any:
        """适配三种节点形态：
        1. async (node_id, input_data) — 引擎原生双参
        2. async (config, input_data, output_dir, *, on_step) — omodul 标准三件套
        3. async (**kwargs) — 通用 callable
        """
        try:
            sig = inspect.signature(task)
            positional = [
                p
                for p in sig.parameters.values()
                if p.kind
                in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
        except (TypeError, ValueError):  # pragma: no cover - builtin 等
            positional = []

        if len(positional) == 3:
            # omodul 标准三件套 (config, input_data, output_dir)
            return task(input_data, {"node_id": node_id}, output_dir)
        if len(positional) >= 2 and positional[0].name == "node_id":
            return task(node_id, input_data)
        return task(**{"node_id": node_id, "task": input_data})


register_skeleton("dag_orchestrator", DagOrchestratorEngine)
