"""Subagent Orchestrator Engine Skeleton.

机制 (固化):
- on_demand 触发
- 调 subagent_runner omodul 执行子任务
- 可选 scheduler layer4 控制任务调度
- cost 通过返回值传播

业务 (注入):
- subagent_runner: omodul (1) — 执行子 agent 任务
- llm_caller: oprim (1) — LLM 调用
- scheduler: layer4 (0..1) — 可选任务调度器

红线对照:
- 红线 2 (机制/业务分离): 子任务业务全靠注入
- 红线 3 (注入契约): 3 注入点声明
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


class SubagentOrchestratorEngine(EngineSkeleton):
    """子代理编排引擎骨架 (on_demand).

    Example::

        engine = SubagentOrchestratorEngine(
            subagent_runner=omodul_run_subagent,
            llm_caller=oprim_llm,
            scheduler=None,
            trigger={"on_demand": True},
            config={"max_parallel": 4},
            name="orchestrator",
        )
        result = asyncio.run(engine.orchestrate(tasks=[...]))
    """

    injection_points: ClassVar[dict] = {
        "subagent_runner": Injection(
            kind="omodul",
            cardinality="1",
            description="run_subagent_task omodul",
        ),
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM call primitive",
        ),
        "scheduler": Injection(
            kind="layer4",
            cardinality="0..1",
            description="Optional task scheduler",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        subagent_runner: Callable[..., Any],
        llm_caller: Callable[..., Any],
        scheduler: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.subagent_runner = subagent_runner
        self.llm_caller = llm_caller
        self.scheduler = scheduler
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._task_count = 0
        self._total_cost_usd = 0.0
        self._last_error: str | None = None

    # ===== 生命周期 =====

    def run(self) -> None:
        """on_demand 引擎: run() 标记就绪, 不阻塞."""
        self._running = True
        logger.info(f"SubagentOrchestratorEngine '{self.name}' ready")

    def stop(self) -> None:
        self._running = False

    # ===== 核心 API =====

    async def orchestrate(
        self,
        tasks: list[dict[str, Any]],
        *,
        parallel: bool = False,
    ) -> dict[str, Any]:
        """Orchestrate a list of subagent tasks.

        Args:
            tasks: List of task dicts passed to subagent_runner.
            parallel: If True, run tasks concurrently (up to config.max_parallel).

        Returns:
            {"status": "completed", "results": [...], "total_cost_usd": float}
        """
        if parallel:
            max_par = self.config.get("max_parallel", 4)
            sem = asyncio.Semaphore(max_par)

            async def _run_one(task: dict[str, Any]) -> Any:
                async with sem:
                    return await self._run_subagent(task)

            results = await asyncio.gather(*[_run_one(t) for t in tasks], return_exceptions=True)
        else:
            results = []
            for task in tasks:
                result = await self._run_subagent(task)
                results.append(result)

        # propagate cost
        total_cost = 0.0
        for r in results:
            if isinstance(r, dict):
                total_cost += float(r.get("cost_usd", 0.0))
        self._total_cost_usd += total_cost
        self._task_count += len(tasks)

        return {
            "status": "completed",
            "results": [r if not isinstance(r, Exception) else {"error": str(r)} for r in results],
            "total_cost_usd": total_cost,
        }

    async def _run_subagent(self, task: dict[str, Any]) -> Any:
        """Invoke subagent_runner with iscoroutinefunction check."""
        try:
            if inspect.iscoroutinefunction(self.subagent_runner):
                result = await self.subagent_runner(task=task, config=self.config)
            else:
                raw = self.subagent_runner(task=task, config=self.config)
                if asyncio.iscoroutine(raw):
                    result = await raw
                else:
                    result = raw
            return result
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"SubagentOrchestratorEngine subagent_runner failed: {e}")
            return {"error": str(e)}

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "task_count": self._task_count,
                "total_cost_usd": self._total_cost_usd,
                "has_scheduler": self.scheduler is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("subagent_orchestrator", SubagentOrchestratorEngine)
