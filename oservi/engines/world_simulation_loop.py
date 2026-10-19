"""World Simulation Loop Engine Skeleton — System-2 世界模型推演引擎.

机制 (固化):
- Phase 1 离线推演：注入的 mcts_planner 在内部世界模型模拟 N 条分支，
  选出胜率最高的 Action 序列
- Phase 2 真实执行：逐动作经注入的 real_executor 下发真实环境
- 推演/执行结果聚合返回

业务 (注入):
- mcts_planner: 1 个推演规划器（oskill 注入面）
- real_executor: 1 个真实执行器（async (action) -> Any）

红线对照:
- 红线 2/3/4/5: 业务全靠注入；无状态骨架；不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import Any, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class WorldSimulationLoopEngine(EngineSkeleton):
    """先在内部世界模型中推演，再向真实环境下发动作的 System-2 引擎.

    Example::

        engine = WorldSimulationLoopEngine(
            mcts_planner=mcts_thought_planner,   # oskill 注入
            real_executor=async_executor,        # async (action) -> Any
            trigger={"on_demand": True},
            config={"num_simulations": 3, "rollout_depth": 3},
            name="system2-agent",
        )
        result = await engine.run_simulated_turn("发布新版本", llm_caller=llm)
    """

    injection_points: ClassVar[dict] = {
        "mcts_planner": Injection(
            kind="oskill",
            cardinality="1",
            description="MCTS 推演规划器（System-2 世界模型）",
        ),
        "real_executor": Injection(
            kind="oprim",
            cardinality="1",
            description="真实环境执行器 async (action) -> Any",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        mcts_planner: Callable[..., Any],
        real_executor: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.mcts_planner = mcts_planner
        self.real_executor = real_executor
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None
        self._turns: list[dict[str, Any]] = []

    async def run_simulated_turn(
        self, task_goal: str, llm_caller: Any
    ) -> dict[str, Any]:
        """先离线推演挑选最优 Action 序列，再向实际环境执行。"""
        if self._stop_event.is_set():
            return {"status": "failed", "error": "engine stopped"}

        # Phase 1: 离线世界模型推演
        plan_res = self.mcts_planner(
            task_goal,
            llm_caller=llm_caller,
            num_simulations=int(self.config.get("num_simulations", 3)),
            rollout_depth=int(self.config.get("rollout_depth", 3)),
        )
        best_plan = list(plan_res.get("best_plan") or [task_goal])

        # Phase 2: 真实环境下发执行
        execution_results: list[Any] = []
        for step_action in best_plan:
            try:
                result = self.real_executor(step_action)
                if inspect.isawaitable(result):
                    result = await result
                execution_results.append(result)
            except Exception as exc:  # noqa: BLE001 - 单步失败隔离
                execution_results.append({"error": f"{type(exc).__name__}: {exc}"})

        outcome = {
            "status": "completed",
            "evaluated_simulations": plan_res.get("evaluated_paths", 0),
            "executed_plan": best_plan,
            "execution_results": execution_results,
        }
        self._turns.append({"task_goal": task_goal, **outcome})
        return outcome

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """on_demand 入口：单任务推演 + 执行。

        input_data: {"task_goal": str, "llm_caller": Any}
        """
        input_data = input_data or {}
        task_goal = input_data.get("task_goal", "")
        llm_caller = input_data.get("llm_caller")
        if not task_goal:
            return {"status": "failed", "error": "task_goal required"}
        if on_step:
            on_step({"stage": "simulate", "task": task_goal})
        outcome = await self.run_simulated_turn(task_goal, llm_caller)
        return {"status": outcome["status"], "turn": outcome, "turns": len(self._turns)}

    def stop(self) -> None:
        """请求停止：后续回合不再执行。"""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "turns": len(self._turns),
                "has_mcts_planner": self.mcts_planner is not None,
                "has_real_executor": self.real_executor is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("world_simulation_loop", WorldSimulationLoopEngine)
