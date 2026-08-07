"""omodul.goal_driven_loop — Goal-Driven 长程编排事务 (goal-driven 3O 内化)。

把 LoopX 长程内核 (GoalKernel/LongTaskDriver/QuotaTracker) 串成 goal-driven
的 while 循环: 子代理持续工作 → 主代理按 criteria(gate) 验证 → 不达标续跑/
失活重启 → 达标停机。

```python
while not kernel.goal.is_complete():
    run_round(engine_call)          # 子代理轮 (复用 LongTaskDriver)
    auto_verify(verifier)           # gate 自动验证 (criteria)
    stalled 检测 → restart           # 失活处理
```

分层: omodul (事务) — 复用 long_task_goal + long_task_driver + 事件存储。
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from omodul.long_task_goal import TODO_DONE, TODO_OPEN, GoalKernel

from oservi.long_task_driver import LongTaskDriver

# verifier 契约: (kernel, todo_id) -> (passed: bool, note: str)
# goal-driven 语义: 验证子代理产出的 todo/evidence; 不达标 → 重开 todo 继续循环
Verifier = Callable[[GoalKernel, str], Awaitable[tuple[bool, str]]]

# heartbeat 事件类型 (事件溯源持久化)
EVENT_HEARTBEAT = "heartbeat"


@dataclass
class LoopStats:
    rounds: int = 0
    gates_resolved: int = 0
    gates_rejected: int = 0
    restarts: int = 0
    started_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)

    def stalled(self, timeout_s: float) -> bool:
        return time.time() - self.last_heartbeat > timeout_s


@dataclass
class CompletionReport:
    goal_id: str
    completed: bool
    status: str                 # completed | max_rounds | max_hours | quota_paused
    rounds: int
    cost_usd: float = 0.0
    gates_resolved: int = 0
    gates_rejected: int = 0
    restarts: int = 0
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "goal_id": self.goal_id, "completed": self.completed,
            "status": self.status, "rounds": self.rounds,
            "cost_usd": round(self.cost_usd, 4),
            "gates_resolved": self.gates_resolved, "gates_rejected": self.gates_rejected,
            "restarts": self.restarts,
            "note": self.note,
        }


class GoalDrivenLoop:
    """Goal-Driven 循环编排: 工作 → 验证 → 续跑/重启 → 达标停机。"""

    def __init__(
        self,
        driver: LongTaskDriver,
        *,
        verifier: Verifier | None = None,
        heartbeat_timeout_s: float = 3600.0,
        max_rounds: int = 500,
        max_hours: float = 168.0,
        on_heartbeat: Callable[[str], None] | None = None,
    ) -> None:
        self.driver = driver
        self.verifier = verifier
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.max_rounds = max_rounds
        self.max_hours = max_hours
        self.on_heartbeat = on_heartbeat
        self.stats = LoopStats()

    # ── 心跳 (W3: 活跃度) ────────────────────────────────────────────
    async def _heartbeat(self) -> None:
        """每轮心跳: 事件溯源持久化 + 统计。"""
        self.stats.last_heartbeat = time.time()
        try:
            await self.driver.kernel.append(
                EVENT_HEARTBEAT, {"loop": str(uuid.uuid4())[:8]})
        except Exception:  # noqa: BLE001 - 心跳失败不阻断 (统计仍更新)
            import logging

            logging.getLogger("veya.goal_driven").warning(
                "heartbeat append failed", exc_info=True)
        if self.on_heartbeat is not None:
            self.on_heartbeat(self.driver.goal_id)

    def stalled(self) -> bool:
        """外部驱动模式: 距上次心跳超时 → 失活。"""
        return self.stats.stalled(self.heartbeat_timeout_s)

    async def restart_prompt(self) -> str:
        """失活重启: 恢复投影 + 生成续跑指令 (复用 wakeup_prompt)。"""
        self.stats.restarts += 1
        await self.driver.resume()
        return self.driver.wakeup_prompt()

    # ── 产出验证 (W2: criteria — goal-driven 语义) ───────────────────
    async def _verify_todos(self, todo_ids: list[str]) -> int:
        """验证本轮完成的 todo: verifier 判定 → 不达标重开 (循环继续)。"""
        kernel = self.driver.kernel
        goal = kernel.goal
        if goal is None or self.verifier is None:
            return 0
        reopened = 0
        for todo_id in todo_ids:
            todo = goal.todos.get(todo_id)
            if todo is None or todo.status != TODO_DONE:
                continue
            try:
                passed, note = await self.verifier(kernel, todo_id)
            except Exception as exc:  # noqa: BLE001 - 验证器失败按不达标
                passed, note = False, f"verifier error: {exc}"
            if passed:
                self.stats.gates_resolved += 1
            else:
                # 不达标 → 重开 todo (auto gate 随之重新 pending, 循环继续)
                await kernel.update_todo(todo_id, status=TODO_OPEN, note=note)
                self.stats.gates_rejected += 1
                reopened += 1
        return reopened

    # ── while 循环 (W1: 核心编排) ────────────────────────────────────
    async def run(
        self,
        engine_call: Callable[[str], Awaitable[dict[str, Any]]],
    ) -> CompletionReport:
        """Goal-Driven 主循环: 工作 → 验证 → 续跑, 直到达标或护栏触发。"""
        goal_id = self.driver.goal_id
        deadline = time.time() + self.max_hours * 3600

        while True:
            kernel = self.driver.kernel
            goal = kernel.goal
            if goal is None:
                return CompletionReport(goal_id, False, "no_goal",
                                        self.stats.rounds, note="goal 未初始化")
            if goal.is_complete():
                return CompletionReport(goal_id, True, "completed",
                                        self.stats.rounds,
                                        gates_resolved=self.stats.gates_resolved,
                                        gates_rejected=self.stats.gates_rejected,
                                        restarts=self.stats.restarts,
                                        note="全部 todo 完成且 gates 通过")
            if self.stats.rounds >= self.max_rounds:
                return CompletionReport(goal_id, False, "max_rounds",
                                        self.stats.rounds,
                                        gates_resolved=self.stats.gates_resolved,
                                        gates_rejected=self.stats.gates_rejected,
                                        note=f"达到轮数上限 {self.max_rounds}")
            if time.time() > deadline:
                return CompletionReport(goal_id, False, "max_hours",
                                        self.stats.rounds, note=f"达到时长上限 {self.max_hours}h")
            quota = goal.quota
            if quota.paused:
                return CompletionReport(goal_id, False, "quota_paused",
                                        self.stats.rounds, note="预算超支暂停")

            # 子代理工作一轮 (run_round 可能重建投影 → 每轮实时取 kernel)
            self.stats.rounds += 1
            await self._heartbeat()
            current = self.driver.kernel.goal
            completed_before = {
                t for t, todo in current.todos.items() if todo.status == TODO_DONE}
            outcome = await self.driver.run_round(engine_call)
            if outcome.get("status") == "paused_by_quota":
                return CompletionReport(goal_id, False, "quota_paused",
                                        self.stats.rounds, note="预算超支暂停")

            # 主代理验证 (criteria): 本轮新完成的 todo 产出
            current = self.driver.kernel.goal
            done_after = {
                t for t, todo in current.todos.items() if todo.status == TODO_DONE}
            await self._verify_todos(list(done_after - completed_before))


__all__ = ["CompletionReport", "GoalDrivenLoop", "LoopStats", "Verifier"]
