"""oservi.long_task_driver — 长程编码任务驱动 (主仓装配层)。

把三个 3O 机制装配成 master 引擎循环可用的驱动:

  * ``obase.loop_event_store.AppendOnlyEventStore`` — 事件溯源 (真相源);
  * ``obase.loop_event_store.QuotaTracker`` — goal 级预算治理;
  * ``omodul.long_task_goal.GoalKernel`` — 投影状态机 (todo/gate/evidence)。

提供四类能力 (对应用户场景: 连续两天"写→测试→修→又写"):

  1. 每轮循环读写: ``pre_round()`` 读投影 (next_action + 配额检查) →
     宿主调引擎执行 → ``post_round()`` 写 todo/evidence/配额消耗;
  2. 跨进程恢复: ``resume()`` 从事件流重建投影 + 对齐运行时配额;
  3. 自动唤醒: ``wakeup_prompt()`` 生成 automata cron 任务的续跑 prompt;
  4. 一次性编排: ``run_round()`` 把 pre_round → engine → post_round 包成
     宿主每轮循环可直接调用的入口。

宿主接线示例 (master 循环每轮)::

    driver = open_long_task(Path("~/.veya/loops"), goal_id="g1", budget_usd=5.0)
    await driver.ensure_goal("重构结算模块")

    # 方式 A: 手动三轮 (每轮读写)
    ctx = await driver.pre_round()
    result = await engine.execute(ctx.prompt_suffix)   # 引擎实际动作
    await driver.post_round(result)                    # result 含 cost/todo/evidence

    # 方式 B: 一键编排 (pre_round → engine → post_round)
    result = await driver.run_round(engine.execute)

    # automata 自动唤醒 (跨天续跑)
    automata.register_cron_task("0 9 * * *", driver.wakeup_prompt())
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from obase.exceptions import BudgetExceeded
from obase.loop_event_store import AppendOnlyEventStore, QuotaTracker
from omodul.long_task_goal import GoalKernel, Todo


class LongTaskError(Exception):
    """长程任务驱动错误。"""


@dataclasses.dataclass
class RoundContext:
    """一轮循环开始时的投影上下文 (宿主可注入给 LLM)。"""

    next_action: Todo | None
    goal_summary: str
    quota_ok: bool
    remaining_usd: float | None
    prompt_suffix: str


@dataclasses.dataclass
class RoundOutcome:
    """一轮循环的结果 (宿主从引擎结果转写)。"""

    cost_usd: float = 0.0
    todo_updates: list[dict] = dataclasses.field(default_factory=list)
    evidence: list[dict] = dataclasses.field(default_factory=list)
    note: str = ""


class LongTaskDriver:
    """长程编码任务驱动: 事件溯源 + 投影状态机 + 配额治理的装配面。"""

    def __init__(
        self,
        store: AppendOnlyEventStore,
        *,
        goal_id: str,
        budget_usd: float | None = None,
    ) -> None:
        self._store = store
        self.goal_id = goal_id
        self._kernel = GoalKernel(store, goal_id=goal_id)
        self._quota = QuotaTracker(
            budget_usd=budget_usd or 0.0,
            goal_id=goal_id,
            store=store,
        )

    # ------------------------------------------------------------------
    # 访问器
    # ------------------------------------------------------------------

    @property
    def kernel(self) -> GoalKernel:
        return self._kernel

    @property
    def quota(self) -> QuotaTracker:
        return self._quota

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def ensure_goal(
        self,
        title: str,
        *,
        meta: dict[str, Any] | None = None,
    ) -> str:
        """首次运行: 创建 goal (幂等)。已存在则重建投影。

        事件流损坏时 rebuild 抛 GoalKernelError, 不静默 (避免向损坏的
        流继续追加事件掩盖问题)。
        """
        self._kernel.rebuild()  # 损坏即抛, 不静默
        if self._kernel.goal is None:
            await self._kernel.add_goal(
                title,
                budget_usd=self._quota.budget_usd or None,
                meta=meta,
            )
        self._sync_quota_from_projection()
        return self.goal_id

    async def pre_round(self) -> RoundContext:
        """一轮开始: 重建投影 + 配额检查。配额超支时 quota_ok=False。

        每轮全量 rebuild: 事件流是真相源 (含 QuotaTracker 写入的 quota_*
        事件), 增量 apply 会漏掉非 kernel 写入的事件导致 seq 错位 ——
        长程任务事件量小, rebuild 成本可忽略, 换取一致性。
        """
        self._kernel.rebuild()
        self._sync_quota_from_projection()
        quota_ok = True
        try:
            self._quota.check()
        except BudgetExceeded:
            quota_ok = False
        action = self._kernel.next_action()
        summary = self._summarize()
        suffix = ""
        if action is not None:
            suffix = f"\n[长程任务] 当前进度: {summary} | 下一步应执行: {action.id}: {action.title}"
        return RoundContext(
            next_action=action,
            goal_summary=summary,
            quota_ok=quota_ok,
            remaining_usd=self._quota.remaining_usd if quota_ok else 0.0,
            prompt_suffix=suffix,
        )

    async def post_round(self, outcome: RoundOutcome | dict[str, Any]) -> None:
        """一轮结束: 写 todo 更新 / 证据 / 配额消耗 (全部落事件流)。"""
        if isinstance(outcome, dict):
            outcome = self._parse_outcome(outcome)
        for upd in outcome.todo_updates:
            todo_id = upd["todo_id"]
            kwargs = {k: v for k, v in upd.items() if k != "todo_id"}
            await self._kernel.update_todo(todo_id, **kwargs)
        for ev in outcome.evidence:
            await self._kernel.append_evidence(
                ev["kind"],
                ev.get("detail"),
                todo_id=ev.get("todo_id"),
            )
        if outcome.cost_usd:
            await self._quota.record_usage(outcome.cost_usd, note=outcome.note)
        # 重建投影: quota_* 事件也在事件流里, 增量 apply 会漏
        self._kernel.rebuild()
        self._sync_quota_from_projection()

    async def run_round(
        self,
        engine_call: Callable[[str], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        """一键编排一轮: pre_round → engine_call → post_round。

        配额超支时跳过引擎执行, 返回 {"status": "paused_by_quota", ...}。
        """
        ctx = await self.pre_round()
        if not ctx.quota_ok:
            return {
                "status": "paused_by_quota",
                "goal_id": self.goal_id,
                "goal_summary": ctx.goal_summary,
                "remaining_usd": ctx.remaining_usd,
            }
        result = await engine_call(ctx.prompt_suffix)
        try:
            await self.post_round(result)
        except Exception as exc:
            # 本轮动作完成但配额超支: 返回 paused, 下一轮 pre_round 硬拦截
            if "budget" in str(exc).lower():
                return {
                    "status": "paused_by_quota",
                    "goal_id": self.goal_id,
                    "reason": str(exc),
                }
            raise
        return result

    # ------------------------------------------------------------------
    # 跨进程恢复 + 自动唤醒
    # ------------------------------------------------------------------

    async def resume(self) -> dict[str, Any]:
        """跨天/跨进程恢复: 从事件流重建投影 + 对齐运行时配额。

        返回续跑上下文 (供宿主/automata 唤醒任务使用)。
        """
        self._kernel = GoalKernel(self._store, goal_id=self.goal_id).rebuild()
        self._sync_quota_from_projection()
        return self._resume_context()

    def wakeup_prompt(self) -> str:
        """生成 automata cron 唤醒任务的续跑 prompt。

        唤醒时宿主把该 prompt 作为 task 传给 master/引擎, 自动续跑下一步。
        """
        ctx = self._resume_context()
        action = ctx["next_action"] or "无 (全部 todo 已完成)"
        return (
            f"[长程任务自动续跑] goal={self.goal_id} | {ctx['title'] or '未命名'}\n"
            f"当前进度: {ctx['goal_summary']}\n"
            f"下一步: {action} | 剩余配额 ${ctx['quota']['remaining']}\n"
            "请从事件流恢复状态, 继续执行下一步; 完成后更新 todo 状态并记录证据。"
        )

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------

    def _parse_outcome(self, result: dict[str, Any]) -> RoundOutcome:
        return RoundOutcome(
            cost_usd=float(result.get("cost_usd", 0.0)),
            todo_updates=list(result.get("todo_updates", [])),
            evidence=list(result.get("evidence", [])),
            note=str(result.get("note", "")),
        )

    def _sync_quota_from_projection(self) -> None:
        """从投影 QuotaView 对齐运行时 QuotaTracker (跨进程/跨轮恢复)。"""
        if self._kernel.goal is not None:
            qv = self._kernel.goal.quota
            self._quota.restore(
                qv.spent_usd,
                budget_usd=qv.budget_usd or None,
                paused=qv.paused,
            )

    def _summarize(self) -> str:
        goal = self._kernel.goal
        if goal is None:
            return "goal 未创建"
        return (
            f"todo {len([t for t in goal.todos.values() if t.status == 'done'])}/"
            f"{len(goal.todos)} done, "
            f"gates {len(goal.pending_gates())} open, "
            f"handoffs {len(goal.handoffs)}"
        )

    def _resume_context(self) -> dict[str, Any]:
        goal = self._kernel.goal
        action = self._kernel.next_action()
        return {
            "goal_id": self.goal_id,
            "title": goal.title if goal else None,
            "goal_summary": self._summarize(),
            "next_action": action.id if action else None,
            "open_todos": [t.id for t in self._kernel.open_todos()],
            "pending_gates": [g.id for g in self._kernel.pending_gates()],
            "quota": {
                "spent": self._quota.spent_usd,
                "budget": self._quota.budget_usd,
                "remaining": self._quota.remaining_usd,
                "paused": self._quota.paused,
            },
            "last_seq": self._kernel.last_seq,
            "integrity_ok": self._kernel.check_integrity().ok,
        }


def open_long_task(
    store_dir: str | Path,
    *,
    goal_id: str,
    budget_usd: float | None = None,
) -> LongTaskDriver:
    """装配工厂: store_dir/{goal_id}.jsonl 事件流 → LongTaskDriver。"""
    d = Path(store_dir)
    store = AppendOnlyEventStore(d / f"{goal_id}.jsonl")
    return LongTaskDriver(store, goal_id=goal_id, budget_usd=budget_usd)


__all__ = [
    "LongTaskDriver",
    "LongTaskError",
    "RoundContext",
    "RoundOutcome",
    "open_long_task",
]
