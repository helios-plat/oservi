"""Mechanism Game Loop Engine Skeleton — 基于博弈竞价机制的多 Agent 调度引擎.

机制 (固化):
- 每轮任务发起竞价：收集各 Agent 的 bid（注入 bid_func）
- 最低竞价者胜出（拍卖机制语义，价低且能力匹配度高者得执行权）
- 竞价账本写入（注入 auction_ledger_op）
- 胜出者执行任务（注入 handler）

业务 (注入):
- biddable_agents: 1..n 个可竞价 Agent（{id, bid_func, handler}）
- auction_ledger_op: 1 个账本写入 callable（obase/layer4 注入面）

红线对照:
- 红线 2/3/4/5: 业务全靠注入；无状态骨架；不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class MechanismGameLoopEngine(EngineSkeleton):
    """基于博弈竞价机制（Auction Mechanism）调度子 Agent 的引擎骨架.

    Example::

        engine = MechanismGameLoopEngine(
            biddable_agents=[
                {"id": "agent-a", "bid_func": async_bidder_a, "handler": async_runner_a},
                {"id": "agent-b", "bid_func": async_bidder_b, "handler": async_runner_b},
            ],
            auction_ledger_op=ledger_append,   # (task_id, winner, bids) -> None
            trigger={"on_demand": True},
            config={"min_bid_delta": 0.0},
            name="auction-swarm",
        )
        result = await engine.run_auction_turn({"id": "task-1", "requirement": "..."})
    """

    injection_points: ClassVar[dict] = {
        "biddable_agents": Injection(
            kind="omodul",
            cardinality="1..n",
            description="可竞价 Agent 表（{id, bid_func, handler}）",
        ),
        "auction_ledger_op": Injection(
            kind="layer4",
            cardinality="1",
            description="竞价账本写入 callable (task_id, winner, bids)",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        biddable_agents: list[dict[str, Any]],
        auction_ledger_op: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.agents = list(biddable_agents)
        self.auction_ledger_op = auction_ledger_op
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None
        self._ledger: list[dict[str, Any]] = []

    async def run_auction_turn(
        self, task_requirement: dict[str, Any]
    ) -> dict[str, Any]:
        """发起一轮任务竞价，最低竞价者胜出获得执行权。"""
        if not self.agents:
            return {"status": "failed", "error": "no biddable agents"}
        if self._stop_event.is_set():
            return {"status": "failed", "error": "engine stopped"}

        bids: list[dict[str, Any]] = []
        for agent in self.agents:
            # 收集每个 Agent 的竞价与消耗承诺
            try:
                bid_price = await agent["bid_func"](task_requirement)
                bids.append(
                    {
                        "agent_id": agent["id"],
                        "bid_price": bid_price,
                        "handler": agent["handler"],
                    }
                )
            except Exception as exc:  # noqa: BLE001 - 单 Agent 竞价失败跳过
                logger.warning("agent %s bid failed: %s", agent.get("id"), exc)

        if not bids:
            return {"status": "failed", "error": "no valid bids"}

        # 拍卖胜出者：最低竞价者胜出
        sorted_bids = sorted(bids, key=lambda x: x["bid_price"])
        winner = sorted_bids[0]

        # 记录竞价账本
        ledger_entry = {
            "task_id": task_requirement.get("id"),
            "winner": winner["agent_id"],
            "winning_bid": winner["bid_price"],
            "bids": [
                {"agent_id": b["agent_id"], "bid_price": b["bid_price"]}
                for b in sorted_bids
            ],
        }
        self._ledger.append(ledger_entry)
        try:
            self.auction_ledger_op(
                task_id=task_requirement.get("id"),
                winner=winner["agent_id"],
                bids=ledger_entry["bids"],
            )
        except Exception as exc:  # pragma: no cover - 账本失败不阻断派发
            logger.warning("auction ledger op failed: %s", exc)

        # 派发任务给胜出者执行
        result = await winner["handler"](task_requirement)

        return {
            "status": "completed",
            "winning_agent": winner["agent_id"],
            "winning_bid": winner["bid_price"],
            "execution_result": result,
        }

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """on_demand 入口：单轮竞价（任务清单批量逐轮执行）。"""
        input_data = input_data or {}
        tasks = input_data.get("tasks") or [input_data]
        outcomes: list[dict[str, Any]] = []
        for task in tasks:
            if on_step:
                on_step({"stage": "auction", "task": task.get("id")})
            outcomes.append(await self.run_auction_turn(task))
        failed = [o for o in outcomes if o.get("status") != "completed"]
        return {
            "status": "completed" if not failed else "completed_with_errors",
            "outcomes": outcomes,
            "ledger": list(self._ledger),
        }

    def stop(self) -> None:
        """请求停止：后续竞价轮不再执行。"""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "agents": len(self.agents),
                "auction_rounds": len(self._ledger),
                "last_error": self._last_error,
            },
        }


register_skeleton("mechanism_game_loop", MechanismGameLoopEngine)
