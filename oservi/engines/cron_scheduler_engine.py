"""Cron Scheduler Engine Skeleton.

机制 (固化):
- 按 trigger["on_cron"] 的标准 5 段 cron 表达式(croniter 解析)计算下次
  触发时间,睡到点后依次串行调用所有注入的 tasks
- 单个 task 失败只记录,不影响其余 task 执行、不影响下一次调度

业务 (注入):
- tasks: omodul callable 列表(如"扫描超时订单"/"清理过期促销"/"弃购清理"
  等,各自独立,互不依赖顺序)

典型用途(SPEC 原话):定时扫描超时订单、过期促销、弃购清理——这些都是
"周期性批量检查 + 逐条处理"的 omodul,骨架只管"到点触发、逐个跑、记录
结果",具体扫描逻辑全在注入的 omodul 里。

红线对照:
- 红线 2 (机制/业务分离): 具体定时任务业务全靠注入
- 红线 3 (注入契约): tasks=omodul(1..n)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
import inspect
import time
from datetime import UTC, datetime
from typing import Any, Callable, ClassVar

from croniter import croniter

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


class CronSchedulerEngine(EngineSkeleton):
    """Cron 定时调度引擎骨架:按 cron 表达式触发,依次跑所有注入任务。

    Example::

        engine = CronSchedulerEngine(
            tasks=[scan_stale_orders, expire_stale_discounts],
            trigger={"on_cron": "0 3 * * *"},
            config={},
            name="nightly-cleanup",
        )
        engine.run()  # blocking
    """

    injection_points: ClassVar[dict] = {
        "tasks": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Callables run sequentially on every cron fire",
        ),
    }
    trigger_mode: str = "on_cron"

    def __init__(
        self,
        *,
        tasks: list[Callable[..., Any]] | Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.task_list = tasks if isinstance(tasks, list) else [tasks]
        if "on_cron" not in trigger:
            raise ValueError("CronSchedulerEngine trigger must contain 'on_cron'")
        self.cron_expr = trigger["on_cron"]
        # Validate the expression eagerly so bad manifests fail at construction,
        # not on the first scheduled fire.
        croniter(self.cron_expr, datetime.now(UTC))
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._fire_count = 0
        self._tick_count = 0
        self._last_error: str | None = None
        self._last_fire_at: str | None = None

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"CronSchedulerEngine {self.name} already running")
        self._running = True
        logger.info(f"CronSchedulerEngine '{self.name}' starting on cron {self.cron_expr!r}")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"CronSchedulerEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止主循环."""
        self._running = False
        self._stop_event.set()

    async def _run_loop(self) -> None:
        if "interval_seconds" in self.config or "on_interval" in self.trigger or isinstance(self.trigger.get("on_cron"), (int, float)):
            interval = max(0.001, self._resolve_interval())
            while self._running:
                await asyncio.sleep(interval)
                if not self._running:
                    break
                self._last_fire_at = datetime.now(UTC).isoformat()
                await self.run_once()
            return
        while self._running:
            now = datetime.now(UTC)
            itr = croniter(self.cron_expr, now)
            next_fire = itr.get_next(datetime)
            sleep_seconds = max(0.0, (next_fire - now).total_seconds())

            # Sleep in short slices so stop() takes effect promptly instead of
            # blocking for up to a full cron period.
            deadline = time.monotonic() + sleep_seconds
            while self._running and time.monotonic() < deadline:
                await asyncio.sleep(min(1.0, deadline - time.monotonic()))

            if not self._running:
                break

            await self._fire_once()

    async def _fire_once(self) -> None:
        self._last_fire_at = datetime.now(UTC).isoformat()
        for task in self.task_list:
            try:
                result = task()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                self._last_error = f"{getattr(task, '__name__', task)}: {e}"
                logger.warning(f"CronSchedulerEngine '{self.name}' task failed: {e}")
        self._fire_count += 1

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "cron_expr": self.cron_expr,
                "tasks_count": len(self.task_list),
                "fire_count": self._fire_count,
                "tick_count": self._tick_count,
                "last_fire_at": self._last_fire_at,
                "last_error": self._last_error,
            },
        }


    async def _invoke(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
            """Sync/async-agnostic callable invocation (keyword args)."""
            if inspect.iscoroutinefunction(fn):
                return await fn(**kwargs)
            raw = fn(**kwargs)
            if asyncio.iscoroutine(raw):
                return await raw
            return raw

    def _resolve_interval(self) -> float:
            """Resolve the tick interval in seconds from config/trigger."""
            if "interval_seconds" in self.config:
                return float(self.config["interval_seconds"])
            cron = self.trigger.get("on_cron")
            if isinstance(cron, (int, float)):
                return float(cron)
            return float(self.trigger.get("on_interval", 60))

    async def run_once(self) -> list[Any]:
            """执行单个 tick: 顺序跑所有 tasks 一次, 返回结果列表 (供测试)."""
            results: list[Any] = []
            for i, task in enumerate(self.task_list):
                try:
                    result = await self._invoke(task, tick_no=self._tick_count, task_no=i)
                except Exception as e:
                    self._last_error = (
                        f"task {i} {getattr(task, '__name__', task)}: {type(e).__name__}: {e}"
                    )
                    logger.warning(f"CronSchedulerEngine '{self.name}' task {i} failed: {e}")
                    result = {"error": str(e), "task_no": i}
                results.append(result)
            self._tick_count += 1
            return results

register_skeleton("cron_scheduler_engine", CronSchedulerEngine)
