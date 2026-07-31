"""Cron Scheduler Engine Skeleton.

机制 (固化):
- 持续运行循环 (on_cron 触发语义)
- 每个 tick 顺序执行所有 tasks
- 按 trigger/config 间隔休眠, stop() 置 stop_event 可提前唤醒退出
- tick_count 计数 + health 上报

业务 (注入):
- tasks: omodul callable 列表 (每 tick 顺序执行)

与 on_interval 的关系:
- cron 与 interval 循环结构同构 (loop/stop/health), 区别在触发语义:
  on_cron 强调"按计划时刻表触发", on_interval 强调"固定间隔". 本骨架
  沿用 alerter 的 interval-loop 结构, 间隔秒数从 config/trigger 解析.

红线对照:
- 红线 2 (机制/业务分离): 任务业务全靠注入
- 红线 3 (注入契约): tasks=omodul(1..n)
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


class CronSchedulerEngine(EngineSkeleton):
    """Cron 调度引擎骨架 (blocking loop + run_once for test).

    机制:
    - run() 阻塞主循环: 每 tick 顺序执行所有 tasks, 然后按解析出的
      间隔秒数休眠, 直到 stop() 置 stop_event (休眠用 wait_for 监听
      stop_event, 可被提前唤醒退出).
    - run_once() 异步执行单个 tick (跑完所有 tasks 一次并返回结果),
      供测试使用, 无需跑无限循环.
    - 每 tick 内 tasks 顺序执行 (deterministic); 单个 task 出错被捕获
      记入该 tick 结果, 不中断其余 task.

    Example::

        engine = CronSchedulerEngine(
            tasks=[rebuild_index, prune_cache],
            trigger={"on_cron": "0 3 * * *"},
            config={"interval_seconds": 3600},
            name="nightly-jobs",
        )
        # 测试单 tick:
        results = asyncio.run(engine.run_once())
        # 生产阻塞启动:
        engine.run()  # blocking
    """

    injection_points: ClassVar[dict] = {
        "tasks": Injection(
            kind="omodul",
            cardinality="1..n",
            description="omoduls run each tick",
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
        self.task_list: list[Callable[..., Any]] = tasks if isinstance(tasks, list) else [tasks]
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._tick_count = 0
        self._last_error: str | None = None

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

    # ===== 主循环 (机制) =====

    def run(self) -> None:
        """启动持续运行循环 (阻塞).

        调用方在 async 上下文时应用 asyncio.create_task / threading.Thread 包装.
        """
        if self._running:
            raise RuntimeError(f"CronSchedulerEngine {self.name} already running")

        self._running = True
        self._stop_event.clear()
        logger.info(f"CronSchedulerEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"CronSchedulerEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止主循环 (置 stop_event, 唤醒休眠)."""
        self._running = False
        self._stop_event.set()

    async def _run_loop(self) -> None:
        """主循环: 每 tick 跑所有 tasks, 然后休眠 interval 秒."""
        interval = self._resolve_interval()

        while self._running and not self._stop_event.is_set():
            try:
                await self.run_once()
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"CronSchedulerEngine '{self.name}' tick failed")

            # 休眠 interval 秒, 但监听 stop_event 以便提前退出
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break  # stop_event 被置 → 退出
            except asyncio.TimeoutError:
                pass  # 正常到点, 进入下一 tick

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

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "tick_count": self._tick_count,
                "tasks_count": len(self.task_list),
                "last_error": self._last_error,
            },
        }


register_skeleton("cron_scheduler_engine", CronSchedulerEngine)
