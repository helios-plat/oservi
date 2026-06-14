"""Feed Tracker Engine Skeleton.

机制 (固化):
- on_interval 触发 (默认 5 秒)
- 调 fetch_event oprim 拉取事件
- 调 ingest omodul 落库 (可选)
- subscription layer4 管理订阅状态

业务 (注入):
- fetch_event: oprim (1) — 拉取事件
- subscription: layer4 (1) — 订阅存储
- ingest: omodul (0..1) — 可选落库

红线对照:
- 红线 2 (机制/业务分离): 抓取/落库业务全靠注入
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


class FeedTrackerEngine(EngineSkeleton):
    """Feed Tracker 引擎骨架 (on_interval, 5 秒周期).

    Example::

        engine = FeedTrackerEngine(
            fetch_event=oprim_fetch,
            subscription=layer4_subscription_store,
            ingest=omodul_ingest,
            trigger={"on_interval": 5},
            config={},
            name="feed-tracker",
        )
        engine.run()  # blocking loop
    """

    injection_points: ClassVar[dict] = {
        "fetch_event": Injection(
            kind="oprim",
            cardinality="1",
            description="Fetch events oprim",
        ),
        "subscription": Injection(
            kind="layer4",
            cardinality="1",
            description="Subscription store",
        ),
        "ingest": Injection(
            kind="omodul",
            cardinality="0..1",
            description="Optional ingest omodul",
        ),
    }
    trigger_mode: str = "on_interval"
    interval_seconds: int = 5

    def __init__(
        self,
        *,
        fetch_event: Callable[..., Any] | list,
        subscription: Callable[..., Any] | list,
        ingest: Callable[..., Any] | list | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        # assembler passes lists; extract single callable for cardinality="1"
        self.fetch_event = fetch_event[0] if isinstance(fetch_event, list) else fetch_event
        self.subscription = subscription[0] if isinstance(subscription, list) else subscription
        self.ingest = (ingest[0] if isinstance(ingest, list) and ingest else ingest) if ingest else None
        self.trigger = trigger
        self.config = config
        self.interval_seconds = int(trigger.get("on_interval", 5))

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._tick_count = 0
        self._last_error: str | None = None

    # ===== 生命周期 =====

    def run(self) -> None:
        """启动主循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"FeedTrackerEngine {self.name} already running")
        self._running = True
        self._stop_event.clear()
        logger.info(f"FeedTrackerEngine '{self.name}' starting (interval={self.interval_seconds}s)")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"FeedTrackerEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止."""
        self._running = False
        self._stop_event.set()

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                events = await self._fetch_events()
                for event in events:
                    await self._process_event(event)
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"FeedTrackerEngine '{self.name}' tick failed")
            self._tick_count += 1
            await asyncio.sleep(self.interval_seconds)

    # ===== 核心方法 =====

    async def tick(self) -> dict[str, Any]:
        """单次轮询 (测试/手动触发用)."""
        events = await self._fetch_events()
        processed = 0
        for event in events:
            await self._process_event(event)
            processed += 1
        self._tick_count += 1
        return {"events_fetched": len(events), "events_processed": processed, "tick_count": self._tick_count}

    async def _fetch_events(self) -> list[dict[str, Any]]:
        """调 fetch_event oprim 拉取事件列表."""
        try:
            if inspect.iscoroutinefunction(self.fetch_event):
                result = await self.fetch_event(config=self.config)
            else:
                raw = self.fetch_event(config=self.config)
                if asyncio.iscoroutine(raw):
                    result = await raw
                else:
                    result = raw
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.warning(f"FeedTrackerEngine fetch_event failed: {e}")
            return []

    async def _process_event(self, event: dict[str, Any]) -> None:
        """处理单个事件: 更新订阅状态, 可选落库."""
        # 更新订阅状态 (layer4)
        try:
            if inspect.iscoroutinefunction(self.subscription):
                await self.subscription(event=event)
            else:
                raw = self.subscription(event=event)
                if asyncio.iscoroutine(raw):
                    await raw
        except Exception as e:
            logger.warning(f"FeedTrackerEngine subscription update failed: {e}")

        # 可选落库 (omodul)
        if self.ingest is not None:
            try:
                if inspect.iscoroutinefunction(self.ingest):
                    await self.ingest(event=event)
                else:
                    raw = self.ingest(event=event)
                    if asyncio.iscoroutine(raw):
                        await raw
            except Exception as e:
                logger.warning(f"FeedTrackerEngine ingest failed: {e}")

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "tick_count": self._tick_count,
                "interval_seconds": self.interval_seconds,
                "has_ingest": self.ingest is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("feed_tracker", FeedTrackerEngine)
