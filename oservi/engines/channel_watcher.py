"""Channel Watcher Engine Skeleton.

机制 (固化):
- on_interval 触发（默认 3600 秒，每小时扫一次）
- 调 list_videos oprim 拉频道视频列表
- diff 对比已处理 video_id，找新视频
- 调 filter_videos oskill 应用订阅规则
- 调 ingest_media omodul 逐个入库
- 返回已处理 video_id（增量，下次只处理新的）

业务 (注入):
- list_videos:   oprim (1)    — channel_list_videos
- filter_videos: oskill (0..1) — video_filter_by_rules（可选）
- ingest_media:  omodul (1)   — process_media_substrate
- subscription:  layer4 (1)   — 频道订阅存储（读已处理 video_id + 写更新）

红线对照:
- 红线 2 (机制/业务分离): 抓取/过滤/入库全靠注入
- 红线 3 (注入契约): 4 注入点声明
- 红线 4 (无状态骨架): 只持运行态 (tick_count/processed_ids)
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


class ChannelWatcherEngine(EngineSkeleton):
    """Channel Watcher 引擎骨架 (on_interval，默认 3600 秒).

    Example::
        engine = ChannelWatcherEngine(
            list_videos=channel_list_videos,
            filter_videos=video_filter_by_rules,   # 可选
            ingest_media=process_media_substrate,
            subscription=layer4_subscription_store,
            trigger={"on_interval": 3600},
            config={"proxy": None, "asr_backend": "local"},
            name="channel-watcher",
        )
        engine.run()  # blocking loop
    """

    injection_points: ClassVar[dict] = {
        "list_videos": Injection(
            kind="oprim",
            cardinality="1",
            description="拉取频道视频列表 (channel_list_videos)",
        ),
        "filter_videos": Injection(
            kind="oskill",
            cardinality="0..1",
            description="视频过滤规则 (video_filter_by_rules，可选)",
        ),
        "ingest_media": Injection(
            kind="omodul",
            cardinality="1",
            description="单视频入库 (process_media_substrate)",
        ),
        "subscription": Injection(
            kind="layer4",
            cardinality="1",
            description="订阅存储：读已处理 video_id + 写更新",
        ),
    }

    def __init__(
        self,
        *,
        list_videos: Callable,
        ingest_media: Callable,
        subscription: Any,
        filter_videos: Callable | None = None,
        trigger: dict | None = None,
        config: dict | None = None,
        name: str = "channel-watcher",
    ) -> None:
        self._list_videos = list_videos
        self._filter_videos = filter_videos
        self._ingest_media = ingest_media
        self._subscription = subscription
        self._trigger = trigger or {"on_interval": 3600}
        self._config = config or {}
        self._name = name
        # 运行态（非业务态）
        self._running = False
        self._tick_count = 0
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 核心：单次 tick
    # ------------------------------------------------------------------

    async def _tick(self) -> dict[str, Any]:
        """单次频道扫描：list → diff → filter → ingest。"""
        channel_url: str = self._config.get("channel_url", "")
        proxy: str | None = self._config.get("proxy")
        limit: int | None = self._config.get("limit")

        if not channel_url:
            logger.warning("channel_watcher.no_channel_url name=%s", self._name)
            return {"ingested": 0, "new_videos": 0}

        # Step 1: 拉视频列表
        try:
            if inspect.iscoroutinefunction(self._list_videos):
                videos = await self._list_videos(
                    channel_url=channel_url, proxy=proxy, limit=limit
                )
            else:
                videos = self._list_videos(
                    channel_url=channel_url, proxy=proxy, limit=limit
                )
        except Exception as exc:
            logger.error("channel_watcher.list_failed name=%s error=%s", self._name, exc)
            self._last_error = str(exc)
            return {"ingested": 0, "new_videos": 0, "error": str(exc)}

        # Step 2: diff（对比已处理 video_id）
        try:
            processed_ids: set[str] = set(
                await self._subscription.get_processed_ids(channel_url)
                if inspect.iscoroutinefunction(self._subscription.get_processed_ids)
                else self._subscription.get_processed_ids(channel_url)
            )
        except Exception:
            processed_ids = set()

        new_videos = [v for v in videos if v.video_id not in processed_ids]

        if not new_videos:
            logger.info("channel_watcher.no_new name=%s", self._name)
            return {"ingested": 0, "new_videos": 0}

        # Step 3: filter（可选）
        if self._filter_videos is not None:
            rules = self._config.get("filter_rules")
            try:
                if rules:
                    if inspect.iscoroutinefunction(self._filter_videos):
                        new_videos = await self._filter_videos(
                            new_videos, rules=rules, llm=None
                        )
                    else:
                        new_videos = self._filter_videos(
                            new_videos, rules=rules, llm=None
                        )
            except Exception as exc:
                logger.warning("channel_watcher.filter_failed error=%s", exc)

        # Step 4: ingest 逐个入库
        ingested: list[str] = []
        failed: list[str] = []

        for video in new_videos:
            try:
                if inspect.iscoroutinefunction(self._ingest_media):
                    result = await self._ingest_media(video_url=video.url, **self._config)
                else:
                    import asyncio as _asyncio
                    result = await _asyncio.to_thread(
                        self._ingest_media, video_url=video.url, **self._config
                    )
                if result.get("status") == "completed":
                    ingested.append(video.video_id)
                else:
                    failed.append(video.video_id)
            except Exception as exc:
                logger.error(
                    "channel_watcher.ingest_failed video_id=%s error=%s",
                    video.video_id, exc,
                )
                failed.append(video.video_id)

        # Step 5: 更新已处理 video_id
        if ingested:
            try:
                if inspect.iscoroutinefunction(self._subscription.mark_processed):
                    await self._subscription.mark_processed(channel_url, ingested)
                else:
                    self._subscription.mark_processed(channel_url, ingested)
            except Exception as exc:
                logger.error("channel_watcher.mark_failed error=%s", exc)

        logger.info(
            "channel_watcher.tick name=%s new=%d ingested=%d failed=%d",
            self._name, len(new_videos), len(ingested), len(failed),
        )
        return {
            "new_videos": len(new_videos),
            "ingested": len(ingested),
            "failed": len(failed),
        }

    # ------------------------------------------------------------------
    # run() — on_interval 阻塞主循环（复用 feed_tracker 模式）
    # ------------------------------------------------------------------

    def run(self) -> None:
        """阻塞运行，on_interval 定时触发。"""
        interval = self._trigger.get("on_interval", 3600)
        self._running = True

        async def _loop() -> None:
            while self._running:
                self._tick_count += 1
                try:
                    await self._tick()
                except Exception as exc:
                    logger.error("channel_watcher.loop_error error=%s", exc)
                    self._last_error = str(exc)
                await asyncio.sleep(interval)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_loop())
        except RuntimeError:
            asyncio.run(_loop())

    def stop(self) -> None:
        self._running = False

    def health(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "status": "running" if self._running else "stopped",
            "tick_count": self._tick_count,
            "last_error": self._last_error,
            "trigger": self._trigger,
        }


register_skeleton("channel_watcher", ChannelWatcherEngine)
