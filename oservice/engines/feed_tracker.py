"""Feed Tracker Engine Skeleton.

收敛设计 (Stratum FeedTrackerAgent + 工业成熟模式):
- 周期 cron 触发 (默认每小时)
- per_user 取订阅列表
- 调 fetch_feed_oprim 抓取 RSS/Atom
- 调 diff_oprim 检测新增条目
- 调 ingest_omodul 落库 (新增条目变 substrate)
- 更新订阅状态 (last_check_at + etag + last_modified)

机制 (固化):
- 调度循环 (cron / interval)
- 订阅查询 + 频率过滤 (last_check_at + frequency_hours)
- HTTP conditional fetch (304 跳过)
- 异常隔离 (单订阅失败不阻塞其他)

业务 (注入):
- fetch_feed_oprim (oprim, 1..n): fetch_rss_feed / parse_atom_feed
- diff_oprim (oprim, 1): feed_diff_detector
- subscription_query (layer4, 1): 项目服务层订阅查询 (thin wrapper)
- subscription_update (layer4, 1): 项目服务层订阅更新
- ingest_omodul (omodul, 1): 主库 omodul 落库

红线对照:
- 红线 1 (≥2 实证): Stratum FeedTrackerAgent + 工业 RSS reader 模式
- 红线 2 (机制/业务分离): RSS 解析 / 数据 IO 全靠注入
- 红线 3 (注入契约): 5 注入点声明 kind + cardinality
- 红线 4 (无状态骨架): 状态只在引擎实例
"""

import asyncio
import logging
from datetime import datetime, UTC, timedelta
from typing import Callable, Any

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class FeedTrackerEngine(EngineSkeleton):
    """Feed Tracker 引擎骨架.
    
    机制 (骨架固化):
    - cron / interval 触发主循环
    - 查所有待检查订阅 (last_check_at + frequency_hours)
    - 并发抓取 RSS (用 etag / last_modified conditional)
    - diff 新增条目
    - 落库新条目 (调 ingest_omodul)
    - 更新订阅状态
    - 异常隔离
    
    Example:
        manifest = ServiceManifest(
            name="stratum-feed-tracker",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [oprim.fetch_rss_feed, oprim.parse_atom_feed],
                "diff_oprim": [oprim.feed_diff_detector],
                "subscription_query": [omodul.query_active_subscriptions],
                "subscription_update": [omodul.update_subscription_state],
                "ingest_omodul": [omodul.process_inbox_substrate],
            },
            trigger={"on_cron": "0 * * * *"},  # 每小时
            config={
                "max_concurrent_fetches": 10,
                "default_frequency_hours": 6,
            },
        )
        engine = assemble(manifest)
        engine.run()  # 阻塞 cron 主循环
    """
    
    injection_points = {
        "fetch_feed_oprim": Injection(
            kind="oprim",
            cardinality="1..n",
            description="feed 抓取 oprim (fetch_rss_feed + parse_atom_feed 等)",
        ),
        "diff_oprim": Injection(
            kind="oprim",
            cardinality="1",
            description="feed diff oprim (e.g. feed_diff_detector)",
        ),
        "subscription_query": Injection(
            kind="layer4",
            cardinality="1",
            description="项目服务层订阅查询 callable (thin wrapper, e.g. Stratum DuckDB)",
        ),
        "subscription_update": Injection(
            kind="layer4",
            cardinality="1",
            description="项目服务层订阅更新 callable (etag/last_modified/last_check_at)",
        ),
        "ingest_omodul": Injection(
            kind="omodul",
            cardinality="1",
            description="落库 omodul (主库元素, e.g. process_inbox_substrate)",
        ),
    }
    
    def __init__(
        self,
        *,
        fetch_feed_oprim: list[Callable[..., Any]],
        diff_oprim: list[Callable[..., Any]],
        subscription_query: list[Callable[..., Any]],
        subscription_update: list[Callable[..., Any]],
        ingest_omodul: list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.fetch_feed_oprim = fetch_feed_oprim
        self.diff_oprim_fn = diff_oprim[0]
        self.subscription_query_fn = subscription_query[0]
        self.subscription_update_fn = subscription_update[0]
        self.ingest_omodul_fn = ingest_omodul[0]
        self.trigger = trigger
        self.config = config
        
        self._running = False
        self._tick_count = 0
        self._last_tick_at: datetime | None = None
        self._last_error: str | None = None
        
        # 校验 trigger
        if "on_cron" not in trigger and "on_interval" not in trigger:
            raise ValueError(
                "FeedTrackerEngine trigger must contain 'on_cron' or 'on_interval'"
            )
    
    # ===== 引擎生命周期 =====
    
    def run(self) -> None:
        """启动主循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"FeedTrackerEngine {self.name} already running")
        
        self._running = True
        logger.info(f"FeedTrackerEngine '{self.name}' starting")
        
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"FeedTrackerEngine '{self.name}' stopped")
    
    def stop(self) -> None:
        self._running = False
    
    async def _run_loop(self) -> None:
        """主循环, 按 trigger 周期触发 tick."""
        # 简化: interval 直接 sleep. cron 需更复杂调度器 (留 v0.3+ 实施)
        interval = self.trigger.get("on_interval", 3600)  # cron 暂用 1h 默认
        
        while self._running:
            try:
                await self.tick()
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"FeedTrackerEngine '{self.name}' tick failed")
            
            self._tick_count += 1
            self._last_tick_at = datetime.now(UTC)
            await asyncio.sleep(interval)
    
    # ===== 核心方法 =====
    
    async def tick(self) -> dict[str, Any]:
        """单次 tick: 查订阅 → 抓 feed → diff → 落库 → 更新状态.
        
        Returns:
            {"feeds_checked": N, "new_entries": M, "errors": [...]}
        """
        # 1. 查待检查订阅
        subscriptions = await self._query_subscriptions()
        
        if not subscriptions:
            return {"feeds_checked": 0, "new_entries": 0, "errors": []}
        
        # 2. 并发处理每个订阅
        concurrency = self.config.get("max_concurrent_fetches", 10)
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_one(sub: dict) -> dict:
            async with semaphore:
                return await self._process_subscription(sub)
        
        results = await asyncio.gather(
            *[process_one(s) for s in subscriptions],
            return_exceptions=True,
        )
        
        # 3. 汇总
        feeds_checked = 0
        new_entries = 0
        errors = []
        for r in results:
            if isinstance(r, Exception):
                errors.append(f"{type(r).__name__}: {r}")
                continue
            feeds_checked += 1
            new_entries += r.get("new_entries", 0)
        
        return {
            "feeds_checked": feeds_checked,
            "new_entries": new_entries,
            "errors": errors,
        }
    
    async def _process_subscription(self, sub: dict[str, Any]) -> dict[str, Any]:
        """处理单个订阅: 抓 feed → diff → 落库 → 更新."""
        sub_id = sub["id"]
        url = sub["url"]
        feed_type = sub.get("feed_type", "rss")  # rss / atom
        etag = sub.get("etag")
        last_modified = sub.get("last_modified")
        previous_entries = sub.get("previous_entry_ids", [])
        user_id = sub["user_id"]
        
        # 1. 选 fetch oprim (按 feed_type 路由, 简单做法用第一个; 多 oprim 可按 type 路由)
        fetch_fn = self.fetch_feed_oprim[0]  # TODO: 按 feed_type 路由
        
        try:
            fetch_result = await self._call_async(
                fetch_fn,
                url=url,
                etag=etag,
                last_modified=last_modified,
            )
        except Exception as e:
            logger.warning(f"fetch failed for sub {sub_id} ({url}): {e}")
            return {"new_entries": 0, "error": str(e)}
        
        # 304 → 跳过
        if not fetch_result or fetch_result.get("status") == 304:
            await self._update_subscription_check_time(sub_id)
            return {"new_entries": 0}
        
        entries = fetch_result.get("entries", [])
        new_etag = fetch_result.get("etag")
        new_last_modified = fetch_result.get("last_modified")
        
        # 2. diff 新增条目
        diff_result = await self._call_async(
            self.diff_oprim_fn,
            current_entries=entries,
            previous_entry_ids=previous_entries,
        )
        new_entries_to_ingest = diff_result.get("new_entries", [])
        
        # 3. 落库
        ingested_ids = []
        for entry in new_entries_to_ingest:
            try:
                ingest_result = await self._call_async(
                    self.ingest_omodul_fn,
                    content=entry.get("content", ""),
                    source_url=entry.get("link"),
                    title=entry.get("title"),
                    tags=[f"feed:{sub_id}"],
                    user_id=user_id,
                )
                if isinstance(ingest_result, dict):
                    findings = ingest_result.get("findings", {})
                    if isinstance(findings, dict):
                        sub_id_ingested = findings.get("substrate_id")
                        if sub_id_ingested:
                            ingested_ids.append(sub_id_ingested)
            except Exception as e:
                logger.warning(f"ingest failed for entry in sub {sub_id}: {e}")
        
        # 4. 更新订阅状态
        await self._call_async(
            self.subscription_update_fn,
            subscription_id=sub_id,
            etag=new_etag,
            last_modified=new_last_modified,
            last_check_at=datetime.now(UTC).isoformat(),
            previous_entry_ids=[e.get("id") for e in entries if e.get("id")],
        )
        
        return {"new_entries": len(ingested_ids)}
    
    async def _query_subscriptions(self) -> list[dict[str, Any]]:
        """查询待检查的订阅列表."""
        default_freq = self.config.get("default_frequency_hours", 6)
        cutoff = (datetime.now(UTC) - timedelta(hours=default_freq)).isoformat()
        
        try:
            result = await self._call_async(
                self.subscription_query_fn,
                last_check_before=cutoff,
                status="active",
            )
            if isinstance(result, dict):
                return result.get("findings", {}).get("subscriptions", [])
            if isinstance(result, list):
                return result
            return []
        except Exception as e:
            logger.exception(f"query subscriptions failed: {e}")
            return []
    
    async def _update_subscription_check_time(self, sub_id: str) -> None:
        """只更新 last_check_at, 其他不变 (304 场景)."""
        try:
            await self._call_async(
                self.subscription_update_fn,
                subscription_id=sub_id,
                last_check_at=datetime.now(UTC).isoformat(),
            )
        except Exception as e:
            logger.warning(f"update check_time failed for sub {sub_id}: {e}")
    
    async def _call_async(self, fn: Callable, **kwargs) -> Any:
        """统一 sync/async 调用."""
        result = fn(**kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        return result
    
    # ===== 健康检查 =====
    
    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "tick_count": self._tick_count,
                "last_tick_at": self._last_tick_at.isoformat() if self._last_tick_at else None,
                "fetch_providers_count": len(self.fetch_feed_oprim),
                "last_error": self._last_error,
            },
        }


# 注册
register_skeleton("feed_tracker", FeedTrackerEngine)
