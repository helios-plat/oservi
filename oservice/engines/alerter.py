"""Alerter Engine Skeleton.

收敛自双实证:
- Aegis C2 AlertEngine: threshold 评估 → 节流 → 去重 → webhook 投递
- Tide v4 AlertSchedulerEngine: 多评估器 → 静音时段 → 多通道推送 (Bell/Telegram/站内)

共性 (固化为机制):
- 持续运行循环 (interval / cron 触发)
- 多 evaluators 调用
- 节流判断 (should_throttle)
- 去重判断 (compute_dedup_key)
- 静音时段过滤 (config-driven)
- 多通道推送

差异 (留 inject + config):
- evaluators 业务逻辑 → 注入 oprim
- 通道选择 → 注入 obase.notify.*
- 阈值 / 节流秒数 / 静音时段 → config

红线对照:
- 红线 1 (≥2 实证): Aegis + Tide ✅
- 红线 2 (机制/业务分离): 评估业务全靠注入, 骨架不含业务判断
- 红线 3 (注入契约): evaluators=oprim(1..n) / channels=obase(1..n)
- 红线 4 (无状态骨架): 状态只在 AlerterEngine 实例
"""

import asyncio
import logging
import time
from datetime import datetime, UTC, time as dtime
from typing import Callable, Any

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class AlerterEngine(EngineSkeleton):
    """告警引擎骨架.
    
    机制 (骨架固化):
    - 按 trigger.on_interval 周期触发
    - 顺序调用所有 evaluators 取告警事件列表
    - 节流过滤 (按 config.throttle_seconds)
    - 去重过滤 (按 config.dedup_bucket_seconds + entity_id)
    - 静音时段过滤 (按 config.quiet_hours)
    - 推送到所有 channels
    
    业务 (注入填料):
    - evaluators: oprim 列表, 每个返回 list[AlertEvent] (业务规则)
    - channels: obase.notify.* 列表 (推送通道)
    
    Example:
        manifest = ServiceManifest(
            name="tide-realtime-alerter",
            skeleton="alerter",
            inject={
                "evaluators": [detect_sector_collapse, detect_dragon_switch],
                "channels": [telegram_send],
            },
            trigger={"on_interval": 300},
            config={
                "throttle_seconds": 600,
                "dedup_bucket_seconds": 3600,
                "quiet_hours": {"start": "22:00", "end": "07:00"},
            },
        )
    """
    
    injection_points = {
        "evaluators": Injection(
            kind="oprim",
            cardinality="1..n",
            description="告警评估器: oprim list, 各返回 list[dict] 告警事件",
        ),
        "channels": Injection(
            kind="obase",
            cardinality="1..n",
            description="推送通道: obase.notify.* (telegram / email / web push 等)",
        ),
    }
    
    def __init__(
        self,
        *,
        evaluators: list[Callable[..., Any]],
        channels: list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.evaluators = evaluators
        self.channels = channels
        self.trigger = trigger
        self.config = config
        
        # 运行期状态 (仅在实例存在, 不污染骨架定义 — 红线 4)
        self._running = False
        self._last_fired: dict[str, float] = {}  # entity_id → unix ts
        self._dedup_keys_seen: set[str] = set()  # 时间桶内已发送的 dedup_key
        self._iteration_count = 0
        self._last_error: str | None = None
        
        # 参数校验
        if "on_interval" not in trigger and "on_cron" not in trigger:
            raise ValueError(
                "AlerterEngine trigger must contain 'on_interval' or 'on_cron'"
            )
    
    # ===== 主循环 (机制) =====
    
    def run(self) -> None:
        """启动持续运行循环 (阻塞).
        
        如果调用方在 async 上下文, 应用 asyncio.create_task / threading.Thread 包装.
        """
        if self._running:
            raise RuntimeError(f"AlerterEngine {self.name} already running")
        
        self._running = True
        logger.info(f"AlerterEngine '{self.name}' starting")
        
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"AlerterEngine '{self.name}' stopped")
    
    def stop(self) -> None:
        """优雅停止主循环."""
        self._running = False
    
    async def _run_loop(self) -> None:
        """主循环 (按 on_interval 触发)."""
        interval = self.trigger.get("on_interval", 60)
        
        while self._running:
            try:
                await self._iterate_once()
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"AlerterEngine '{self.name}' iteration failed")
            
            self._iteration_count += 1
            await asyncio.sleep(interval)
    
    async def _iterate_once(self) -> None:
        """单次迭代: 评估 → 过滤 → 推送."""
        # 静音时段判断 (config-driven, 机制)
        if self._is_in_quiet_hours():
            logger.debug(f"AlerterEngine '{self.name}' in quiet hours, skipping")
            return
        
        # 1. 调用所有 evaluators (注入的 oprim) 收集事件
        all_events: list[dict[str, Any]] = []
        for evaluator in self.evaluators:
            try:
                events = await self._call_evaluator(evaluator)
                if events:
                    all_events.extend(events)
            except Exception as e:
                logger.warning(
                    f"evaluator {evaluator.__name__} failed: {type(e).__name__}: {e}"
                )
        
        if not all_events:
            return
        
        # 2. 节流 + 去重过滤 (机制)
        filtered = self._filter_throttled_and_deduped(all_events)
        
        if not filtered:
            return
        
        # 3. 推送到所有 channels (注入的 obase.notify.*)
        for event in filtered:
            await self._dispatch_to_channels(event)
    
    async def _call_evaluator(
        self, evaluator: Callable[..., Any]
    ) -> list[dict[str, Any]]:
        """调用单个 evaluator. 支持 sync / async oprim.
        
        evaluator 契约: 接收 config (业务参数) → 返回 list[dict] 告警事件.
        每个 event dict 必须含: entity_id, severity, message (其他字段透传).
        """
        evaluator_config = self.config.get("evaluator_configs", {}).get(
            evaluator.__name__, {}
        )
        
        result = evaluator(config=evaluator_config)
        if asyncio.iscoroutine(result):
            result = await result
        
        if result is None:
            return []
        
        # 单个事件 → 包装成 list
        if isinstance(result, dict):
            return [result]
        if isinstance(result, list):
            return result
        
        logger.warning(
            f"evaluator {evaluator.__name__} returned unexpected type "
            f"{type(result).__name__}, ignoring"
        )
        return []
    
    def _is_in_quiet_hours(self) -> bool:
        """检查当前是否在 config.quiet_hours 内."""
        quiet = self.config.get("quiet_hours")
        if not quiet:
            return False
        
        try:
            start_str = quiet.get("start", "")
            end_str = quiet.get("end", "")
            if not start_str or not end_str:
                return False
            
            start = dtime.fromisoformat(start_str)
            end = dtime.fromisoformat(end_str)
            now = datetime.now(UTC).time()
            
            # 跨日时段 (e.g. 22:00 - 07:00)
            if start > end:
                return now >= start or now <= end
            return start <= now <= end
        except Exception:
            return False
    
    def _filter_throttled_and_deduped(
        self, events: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """节流 + 去重过滤."""
        throttle_seconds = self.config.get("throttle_seconds", 0)
        dedup_bucket = self.config.get("dedup_bucket_seconds", 0)
        now = time.time()
        
        filtered: list[dict[str, Any]] = []
        for event in events:
            entity_id = event.get("entity_id", "")
            if not entity_id:
                logger.warning(f"event missing entity_id: {event}")
                continue
            
            # 节流: 上次触发后未到 throttle_seconds → 跳过
            if throttle_seconds > 0:
                last = self._last_fired.get(entity_id, 0)
                if now - last < throttle_seconds:
                    continue
            
            # 去重: 时间桶 + entity_id 同 key → 跳过
            if dedup_bucket > 0:
                bucket_start = int(now // dedup_bucket) * dedup_bucket
                dedup_key = f"{entity_id}\x00{bucket_start}\x00{event.get('severity', '')}"
                if dedup_key in self._dedup_keys_seen:
                    continue
                self._dedup_keys_seen.add(dedup_key)
            
            self._last_fired[entity_id] = now
            filtered.append(event)
        
        # 清理 dedup_keys (防内存膨胀, 简化版: 保留最近 10000 条)
        if len(self._dedup_keys_seen) > 10000:
            self._dedup_keys_seen.clear()
        
        return filtered
    
    async def _dispatch_to_channels(self, event: dict[str, Any]) -> None:
        """推送单个事件到所有 channels."""
        for channel in self.channels:
            try:
                result = channel(**self._build_channel_payload(event, channel))
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning(
                    f"channel {channel.__name__} failed: {type(e).__name__}: {e}"
                )
    
    def _build_channel_payload(
        self, event: dict[str, Any], channel: Callable[..., Any]
    ) -> dict[str, Any]:
        """构造单个 channel 的 payload.
        
        最简实现: 通道接收 'text' 字段. 实际项目可根据 channel.__name__ 定制.
        """
        text = (
            f"[{event.get('severity', 'info').upper()}] "
            f"{event.get('entity_id', '?')}: "
            f"{event.get('message', '')}"
        )
        # 默认按 telegram_send 签名 (chat_id 由 config 提供)
        chat_id = self.config.get("channel_configs", {}).get(
            channel.__name__, {}
        ).get("chat_id", "")
        bot_token = self.config.get("channel_configs", {}).get(
            channel.__name__, {}
        ).get("bot_token", "")
        return {
            "text": text,
            "chat_id": chat_id,
            "bot_token": bot_token,
        }
    
    # ===== 健康检查 =====
    
    def health(self) -> dict[str, Any]:
        """健康检查. 返回当前 engine 状态."""
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "iteration_count": self._iteration_count,
                "evaluators_count": len(self.evaluators),
                "channels_count": len(self.channels),
                "last_error": self._last_error,
            },
        }


# 注册到 oservice registry
register_skeleton("alerter", AlerterEngine)
