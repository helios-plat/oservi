"""Triage Engine Skeleton.

收敛自双实证:
- Aegis C2 TriageEngine: LLM 优先级评分 → 过滤管道 → 去重 → 升级调度
- Tide v5 SignalClassifier: 异常分诊 → 分级 → 升级逻辑

共性 (固化为机制):
- 持续运行循环 (interval / cron 触发)
- 按 filters 链式过滤事件
- LLM 调用评分/分类 (可选, 由注入 llm_caller 决定)
- 去重 (dedup_bucket_seconds 内 entity_id 不重复推送)
- 优先级排序后输出 triage_result

差异 (留 inject + config):
- llm_caller: LLM 评分业务逻辑 → 注入 oprim
- filters: 过滤管道 → 注入 layer4 (项目特定过滤规则)

红线对照:
- 红线 1 (≥2 实证): Aegis C2 TriageEngine + Tide v5 SignalClassifier ✅
- 红线 2 (机制/业务分离): 评分/过滤业务全靠注入, 骨架只做调度 + 去重
- 红线 3 (注入契约): llm_caller=oprim(1) / filters=layer4(0..n)
- 红线 4 (无状态骨架): 状态只在 TriageEngine 实例
- 红线 5 (不反向依赖): 本文件不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class TriageEngine(EngineSkeleton):
    """分诊引擎骨架.

    机制 (骨架固化):
    - 按 trigger.on_interval 周期拉取原始事件 (由 llm_provider 提供)
    - 顺序执行 filters 链式过滤
    - LLM 对剩余事件打优先级分 (0..100)
    - 去重 (entity_id + 时间桶)
    - 优先级降序排列后通过 on_triage_result 回调输出

    业务 (注入填料):
    - llm_caller: oprim LLM callable, 接收 events list → 返回 scored events list
    - filters: layer4 callable 列表, 各接收 event → 返回 bool (True=保留)

    Example::

        manifest = ServiceManifest(
            name="aegis-c2-triage",
            skeleton="triage",
            inject={
                "llm_caller": [oprim_llm_score],
                "filters": [filter_low_confidence, filter_known_false_positive],
            },
            trigger={"on_interval": 60},
            config={
                "dedup_bucket_seconds": 300,
                "min_score": 30,
                "max_events_per_cycle": 50,
            },
        )
    """

    injection_points = {
        "llm_caller": Injection(
            kind="layer4",
            cardinality="1",
            description="LLM caller oprim: 接收原始事件列表 → 返回带 priority_score 字段的事件列表",
        ),
        "filters": Injection(
            kind="layer4",
            cardinality="0..n",
            description="项目特定噪声过滤 callable: 各接收单个事件 dict → 返回 bool (True=保留)",
        ),
    }


    def __init__(
        self,
        *,
        llm_caller: Callable[..., Any],
        filters: list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_triage_result: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller
        self.filters = filters or []
        self.trigger = trigger
        self.config = config
        self.on_triage_result = on_triage_result

        # 运行期状态 (仅在实例存在 — 红线 4)
        self._running = False
        self._ready = False
        self._iteration_count = 0
        self._last_error: str | None = None
        self._dedup_seen: dict[str, float] = {}  # entity_id → ts

        _VALID_TRIGGERS = frozenset({"on_interval", "on_cron", "on_demand", "on_signal"})
        if not (_VALID_TRIGGERS & trigger.keys()):
            raise ValueError(
                "TriageEngine trigger must contain 'on_signal', 'on_demand', 'on_interval', or 'on_cron'"
            )

    # ===== 主循环 =====

    def run(self) -> None:
        """启动引擎. on_signal/on_demand 模式非阻塞; on_interval/on_cron 阻塞循环."""
        if "on_signal" in self.trigger or "on_demand" in self.trigger:
            self._ready = True
            logger.info(f"TriageEngine '{self.name}' ready for on_signal dispatch")
            return
        if self._running:
            raise RuntimeError(f"TriageEngine {self.name} already running")
        self._running = True
        logger.info(f"TriageEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"TriageEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止."""
        self._running = False
        self._ready = False

    async def process(self, signal: dict[str, Any]) -> dict[str, Any] | None:
        """事件驱动入口 (on_signal 模式). Caller 收信号 → 调本方法."""
        if not self._ready:
            raise RuntimeError(f"TriageEngine '{self.name}' not started, call run() first")
        filtered = self._apply_filters([signal])
        if not filtered:
            return None
        scored = await self._score_events(filtered)
        return scored[0] if scored else None

    async def _run_loop(self) -> None:
        interval = self.trigger.get("on_interval", 60)
        while self._running:
            try:
                await self._iterate_once()
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"TriageEngine '{self.name}' iteration failed")
            self._iteration_count += 1
            await asyncio.sleep(interval)

    async def _iterate_once(self) -> None:
        """单次迭代: 拉取 → 过滤 → LLM 评分 → 去重 → 输出."""
        max_events = self.config.get("max_events_per_cycle", 50)

        # 1. 从 llm_caller 拉取原始事件列表
        raw_events = await self._fetch_events(max_events)
        if not raw_events:
            return

        # 2. 链式过滤
        filtered = self._apply_filters(raw_events)
        if not filtered:
            return

        # 3. LLM 优先级评分
        scored = await self._score_events(filtered)

        # 4. 去重
        dedup_bucket = self.config.get("dedup_bucket_seconds", 300)
        now = time.monotonic()
        fresh = []
        for event in scored:
            eid = event.get("entity_id", "")
            last_seen = self._dedup_seen.get(eid, 0.0)
            if now - last_seen >= dedup_bucket:
                self._dedup_seen[eid] = now
                fresh.append(event)

        if not fresh:
            return

        # 5. 优先级排序 + min_score 过滤
        min_score = self.config.get("min_score", 0)
        result = sorted(
            [e for e in fresh if e.get("priority_score", 0) >= min_score],
            key=lambda e: e.get("priority_score", 0),
            reverse=True,
        )

        if result and self.on_triage_result:
            try:
                self.on_triage_result(result)
            except Exception as e:
                logger.warning(f"on_triage_result callback failed: {e}")

    async def _fetch_events(self, max_events: int) -> list[dict[str, Any]]:
        """调 llm_caller 拉取原始事件."""
        try:
            result = self.llm_caller(
                config=self.config.get("llm_config", {}),
                max_events=max_events,
            )
            if asyncio.iscoroutine(result):
                result = await result
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.warning(f"TriageEngine llm_caller fetch failed: {e}")
            return []

    def _apply_filters(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """顺序应用 filters 链."""
        result = events
        for flt in self.filters:
            try:
                result = [e for e in result if flt(event=e)]
            except Exception as e:
                logger.warning(f"filter {getattr(flt, '__name__', flt)} failed: {e}")
        return result

    async def _score_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """调 llm_caller 对事件打分 (priority_score 字段)."""
        try:
            result = self.llm_caller(
                config=self.config.get("llm_config", {}),
                events=events,
                mode="score",
            )
            if asyncio.iscoroutine(result):
                result = await result
            if isinstance(result, list):
                return result
        except Exception as e:
            logger.warning(f"TriageEngine scoring failed, using raw events: {e}")
        # fallback: 不打分直接返回
        return [{"priority_score": 50, **e} for e in events]

    def health(self) -> dict[str, Any]:
        if self._ready:
            status = "ready"
        elif self._running:
            status = "healthy"
        else:
            status = "stopped"
        return {
            "status": status,
            "details": {
                "name": self.name,
                "running": self._running,
                "ready": self._ready,
                "iteration_count": self._iteration_count,
                "filters_count": len(self.filters),
                "last_error": self._last_error,
            },
        }


register_skeleton("triage", TriageEngine)
